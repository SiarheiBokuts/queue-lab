<script setup lang="ts">
import BatchDiagram from '../components/BatchDiagram.vue'
import { computed, onMounted, ref } from 'vue'
import LangIcon from '../components/LangIcon.vue'
import LoadingCard from '../components/LoadingCard.vue'
import { api } from '../api'
import { LANGS, langName } from '../langs'
import CodeBlock from '../components/CodeBlock.vue'
import { SHIKI_LANG } from '../highlight'
import PageHeader from '../components/PageHeader.vue'

const data = ref<any>(null)
const lang = ref('node')
const err = ref('')

onMounted(async () => {
  try {
    data.value = await api.code()
  } catch (e: any) {
    err.value = e.message
  }
})
const cur = computed(() => data.value?.[lang.value])
const conf = computed(() => data.value?.client_conf)
const notes = computed<Record<string, string>>(() => data.value?.client_notes ?? {})
const commonWork = computed<string[]>(() => data.value?.common_work ?? [])
const reading = computed<Record<string, any>>(() => data.value?.reading ?? {})
const meta = computed(() => LANGS.find((l) => l.id === lang.value))

/** The six steps of the consumer loop and what each one is called in every language. Keep in step with the contract test. */
const units = [
  {
    what: 'Read a batch',
    text: 'up to N messages that have already arrived',
    names: { python: 'read_batch', node: 'eachBatch (client)', java: 'readBatch', go: 'readBatch', rust: 'read_batch' },
  },
  {
    what: 'Group by user',
    text: 'one group per user, in the order sent',
    names: {
      python: 'group_by_user',
      node: 'groupByUser',
      java: 'Lib.groupByUser',
      go: 'groupByUser',
      rust: 'group_by_user',
    },
  },
  {
    what: 'Handle one message',
    text: 'the scenario step (echo, delay, json)',
    names: { python: 'handle', node: 'handle', java: 'handle', go: 'handle', rust: 'handle' },
  },
  {
    what: 'Handle a group',
    text: "one user's messages, one after another",
    names: {
      python: 'handle_group',
      node: 'handleGroup',
      java: 'handleGroup',
      go: 'handleGroup',
      rust: 'handle_group',
    },
  },
  {
    what: 'Run the batch',
    text: 'all groups at the same time, wait for all',
    names: { python: 'run_batch', node: 'runBatch', java: 'runBatch', go: 'runBatch', rust: 'run_batch' },
  },
  {
    what: 'Commit',
    text: 'tell Kafka the batch is done',
    names: { python: 'commit', node: 'commitOffsets (client)', java: 'commit', go: 'commit', rust: 'commit' },
  },
] as { what: string; text: string; names: Record<string, string> }[]
</script>

<template>
  <PageHeader
    title="Code"
    subtitle="The two parts that matter: how messages are sent, and what a worker does with each message. This is the real code that runs in the containers, in every language."
    :crumbs="[{ label: 'Dashboard', to: '/' }, { label: 'Code' }]"
  />
  <div v-if="err" class="card text-red-300">{{ err }}</div>
  <LoadingCard v-else-if="!data" text="Loading the code…" />

  <div class="flex gap-2 flex-wrap">
    <button v-for="l in LANGS" :key="l.id" class="btn" :class="lang === l.id ? 'btn-primary' : ''" @click="lang = l.id">
      <LangIcon :id="l.id" :size="14" style="display: inline-block; vertical-align: -2px; margin-right: 6px" />{{
        l.name
      }}
    </button>
  </div>
  <div class="lbl" v-if="meta">
    Kafka client: <b class="text-zinc-200">{{ meta.client }}</b>
  </div>

  <template v-if="cur">
    <div class="card">
      <h2 class="mb-1">1. Sending messages</h2>
      <p class="lbl mb-3">
        Message number i goes to partition i mod P, so every partition gets the same load. Each message carries a small
        header with a counter and the send time, which is how loss, duplicates and latency are measured. The sender
        waits when it is ahead of the speed you asked for.
      </p>
      <CodeBlock :code="cur.producer" :lang="SHIKI_LANG[lang]" :title="langName(lang) + ' · sender'" />
    </div>
    <div class="card">
      <h2 class="mb-1">2. What a worker does with each message</h2>
      <p class="lbl mb-3">
        Every worker, in every language, runs the same loop. Before the step below it has already checked each message
        and written down its latency. The step is the part you choose in the run form: wait, parse JSON, or nothing.
      </p>
      <BatchDiagram class="mb-3" />
      <CodeBlock :code="cur.scenario" :lang="SHIKI_LANG[lang]" :title="langName(lang) + ' · processing step'" />
    </div>
    <div class="card">
      <h2 class="mb-1">3. Change it yourself</h2>
      <p class="lbl">
        Everything about a run is set in the form. To change what a worker does, edit the file in
        <span class="mono">workers/{{ lang }}</span> and start a run: the image is rebuilt automatically when the
        sources change.
      </p>
    </div>
  </template>

  <div class="card" v-if="commonWork.length">
    <h2 class="mb-1">What every worker does for every message</h2>
    <p class="lbl mb-3">
      The same seven steps in the same order in all five languages, so a comparison measures the language and its Kafka
      client, not different amounts of work. Each language writes them the way that is fastest and most natural for it.
    </p>
    <ol class="info-list" style="counter-reset: step">
      <li v-for="(w, k) in commonWork" :key="w">
        <span class="step-no">{{ k + 1 }}</span
        >{{ w }}
      </li>
    </ol>
  </div>

  <div class="card">
    <h2 class="mb-1">The same steps in every language</h2>
    <p class="lbl mb-3">
      Every consumer is built from the same six methods, in the same order, so the five files can be read side by side.
      Names follow each language (snake_case, camelCase); the comments above them say the same thing.
    </p>
    <div style="overflow-x: auto">
      <table class="tbl">
        <thead>
          <tr>
            <th>Step</th>
            <th v-for="l in LANGS" :key="l.id">
              <span class="inline-flex items-center gap-1.5"><LangIcon :id="l.id" :size="14" />{{ l.name }}</span>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="u in units" :key="u.what">
            <td>
              <b>{{ u.what }}</b>
              <div class="lbl">{{ u.text }}</div>
            </td>
            <td v-for="l in LANGS" :key="l.id" class="mono">{{ u.names[l.id] }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>

  <div class="card" v-if="Object.keys(reading).length">
    <h2 class="mb-1">How each language does it</h2>
    <p class="lbl mb-3">
      All languages read under the same conditions: the Kafka settings below are identical and come from
      <span class="mono">backend/app/config.py</span>, and the batch size is the one you set under "At the same time per
      worker". Every worker and sender gets the same share of the cores. What differs is only the call of each Kafka
      library and the tools of the language (threads, tasks, goroutines). That is how each library works, not a
      difference in what is read.
    </p>
    <div style="overflow-x: auto">
      <table class="tbl">
        <thead>
          <tr>
            <th>Language</th>
            <th>How it reads a batch</th>
            <th>How it handles the groups</th>
            <th>How the JSON work runs</th>
            <th>Where in the code</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(r, l) in reading" :key="l">
            <td>
              <b>{{ langName(String(l)) }}</b>
            </td>
            <td>{{ r.read }}</td>
            <td>{{ r.handle }}</td>
            <td>{{ r.cpu }}</td>
            <td class="mono">{{ r.file }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
  <div class="card" v-if="conf">
    <h2 class="mb-1">Kafka client settings: identical for every language</h2>
    <p class="lbl mb-3">
      The backend sends exactly these values to all workers. The Java client uses different names for a few of them and
      they are translated, the values stay the same. That keeps comparisons fair.
    </p>
    <div class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(320px, 1fr))">
      <div v-for="(values, role) in conf" :key="role">
        <div class="lbl mb-1">{{ role }}</div>
        <table class="tbl">
          <tbody>
            <tr v-for="(v, k) in values" :key="k">
              <td class="mono">{{ k }}</td>
              <td>
                <b>{{ v }}</b>
              </td>
              <td class="lbl">{{ notes[k] }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
    <div class="lbl mt-2">Compression is chosen per run in the form and replaces the default here.</div>
  </div>
</template>
