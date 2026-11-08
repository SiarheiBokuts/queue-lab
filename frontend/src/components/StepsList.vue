<script setup lang="ts">
/** The preparation of a run, step by step: what is done, what is happening now, what comes next. */
import { computed } from 'vue'
import { Check, Circle, LoaderCircle, X } from '@lucide/vue'

const props = defineProps<{ run: any }>()

/** What each step does, in plain words, so a slow step does not look like a hang. */
const WHAT: Record<string, string> = {
  images: 'Builds the worker image for each language if it is new or its code changed. The first time this takes a few minutes.',
  broker: 'Starts the Kafka broker, or reuses the one that is already running.',
  topic: 'Creates the queue with the number of partitions you chose.',
  workers: 'Starts the worker containers with their memory limits.',
  join: 'Waits until every worker has received its partitions, so no message is processed late.',
  producer: 'Starts the senders. The messages start to flow right after.',
  run: 'Sends and processes the messages.',
}

const steps = computed<any[]>(() => props.run?.steps ?? [])
const done = computed(() => steps.value.filter((s) => s.state === 'done').length)
const percent = computed(() => (steps.value.length ? Math.round((done.value / steps.value.length) * 100) : 0))
const current = computed(() => steps.value.find((s) => s.state === 'running'))
const failed = computed(() => props.run?.status === 'failed')
const showLog = computed(() => props.run?.log?.length && (props.run.status === 'pending' || failed.value))
</script>

<template>
  <div class="card" v-if="run">
    <div class="flex items-center justify-between flex-wrap gap-2 mb-1">
      <h2>{{ failed ? 'Setup failed' : 'Getting ready' }}</h2>
      <span class="lbl">Step {{ Math.min(done + 1, steps.length) }} of {{ steps.length }}<template v-if="current"> · {{ current.label }}</template></span>
    </div>
    <div class="bar mb-3"><i :style="{ width: Math.max(3, percent) + '%', background: failed ? '#ef4444' : '#3b82f6' }" /></div>

    <div class="flex flex-col">
      <div v-for="s in steps" :key="s.key" class="flex gap-3 items-start" style="padding: 7px 0">
        <span style="flex: none; width: 24px; height: 24px; border-radius: 50%; display: grid; place-items: center"
          :style="{ background: s.state === 'done' ? '#052e1c' : s.state === 'running' ? '#0c2547' : s.state === 'error' ? '#3b0d0d' : '#1f1f23' }">
          <Check v-if="s.state === 'done'" :size="14" color="#34d399" />
          <LoaderCircle v-else-if="s.state === 'running'" :size="14" color="#60a5fa" class="spin" />
          <X v-else-if="s.state === 'error'" :size="14" color="#f87171" />
          <Circle v-else :size="8" color="#52525b" />
        </span>
        <div style="min-width: 0">
          <div :class="s.state === 'pending' ? 'text-zinc-500' : ''">
            <b>{{ s.label }}</b>
            <span class="lbl" v-if="s.detail && s.state !== 'error'"> · {{ s.detail }}</span>
          </div>
          <div class="lbl" v-if="s.state === 'running' || s.state === 'error'">{{ WHAT[s.key] }}</div>
        </div>
      </div>
    </div>

    <div v-if="run.error" class="mt-3 text-red-300 text-sm">{{ run.error }}</div>
    <details v-if="showLog" class="mt-3" :open="failed || current?.key === 'images'">
      <summary class="lbl" style="cursor: pointer">Details from Docker</summary>
      <pre class="mono lbl mt-1 max-h-40 overflow-auto whitespace-pre-wrap">{{ run.log.join('\n') }}</pre>
    </details>
  </div>
</template>

<style scoped>
.spin { animation: spin 1s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
</style>
