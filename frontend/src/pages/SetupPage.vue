<script setup lang="ts">
import { reactive, ref, watch, onMounted, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '../api'
import { useLive } from '../stores/live'
import { LANGS, langName } from '../langs'
import { workerCapacity } from '../capacity'
import { estimateNetwork, fmtMiB } from '../compression'
import LangIcon from '../components/LangIcon.vue'
import NumberInput from '../components/NumberInput.vue'
import { groupDigits } from '../format'
import Glossary from '../components/Glossary.vue'
import LangPicker from '../components/LangPicker.vue'
import Spinner from '../components/Spinner.vue'
import { AlertTriangle, Check, CheckCircle2, Cpu, Eye, Hourglass, Lightbulb, ListChecks, Rocket, Zap } from '@lucide/vue'
import CodeBlock from '../components/CodeBlock.vue'
import Hint from '../components/Hint.vue'
import PageHeader from '../components/PageHeader.vue'

const live = useLive()
const router = useRouter()
const route = useRoute()

/** The whole run form. Its shape is exactly what POST /api/setups expects. */
const form = reactive({
  queue: 'lab-queue',
  partitions: 5,
  workers: { lang: 'node', count: 5, memory_mb: 400, concurrency: 100, ordering: 'key' },
  producer: { lang: 'python', memory_mb: 384, count: 1 },
  messages: 100000,
  message_size: 256,
  rate: 0,
  compression: 'none',
  payload: 'compressible',
  users: 1000,
  scenario: { type: 'echo', param: 0 },
})

/** Compare mode: the same test is run once per selected language, one after the other, then compared. */
const compare = ref(false)
const compareLangs = ref<string[]>(LANGS.map((l) => l.id)) // a comparison starts with every language selected
const randomOrder = ref(true)
const cooldown = ref(5)
const producerFollows = ref(true)

const plan = ref<any>(null)
const planErr = ref('')
/** Every language handles one message at a time, so the delay sets the ceiling for all of them. */
/** Computing messages cannot be overlapped: in Python (one thread at a time) and Node.js (one thread) a high number only adds overhead. */
const cpuNote = computed(() =>
  form.scenario.type === 'cpu' && form.workers.concurrency > 1
    ? 'This scenario computes. A message that computes keeps its core busy, so handling many at once does not make it faster; the speed comes from more workers or more cores. In Python and Node.js it only adds overhead.'
    : '')
const capacity = computed(() =>
  form.scenario.type === 'delay'
    ? workerCapacity(form.scenario.param, form.workers.count, form.partitions, form.workers.concurrency, form.workers.ordering, form.users, form.messages, form.rate)
    : null)
const submitErr = ref('')
const busy = ref(false)
const scenarios = ref<any[]>([])
const code = ref<any>(null)
const showCode = ref(false)
const codeLang = ref('node')
let timer: ReturnType<typeof setTimeout> | undefined

const presets = [
  { name: 'Baseline speed', note: 'No work per message: how fast can this setup move messages?', apply: () => Object.assign(form, { partitions: 5, messages: 200000, rate: 0, workers: { ...form.workers, count: 5, memory_mb: 300 }, scenario: { type: 'echo', param: 0 } }) },
  { name: 'Slow service (waiting)', note: 'Each message waits 5 ms, like a database or API call.', apply: () => Object.assign(form, { partitions: 5, messages: 30000, rate: 0, workers: { ...form.workers, count: 5, memory_mb: 300 }, scenario: { type: 'delay', param: 5 } }) },
  { name: 'Heavy computing', note: 'Each message burns CPU, like parsing or encryption.', apply: () => Object.assign(form, { partitions: 5, messages: 50000, rate: 0, workers: { ...form.workers, count: 5, memory_mb: 300 }, scenario: { type: 'cpu', param: 500 } }) },
  { name: 'Hard sustained load', note: '20,000 messages per second for one minute from 2 producers, 5 workers.', apply: () => Object.assign(form, { partitions: 10, messages: 1200000, rate: 20000, producer: { ...form.producer, count: 2 }, workers: { ...form.workers, count: 5, memory_mb: 400 }, scenario: { type: 'echo', param: 0 } }) },
]

/** What this run will start, in one sentence. */
const runSummary = computed(() => {
  const who = compare.value
    ? `The same test in ${compareLangs.value.length} languages, one after another`
    : `${form.workers.count} × ${langName(form.workers.lang)} workers and ${form.producer.count} × ${langName(form.producer.lang)} sender${form.producer.count > 1 ? 's' : ''}`
  const rate = form.rate > 0 ? `, ${groupDigits(form.rate)} messages per second` : ', at full speed'
  return `${who}: ${groupDigits(form.messages)} messages of ${form.message_size} bytes${rate}.`
})
/** The memory this run needs, split into workers, senders and the Kafka broker, as shares of a bar. */
const memSegments = computed(() => {
  const b = plan.value?.breakdown
  if (!b) return []
  const total = Math.max(1, plan.value.requested_mb, plan.value.remaining_mb)
  return [
    { label: 'Workers', mb: b.workers_mb, color: '#3b82f6' },
    { label: 'Senders', mb: b.producer_mb, color: '#8b5cf6' },
    ...(b.broker_mb ? [{ label: 'Kafka broker', mb: b.broker_mb, color: '#06b6d4' }] : []),
  ].map((s) => ({ ...s, pct: (s.mb / total) * 100 }))
})
/** Approximate network use before and after compression, for the tile under the load options. */
const net = computed(() => estimateNetwork(form.messages || 0, form.message_size || 0, form.compression as any, form.payload as any))
const memLeft = computed(() => Math.max(0, (plan.value?.remaining_mb ?? 0) - (plan.value?.requested_mb ?? 0)))
const duration = computed({
  get: () => (form.rate > 0 ? Math.round(form.messages / form.rate) : 0),
  set: (s: number) => {
    if (form.rate > 0 && s > 0) form.messages = Math.min(50_000_000, Math.round(form.rate * s))
  },
})
/** Ready-made amounts as buttons; the number field next to them takes any other value. */
const MESSAGE_STEPS = [1_000, 10_000, 100_000, 1_000_000, 10_000_000]
const RATE_STEPS = [0, 1_000, 10_000, 50_000, 100_000]
const short = (n: number) => (n === 0 ? 'Max' : n >= 1_000_000 ? n / 1_000_000 + 'M' : n / 1_000 + 'k')
/** Icon and accent colour of each scenario card. */
const SCENARIO_LOOK: Record<string, { icon: any; color: string }> = {
  echo: { icon: Zap, color: '#f59e0b' },
  delay: { icon: Hourglass, color: '#60a5fa' },
  cpu: { icon: Cpu, color: '#f87171' },
}
const mib = 1024 * 1024
const rateMibS = computed(() => (form.rate * form.message_size) / mib)
// Switching to a comparison: every language, a million messages and lz4, the setup the comparison is meant for.
watch(compare, (on) => {
  if (!on) return
  compareLangs.value = LANGS.map((l) => l.id)
  form.messages = 1_000_000
  form.compression = 'lz4'
})
const effectiveLang = computed(() => (compare.value ? compareLangs.value[0] ?? form.workers.lang : form.workers.lang))

/** Opened from a finished run with `?like=<run>`: start from that run's setup (optionally in another language). */
async function prefillFromRun() {
  const like = String(route.query.like || '')
  if (!like) return
  try {
    const r = await api.run(like)
    Object.assign(form, r.config)
    form.workers = { concurrency: 1, ordering: 'key', ...r.config.workers } // runs saved before these options were one at a time
    form.users = r.config.users ?? 1000
    const lang = String(route.query.workersLang || '')
    if (LANGS.some((l) => l.id === lang)) form.workers.lang = lang
  } catch {
    /* the run may have been deleted: keep the defaults */
  }
}

async function refreshPlan() {
  try {
    plan.value = await api.plan(form)
    planErr.value = ''
  } catch (e: any) {
    plan.value = null
    planErr.value = e.message
  }
}
watch(form, () => {
  clearTimeout(timer)
  timer = setTimeout(refreshPlan, 300)
}, { deep: true })

onMounted(async () => {
  await prefillFromRun()
  refreshPlan()
  try {
    scenarios.value = await api.scenarios()
    code.value = await api.code()
  } catch {
    /* the form still works without the descriptions */
  }
})

const current = computed(() => scenarios.value.find((s) => s.id === form.scenario.type))

function pick(s: any) {
  form.scenario.type = s.id
  form.scenario.param = s.param ? s.param.default : 0
}

watch(() => form.workers.lang, (l) => {
  codeLang.value = l
})

function toggleLang(id: string) {
  const i = compareLangs.value.indexOf(id)
  if (i >= 0) compareLangs.value.splice(i, 1)
  else compareLangs.value.push(id)
}

async function submit() {
  busy.value = true
  submitErr.value = ''
  try {
    if (compare.value) {
      if (compareLangs.value.length < 2) throw new Error('Pick at least two languages to compare.')
      form.workers.lang = compareLangs.value[0] as string
      await api.startComparison({ setup: form, langs: compareLangs.value, cooldown_s: cooldown.value, producer_follows: producerFollows.value, random_order: randomOrder.value })
    } else {
      await api.start(form)
    }
    router.push('/')
  } catch (e: any) {
    submitErr.value = e.message
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <PageHeader
    title="New run"
    subtitle="Set up a queue, the senders and the workers, pick what the workers do with each message, and start. You can run one test, or the same test in several languages one after another and compare them."
    :crumbs="[{ label: 'Dashboard', to: '/' }, { label: 'New run' }]"
  />

  <Glossary />

  <div class="card">
    <div class="lbl mb-2">Start from a ready-made setup, then change anything you like.</div>
    <div class="flex gap-3 flex-wrap">
      <button v-for="p in presets" :key="p.name" type="button" class="btn" :title="p.note" @click="p.apply()">{{ p.name }}</button>
    </div>
  </div>

  <div class="card">
    <h2 class="mb-2">1. What do you want to do?</h2>
    <div class="grid gap-3" style="grid-template-columns: repeat(auto-fit, minmax(280px, 1fr))">
      <button type="button" class="text-left" :style="{ background: !compare ? '#0c2547' : '#1f1f23', border: '1px solid ' + (!compare ? '#3b82f6' : '#3f3f46'), borderRadius: '10px', padding: '12px 14px', cursor: 'pointer', color: 'inherit' }" @click="compare = false">
        <div class="font-semibold mb-1">One test</div>
        <div class="lbl">Run the setup below once, in the languages you choose for the senders and workers.</div>
      </button>
      <button type="button" class="text-left" :style="{ background: compare ? '#0c2547' : '#1f1f23', border: '1px solid ' + (compare ? '#3b82f6' : '#3f3f46'), borderRadius: '10px', padding: '12px 14px', cursor: 'pointer', color: 'inherit' }" @click="compare = true">
        <div class="font-semibold mb-1">Compare languages</div>
        <div class="lbl">Run the same test once per language, one after the other with a pause, then see them side by side.</div>
      </button>
    </div>
    <div v-if="compare" class="mt-4">
      <div class="lbl mb-2">Languages to compare (pick two or more)</div>
      <div class="flex gap-2 flex-wrap mb-3">
        <button
          v-for="l in LANGS"
          :key="l.id"
          type="button"
          class="btn"
          :style="compareLangs.includes(l.id) ? { borderColor: l.color, background: '#1a1a1d' } : {}"
          :title="l.client"
          @click="toggleLang(l.id)"
        >
          <LangIcon :id="l.id" :size="14" style="display: inline-block; vertical-align: -2px; margin-right: 6px" />
          <span class="inline-block w-2 h-2 rounded-full mr-1.5" :style="{ background: l.color }" />{{ l.name }}
          <span v-if="!randomOrder && compareLangs.includes(l.id)" class="lbl"> · #{{ compareLangs.indexOf(l.id) + 1 }}</span>
        </button>
      </div>
      <div class="grid gap-3" style="grid-template-columns: repeat(auto-fit, minmax(240px, 1fr))">
        <label class="field">Pause between the runs (seconds)<input class="input" type="number" min="0" max="120" v-model.number="cooldown" /></label>
        <label class="flex items-start gap-2 text-sm"><input type="checkbox" v-model="randomOrder" class="mt-1" /><span>Random order<br /><span class="lbl">The order is drawn when you start, so no language always runs first on a cold machine. Switch off to run them in the order you picked.</span></span></label>
        <label class="flex items-start gap-2 text-sm"><input type="checkbox" v-model="producerFollows" class="mt-1" /><span>The sender uses the same language as the workers<br /><span class="lbl">Otherwise the sender language below stays the same in every run.</span></span></label>
      </div>
      <div class="lbl mt-2">All Kafka settings are identical for every language. Only the language and its client library differ.</div>
    </div>
  </div>

  <div class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(320px, 1fr))">
    <div class="card flex flex-col gap-3">
      <h2>2. Queue</h2>
      <label class="field">Name
        <input class="input" v-model="form.queue" list="existing-queues" />
        <datalist id="existing-queues"><option v-for="q in live.queues" :key="q.name" :value="q.name" /></datalist>
      </label>
      <label class="field">Partitions (1-64)<Hint text="A partition is a slice of the queue. Each partition is read by only one worker of the group at a time, so more partitions allow more workers to work in parallel." />
        <input class="input" type="number" min="1" max="64" v-model.number="form.partitions" />
      </label>
    </div>

    <div class="card flex flex-col gap-3">
      <h2>3. Workers (they receive the messages)</h2>
      <div v-if="!compare"><div class="lbl mb-1.5">Language</div><LangPicker v-model="form.workers.lang" /></div>
      <div v-else class="lbl">The language changes from run to run: {{ compareLangs.map(langName).join(', ') || 'none selected' }}.</div>
      <div class="grid grid-cols-2 gap-3">
        <label class="field">Count (1-32)<input class="input" type="number" min="1" max="32" v-model.number="form.workers.count" /></label>
        <label class="field">Memory each (MB)<input class="input" type="number" min="128" step="64" v-model.number="form.workers.memory_mb" /></label>
        <label class="field">At the same time per worker<Hint text="How many messages one worker handles at once. While one message waits for a database or an API, the others are processed, like a production service does. 1 means strictly one at a time. Order inside a partition is not guaranteed when this is above 1." /><input class="input" type="number" min="1" max="1024" v-model.number="form.workers.concurrency" /></label>
        <label class="field">Order of messages<Hint text="Per user: all messages of one user (for example the payments of one customer) are handled strictly in order, while different users are handled side by side. This is how production services keep payments correct. Any order: the fastest, but messages of the same user can overtake each other." />
          <select class="input" v-model="form.workers.ordering"><option value="key">Per user, in order</option><option value="none">Any order (fastest)</option></select></label>
      </div>
      <div v-if="plan && plan.idle_workers" class="text-amber-400 text-xs">{{ plan.idle_workers }} worker(s) will stay idle: there are more workers than partitions.</div>
    </div>

    <div class="card flex flex-col gap-3">
      <h2>4. Senders (producers)</h2>
      <div v-if="!(compare && producerFollows)"><div class="lbl mb-1.5">Language</div><LangPicker v-model="form.producer.lang" /></div>
      <div v-else class="lbl">The sender follows the workers' language.</div>
      <div class="grid grid-cols-2 gap-3">
        <label class="field">How many (1-8)<Hint text="Several sender containers share the load. Use more than one when a single sender cannot reach the rate you want." />
          <input class="input" type="number" min="1" max="8" v-model.number="form.producer.count" /></label>
        <label class="field">Memory each (MB)<input class="input" type="number" min="128" step="64" v-model.number="form.producer.memory_mb" /></label>
      </div>
    </div>
  </div>

  <div class="card">
    <h2 class="mb-3">5. The load</h2>
    <div class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(220px, 1fr))">
      <label class="field">Messages in total<Hint text="Up to 50 million. The run is stopped after 30 minutes." />
        <NumberInput v-model="form.messages" :min="1" :max="50000000" />
        <span class="chips"><button v-for="n in MESSAGE_STEPS" :key="n" type="button" class="chip" :class="{ on: form.messages === n }" @click.prevent="form.messages = n">{{ short(n) }}</button></span>
        <span class="lbl">= {{ groupDigits(form.messages || 0) }} messages</span></label>
      <label class="field">Message size (bytes)<Hint text="Includes a small header of about 30 bytes. Kafka's default limit is about 1 MB per message." />
        <NumberInput v-model="form.message_size" :min="64" :max="900000" /></label>
      <label class="field">Send speed (messages per second)<Hint text="The target for all senders together. 0 means as fast as possible. Set a number to hold a steady load, for example 20000 per second." />
        <NumberInput v-model="form.rate" :min="0" :max="5000000" />
        <span class="chips"><button v-for="n in RATE_STEPS" :key="n" type="button" class="chip" :class="{ on: form.rate === n }" @click.prevent="form.rate = n">{{ short(n) }}</button></span>
        <span class="lbl">{{ form.rate ? groupDigits(form.rate) + " messages per second" : "as fast as possible" }}</span></label>
      <label class="field" v-if="form.rate > 0">...or for how long (seconds)<Hint text="Sets the number of messages to rate x seconds." />
        <input class="input" type="number" min="1" v-model.number="duration" /></label>
      <label class="field">Compression<Hint text="Really compresses every batch inside the senders before it is sent, with the same codec in every language. It saves network and disk and costs some CPU. It only helps with data that repeats. The tile below shows an approximate result, the report the real one." />
        <select class="input" v-model="form.compression">
          <option value="none">None</option><option value="lz4">lz4 (fast)</option><option value="snappy">snappy</option>
          <option value="gzip">gzip (small, slower)</option><option value="zstd">zstd (balanced)</option>
        </select></label>
      <label class="field">Different users<Hint text="Every message belongs to a user (its key), like a payment of a customer. A user always lands in the same partition, so Kafka keeps that user's messages in order. More users means more messages can be handled side by side." />
        <NumberInput v-model="form.users" :min="1" :max="1000000" /></label>
      <label class="field">Message content<Hint text="Compressible: repeated filler, shrinks a lot. Random: like encrypted data, does not shrink. Use random to see what compression really costs." />
        <select class="input" v-model="form.payload"><option value="compressible">Compressible filler</option><option value="random">Random bytes</option></select></label>
    </div>
    <div class="grid gap-2.5 mt-4" style="grid-template-columns: repeat(auto-fit, minmax(200px, 1fr))">
      <div class="tile"><div class="lbl">Data in total</div><div class="val">{{ fmtMiB(net.beforeMiB) }}</div><div class="lbl">messages x size, before compression</div></div>
      <div class="tile" :style="{ borderLeft: '3px solid ' + (net.ratio > 1 ? '#10b981' : '#3f3f46') }">
        <div class="lbl">On the network (estimate)</div>
        <div class="val">≈ {{ fmtMiB(net.afterMiB) }}</div>
        <div class="lbl" v-if="form.compression === 'none'">no compression: the data goes as it is</div>
        <div class="lbl" v-else-if="net.ratio === 1">random data does not shrink, so {{ form.compression }} saves nothing</div>
        <div class="lbl" v-else>≈ {{ Math.round(net.ratio) }}× smaller with {{ form.compression }}, about {{ net.savedPct }}% less. The report shows the real numbers.</div>
      </div>
      <div class="tile" v-if="form.rate > 0"><div class="lbl">Network needed while sending</div><div class="val">{{ rateMibS.toFixed(1) }} MiB/s</div><div class="lbl">≈ {{ (rateMibS * 8).toFixed(0) }} Mbit/s, before compression</div></div>
      <div class="tile" v-if="form.rate > 0"><div class="lbl">Sending takes about</div><div class="val">{{ duration }} s</div></div>
      <div class="tile"><div class="lbl">Each sender</div><div class="val">{{ form.rate > 0 ? Math.round(form.rate / form.producer.count).toLocaleString() + ' msg/s' : 'full speed' }}</div></div>
    </div>
    <div class="lbl mt-2">The report afterwards shows how much network the data needed and how much was really sent.</div>
  </div>

  <div class="card">
    <h2 class="mb-1">6. What should the workers do with each message?</h2>
    <p class="lbl mb-3">Pick the kind of work to simulate. The same setup behaves very differently depending on it, so try them one after another and compare the runs.</p>
    <div class="grid gap-3" style="grid-template-columns: repeat(auto-fit, minmax(240px, 1fr))">
      <button
        v-for="s in scenarios"
        :key="s.id"
        type="button"
        class="text-left"
        :style="{ background: form.scenario.type === s.id ? '#0c2547' : '#1f1f23', border: '1px solid ' + (form.scenario.type === s.id ? '#3b82f6' : '#3f3f46'), borderRadius: '12px', padding: '14px 16px', cursor: 'pointer', color: 'inherit' }"
        @click="pick(s)"
      >
        <div class="flex items-center gap-2.5 mb-2">
          <span class="icon-chip" :style="{ background: (SCENARIO_LOOK[s.id]?.color ?? '#a1a1aa') + '24', color: SCENARIO_LOOK[s.id]?.color }">
            <component :is="SCENARIO_LOOK[s.id]?.icon ?? Zap" :size="17" />
          </span>
          <b>{{ s.title }}</b>
          <CheckCircle2 v-if="form.scenario.type === s.id" :size="17" color="#3b82f6" class="ml-auto" />
        </div>
        <div class="lbl">{{ s.summary }}</div>
      </button>
    </div>

    <div v-if="current" class="mt-5 grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(260px, 1fr))">
      <div class="info-block" style="border-color: #f59e0b">
        <div class="info-head" style="color: #f59e0b"><Lightbulb :size="14" />What it simulates</div>
        <div class="text-sm" style="line-height: 1.55">{{ current.simulates }}</div>
      </div>
      <div class="info-block" style="border-color: #3b82f6">
        <div class="info-head" style="color: #60a5fa"><ListChecks :size="14" />What the worker does</div>
        <ul class="info-list"><li v-for="d in current.does" :key="d"><Check :size="14" color="#60a5fa" />{{ d }}</li></ul>
      </div>
      <div class="info-block" style="border-color: #10b981">
        <div class="info-head" style="color: #34d399"><Eye :size="14" />What you will see in the results</div>
        <ul class="info-list"><li v-for="w in current.watch" :key="w"><Check :size="14" color="#34d399" />{{ w }}</li></ul>
      </div>
    </div>
    <label class="field mt-4" v-if="current && current.param" style="max-width: 300px">{{ current.param.label }}
      <input class="input" type="number" :min="current.param.min" v-model.number="form.scenario.param" />
      <span class="lbl">{{ current.param.hint }}</span></label>
    <div class="mt-4">
      <button class="btn" @click="showCode = !showCode">{{ showCode ? 'Hide the code' : 'Show the code of this step' }}</button>
      <router-link to="/code" class="lbl ml-3 hover:underline">Open the code page</router-link>
    </div>
    <div v-if="showCode && code" class="mt-3">
      <div class="flex gap-2 mb-2 flex-wrap">
        <button v-for="l in LANGS" :key="l.id" class="btn" :class="codeLang === l.id ? 'btn-primary' : ''" @click="codeLang = l.id">{{ l.name }}</button>
      </div>
      <div class="lbl mb-1">The step that handles each message. Only this part changes between scenarios.</div>
      <CodeBlock :code="code[codeLang].scenario" :title="langName(codeLang) + ' · scenario'" />
    </div>
  </div>

  <div class="card" :style="{ borderColor: plan && !plan.ok ? '#7f1d1d' : '' }">
    <h2 class="mb-1 flex items-center gap-2"><Rocket :size="17" color="#60a5fa" />7. Check and start</h2>
    <p class="lbl mb-4">{{ runSummary }}</p>

    <template v-if="plan">
      <div class="mem-bar" role="img" :aria-label="'Memory needed: ' + plan.requested_mb + ' MB'">
        <i v-for="s in memSegments" :key="s.label" :style="{ width: s.pct + '%', background: s.color }" />
      </div>
      <div class="flex flex-wrap gap-x-6 gap-y-2 mt-3">
        <div v-for="s in memSegments" :key="s.label" class="flex items-center gap-2 text-sm">
          <span class="dot" :style="{ background: s.color }" />{{ s.label }} <b>{{ s.mb }} MB</b>
        </div>
        <div v-if="!plan.breakdown.broker_mb" class="flex items-center gap-2 text-sm"><span class="dot" style="background: #06b6d4" />Kafka broker <b>already running</b></div>
      </div>
      <div class="grid gap-3 mt-4" style="grid-template-columns: repeat(auto-fit, minmax(180px, 1fr))">
        <div class="tile"><div class="lbl">Memory this run needs</div><div class="val">{{ plan.requested_mb }} MB</div></div>
        <div class="tile"><div class="lbl">Memory budget</div><div class="val">{{ plan.remaining_mb }} MB</div><div class="lbl">free for the lab</div></div>
        <div class="tile"><div class="lbl">Left after start</div><div class="val">{{ memLeft }} MB</div></div>
      </div>
      <div class="mt-4">
        <span v-if="plan.ok" class="pill ok"><CheckCircle2 :size="15" />Fits into your budget</span>
        <span v-else class="pill bad"><AlertTriangle :size="15" />{{ plan.reason }}</span>
      </div>
    </template>
    <div v-if="planErr" class="mt-3 text-red-300 text-sm">{{ planErr }}</div>
    <div v-if="cpuNote" class="mt-3 flex items-start gap-2 text-sm" style="color: #fbbf24"><AlertTriangle :size="16" style="flex: none; margin-top: 2px" />{{ cpuNote }}</div>
    <div v-if="capacity" class="mt-3 flex items-start gap-2 text-sm" :style="{ color: capacity.ok ? '#34d399' : '#fbbf24' }">
      <component :is="capacity.ok ? CheckCircle2 : AlertTriangle" :size="16" style="flex: none; margin-top: 2px" />{{ capacity.text }}
    </div>
    <div class="lbl mt-3" v-if="compare">This will run {{ compareLangs.length }} tests one after another ({{ randomOrder ? 'in random order' : compareLangs.map(langName).join(' → ') }}) with {{ cooldown }} s pauses. The first run builds the image of a language if needed, which can take a few minutes.</div>
    <div class="flex items-center gap-3 mt-5 flex-wrap">
      <button class="btn btn-primary inline-flex items-center gap-2" style="padding: 11px 22px; font-size: 14px" :disabled="busy || !plan || !plan.ok || (compare && compareLangs.length < 2)" @click="submit">
        <Spinner v-if="busy" :size="16" /><Rocket v-else :size="16" />
        {{ busy ? 'Starting…' : compare ? 'Run and compare ' + compareLangs.length + ' languages' : 'Create and run' }}
      </button>
      <span class="text-red-300 text-sm">{{ submitErr }}</span>
    </div>
  </div></template>
