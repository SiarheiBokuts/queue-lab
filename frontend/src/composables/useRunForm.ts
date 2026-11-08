/**
 * The state and the rules of the "New run" page, kept out of the components.
 *
 * The page creates it once (`provideRunForm`) and every section of the form reads it (`useRunForm`), so a section is
 * only a template and its own small details. Nothing here touches the DOM.
 */
import { computed, inject, onMounted, provide, reactive, ref, watch, type InjectionKey } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '../api'
import { workerCapacity } from '../capacity'
import { estimateNetwork } from '../compression'
import { groupDigits } from '../format'
import { LANGS, langName } from '../langs'
import { useLive } from '../stores/live'

/** The whole run form. Its shape is exactly what POST /api/setups expects. */
function defaultForm() {
  return {
    queue: 'lab-queue',
    partitions: 5,
    workers: { lang: 'node', count: 5, memory_mb: 400, concurrency: 100 },
    producer: { lang: 'python', memory_mb: 384, count: 1 },
    messages: 100000,
    message_size: 256,
    rate: 0,
    compression: 'none',
    payload: 'compressible',
    users: 1000,
    scenario: { type: 'echo', param: 0 },
  }
}

const MIB = 1024 * 1024

function createRunForm() {
  const live = useLive()
  const router = useRouter()
  const route = useRoute()
  const form = reactive(defaultForm())

  // ---- compare mode: the same test once per selected language, one after the other, then compared
  const compare = ref(false)
  const compareLangs = ref<string[]>(LANGS.map((l) => l.id)) // a comparison starts with every language selected
  const randomOrder = ref(true)
  const cooldown = ref(5)
  /** Usually one sender language serves every run, so only the workers differ. Optional: the sender follows the workers. */
  const producerFollows = ref(false)
  watch(compare, (on) => {
    if (!on) return
    compareLangs.value = LANGS.map((l) => l.id)
    form.messages = 1_000_000
    form.compression = 'lz4'
    form.producer.lang = 'go' // a fast, light sender, so it never slows the workers down
  })
  function toggleLang(id: string) {
    const i = compareLangs.value.indexOf(id)
    if (i >= 0) compareLangs.value.splice(i, 1)
    else compareLangs.value.push(id)
  }

  // ---- what the backend says about this setup (memory, cores), refreshed shortly after every change
  const plan = ref<any>(null)
  const planErr = ref('')
  let timer: ReturnType<typeof setTimeout> | undefined
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

  // ---- scenario descriptions and the code of the scenario step
  const scenarios = ref<any[]>([])
  const code = ref<any>(null)
  const current = computed(() => scenarios.value.find((s) => s.id === form.scenario.type))
  function pickScenario(s: any) {
    form.scenario.type = s.id
    form.scenario.param = s.param ? s.param.default : 0
  }

  // ---- ready-made setups. Each one sets everything that matters, so the result does not depend on what was typed before.
  /** The common ground of every preset: 5 partitions, 5 workers with a batch of 100, one sender, 1000 users, no compression. */
  const preset = (messages: number, scenario: { type: string; param: number }, more: Record<string, unknown> = {}) => () => {
    Object.assign(form, {
      partitions: 5, messages, rate: 0, message_size: 256, compression: 'none', payload: 'compressible', users: 1000, scenario,
      workers: { ...form.workers, count: 5, memory_mb: 400, concurrency: 100 },
      producer: { ...form.producer, count: 1, memory_mb: 384 },
      ...more,
    })
  }
  const presets = [
    { name: 'Baseline speed', note: 'No work per message: how fast can this setup move a million messages?', apply: preset(1_000_000, { type: 'echo', param: 0 }) },
    { name: 'Slow service (waiting)', note: 'Each message waits 20 ms, like a database call. 5 workers with a batch of 100 can handle about 25,000 messages per second.', apply: preset(200_000, { type: 'delay', param: 20 }) },
    { name: 'Work with JSON', note: 'Each message is parsed as JSON and written back 5 times, like a real service does. The CPU is the limit.', apply: preset(200_000, { type: 'json', param: 5 }) },
    { name: 'Few users', note: 'Only 10 users: a user is handled one message at a time, so at most 2 messages per worker run together and the waiting service cannot be used well.', apply: preset(20_000, { type: 'delay', param: 20 }, { users: 10 }) },
    { name: 'Hard sustained load', note: '20,000 messages per second for one minute from 2 senders, 10 partitions, 5 workers.', apply: preset(1_200_000, { type: 'echo', param: 0 }, { partitions: 10, rate: 20000, producer: { ...form.producer, count: 2, memory_mb: 384 } }) },
  ]
  // ---- derived values for the page
  const duration = computed({
    get: () => (form.rate > 0 ? Math.round(form.messages / form.rate) : 0),
    set: (seconds: number) => {
      if (form.rate > 0 && seconds > 0) form.messages = Math.min(50_000_000, Math.round(form.rate * seconds))
    },
  })
  const rateMibS = computed(() => (form.rate * form.message_size) / MIB)
  /** Approximate network use before and after compression. */
  const net = computed(() => estimateNetwork(form.messages || 0, form.message_size || 0, form.compression as any, form.payload as any))
  const capacity = computed(() =>
    form.scenario.type === 'delay'
      ? workerCapacity(form.scenario.param, form.workers.count, form.partitions, form.workers.concurrency, form.users, form.messages, form.rate)
      : null)
  /** Computing messages cannot be overlapped: one that computes keeps its core busy. */
  const cpuNote = computed(() =>
    form.scenario.type === 'json' && form.workers.concurrency > 1
      ? 'This scenario computes. A message that is being parsed keeps its core busy, so handling many at once does not make it faster; the speed comes from more workers or more cores.'
      : '')
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
  const memLeft = computed(() => Math.max(0, (plan.value?.remaining_mb ?? 0) - (plan.value?.requested_mb ?? 0)))

  // ---- start from an earlier run (`?like=<run>`), optionally in another language
  async function prefillFromRun() {
    const like = String(route.query.like || '')
    if (!like) return
    try {
      const r = await api.run(like)
      Object.assign(form, r.config)
      form.workers = { concurrency: 1, ...r.config.workers } // runs saved before these options were one at a time
      form.users = r.config.users ?? 1000
      const lang = String(route.query.workersLang || '')
      if (LANGS.some((l) => l.id === lang)) form.workers.lang = lang
    } catch {
      /* the run may have been deleted: keep the defaults */
    }
  }

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

  // ---- start the run
  const busy = ref(false)
  const submitErr = ref('')
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

  return {
    live, form, compare, compareLangs, randomOrder, cooldown, producerFollows, toggleLang,
    plan, planErr, scenarios, code, current, pickScenario, presets,
    duration, rateMibS, net, capacity, cpuNote, runSummary, memSegments, memLeft,
    busy, submitErr, submit,
  }
}

export type RunForm = ReturnType<typeof createRunForm>
const KEY: InjectionKey<RunForm> = Symbol('run-form')

/** Called once, by the page: creates the state and hands it to every section below it. */
export function provideRunForm(): RunForm {
  const runForm = createRunForm()
  provide(KEY, runForm)
  return runForm
}

/** Called by a section of the form. */
export function useRunForm(): RunForm {
  const runForm = inject(KEY)
  if (!runForm) throw new Error('useRunForm needs a page that called provideRunForm')
  return runForm
}
