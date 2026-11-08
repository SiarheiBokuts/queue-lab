import { defineStore } from 'pinia'
import { ref } from 'vue'
import { api } from '../api'

const MAX = 400

const toPoint = (s: any) => ({ t: s.t, send: s.send_rate, recv: s.recv_rate, p50: s.p50, p95: s.p95, p99: s.p99, lag: s.lag_total })

export const useLive = defineStore('live', () => {
  const connected = ref(false)
  const dockerOk = ref(true)
  const dockerError = ref<string | null>(null)
  const warnings = ref<string[]>([])
  const containers = ref<any[]>([])
  const budget = ref<any>(null)
  const run = ref<any>(null)
  const comparison = ref<any>(null)
  const queues = ref<any[]>([])
  const runs = ref<any[]>([])
  const settings = ref<any>({ budget_mb: 2048, light_mode: false, remove_on_stop: true })
  const host = ref<any>({ total_mb: 0, available_mb: 0 })
  const projects = ref({ project: '', sandbox_project: '' })
const version = ref('')
  const history = ref<any[]>([])
  const lagHistory = ref<number[][]>([])
  const cHist = ref<Record<string, any[]>>({})
  let lastRunId = ''
  let lastT = -1
  let seeding = false
  let ws: WebSocket | null = null

  /** A page opened mid-run or after it: load all samples of the current run so charts are complete. */
  async function seed() {
    seeding = true
    try {
      const all: any[] = await api.currentSamples()
      history.value = all.map(toPoint).slice(-MAX)
      lagHistory.value = all.map((s) => s.lag).slice(-MAX)
      lastT = all.length ? all[all.length - 1].t : -1
    } catch {
      /* ignore */
    } finally {
      seeding = false
    }
  }

  function applyTick(m: any) {
    if (!m.docker_ok) {
      dockerOk.value = false
      dockerError.value = m.error || null
      return
    }
    dockerOk.value = true
    dockerError.value = null
    containers.value = m.containers
    budget.value = m.budget
    const r = m.run
    if (r && r.id !== lastRunId) {
      lastRunId = r.id
      lastT = -1
      history.value = []
      lagHistory.value = []
      seed()
    }
    run.value = r
    comparison.value = m.comparison ?? null
    if (r && !seeding) {
      for (const s of r.tail ?? []) {
        if (s.t > lastT) {
          lastT = s.t
          history.value = [...history.value, toPoint(s)].slice(-MAX)
          lagHistory.value = [...lagHistory.value, s.lag].slice(-MAX)
        }
      }
    }
    const h = { ...cHist.value }
    for (const c of m.containers) {
      const arr = (h[c.name] ||= [])
      arr.push({ t: m.t, cpu: c.cpu, mem: c.mem_mb, limit: c.limit_mb, rx: c.rx_kbs, tx: c.tx_kbs, msgs: c.msgs_s })
      if (arr.length > 360) arr.shift()
    }
    cHist.value = h
  }

  async function refresh() {
    try {
      const s = await api.state()
      dockerOk.value = s.docker_ok
      dockerError.value = s.docker_error
      warnings.value = s.warnings
      settings.value = s.settings
      queues.value = s.queues
      runs.value = s.runs
      host.value = s.host
      projects.value = { project: s.project, sandbox_project: s.sandbox_project }
      version.value = s.version ?? ''
      if (s.tick) applyTick(s.tick)
    } catch {
      /* backend not reachable yet */
    }
  }

  function connect() {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws'
    ws = new WebSocket(`${proto}://${location.host}/ws`)
    ws.onopen = () => {
      connected.value = true
      refresh()
    }
    ws.onmessage = (e) => applyTick(JSON.parse(e.data))
    ws.onclose = () => {
      connected.value = false
      setTimeout(connect, 2000)
    }
  }

  function start() {
    refresh()
    connect()
    setInterval(refresh, 5000)
  }

  return { connected, dockerOk, dockerError, warnings, containers, budget, run, comparison, queues, runs, settings, host, projects, version, history, lagHistory, cHist, refresh, start }
})
