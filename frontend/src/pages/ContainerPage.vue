<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { useLive } from '../stores/live'
import { api } from '../api'
import { containerState } from '../containerState'
import { vTooltip } from 'floating-vue'
import LineChart from '../components/LineChart.vue'
import { C } from '../chart'
import { containerRows, marksFrom, statsOf, windowRange } from '../window'

const live = useLive()
const route = useRoute()
const name = computed(() => String(route.params.name))
const c = computed(() => live.containers.find((x) => x.name === name.value))
const logs = ref('')
const full = ref<any>(null)
const mode = ref<'window' | 'live'>('window')
let timer: any
let timer2: any

async function loadLogs() {
  try { logs.value = (await api.logs(name.value, 200)).logs } catch { /* ignore */ }
}
async function loadFull() {
  try { full.value = await api.currentFull() } catch { full.value = null }
}
onMounted(() => {
  loadLogs(); loadFull()
  timer = setInterval(loadLogs, 2000)
  timer2 = setInterval(() => { if (live.run?.status === 'running' || live.run?.status === 'pending') loadFull() }, 2000)
})
onUnmounted(() => { clearInterval(timer); clearInterval(timer2) })

const win = computed(() => full.value?.window ?? {})
const hasRun = computed(() => !!full.value?.container_samples?.length)
const useWindow = computed(() => mode.value === 'window' && hasRun.value)
const range = computed(() => (useWindow.value ? windowRange(win.value) : null))
const rows = computed(() => containerRows(full.value?.container_samples ?? [], name.value, range.value))
const marks = computed(() => (useWindow.value ? marksFrom(win.value) : []))
const shade = computed(() => (useWindow.value ? windowRange(win.value, 0) : null))
const messages = computed(() => {
  const d = full.value?.summary?.distribution?.find((x: any) => x.name === name.value)
  return d ? d.processed : undefined
})
const st = computed(() => statsOf(rows.value, messages.value))

// live mode (rolling history kept by the store)
const hist = computed(() => live.cHist[name.value] || [])
const lx = computed(() => hist.value.map((_, i) => i))

const x = computed(() => (useWindow.value ? rows.value.map((r) => r.t) : lx.value))
const pick = (w: (r: any) => number, l: (h: any) => number) => (useWindow.value ? rows.value.map((r) => w(r.c)) : hist.value.map(l))
const cpu = computed(() => [{ name: 'CPU % of one core', color: C.amber, data: pick((r) => r.cpu, (h) => h.cpu) }])
const mem = computed(() => [
  { name: 'Memory MB', color: C.blue, data: pick((r) => r.mem_mb, (h) => h.mem) },
  { name: 'Limit MB', color: C.red, data: pick((r) => r.limit_mb, (h) => h.limit), dashed: true },
])
const net = computed(() => [
  { name: 'In KB/s', color: C.green, data: pick((r) => r.rx_kbs, (h) => h.rx) },
  { name: 'Out KB/s', color: C.violet, data: pick((r) => r.tx_kbs, (h) => h.tx) },
])
const msgs = computed(() => [{ name: 'Messages / s', color: C.blue, data: pick((r) => r.msgs_s, (h) => h.msgs) }])
const xr = computed(() => (range.value ? { xmin: range.value[0], xmax: range.value[1] } : {}))
</script>

<template>
  <div class="flex items-center gap-3 flex-wrap">
    <router-link to="/" class="btn">Back</router-link>
    <div><div class="lbl"><router-link to="/" class="hover:underline">Dashboard</router-link> / Container</div><h1>{{ name.replace('queuelab-', '') }}</h1></div>
    <span v-if="c" v-tooltip="containerState(c).hint" class="badge" :class="containerState(c).tone">{{ containerState(c).label }}</span>
    <div class="flex gap-2 ml-auto">
      <button class="btn" :class="useWindow ? 'btn-primary' : ''" :disabled="!hasRun" @click="mode = 'window'">Run window</button>
      <button class="btn" :class="!useWindow ? 'btn-primary' : ''" @click="mode = 'live'">Live (last minutes)</button>
    </div>
  </div>
  <div class="lbl" v-if="useWindow">Showing only the time between the first message sent and the last one processed, so the load is not squashed by idle time before and after.</div>
  <div v-if="!c" class="card lbl">This container is not running any more. Logs below are from the last known state, if still available.</div>

  <div class="grid gap-2.5" style="grid-template-columns: repeat(auto-fit, minmax(150px, 1fr))" v-if="c">
    <div class="tile"><div class="lbl">Role / language</div><div class="val" style="font-size: 16px">{{ c.role }} {{ c.lang }}</div></div>
    <div class="tile"><div class="lbl">Memory limit</div><div class="val">{{ c.limit_mb }} MB</div></div>
    <div class="tile"><div class="lbl">Memory now</div><div class="val">{{ c.mem_mb }} MB</div></div>
    <div class="tile"><div class="lbl">CPU now</div><div class="val">{{ c.cpu }}%</div></div>
    <div class="tile"><div class="lbl">Restarts</div><div class="val">{{ c.restarts }}</div></div>
    <div class="tile" v-if="c.role === 'consumer' && live.run"><div class="lbl">Handles at once</div><div class="val" style="font-size: 16px">{{ live.run.config.workers.concurrency ?? 1 }}</div></div>
    <div class="tile"><div class="lbl">Partitions</div><div class="val" style="font-size: 16px">{{ c.partitions?.length ? c.partitions.join(', ') : '-' }}</div></div>
  </div>

  <div class="card" v-if="useWindow && rows.length">
    <h2 class="mb-2">In the message window</h2>
    <div class="grid gap-2.5" style="grid-template-columns: repeat(auto-fit, minmax(150px, 1fr))">
      <div class="tile"><div class="lbl">CPU avg / peak</div><div class="val" style="font-size: 18px">{{ st.cpuAvg }}% / {{ st.cpuPeak }}%</div></div>
      <div class="tile"><div class="lbl">CPU time used</div><div class="val" style="font-size: 18px">{{ st.cpuSeconds }} s</div></div>
      <div class="tile"><div class="lbl">Memory avg / peak</div><div class="val" style="font-size: 18px">{{ st.memAvg }} / {{ st.memPeak }} MB</div></div>
      <div class="tile"><div class="lbl">Memory headroom</div><div class="val" style="font-size: 18px">{{ st.headroom ?? '-' }} MB</div></div>
      <div class="tile"><div class="lbl">Network in / out</div><div class="val" style="font-size: 18px">{{ st.netIn }} / {{ st.netOut }} MB</div></div>
      <div class="tile" v-if="st.costPer1k != null"><div class="lbl">CPU per 1k messages</div><div class="val" style="font-size: 18px">{{ st.costPer1k }} ms</div></div>
    </div>
  </div>

  <div class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(340px, 1fr))">
    <div class="card"><h2 class="mb-1">CPU</h2><LineChart title="CPU" :x="x" :series="cpu" :marks="marks" :shade="shade" v-bind="xr" area /></div>
    <div class="card"><h2 class="mb-1">Memory against limit</h2><LineChart title="Memory against limit" :x="x" :series="mem" :marks="marks" v-bind="xr" /></div>
    <div class="card"><h2 class="mb-1">Network</h2><LineChart title="Network" :x="x" :series="net" :marks="marks" v-bind="xr" /></div>
    <div class="card"><h2 class="mb-1">Messages per second</h2><LineChart title="Messages per second" :x="x" :series="msgs" :marks="marks" v-bind="xr" area /></div>
  </div>
  <div class="card">
    <h2 class="mb-2">Logs</h2>
    <pre class="mono lbl whitespace-pre-wrap overflow-auto" style="max-height: 320px">{{ logs || 'No log lines yet.' }}</pre>
  </div>
</template>
