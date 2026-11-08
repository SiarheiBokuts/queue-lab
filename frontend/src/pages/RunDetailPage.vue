<script setup lang="ts">
import { Repeat, Trash2 } from '@lucide/vue'
import Hint from '../components/Hint.vue'
import { fmtMs } from '../format'
import LoadingCard from '../components/LoadingCard.vue'
import Spinner from '../components/Spinner.vue'
import { computed, nextTick, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '../api'
import { C, fmt } from '../chart'
import { containerRows, marksFrom, windowRange } from '../window'
import LineChart from '../components/LineChart.vue'
import BarChart from '../components/BarChart.vue'
import LagHeatmap from '../components/LagHeatmap.vue'
import RunTimeline from '../components/RunTimeline.vue'
import RunVerdict from '../components/RunVerdict.vue'
import RunHighlights from '../components/RunHighlights.vue'
import RunNetwork from '../components/RunNetwork.vue'
import PageHeader from '../components/PageHeader.vue'
import { askConfirm } from '../confirm'
import { notify } from '../notify'
import { langName, workersText } from '../langs'

const route = useRoute()
const router = useRouter()
const run = ref<any>(null)
const err = ref('')
const picked = ref('')
const deleting = ref(false)
const zoom = ref(true)

onMounted(async () => {
  try {
    run.value = await api.run(String(route.params.id))
    const res = run.value.summary.resources || {}
    const wanted = String(route.query.container || '')
    picked.value = res[wanted] ? wanted : Object.keys(res).find((k) => res[k].role === 'consumer') || Object.keys(res)[0] || ''
  } catch (e: any) {
    err.value = e.message
  }
  await nextTick()
  scrollToHash()
})

/** Open the section named in the URL hash (the highlight cards link to them). */
function scrollToHash() {
  const id = route.hash.replace('#', '')
  if (id) document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' })
}
watch(() => [route.hash, route.query.container], async () => {
  const wanted = String(route.query.container || '')
  if (wanted && run.value?.summary?.resources?.[wanted]) picked.value = wanted
  await nextTick()
  scrollToHash()
})

async function del() {
  if (!(await askConfirm({ title: 'Delete this run?', text: 'Its report and all its measurements are removed. This cannot be undone.', okLabel: 'Delete run', danger: true }))) return
  deleting.value = true
  try {
    await api.deleteRun(String(route.params.id))
    notify('Run deleted')
    router.push('/runs')
  } catch (e: any) {
    notify(e.message, 'error')
  } finally {
    deleting.value = false
  }
}

const s = computed(() => run.value?.summary)
const win = computed(() => s.value?.window ?? {})
const marks = computed(() => marksFrom(win.value))
const shade = computed(() => windowRange(win.value, 0))
const samples = computed(() => run.value?.samples ?? [])
const x = computed(() => samples.value.map((p: any) => p.t))
const throughput = computed(() => [
  { name: 'Sent / s', color: C.blue, data: samples.value.map((p: any) => p.send_rate), dashed: true },
  { name: 'Processed / s', color: C.amber, data: samples.value.map((p: any) => p.recv_rate) },
])
const latency = computed(() => [
  { name: 'p50', color: C.green, data: samples.value.map((p: any) => p.p50) },
  { name: 'p95', color: C.amber, data: samples.value.map((p: any) => p.p95) },
  { name: 'p99', color: C.red, data: samples.value.map((p: any) => p.p99) },
])
const lag = computed(() => [{ name: 'Messages waiting', color: C.violet, data: samples.value.map((p: any) => p.lag_total) }])
const histLabels = computed(() => {
  const e: number[] = s.value?.latency_hist?.edges ?? []
  return [...e.map((v) => `≤${v}`), `>${e[e.length - 1]}`]
})
const histCounts = computed<number[]>(() => s.value?.latency_hist?.counts ?? [])

const resList = computed(() => Object.entries(s.value?.resources ?? {}).map(([name, r]: any) => ({ name, ...r })))
const short = (n: string) => n.replace('queuelab-', '')
const range = computed(() => (zoom.value ? windowRange(win.value) : null))
const rows = computed(() => containerRows(run.value?.container_samples ?? [], picked.value, range.value))
const cx = computed(() => rows.value.map((r) => r.t))
const ccpu = computed(() => [{ name: 'CPU % of one core', color: C.amber, data: rows.value.map((r) => r.c.cpu) }])
const cmem = computed(() => [
  { name: 'Memory MB', color: C.blue, data: rows.value.map((r) => r.c.mem_mb) },
  { name: 'Limit MB', color: C.red, data: rows.value.map((r) => r.c.limit_mb), dashed: true },
])
const cnet = computed(() => [
  { name: 'In KB/s', color: C.green, data: rows.value.map((r) => r.c.rx_kbs) },
  { name: 'Out KB/s', color: C.violet, data: rows.value.map((r) => r.c.tx_kbs) },
])
const cmsg = computed(() => [{ name: 'Messages / s', color: C.blue, data: rows.value.map((r) => r.c.msgs_s) }])
const xr = computed(() => (range.value ? { xmin: range.value[0], xmax: range.value[1] } : {}))
const dist = computed(() => s.value?.distribution ?? [])
const peaks = computed<number[]>(() => s.value?.partition_lag_peaks ?? [])
const peakMax = computed(() => Math.max(1, ...peaks.value))
</script>

<template>
  <div class="flex items-start justify-between gap-3 flex-wrap">
    <PageHeader
      :title="'Run report ' + $route.params.id"
      subtitle="Everything about one finished test: how fast it was, what it cost, and what to look at. The cards below open the statistics behind each number."
      :crumbs="[{ label: 'Dashboard', to: '/' }, { label: 'Runs', to: '/runs' }, { label: 'Report' }]"
    />
    <div class="flex items-center gap-2">
      <span v-if="run" class="badge" :class="run.status === 'completed' ? 'ok' : 'warn'">{{ run.status }}</span>
      <router-link v-if="run" :to="{ path: '/setup', query: { like: String($route.params.id) } }" class="btn inline-flex items-center gap-1.5"><Repeat :size="14" />Run again</router-link>
      <button class="btn btn-danger inline-flex items-center gap-1.5" v-if="run" :disabled="deleting" @click="del"><Spinner v-if="deleting" :size="14" /><Trash2 v-else :size="14" />Delete this run</button>
    </div>
  </div>
  <div v-if="err" class="card text-red-300">{{ err }}</div>
  <LoadingCard v-else-if="!run" text="Loading the report…" />

  <template v-if="run && s">
    <div class="lbl">
      {{ run.config.queue }} · {{ run.config.partitions }} partitions · {{ workersText(run.config.workers, run.config.scenario.type) }}, {{ run.config.workers.memory_mb }} MB each ·
      {{ run.config.producer.count }} × {{ langName(run.config.producer.lang) }} sender{{ run.config.producer.count > 1 ? 's' : '' }} ·
      {{ fmt(run.config.messages) }} messages × {{ run.config.message_size }} B{{ run.config.rate ? ' at ' + run.config.rate.toLocaleString() + ' msg/s' : ' at full speed' }} ·
      {{ run.config.compression }} compression, {{ run.config.payload }} data · scenario {{ run.config.scenario.type }} {{ run.config.scenario.param || '' }}
    </div>

    <RunTimeline :win="win" :sent="s.sent" :received="s.received - s.duplicates" />
    <RunHighlights :highlights="s.highlights" :run-id="String(route.params.id)" />
    <RunVerdict :insights="s.insights" />
    <RunNetwork :network="s.network" />

    <div class="grid gap-2.5" style="grid-template-columns: repeat(auto-fit, minmax(150px, 1fr))">
      <div class="tile"><div class="lbl">Avg processed / s</div><div class="val">{{ fmt(s.avg_recv_rate) }}</div></div>
      <div class="tile"><div class="lbl">Peak processed / s</div><div class="val">{{ fmt(s.peak_recv_rate) }}</div></div>
      <div class="tile"><div class="lbl">Latency p50 / p90 / p99</div><div class="val" style="font-size: 18px">{{ fmtMs(s.p50) }} / {{ fmtMs(s.p90) }} / {{ fmtMs(s.p99) }}</div></div>
      <div class="tile"><div class="lbl">Most messages waiting</div><div class="val">{{ fmt(s.peak_lag) }}</div></div>
      <div class="tile"><div class="lbl">Lost / duplicates</div><div class="val">{{ s.lost }} / {{ s.duplicates }}</div></div>
    </div>

    <div class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(360px, 1fr))">
      <div class="card" id="throughput"><h2 class="mb-1">Throughput</h2><LineChart title="Throughput" :x="x" :series="throughput" :marks="marks" :shade="shade" area /></div>
      <div class="card" id="latency"><h2 class="mb-1">Latency over time</h2><LineChart :x="x" :series="latency" :marks="marks" latency title="Latency over time" /></div>
      <div class="card">
        <h2 class="mb-1">Latency distribution</h2>
        <div class="lbl">How many messages fell into each latency bucket (ms)</div>
        <BarChart :labels="histLabels" :counts="histCounts" />
      </div>
      <div class="card" id="lag"><h2 class="mb-1">Messages waiting</h2><LineChart :x="x" :series="lag" :marks="marks" area title="Messages waiting" /></div>
    </div>
    <div class="card"><h2 class="mb-1">Messages waiting per partition<Hint text="Also called lag: the number of messages that are in the queue but not yet taken by a worker. Each row is one partition (a slice of the queue) and time runs from left to right. Dark blue: nothing is waiting. Yellow to red: a growing backlog on that partition. If only some rows turn red, the work is spread unevenly between the partitions." /></h2><div class="lbl mb-2">Each row is a partition, time runs left to right. Dark = nothing waiting, orange and red = messages are piling up.</div><LagHeatmap :rows="samples.map((p: any) => p.lag)" :partitions="run.config.partitions" /></div>

    <div class="card" id="resources">
      <div class="flex items-center justify-between mb-3 flex-wrap gap-2">
        <h2>Resources inside the message window</h2>
        <span class="lbl">Only the time between the first message sent and the last one processed. Click a container to zoom in.</span>
      </div>
      <div class="grid gap-3" style="grid-template-columns: repeat(auto-fill, minmax(240px, 1fr))">
        <button
          v-for="r in resList"
          :key="r.name"
          type="button"
          class="text-left"
          :style="{ background: '#1f1f23', border: '1px solid ' + (picked === r.name ? '#3b82f6' : '#3f3f46'), borderRadius: '10px', padding: '12px', cursor: 'pointer', color: 'inherit' }"
          @click="picked = r.name"
        >
          <div class="flex items-center justify-between mb-2">
            <b>{{ short(r.name) }}</b>
            <span class="badge" :class="r.oom ? 'bad' : ''">{{ r.role }}{{ r.lang ? ' · ' + r.lang : '' }}</span>
          </div>
          <div class="lbl">CPU: avg {{ r.cpu_avg }}% · peak {{ r.cpu_peak }}%</div>
          <div class="bar mb-2"><i :style="{ width: Math.min(100, r.cpu_avg) + '%', background: r.cpu_avg >= 80 ? '#f59e0b' : '' }" /></div>
          <div class="lbl">Memory: avg {{ r.mem_avg_mb }} · peak {{ r.mem_peak_mb }} of {{ r.limit_mb }} MB</div>
          <div class="bar mb-2"><i :style="{ width: Math.min(100, r.limit_mb ? (r.mem_peak_mb / r.limit_mb) * 100 : 0) + '%', background: r.limit_mb && r.mem_peak_mb / r.limit_mb > 0.9 ? '#f59e0b' : '#8b5cf6' }" /></div>
          <div class="lbl">Network: {{ r.net_in_mb }} MB in · {{ r.net_out_mb }} MB out</div>
          <div class="lbl">CPU time: {{ r.cpu_seconds }} s<span v-if="r.cpu_ms_per_1k != null"> · {{ r.cpu_ms_per_1k }} ms CPU per 1k messages</span></div>
          <div class="lbl" v-if="r.mem_headroom_mb != null">Memory headroom: {{ r.mem_headroom_mb }} MB</div>
        </button>
      </div>
    </div>

    <div v-if="picked" class="card">
      <div class="flex items-center justify-between mb-2 flex-wrap gap-2">
        <h2>{{ short(picked) }}</h2>
        <div class="flex gap-2">
          <button class="btn" :class="zoom ? 'btn-primary' : ''" @click="zoom = true">Message window</button>
          <button class="btn" :class="!zoom ? 'btn-primary' : ''" @click="zoom = false">Whole run</button>
        </div>
      </div>
      <div v-if="!rows.length" class="lbl">No samples for this container in the selected range.</div>
      <div v-else class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(340px, 1fr))">
        <div><div class="lbl mb-1">CPU</div><LineChart title="CPU" :x="cx" :series="ccpu" :marks="marks" v-bind="xr" area /></div>
        <div><div class="lbl mb-1">Memory against limit</div><LineChart title="Memory against limit" :x="cx" :series="cmem" :marks="marks" v-bind="xr" /></div>
        <div><div class="lbl mb-1">Network</div><LineChart title="Network" :x="cx" :series="cnet" :marks="marks" v-bind="xr" /></div>
        <div><div class="lbl mb-1">Messages per second</div><LineChart title="Messages per second" :x="cx" :series="cmsg" :marks="marks" v-bind="xr" area /></div>
      </div>
    </div>

    <div class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(360px, 1fr))">
      <div class="card" id="distribution">
        <h2 class="mb-2">Who processed what</h2>
        <table class="tbl">
          <thead><tr><th>Worker</th><th>Processed</th><th style="width: 30%">Share</th><th>Partitions</th><th>Avg / s</th></tr></thead>
          <tbody>
            <tr v-for="d in dist" :key="d.name">
              <td>{{ short(d.name) }}</td>
              <td>{{ fmt(d.processed) }}</td>
              <td><div class="bar"><i :style="{ width: d.share + '%' }" /></div><span class="lbl">{{ d.share }}%</span></td>
              <td class="lbl">{{ d.partitions.length ? d.partitions.join(', ') : 'idle' }}</td>
              <td>{{ d.avg_rate != null ? fmt(d.avg_rate) : '-' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="card">
        <h2 class="mb-2">Most messages waiting per partition</h2>
        <div v-for="(v, i) in peaks" :key="i" class="flex items-center gap-3 mb-1.5">
          <span class="lbl" style="width: 28px">P{{ i }}</span>
          <div class="bar" style="flex: 1"><i :style="{ width: (v / peakMax) * 100 + '%', background: v === peakMax && peakMax > 1 ? '#f59e0b' : '' }" /></div>
          <span class="lbl" style="width: 60px; text-align: right">{{ fmt(v) }}</span>
        </div>
      </div>
    </div>
  </template>
</template>
