<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { Trophy } from '@lucide/vue'
import LangIcon from '../components/LangIcon.vue'
import { fmtMs } from '../format'
import LoadingCard from '../components/LoadingCard.vue'
import { api } from '../api'
import { C, fmt } from '../chart'
import { langColor } from '../langs'
import LineChart from '../components/LineChart.vue'
import PageHeader from '../components/PageHeader.vue'
import RunVerdict from '../components/RunVerdict.vue'

const route = useRoute()
const ids = computed(() => String(route.params.ids).split(',').filter(Boolean))
const data = ref<any>(null)
const runs = ref<any[]>([])
const err = ref('')

onMounted(async () => {
  try {
    data.value = await api.compare(ids.value.join(','))
    runs.value = await Promise.all(ids.value.map((id) => api.run(id)))
  } catch (e: any) {
    err.value = e.message
  }
})

const names = computed<string[]>(() => data.value?.names ?? [])
const colors = computed(() => runs.value.map((r, i) => langColor(r.config.workers.lang) || [C.blue, C.amber, C.green][i % 3]))
const rows = computed<any[]>(() => data.value?.rows ?? [])
const sc = computed<any>(() => data.value?.scores ?? { winner: null, overall: [], disqualified: [], categories: [] })
const CLEAR_LEAD = 5 // keep in sync with CLEAR_LEAD in backend/app/analysis.py

/** Time axis aligned to the first message sent, so runs of different speed line up at zero. */
const origin = (r: any) => r.summary?.window?.send_start ?? 0
function overlay(pick: (s: any) => number | null) {
  return runs.value.map((r, i) => {
    const samples = r.samples ?? []
    return { name: names.value[i] ?? r.id, color: colors.value[i] as string, data: samples.map(pick), x: samples.map((s: any) => Math.round((s.t - origin(r)) * 100) / 100) }
  })
}
const processed = computed(() => overlay((s) => s.recv_rate))
const latency = computed(() => overlay((s) => s.p99))
const lag = computed(() => overlay((s) => s.lag_total))
const workerCpu = computed(() =>
  runs.value.map((r, i) => {
    const cs = (r.container_samples ?? []).map((e: any) => {
      const consumers = (e.containers ?? []).filter((c: any) => c.role === 'consumer')
      const avg = consumers.length ? consumers.reduce((a: number, c: any) => a + c.cpu, 0) / consumers.length : null
      return { t: Math.round((e.t - origin(r)) * 100) / 100, v: avg === null ? null : Math.round(avg * 10) / 10 }
    })
    return { name: names.value[i] ?? r.id, color: colors.value[i] as string, data: cs.map((p: any) => p.v), x: cs.map((p: any) => p.t) }
  }),
)

const LATENCY_ROWS = ['p50', 'p99', 'max_latency']
function fmtVal(row: any, v: number | null) {
  if (LATENCY_ROWS.includes(row.key)) return fmtMs(v)
  if (v === null || v === undefined) return '-'
  return row.unit === 'MB' || row.unit === 'MiB' || row.unit === 's' || row.unit === 'ms' || row.unit.includes('%') ? String(v) : fmt(v)
}
</script>

<template>
  <PageHeader
    :title="names.length ? names.join(' vs ') : 'Comparison'"
    subtitle="The same test run in different languages, measured the same way. Green marks the best value in each row."
    :crumbs="[{ label: 'Dashboard', to: '/' }, { label: 'Runs', to: '/runs' }, { label: 'Comparison' }]"
  />
  <div v-if="err" class="card text-red-300">{{ err }}</div>
  <LoadingCard v-else-if="!data" text="Comparing the runs…" />

  <template v-if="data">
    <div class="card" :style="{ borderColor: data.fair ? '#065f46' : '#78350f' }">
      <div class="flex items-center gap-2 mb-1">
        <span class="badge" :class="data.fair ? 'ok' : 'warn'">{{ data.fair ? 'Fair comparison' : 'Not directly comparable' }}</span>
        <span class="lbl">{{ data.fair ? 'Only the language differs.' : 'The setups differ in more than the language.' }}</span>
      </div>
      <div class="flex flex-wrap gap-2 mt-2" v-if="data.differences.length">
        <span v-for="d in data.differences" :key="d.field" class="badge" :class="d.expected ? 'info' : 'warn'">{{ d.field }}: {{ d.values.join(' · ') }}</span>
      </div>
    </div>

    <RunVerdict :insights="data.verdict" />

    <div class="card" :style="{ borderColor: sc.winner != null ? colors[sc.winner] : undefined }">
      <div class="flex items-center gap-3 mb-3 flex-wrap">
        <Trophy :size="26" :color="sc.winner != null ? '#fbbf24' : '#71717a'" />
        <div>
          <h2 v-if="sc.winner != null">Winner: <span :style="{ color: colors[sc.winner] }">{{ names[sc.winner] }}</span></h2>
          <h2 v-else>No clear winner</h2>
          <div class="lbl">
            <template v-if="sc.winner != null">{{ names[sc.winner] }} scored {{ sc.overall[sc.winner] }} of 100, at least {{ CLEAR_LEAD }} points ahead of the next one.</template>
            <template v-else>The best scores are less than {{ CLEAR_LEAD }} points apart, or there is nothing to rank. Look at the categories below.</template>
          </div>
        </div>
      </div>
      <div v-for="(n, i) in names" :key="n" class="mb-3" :style="{ opacity: sc.disqualified[i] ? 0.55 : 1 }">
        <div class="flex items-center gap-3">
          <span class="inline-flex items-center gap-2" style="width: 120px; font-weight: 600" :style="{ color: colors[i] }"><LangIcon :id="data.runs[i].config.workers.lang" :size="16" />{{ n }}</span>
          <div class="bar" style="flex: 1; height: 12px"><i :style="{ width: (sc.overall[i] ?? 0) + '%', background: colors[i] }" /></div>
          <b style="width: 44px; text-align: right">{{ sc.overall[i] ?? '-' }}</b>
          <router-link :to="'/runs/' + data.runs[i].id" class="text-blue-400 text-sm">Report</router-link>
        </div>
        <div class="flex gap-4 flex-wrap lbl" style="margin-left: 122px; margin-top: 2px">
          <span v-for="c in sc.categories" :key="c.key">{{ c.label }} <b>{{ c.values[i] ?? '-' }}</b></span>
          <span v-if="sc.disqualified[i]" style="color: #f87171">Lost or duplicated messages: cannot win</span>
        </div>
      </div>
      <details class="lbl">
        <summary style="cursor: pointer">How the winner is decided</summary>
        <p class="mt-1">
          Every run gets 0 to 100 points per category: 100 means best (or within 5% of the best) on every metric of the category.
          Categories: {{ sc.categories.map((c: any) => c.label + ' ' + Math.round(c.weight * 100) + '%').join(', ') }}.
          A run that lost or duplicated messages cannot win. The winner needs a lead of at least {{ CLEAR_LEAD }} points, otherwise it is a draw.
          Every single number is in the table below, with the best one in green.
        </p>
      </details>
    </div>

    <div class="card">
      <h2 class="mb-3">All numbers side by side</h2>
      <div style="overflow-x: auto">
        <table class="tbl">
          <thead>
            <tr><th>Measure</th><th v-for="(n, i) in names" :key="n" :style="{ color: colors[i] }"><span class="inline-flex items-center gap-1.5"><LangIcon :id="data.runs[i].config.workers.lang" :size="14" />{{ n }}</span></th><th>Unit</th></tr>
          </thead>
          <tbody>
            <tr v-for="row in rows" :key="row.key" :title="row.hint">
              <td>{{ row.label }}</td>
              <td v-for="(v, i) in row.values" :key="i" :style="{ background: row.winners?.includes(i) ? 'rgba(16,185,129,0.14)' : '', fontWeight: row.winners?.includes(i) ? 600 : 400 }">
                {{ fmtVal(row, v) }}
                <span v-if="row.ratios[i] && !row.winners?.includes(i)" class="lbl"> {{ row.capped?.[i] ? 'at least ' : '' }}{{ row.ratios[i] }}x worse</span>
                <span v-else-if="row.winners?.includes(i) && row.ratios.filter((r: any) => r).length" class="lbl"> best</span>
              </td>
              <td class="lbl">{{ LATENCY_ROWS.includes(row.key) ? 'time' : row.unit }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(380px, 1fr))" v-if="runs.length">
      <div class="card"><h2 class="mb-1">Messages processed per second</h2><div class="lbl">Time from the first message sent, in seconds</div><LineChart :x="[]" :series="processed" /></div>
      <div class="card"><h2 class="mb-1">Latency p99</h2><LineChart :x="[]" :series="latency" latency title="Latency p99" /></div>
      <div class="card"><h2 class="mb-1">Messages waiting (lag)</h2><LineChart :x="[]" :series="lag" title="Messages waiting (lag)" /></div>
      <div class="card"><h2 class="mb-1">Worker CPU, average (% of a core)</h2><LineChart title="Worker CPU, average (% of a core)" :x="[]" :series="workerCpu" /></div>
    </div>
  </template>
</template>
