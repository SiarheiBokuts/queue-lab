<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import LangIcon from '../components/LangIcon.vue'
import Hint from '../components/Hint.vue'
import { fmtMs } from '../format'
import LoadingCard from '../components/LoadingCard.vue'
import { api } from '../api'
import { C, fmt } from '../chart'
import { langColor } from '../langs'
import LineChart from '../components/LineChart.vue'
import PageHeader from '../components/PageHeader.vue'
import RunNotes from '../components/RunNotes.vue'

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
const colors = computed(() =>
  runs.value.map((r, i) => langColor(r.config.workers.lang) || [C.blue, C.amber, C.green][i % 3]),
)
const rows = computed<any[]>(() => data.value?.rows ?? [])
const setup = computed<any>(() => runs.value[0]?.config ?? {}) // the setup is the same in every run except the language

/** Time axis aligned to the first message sent, so runs of different speed line up at zero. */
const origin = (r: any) => r.summary?.window?.send_start ?? 0
function overlay(pick: (s: any) => number | null) {
  return runs.value.map((r, i) => {
    const samples = r.samples ?? []
    return {
      name: names.value[i] ?? r.id,
      color: colors.value[i] as string,
      data: samples.map(pick),
      x: samples.map((s: any) => Math.round((s.t - origin(r)) * 100) / 100),
    }
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
    return {
      name: names.value[i] ?? r.id,
      color: colors.value[i] as string,
      data: cs.map((p: any) => p.v),
      x: cs.map((p: any) => p.t),
    }
  }),
)

const LATENCY_ROWS = ['p50', 'p99', 'max_latency']
function fmtVal(row: any, v: number | null) {
  if (LATENCY_ROWS.includes(row.key)) return fmtMs(v)
  if (v === null || v === undefined) return '-'
  return row.unit === 'MB' || row.unit === 'MiB' || row.unit === 's' || row.unit === 'ms' || row.unit.includes('%')
    ? String(v)
    : fmt(v)
}
</script>

<template>
  <PageHeader
    :title="names.length ? names.join(' vs ') : 'Comparison'"
    subtitle="The same test run in different languages, measured the same way. Green marks the best value in each row. These are numbers, not grades."
    :crumbs="[{ label: 'Dashboard', to: '/' }, { label: 'Runs', to: '/runs' }, { label: 'Comparison' }]"
  />
  <div v-if="err" class="card text-red-300">{{ err }}</div>
  <LoadingCard v-else-if="!data" text="Comparing the runs…" />

  <template v-if="data">
    <div class="card" :style="{ borderColor: data.fair ? '#065f46' : '#78350f' }">
      <div class="flex items-center gap-2 mb-1">
        <span class="badge" :class="data.fair ? 'ok' : 'warn'">{{
          data.fair ? 'Fair comparison' : 'Not directly comparable'
        }}</span>
        <span class="lbl">{{
          data.fair ? 'Only the language differs.' : 'The setups differ in more than the language.'
        }}</span>
      </div>
      <div class="flex flex-wrap gap-2 mt-2" v-if="data.differences.length">
        <span v-for="d in data.differences" :key="d.field" class="badge" :class="d.expected ? 'info' : 'warn'"
          >{{ d.field }}: {{ d.values.join(' · ') }}</span
        >
      </div>
    </div>

    <RunNotes :notes="data.notes" />

    <div class="card" v-if="runs.length">
      <h2 class="mb-1">Setup of the runs</h2>
      <p class="lbl mb-2">
        The numbers below only hold for this setup. Another scenario, message size, batch size or machine can give
        another picture.
      </p>
      <div class="lbl">
        scenario
        <b class="text-zinc-200"
          >{{ setup.scenario.type }}{{ setup.scenario.param ? ' ' + setup.scenario.param : '' }}</b
        >
        · <b class="text-zinc-200">{{ fmt(setup.messages) }}</b> messages × {{ setup.message_size }} B{{
          setup.rate ? ' at ' + setup.rate.toLocaleString() + ' msg/s' : ' at full speed'
        }}
        · {{ setup.partitions }} partitions · {{ setup.workers.count }} workers, batch of
        {{ setup.workers.concurrency }}, {{ setup.workers.memory_mb }} MB each ·
        {{ setup.users.toLocaleString() }} users · {{ setup.compression }} compression, {{ setup.payload }} data
        <template v-if="runs[0].summary?.cpu"> · {{ runs[0].summary.cpu.cpus }} cores per container</template>
      </div>
    </div>
    <div class="card">
      <h2 class="mb-3">All numbers side by side</h2>
      <div style="overflow-x: auto">
        <table class="tbl">
          <thead>
            <tr>
              <th>Measure</th>
              <th v-for="(n, i) in names" :key="n" :style="{ color: colors[i] }">
                <span class="inline-flex items-center gap-1.5"
                  ><LangIcon :id="data.runs[i].config.workers.lang" :size="14" />{{ n }}</span
                >
              </th>
              <th>Unit</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in rows" :key="row.key">
              <td>{{ row.label }}<Hint :text="row.hint" /></td>
              <td
                v-for="(v, i) in row.values"
                :key="i"
                :style="{
                  background: row.winners?.includes(i) ? 'rgba(16,185,129,0.14)' : '',
                  fontWeight: row.winners?.includes(i) ? 600 : 400,
                }"
              >
                {{ fmtVal(row, v) }}
                <span v-if="row.ratios[i] && !row.winners?.includes(i)" class="lbl">
                  {{ row.capped?.[i] ? 'at least ' : '' }}{{ row.ratios[i] }}x the best</span
                >
                <span v-else-if="row.winners?.includes(i) && row.ratios.filter((r: any) => r).length" class="lbl">
                  best</span
                >
              </td>
              <td class="lbl">{{ LATENCY_ROWS.includes(row.key) ? 'time' : row.unit }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div
      class="grid gap-4"
      style="grid-template-columns: repeat(auto-fit, minmax(min(100%, 520px), 1fr))"
      v-if="runs.length"
    >
      <div class="card">
        <h2 class="mb-1">Messages processed per second</h2>
        <div class="lbl">Time from the first message sent, in seconds</div>
        <LineChart :x="[]" :series="processed" />
      </div>
      <div class="card">
        <h2 class="mb-1">Latency p99</h2>
        <LineChart :x="[]" :series="latency" latency title="Latency p99" />
      </div>
      <div class="card">
        <h2 class="mb-1">Messages waiting (lag)</h2>
        <LineChart :x="[]" :series="lag" title="Messages waiting (lag)" />
      </div>
      <div class="card">
        <h2 class="mb-1">Worker CPU, average (% of a core)</h2>
        <LineChart title="Worker CPU, average (% of a core)" :x="[]" :series="workerCpu" />
      </div>
    </div>
  </template>
</template>
