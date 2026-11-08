<script setup lang="ts">
import { FileText, GitCompare, Play, RotateCcw, Square } from '@lucide/vue'
import { computed, ref } from 'vue'
import { useLive } from '../stores/live'
import { api } from '../api'
import BudgetBar from '../components/BudgetBar.vue'
import TopologyFlow from '../components/TopologyFlow.vue'
import MetricTiles from '../components/MetricTiles.vue'
import StepsList from '../components/StepsList.vue'
import ContainerTable from '../components/ContainerTable.vue'
import LineChart from '../components/LineChart.vue'
import LagHeatmap from '../components/LagHeatmap.vue'
import RunTimeline from '../components/RunTimeline.vue'
import RunVerdict from '../components/RunVerdict.vue'
import RunPhases from '../components/RunPhases.vue'
import LiveHealth from '../components/LiveHealth.vue'
import Spinner from '../components/Spinner.vue'
import Hint from '../components/Hint.vue'
import Glossary from '../components/Glossary.vue'
import LangIcon from '../components/LangIcon.vue'
import PageHeader from '../components/PageHeader.vue'
import { askConfirm } from '../confirm'
import { notify } from '../notify'
import { langName, workersText } from '../langs'
import RunHighlights from '../components/RunHighlights.vue'
import { C } from '../chart'
import { marksFrom, windowRange } from '../window'

const live = useLive()
const run = computed(() => live.run)
const err = ref('')
const x = computed(() => live.history.map((h) => h.t))
const win = computed(() => run.value?.window ?? {})
const marks = computed(() => marksFrom(win.value))
const shade = computed(() => windowRange(win.value, 0))
const throughput = computed(() => [
  { name: 'Sent / s', color: C.blue, data: live.history.map((h) => h.send), dashed: true },
  { name: 'Processed / s', color: C.amber, data: live.history.map((h) => h.recv) },
])
const latency = computed(() => [
  { name: 'p50', color: C.green, data: live.history.map((h) => h.p50) },
  { name: 'p95', color: C.amber, data: live.history.map((h) => h.p95) },
  { name: 'p99', color: C.red, data: live.history.map((h) => h.p99) },
])
const showSteps = computed(() => run.value && (run.value.status === 'pending' || run.value.status === 'failed'))
const otherLangs = computed(() => ['python', 'node', 'java', 'go', 'rust'].filter((l) => l !== run.value?.config?.workers?.lang))
const cmp = computed(() => live.comparison)
const cmpDone = computed(() => cmp.value && cmp.value.status !== 'running' && cmp.value.run_ids.length >= 2)
const finished = computed(() => run.value?.summary && ['completed', 'cancelled'].includes(run.value.status))
const empty = computed(() => !run.value && live.containers.length === 0)

const cancelling = ref(false)
const resetting = ref(false)
async function cancel() {
  err.value = ''
  cancelling.value = true
  try { await api.cancel() } catch (e: any) { err.value = e.message } finally { cancelling.value = false }
}
async function reset() {
  if (!(await askConfirm({ title: 'Reset the lab?', text: 'All lab containers (Kafka, workers, senders) are removed and this view is cleared. Run history is kept.', okLabel: 'Reset', danger: true }))) return
  resetting.value = true
  try {
    await api.reset()
    await live.refresh()
    notify('Lab reset')
  } catch (e: any) {
    notify(e.message, 'error')
  } finally {
    resetting.value = false
  }
}
</script>

<template>
  <div class="flex flex-wrap items-start justify-between gap-3">
    <PageHeader
      title="Dashboard"
      :subtitle="run ? `Queue ${run.config.queue} · ${run.config.partitions} partitions · ${workersText(run.config.workers, run.config.scenario.type)} · ${run.config.producer.count} × ${langName(run.config.producer.lang)} sender${run.config.producer.count > 1 ? 's' : ''} · scenario ${run.config.scenario.type}${run.config.rate ? ' · ' + run.config.rate.toLocaleString() + ' msg/s' : ''}` : 'The live view of the current run. Start one from New run.'"
    />
    <div class="flex gap-2 items-center">
      <span v-if="run" class="badge" :class="run.status === 'running' ? 'ok' : run.status === 'completed' ? 'info' : run.status === 'failed' ? 'bad' : 'warn'">{{ run.status }}</span>
      <button v-if="run && (run.status === 'running' || run.status === 'pending')" class="btn btn-danger inline-flex items-center gap-1.5" :disabled="cancelling" @click="cancel"><Spinner v-if="cancelling" :size="14" /><Square v-else :size="14" />Cancel run</button>
      <button v-if="live.containers.length || run" class="btn inline-flex items-center gap-1.5" :disabled="resetting" @click="reset"><Spinner v-if="resetting" :size="14" /><RotateCcw v-else :size="14" />Reset</button>
      <router-link to="/setup" class="btn btn-primary inline-flex items-center gap-1.5"><Play :size="15" />New run</router-link>
    </div>
  </div>
  <div v-if="err" class="text-red-300 text-sm">{{ err }}</div>
  <RunPhases v-if="run" :run="run" :comparison="live.comparison" />
  <div v-if="cmpDone" class="card flex items-center justify-between gap-3 flex-wrap" style="border-color: #1d4ed8">
    <div><b>Comparison {{ cmp.status === 'completed' ? 'finished' : cmp.status }}.</b> <span class="lbl">{{ cmp.langs.map(langName).join(' → ') }}</span></div>
    <router-link :to="'/compare/' + cmp.run_ids.join(',')" class="btn btn-primary inline-flex items-center gap-1.5"><GitCompare :size="15" />Open the comparison</router-link>
  </div>

  <div v-if="empty" class="card text-center" style="padding: 48px 16px">
    <h2 class="mb-1">Start your first run</h2>
    <p class="lbl mb-4">Create a Kafka queue, add workers in Python, Node.js, Java, Go or Rust, send messages and watch the real load on your machine. You can also run the same test in several languages and compare them.</p>
    <router-link to="/setup" class="btn btn-primary inline-flex items-center gap-1.5"><Play :size="15" />Create a run</router-link>
  </div>
  <Glossary v-if="empty" />

  <template v-else>
    <BudgetBar :budget="live.budget" :containers="live.containers" />
    <StepsList v-if="showSteps" :run="run" />

    <template v-if="finished">
      <RunTimeline :win="win" :sent="run.summary.sent" :received="run.summary.received - run.summary.duplicates" />
      <RunHighlights :highlights="run.summary.highlights" :run-id="run.id" />
      <RunVerdict :insights="run.summary.insights" :limit="5" />
      <div class="flex justify-end gap-2 flex-wrap">
        <router-link v-for="l in otherLangs" :key="l" :to="{ path: '/setup', query: { like: run.id, workersLang: l } }" class="btn inline-flex items-center gap-2" :title="'Same setup, with ' + langName(l) + ' workers'"><LangIcon :id="l" :size="14" />Repeat in {{ langName(l) }}</router-link>
        <router-link :to="'/runs/' + run.id" class="btn btn-primary inline-flex items-center gap-1.5"><FileText :size="15" />Open the full report</router-link>
      </div>
    </template>

    <TopologyFlow :run="run" :containers="live.containers" />
    <LiveHealth :run="run" :history="live.history" :containers="live.containers" />
    <MetricTiles :run="run" />
    <div class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(340px, 1fr))">
      <div class="card"><h2 class="mb-1">Throughput</h2><LineChart :x="x" :series="throughput" :marks="marks" :shade="shade" area title="Throughput" /></div>
      <div class="card"><h2 class="mb-1">Latency<Hint text="How long a message waited from being sent to being processed. p50 is the typical message, p99 the slow tail. The scale shows milliseconds, seconds or minutes, whatever fits. The measurement stops at 10 s, so a line at the top means 10 s or more." /></h2><LineChart :x="x" :series="latency" :marks="marks" latency title="Latency" /></div>
    </div>
    <div class="card">
      <h2 class="mb-1">Messages waiting per partition<Hint text="Also called lag: the number of messages that are in the queue but not yet taken by a worker. Each row is one partition (a slice of the queue) and time runs from left to right. Dark blue: nothing is waiting. Yellow to red: a growing backlog on that partition. If only some rows turn red, the work is spread unevenly between the partitions." /></h2>
      <div class="lbl mb-2">Each row is a partition, time runs left to right. Dark = nothing waiting, orange and red = messages are piling up.</div>
      <LagHeatmap :rows="live.lagHistory" :partitions="run?.config?.partitions ?? 0" />
    </div>
    <ContainerTable :containers="live.containers" />
  </template>
</template>
