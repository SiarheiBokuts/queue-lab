<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { api } from '../api'
import { fmt } from '../chart'
import { fmtMs } from '../format'
import { langName } from '../langs'
import { askConfirm } from '../confirm'
import { notify } from '../notify'
import PageHeader from '../components/PageHeader.vue'
import LoadingCard from '../components/LoadingCard.vue'
import LangIcon from '../components/LangIcon.vue'

const router = useRouter()
const runs = ref<any[]>([])
const picked = ref<string[]>([])

const loading = ref(true)
async function load() {
  try {
    runs.value = await api.runs()
  } finally {
    loading.value = false
  }
}
onMounted(load)

function toggle(id: string) {
  picked.value = picked.value.includes(id) ? picked.value.filter((x) => x !== id) : [...picked.value, id]
}
async function del(id: string) {
  if (!(await askConfirm({ title: 'Delete this run?', text: 'Its report and all its measurements are removed. This cannot be undone.', okLabel: 'Delete run', danger: true }))) return
  try {
    await api.deleteRun(id)
    picked.value = picked.value.filter((x) => x !== id)
    await load()
    notify('Run deleted')
  } catch (e: any) {
    notify(e.message, 'error')
  }
}
async function delAll() {
  if (!(await askConfirm({ title: 'Delete the whole run history?', text: `All ${runs.value.length} runs and their reports are removed. This cannot be undone.`, okLabel: 'Delete everything', danger: true }))) return
  try {
    await api.deleteRuns()
    picked.value = []
    await load()
    notify('Run history deleted')
  } catch (e: any) {
    notify(e.message, 'error')
  }
}
function openCompare(ids: string[]) {
  router.push('/compare/' + ids.join(','))
}

const when = (t: number) => new Date(t * 1000).toLocaleString()
/** Comparisons started with "Compare languages": runs sharing a group id, oldest first. */
const groups = computed(() => {
  const map = new Map<string, any[]>()
  for (const r of runs.value) {
    const g = r.config.group
    if (g) map.set(g, [...(map.get(g) ?? []), r])
  }
  return [...map.entries()].map(([id, list]) => ({ id, list: [...list].sort((a, b) => a.created - b.created) })).filter((g) => g.list.length >= 2)
})
const label = (r: any) => `${langName(r.config.workers.lang)}${r.config.producer.lang !== r.config.workers.lang ? ' + ' + langName(r.config.producer.lang) + ' sender' : ''}`
</script>

<template>
  <div class="flex items-start justify-between gap-3 flex-wrap">
    <PageHeader
      title="Runs"
      subtitle="Every finished test. Open one for its report, select two or more to compare them, or delete the ones you no longer need."
      :crumbs="[{ label: 'Dashboard', to: '/' }, { label: 'Runs' }]"
    />
    <button class="btn btn-danger" v-if="runs.length" @click="delAll">Delete all</button>
  </div>

  <div class="card" v-if="groups.length">
    <h2 class="mb-2">Language comparisons</h2>
    <div class="flex flex-col gap-2">
      <div v-for="g in groups" :key="g.id" class="flex items-center justify-between gap-3 flex-wrap" style="border-top: 1px solid #27272a; padding-top: 8px">
        <div>
          <span class="inline-flex items-center gap-1.5 flex-wrap"><template v-for="(r, k) in g.list" :key="r.id"><span v-if="k" class="lbl">→</span><b class="inline-flex items-center gap-1"><LangIcon :id="r.config.workers.lang" :size="14" />{{ langName(r.config.workers.lang) }}</b></template></span>
          <span class="lbl"> · {{ fmt(g.list[0].config.messages) }} messages · {{ g.list[0].config.partitions }} partitions · {{ when(g.list[0].created) }}</span>
        </div>
        <button class="btn btn-primary" @click="openCompare(g.list.map((r) => r.id))">Open the comparison</button>
      </div>
    </div>
  </div>

  <LoadingCard v-if="loading" text="Loading the runs…" />
  <div class="card" v-else-if="runs.length">
    <div class="flex items-center justify-between mb-2 flex-wrap gap-2">
      <div class="lbl">Select two or more runs to compare them side by side.</div>
      <button class="btn btn-primary" :disabled="picked.length < 2" @click="openCompare(picked)">Compare selected ({{ picked.length }})</button>
    </div>
    <table class="tbl">
      <thead><tr><th></th><th>Started</th><th>Setup</th><th>Status</th><th>Avg / s</th><th>p99</th><th>Lost</th><th></th></tr></thead>
      <tbody>
        <tr v-for="r in runs" :key="r.id">
          <td><input type="checkbox" :checked="picked.includes(r.id)" @change="toggle(r.id)" /></td>
          <td>{{ when(r.created) }}</td>
          <td class="lbl">
            <b class="text-zinc-200 inline-flex items-center gap-1.5"><LangIcon :id="r.config.workers.lang" :size="14" />{{ label(r) }}</b> · {{ r.config.queue }} · {{ r.config.partitions }}p · {{ r.config.workers.count }} workers · {{ fmt(r.config.messages) }} msgs{{ r.config.rate ? ' @ ' + fmt(r.config.rate) + '/s' : '' }} · {{ r.config.scenario.type }}
          </td>
          <td><span class="badge" :class="r.status === 'completed' ? 'ok' : r.status === 'failed' ? 'bad' : 'warn'">{{ r.status }}</span></td>
          <td>{{ fmt(r.summary.avg_recv_rate) }}</td>
          <td>{{ fmtMs(r.summary.p99) }}</td>
          <td>{{ r.summary.lost }}</td>
          <td class="whitespace-nowrap">
            <router-link :to="'/runs/' + r.id" class="text-blue-400">Open</router-link> ·
            <a href="#" class="text-zinc-400 hover:text-red-300" @click.prevent="del(r.id)">Delete</a>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
  <div class="card text-center" style="padding: 40px 16px" v-else>
    <h2 class="mb-1">No runs yet</h2>
    <p class="lbl mb-3">Finished tests appear here, with a report for each one.</p>
    <router-link to="/setup" class="btn btn-primary">Create a run</router-link>
  </div>
</template>
