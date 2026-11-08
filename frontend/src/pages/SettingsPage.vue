<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { api } from '../api'
import { useLive } from '../stores/live'
import { askConfirm } from '../confirm'
import { notify } from '../notify'
import PageHeader from '../components/PageHeader.vue'

const live = useLive()
const form = reactive({ budget_mb: 2048, light_mode: false, remove_on_stop: true })

onMounted(async () => {
  await live.refresh()
  Object.assign(form, live.settings)
})
watch(() => live.settings, (s) => Object.assign(form, s))

async function save() {
  try {
    await api.settings(form)
    await live.refresh()
    notify('Settings saved')
  } catch (e: any) {
    notify(e.message, 'error')
  }
}
const labCpu = computed(() => Math.round(live.containers.reduce((sum: number, c: any) => sum + (c.cpu || 0), 0)))
const labCpuShare = computed(() => (live.host.cpus ? Math.round(labCpu.value / live.host.cpus) : 0))
const suggest = (p: number) => Math.max(512, Math.floor((live.host.available_mb * p) / 100 / 64) * 64)

async function delQueue(name: string) {
  if (!(await askConfirm({ title: `Delete the queue "${name}"?`, text: 'The queue and its workers are removed from Kafka. Reports of finished runs stay.', okLabel: 'Delete queue', danger: true }))) return
  try {
    await api.deleteQueue(name)
    await live.refresh()
    notify(`Queue "${name}" deleted`)
  } catch (e: any) {
    notify(e.message, 'error')
  }
}
async function reset() {
  if (!(await askConfirm({ title: 'Remove all lab containers?', text: 'Kafka, the workers and the senders are stopped and removed, and the dashboard is cleared. Run history is kept. Kafka starts again with the next run.', okLabel: 'Remove containers', danger: true }))) return
  try {
    await api.reset()
    await live.refresh()
    notify('Lab containers removed')
  } catch (e: any) {
    notify(e.message, 'error')
  }
}
async function delRuns() {
  if (!(await askConfirm({ title: 'Delete the whole run history?', text: `All ${live.runs.length} runs and their reports are removed. This cannot be undone.`, okLabel: 'Delete everything', danger: true }))) return
  try {
    await api.deleteRuns()
    await live.refresh()
    notify('Run history deleted')
  } catch (e: any) {
    notify(e.message, 'error')
  }
}
</script>

<template>
  <PageHeader
    title="Settings"
    subtitle="What the lab may use of your computer (processor and memory), how Kafka starts, and cleanup. Nothing here changes a run that is already going."
    :crumbs="[{ label: 'Dashboard', to: '/' }, { label: 'Settings' }]"
  />

  <div class="card" style="max-width: 680px">
    <h2 class="mb-1">Processor (CPU)</h2>
    <p class="lbl mb-3">Docker can use <b class="text-zinc-200">{{ live.host.cpus || '?' }} CPU cores</b>. The lab does not limit the CPU of its containers: they share the cores with each other and with your computer, so a heavy test can slow both down.</p>
    <div class="grid gap-3" style="grid-template-columns: repeat(auto-fit, minmax(180px, 1fr))">
      <div class="tile"><div class="lbl">Lab containers use now</div><div class="val">{{ labCpu }}%</div><div class="lbl">of one core, {{ labCpuShare }}% of all Docker cores</div></div>
      <div class="tile"><div class="lbl">Cores in use</div><div class="val">{{ (labCpu / 100).toFixed(1) }} / {{ live.host.cpus || '?' }}</div><div class="lbl">100% = one core fully busy</div></div>
    </div>
    <p class="lbl mt-3">CPU numbers in the lab are percent of <b>one</b> core: 100% is one core, 400% is four cores. A worker that mostly waits (a delay, a database) uses little CPU, one that computes can use a whole core. To change the CPU available to Docker, set it in Docker Desktop (Settings, Resources) or in <span class="mono">.wslconfig</span> on Windows.</p>
  </div>

  <div class="card flex flex-col gap-4" style="max-width: 680px">
    <h2>Memory and Kafka</h2>
    <div>
      <label class="field">Memory budget for all lab containers (MB)
        <input class="input" type="number" min="512" step="64" v-model.number="form.budget_mb" />
      </label>
      <div class="lbl mt-2">
        Docker has {{ live.host.available_mb }} MB free of {{ live.host.total_mb }} MB. Quick pick:
        <a href="#" class="text-blue-400" @click.prevent="form.budget_mb = suggest(25)">25% ({{ suggest(25) }})</a> ·
        <a href="#" class="text-blue-400" @click.prevent="form.budget_mb = suggest(50)">50% ({{ suggest(50) }})</a> ·
        <a href="#" class="text-blue-400" @click.prevent="form.budget_mb = suggest(75)">75% ({{ suggest(75) }})</a>
      </div>
      <div v-if="form.budget_mb > live.host.available_mb" class="text-amber-400 text-xs mt-1">This is more than the memory available to Docker now. The real free memory is still enforced.</div>
    </div>
    <label class="flex items-start gap-3 text-sm">
      <input type="checkbox" v-model="form.light_mode" class="mt-1" />
      <span>Light Kafka mode (apache/kafka-native)<br /><span class="lbl">Starts faster and uses less memory, intended for local testing. Applies the next time the broker is created.</span></span>
    </label>
    <label class="flex items-start gap-3 text-sm">
      <input type="checkbox" v-model="form.remove_on_stop" class="mt-1" />
      <span>Remove lab containers when the app stops<br /><span class="lbl">Recommended. Otherwise they keep running after docker compose down.</span></span>
    </label>
    <div><button class="btn btn-primary" @click="save">Save settings</button></div>
  </div>

  <div class="card" style="max-width: 680px">
    <h2 class="mb-1">Queues</h2>
    <p class="lbl mb-2">A queue is a Kafka topic. A run creates it if it is missing. Deleting one removes the topic and its workers; finished reports stay.</p>
    <div v-if="!live.queues.length" class="lbl">No queues yet. They appear after the first run.</div>
    <div v-for="q in live.queues" :key="q.name" class="flex items-center justify-between py-1.5 border-t border-zinc-800">
      <span>{{ q.name }} <span class="lbl">· {{ q.partitions }} partitions</span></span>
      <button class="btn btn-danger" @click="delQueue(q.name)">Delete queue</button>
    </div>
  </div>

  <div class="card" style="max-width: 680px">
    <h2 class="mb-1">Cleanup</h2>
    <div class="flex items-center justify-between py-2">
      <span class="text-sm">Lab containers now running: <b>{{ live.containers.length }}</b><br /><span class="lbl">Project {{ live.projects.project }} · sandbox {{ live.projects.sandbox_project }}</span></span>
      <button class="btn btn-danger" :disabled="!live.containers.length" @click="reset">Remove all lab containers</button>
    </div>
    <div class="flex items-center justify-between py-2 border-t border-zinc-800">
      <span class="text-sm">Run history: <b>{{ live.runs.length }}</b> runs<br /><span class="lbl">Reports and measurements of finished tests.</span></span>
      <button class="btn btn-danger" :disabled="!live.runs.length" @click="delRuns">Delete run history</button>
    </div>
  </div>
</template>
