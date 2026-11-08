<script setup lang="ts">
/** Always visible while something is running, on every page: what is happening now and a way back to it. */
import { computed } from 'vue'
import { useLive } from '../stores/live'
import { langName, workersText } from '../langs'
import LangIcon from './LangIcon.vue'

const live = useLive()
const run = computed(() => live.run)
const cmp = computed(() => live.comparison)
const runActive = computed(() => !!run.value && ['pending', 'running'].includes(run.value.status))
const cmpActive = computed(() => cmp.value?.status === 'running')
const visible = computed(() => runActive.value || cmpActive.value)

const steps = computed<any[]>(() => run.value?.steps ?? [])
const stepIndex = computed(() => {
  const i = steps.value.findIndex((s) => s.state === 'running')
  return i < 0 ? steps.value.filter((s) => s.state === 'done').length : i
})
const stepLabel = computed(() => steps.value[stepIndex.value]?.label ?? 'Starting')
const text = computed(() => {
  if (cmpActive.value && cmp.value) {
    const lang = langName(cmp.value.langs[cmp.value.step - 1] ?? '')
    if (cmp.value.cooldown_left > 0) return `Comparison: pause ${cmp.value.cooldown_left} s before the next run`
    return `Comparison ${cmp.value.step} of ${cmp.value.total} (${lang})`
  }
  return run.value?.status === 'running' ? 'Messages are being sent and processed' : `Setting up: step ${stepIndex.value + 1} of ${steps.value.length}, ${stepLabel.value}`
})
</script>

<template>
  <router-link v-if="visible" to="/" class="card flex items-center gap-3 flex-wrap" style="border-color: #1d4ed8; background: #0c1a33; text-decoration: none">
    <span class="pulse" />
    <b>Running</b>
    <span v-if="run" class="inline-flex items-center gap-1.5"><LangIcon :id="run.config.workers.lang" :size="15" />{{ run.config.queue }} · {{ workersText(run.config.workers, run.config.scenario.type) }}</span>
    <span class="lbl">{{ text }}</span>
    <span class="ml-auto" style="color: #93c5fd">Open the live dashboard →</span>
  </router-link>
</template>

<style scoped>
.pulse {
  width: 10px;
  height: 10px;
  border-radius: 999px;
  background: #3b82f6;
  animation: pulse 1.2s ease-in-out infinite;
}
@keyframes pulse {
  0%, 100% { opacity: 1; transform: scale(1); }
  50% { opacity: 0.4; transform: scale(0.7); }
}
</style>