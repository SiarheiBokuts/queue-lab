<script setup lang="ts">
/** Plain-language indicators for a running test: progress, time left, and what limits the speed. Three fixed rows, so the card never jumps. */
import { computed, ref, watch } from 'vue'
import { AlertTriangle, CheckCircle2, Info, OctagonAlert } from '@lucide/vue'
import { duration, liveIndicators } from '../health'

const props = defineProps<{ run: any; history: any[]; containers: any[] }>()
const active = computed(() => ['running', 'pending'].includes(props.run?.status))
const fresh = computed(() => liveIndicators(props.history, props.containers, props.run?.config?.workers?.count ?? 0))
// When there is briefly not enough data (a new run, a gap in the samples) keep showing the last texts instead of blanks.
const lastShown = ref<ReturnType<typeof liveIndicators>>([])
// The numbers in the texts change every half second; showing every change would make the card flicker, so it is
// refreshed at most once per REFRESH_MS.
const REFRESH_MS = 1500
let shownAt = 0
watch(fresh, (v) => {
  if (!v.length) return
  const now = Date.now()
  if (lastShown.value.length && now - shownAt < REFRESH_MS) return
  lastShown.value = v
  shownAt = now
}, { immediate: true })
watch(() => props.run?.id, () => { lastShown.value = []; shownAt = 0 }) // a different run starts from scratch
const items = computed(() => (fresh.value.length ? fresh.value : lastShown.value))
const progress = computed(() => {
  const total = props.run?.config?.messages ?? 0
  return total ? Math.min(100, ((props.run?.totals?.unique ?? 0) / total) * 100) : 0
})
const elapsed = computed(() => (props.history.length ? duration(props.history[props.history.length - 1].t) : '-'))
const icon = { ok: CheckCircle2, info: Info, warn: AlertTriangle, bad: OctagonAlert }
const color = { ok: '#34d399', info: '#60a5fa', warn: '#fbbf24', bad: '#f87171' }
const placeholder = ['Time to catch up', 'Backlog', 'What limits the speed']
</script>

<template>
  <div v-if="active" class="card">
    <div class="flex items-center justify-between flex-wrap gap-2 mb-2">
      <h2>How it is going</h2>
      <span class="lbl">{{ progress.toFixed(1) }}% processed · running for {{ elapsed }}</span>
    </div>
    <div class="bar mb-3"><i :style="{ width: Math.max(1, progress) + '%', background: '#3b82f6' }" /></div>
    <!-- three rows of a fixed height: the text changes, the space does not -->
    <div v-for="(slot, k) in placeholder" :key="slot" class="flex gap-2.5 items-start health-row">
      <template v-if="items[k]">
        <component :is="icon[items[k]!.level]" :size="18" :color="color[items[k]!.level]" style="flex: none; margin-top: 2px" />
        <div style="min-width: 0"><b>{{ items[k]!.title }}</b><div class="lbl health-text">{{ items[k]!.text }}</div></div>
      </template>
      <template v-else>
        <Info :size="18" color="#52525b" style="flex: none; margin-top: 2px" />
        <div><b class="text-zinc-500">{{ slot }}</b><div class="lbl">Collecting data…</div></div>
      </template>
    </div>
  </div>
</template>

<style scoped>
.health-row { height: 58px; overflow: hidden; }
.health-text { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
</style>
