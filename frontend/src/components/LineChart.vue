<script setup lang="ts">
/**
 * A line chart. The button in the corner opens a big view of the same chart: zoom with the wheel or the slider, switch
 * series on and off by clicking the legend, save the picture. Escape or a click outside closes it.
 */
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import VChart from 'vue-echarts'
import { Maximize2, X } from '@lucide/vue'
import { lineOption, type Series, type Mark } from '../chart'

const props = withDefaults(
  defineProps<{
    x: number[]
    series: Series[]
    height?: number
    area?: boolean
    max?: number
    marks?: Mark[]
    shade?: [number, number] | null
    xmin?: number
    xmax?: number
    latency?: boolean
    /** A name for the big view, for example "Latency". */
    title?: string
  }>(),
  { height: 210 },
)
const open = ref(false)
const base = () => ({ area: props.area, max: props.max, marks: props.marks, shade: props.shade, xmin: props.xmin, xmax: props.xmax, latency: props.latency })
const option = computed(() => lineOption(props.x, props.series, base()))
const big = computed(() => lineOption(props.x, props.series, { ...base(), zoom: true }))

function onKey(e: KeyboardEvent) { if (e.key === 'Escape') open.value = false }
onMounted(() => window.addEventListener('keydown', onKey))
onBeforeUnmount(() => window.removeEventListener('keydown', onKey))
</script>

<template>
  <div class="chart-box">
    <button type="button" class="chart-expand" title="Open a big view: zoom, save as image" aria-label="Open a big view" @click="open = true"><Maximize2 :size="14" /></button>
    <VChart :option="option" autoresize :style="{ height: height + 'px', width: '100%' }" />
  </div>
  <Teleport to="body">
    <div v-if="open" class="chart-modal" role="dialog" aria-modal="true" @click.self="open = false">
      <div class="chart-modal-card">
        <div class="flex items-center justify-between mb-2">
          <b>{{ title || 'Chart' }}</b>
          <span class="lbl">Wheel or the slider to zoom · click a name in the legend to hide a line · Esc to close</span>
          <button type="button" class="chart-close" aria-label="Close" @click="open = false"><X :size="16" /></button>
        </div>
        <VChart :option="big" autoresize style="height: min(72vh, 720px); width: 100%" />
      </div>
    </div>
  </Teleport>
</template>

<style scoped>
.chart-box { position: relative; }
.chart-expand { position: absolute; top: -2px; right: 0; z-index: 2; width: 26px; height: 26px; display: grid; place-items: center; border-radius: 7px; border: 1px solid #3f3f46; background: #1f1f23; color: #a1a1aa; cursor: pointer; opacity: 0; transition: opacity 0.15s, color 0.15s; }
.chart-box:hover .chart-expand, .chart-expand:focus-visible { opacity: 1; }
.chart-expand:hover { color: #fff; border-color: #52525b; }
.chart-modal { position: fixed; inset: 0; z-index: 60; display: grid; place-items: center; background: rgba(0, 0, 0, 0.66); backdrop-filter: blur(3px); padding: 24px; }
.chart-modal-card { width: min(1400px, 100%); background: #18181b; border: 1px solid #3f3f46; border-radius: 14px; padding: 16px 18px 12px; box-shadow: 0 30px 80px -20px rgba(0, 0, 0, 0.9); animation: pop 0.18s ease; }
.chart-close { width: 28px; height: 28px; display: grid; place-items: center; border-radius: 8px; border: 1px solid #3f3f46; background: #1f1f23; color: #d4d4d8; cursor: pointer; }
@keyframes pop { from { opacity: 0; transform: scale(0.97); } to { opacity: 1; transform: none; } }
</style>
