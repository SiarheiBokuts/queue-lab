<script setup lang="ts">
/**
 * The most interesting facts of a finished run as cards. Every card links to the statistics behind its number:
 * a section of the run report, and for resource facts the container to open there.
 */
import { computed } from 'vue'

interface Highlight {
  kind: string
  title: string
  text: string
  anchor: string
  container: string | null
}

const props = defineProps<{ highlights: Highlight[]; runId: string }>()

const labels: Record<string, string> = {
  speed: 'Fastest processing',
  send: 'Producer speed',
  latency: 'Slowest message',
  cpu: 'Highest CPU',
  memory: 'Closest to a memory limit',
  lag: 'Peak backlog',
  workers: 'Busiest worker',
}
const colors: Record<string, string> = {
  speed: '#f59e0b',
  send: '#8b5cf6',
  latency: '#ef4444',
  cpu: '#f59e0b',
  memory: '#3b82f6',
  lag: '#8b5cf6',
  workers: '#10b981',
}

const items = computed(() =>
  (props.highlights ?? []).map((h) => ({
    ...h,
    label: labels[h.kind] ?? h.kind,
    color: colors[h.kind] ?? '#3b82f6',
    to: { path: `/runs/${props.runId}`, query: h.container ? { container: h.container } : {}, hash: `#${h.anchor}` },
  })),
)
</script>

<template>
  <div v-if="items.length">
    <div class="flex items-baseline justify-between mb-2">
      <h2>Highlights</h2>
      <span class="lbl">Each card opens the statistics behind it</span>
    </div>
    <div class="grid gap-3" style="grid-template-columns: repeat(auto-fill, minmax(250px, 1fr))">
      <router-link
        v-for="h in items"
        :key="h.kind"
        :to="h.to"
        class="card block"
        :style="{ borderTop: '3px solid ' + h.color, textDecoration: 'none' }"
      >
        <div class="lbl">{{ h.label }}</div>
        <div class="val" style="margin: 4px 0 6px">{{ h.title }}</div>
        <div class="text-sm" style="color: #d4d4d8; line-height: 1.45">{{ h.text }}</div>
        <div class="mt-2 text-sm" :style="{ color: h.color }">See the statistics →</div>
      </router-link>
    </div>
  </div>
</template>
