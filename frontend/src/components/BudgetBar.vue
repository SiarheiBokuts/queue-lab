<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ budget: any; containers: any[] }>()

const parts = computed(() => {
  const sum = (roles: string[]) =>
    props.containers.filter((c) => roles.includes(c.role) && ['running', 'created', 'restarting'].includes(c.state)).reduce((a, c) => a + c.limit_mb, 0)
  return [
    { label: 'Kafka', mb: sum(['broker']), color: '#3b82f6' },
    { label: 'Producer', mb: sum(['producer']), color: '#8b5cf6' },
    { label: 'Workers', mb: sum(['consumer']), color: '#10b981' },
  ]
})
const total = computed(() => Math.max(props.budget?.budget_mb || 1, parts.value.reduce((a, p) => a + p.mb, 0)))
</script>

<template>
  <div class="card" v-if="budget">
    <div class="flex flex-wrap justify-between gap-2 mb-2">
      <span class="lbl">Memory budget</span>
      <span class="text-sm font-medium">
        {{ budget.used_mb }} / {{ budget.budget_mb }} MB used
        <span class="lbl"> · {{ budget.remaining_mb }} MB left · Docker has {{ budget.host_available_mb }} MB free of {{ budget.host_total_mb }} MB</span>
      </span>
    </div>
    <div class="flex h-2.5 rounded-full overflow-hidden gap-0.5 bg-zinc-800">
      <span v-for="p in parts" :key="p.label" :style="{ width: (p.mb / total) * 100 + '%', background: p.color }" :title="p.label + ' ' + p.mb + ' MB'" />
    </div>
    <div class="flex gap-4 mt-2 lbl flex-wrap">
      <span v-for="p in parts" :key="p.label"><b class="inline-block w-2 h-2 rounded-sm mr-1" :style="{ background: p.color }" />{{ p.label }} {{ p.mb }} MB</span>
    </div>
    <div v-if="budget.above_available" class="mt-2 text-amber-400 text-xs">
      Your budget is larger than the memory Docker has available now. The real free memory is still enforced.
    </div>
  </div>
</template>
