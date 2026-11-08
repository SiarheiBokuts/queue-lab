<script setup lang="ts">
defineProps<{ insights: { level: string; text: string }[]; limit?: number }>()
const mark: Record<string, string> = { ok: '✓', info: 'i', warn: '!', bad: '✕' }
const color: Record<string, string> = { ok: '#10b981', info: '#3b82f6', warn: '#f59e0b', bad: '#ef4444' }
const bg: Record<string, string> = { ok: '#052e22', info: '#0c2547', warn: '#3a2a05', bad: '#3b0d0d' }
</script>

<template>
  <div class="card" v-if="insights && insights.length">
    <h2 class="mb-3">What happened</h2>
    <div class="flex flex-col gap-2.5">
      <div v-for="(i, k) in insights.slice(0, limit ?? 99)" :key="k" class="flex gap-3 items-start">
        <span :style="{ background: bg[i.level], color: color[i.level], width: '22px', height: '22px', borderRadius: '999px', display: 'inline-flex', alignItems: 'center', justifyContent: 'center', fontSize: '12px', fontWeight: 700, flex: 'none' }">{{ mark[i.level] }}</span>
        <span class="text-sm" style="line-height: 1.5">{{ i.text }}</span>
      </div>
    </div>
  </div>
</template>
