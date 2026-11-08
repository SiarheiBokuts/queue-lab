<script setup lang="ts">
import { toasts } from '../notify'
</script>

<template>
  <div style="position: fixed; right: 16px; bottom: 16px; display: flex; flex-direction: column; gap: 8px; z-index: 60">
    <div
      v-for="t in toasts"
      :key="t.id"
      class="card text-sm"
      :style="{ borderColor: t.kind === 'error' ? '#7f1d1d' : '#065f46', background: t.kind === 'error' ? '#2a1010' : '#0b2a22', maxWidth: '360px' }"
    >
      {{ t.text }}
    </div>
  </div>
</template>