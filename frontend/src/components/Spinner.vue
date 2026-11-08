<script setup lang="ts">
/** A small spinning ring: put it inside a button that is working, or next to "Loading…". */
import { LoaderCircle } from '@lucide/vue'

withDefaults(defineProps<{ size?: number }>(), { size: 14 })
</script>

<template>
  <LoaderCircle :size="size" class="qa-spin" aria-label="Loading" />
</template>

<style>
.qa-spin { animation: qa-spin 0.9s linear infinite; flex: none; }
@keyframes qa-spin { to { transform: rotate(360deg); } }
@media (prefers-reduced-motion: reduce) { .qa-spin { animation-duration: 3s; } }
</style>
