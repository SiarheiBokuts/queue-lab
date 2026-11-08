<script setup lang="ts">
/** A small info icon that explains a term in a tooltip (floating-vue). Keeps the screens uncluttered while nothing stays unexplained. */
import { Info } from '@lucide/vue'
import { vTooltip } from 'floating-vue'

defineProps<{ text: string }>()
</script>

<template>
  <span v-tooltip="{ content: text, placement: 'top', distance: 8 }" class="hint" tabindex="0" :aria-label="text">
    <Info :size="14" :stroke-width="2" />
  </span>
</template>

<style scoped>
.hint {
  display: inline-flex;
  vertical-align: -3px;
  margin-left: 5px;
  color: #71717a;
  cursor: help;
  transition: color 0.15s;
}
.hint:hover, .hint:focus { color: #60a5fa; outline: none; }
</style>
