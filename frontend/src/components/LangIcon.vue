<script setup lang="ts">
/** The logo of a worker language in its own colour, so a language is recognised at a glance. */
import { computed } from 'vue'
import { LANGS } from '../langs'

const props = withDefaults(defineProps<{ id: string; size?: number }>(), { size: 16 })
const lang = computed(() => LANGS.find((l) => l.id === props.id))
</script>

<template>
  <svg v-if="lang" :width="size" :height="size" viewBox="0 0 24 24" role="img" :aria-label="lang.name" style="flex: none">
    <path :d="lang.icon" :fill="lang.color" />
  </svg>
</template>
