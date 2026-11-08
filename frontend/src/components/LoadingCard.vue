<script setup lang="ts">
/** Shown in place of a page's content while its data is on the way, so the page never looks empty or broken. */
import Spinner from './Spinner.vue'

withDefaults(defineProps<{ text?: string }>(), { text: 'Loading…' })
</script>

<template>
  <div class="card flex items-center gap-3" style="padding: 28px 20px; justify-content: center" role="status">
    <Spinner :size="20" />
    <span class="lbl">{{ text }}</span>
  </div>
</template>
