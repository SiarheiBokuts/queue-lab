<script setup lang="ts">
/** Pick a language with its logo. Hover shows the Kafka client library that language uses. */
import { LANGS } from '../langs'
import LangIcon from './LangIcon.vue'

defineProps<{ modelValue: string }>()
defineEmits<{ 'update:modelValue': [id: string] }>()
</script>

<template>
  <div class="flex gap-2 flex-wrap">
    <button
      v-for="l in LANGS"
      :key="l.id"
      type="button"
      class="lang-pick"
      :class="{ on: modelValue === l.id }"
      :style="modelValue === l.id ? { borderColor: l.color } : {}"
      :title="'Kafka client: ' + l.client"
      @click="$emit('update:modelValue', l.id)"
    >
      <LangIcon :id="l.id" :size="16" />{{ l.name }}
    </button>
  </div>
</template>

<style scoped>
.lang-pick { display: inline-flex; align-items: center; gap: 7px; padding: 6px 11px; border-radius: 8px; background: #1f1f23; border: 1px solid #3f3f46; color: #d4d4d8; font-size: 13px; cursor: pointer; }
.lang-pick:hover { background: #27272a; }
.lang-pick.on { background: #1a1a1d; color: #fff; }
</style>
