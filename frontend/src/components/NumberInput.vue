<script setup lang="ts">
/** A number field that shows thousands separators (1,000,000) while you type and keeps the cursor where it was. */
import { ref, watch } from 'vue'
import { digitsToNumber, groupDigits } from '../format'

const props = defineProps<{ modelValue: number; min?: number; max?: number }>()
const emit = defineEmits<{ 'update:modelValue': [value: number] }>()

const el = ref<HTMLInputElement | null>(null)
const shown = ref(groupDigits(props.modelValue))
watch(() => props.modelValue, (v) => { if (digitsToNumber(shown.value) !== v) shown.value = groupDigits(v) })

function clamp(n: number): number {
  return Math.min(props.max ?? Infinity, Math.max(props.min ?? 0, n))
}

function onInput(event: Event) {
  const input = event.target as HTMLInputElement
  const before = input.value.slice(0, input.selectionStart ?? input.value.length)
  const digitsBefore = before.replace(/\D/g, '').length // how many digits are left of the cursor
  const value = digitsToNumber(input.value)
  shown.value = input.value === '' ? '' : groupDigits(value)
  emit('update:modelValue', value)
  requestAnimationFrame(() => {
    // put the cursor after the same digit as before, the separators moved
    let seen = 0
    let pos = 0
    while (pos < shown.value.length && seen < digitsBefore) { if (/\d/.test(shown.value[pos] as string)) seen++; pos++ }
    el.value?.setSelectionRange(pos, pos)
  })
}

function onBlur() {
  const value = clamp(digitsToNumber(shown.value))
  shown.value = groupDigits(value)
  emit('update:modelValue', value)
}
</script>

<template>
  <input ref="el" class="input" type="text" inputmode="numeric" autocomplete="off" :value="shown" @input="onInput" @blur="onBlur" />
</template>
