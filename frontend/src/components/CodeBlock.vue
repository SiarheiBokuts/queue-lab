<script setup lang="ts">
import { computed, ref } from 'vue'

const props = defineProps<{ code: string; title?: string }>()
const lines = computed(() => props.code.split('\n'))
const copied = ref(false)

async function copy() {
  try {
    await navigator.clipboard.writeText(props.code)
    copied.value = true
    setTimeout(() => (copied.value = false), 1500)
  } catch {
    /* clipboard not available */
  }
}
</script>

<template>
  <div style="background: #0f0f12; border: 1px solid #27272a; border-radius: 10px; overflow: hidden">
    <div class="flex items-center justify-between px-3 py-1.5" style="border-bottom: 1px solid #27272a">
      <span class="lbl mono">{{ title }}</span>
      <button class="btn" style="padding: 2px 10px; font-size: 12px" @click="copy">{{ copied ? 'Copied' : 'Copy' }}</button>
    </div>
    <div class="overflow-auto" style="max-height: 460px">
      <table class="mono" style="font-size: 12px; line-height: 1.55; border-collapse: collapse; width: 100%">
        <tbody>
          <tr v-for="(l, i) in lines" :key="i">
            <td style="color: #52525b; text-align: right; padding: 0 10px; user-select: none; width: 1%; vertical-align: top">{{ i + 1 }}</td>
            <td style="white-space: pre; padding-right: 12px; color: #d4d4d8">{{ l }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
