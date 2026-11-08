<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue'
import { answerConfirm, confirmState } from '../confirm'

function onKey(e: KeyboardEvent) {
  if (confirmState.open && e.key === 'Escape') answerConfirm(false)
}
onMounted(() => window.addEventListener('keydown', onKey))
onUnmounted(() => window.removeEventListener('keydown', onKey))
</script>

<template>
  <div
    v-if="confirmState.open"
    style="position: fixed; inset: 0; background: rgba(0, 0, 0, 0.6); display: flex; align-items: center; justify-content: center; z-index: 50"
    @click.self="answerConfirm(false)"
  >
    <div class="card" role="dialog" aria-modal="true" style="max-width: 440px; width: calc(100% - 32px); box-shadow: 0 20px 50px rgba(0, 0, 0, 0.5)">
      <h2 class="mb-2">{{ confirmState.title }}</h2>
      <p v-if="confirmState.text" class="text-sm mb-4" style="color: #d4d4d8; line-height: 1.5">{{ confirmState.text }}</p>
      <div class="flex justify-end gap-2">
        <button class="btn" @click="answerConfirm(false)">Cancel</button>
        <button class="btn" :class="confirmState.danger ? 'btn-danger' : 'btn-primary'" @click="answerConfirm(true)">{{ confirmState.okLabel }}</button>
      </div>
    </div>
  </div>
</template>