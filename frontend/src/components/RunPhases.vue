<script setup lang="ts">
/** "You are here": the three phases of a run, with the current one highlighted. */
import { computed } from 'vue'
import { Check, ChevronRight, LoaderCircle } from '@lucide/vue'
import { langName } from '../langs'
import LangIcon from './LangIcon.vue'

const props = defineProps<{ run: any; comparison?: any }>()

const phase = computed(() => {
  const s = props.run?.status
  if (s === 'pending') return 1
  if (s === 'running') return 2
  if (s) return 3
  return 0
})
const failed = computed(() => props.run?.status === 'failed')
const items = computed(() => [
  { n: 1, title: 'Prepare', text: 'Images, Kafka, the queue, workers and the producer are started' },
  { n: 2, title: 'Send and process', text: 'The producer sends the messages, the workers handle them' },
  { n: 3, title: failed.value ? 'Failed' : 'Report', text: failed.value ? 'Something went wrong: see the details below' : 'Results, highlights and the verdict' },
])
const langs = computed<string[]>(() => props.comparison?.langs ?? [])
</script>

<template>
  <div>
    <div class="grid gap-2" style="grid-template-columns: repeat(3, minmax(0, 1fr))">
      <div
        v-for="i in items"
        :key="i.n"
        class="card"
        :style="{
          padding: '10px 14px',
          borderColor: phase === i.n ? (failed && i.n === 3 ? '#7f1d1d' : '#3b82f6') : '',
          opacity: phase < i.n ? 0.55 : 1,
        }"
      >
        <div class="flex items-center gap-2">
          <span class="badge" :class="phase > i.n ? 'ok' : phase === i.n ? (failed && i.n === 3 ? 'bad' : 'info') : ''" style="display: inline-flex; align-items: center; justify-content: center; min-width: 22px">
            <Check v-if="phase > i.n" :size="12" />
            <LoaderCircle v-else-if="phase === i.n && i.n < 3" :size="12" class="spin" />
            <template v-else>{{ i.n }}</template>
          </span>
          <b>{{ i.title }}</b>
          <span v-if="phase === i.n" class="lbl">you are here</span>
        </div>
        <div class="lbl mt-1">{{ i.text }}</div>
      </div>
    </div>
    <div v-if="comparison && comparison.status === 'running'" class="cmp-line">
      <span class="lbl">Comparison</span>
      <template v-for="(l, k) in langs" :key="l">
        <span class="cmp-chip" :class="{ done: comparison.step > k + 1, now: comparison.step === k + 1 }">
          <Check v-if="comparison.step > k + 1" :size="13" color="#34d399" />
          <LangIcon v-else :id="l" :size="14" />
          {{ langName(l) }}
          <LoaderCircle v-if="comparison.step === k + 1 && !(comparison.cooldown_left > 0)" :size="12" class="spin" />
        </span>
        <span v-if="k < langs.length - 1" class="cmp-gap">
          <ChevronRight :size="13" />
          <template v-if="comparison.step === k + 1 && comparison.cooldown_left > 0">pause {{ comparison.cooldown_left }} s<ChevronRight :size="13" /></template>
        </span>
      </template>
      <ChevronRight :size="13" class="cmp-gap" /><span class="lbl">compare</span>
    </div>
  </div>
</template>

<style scoped>
.spin { animation: spin 1s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
.cmp-line { display: flex; align-items: center; flex-wrap: wrap; gap: 6px; margin-top: 10px; }
.cmp-chip { display: inline-flex; align-items: center; gap: 6px; padding: 4px 11px; border-radius: 999px; font-size: 12px; background: #1b1b1f; border: 1px solid #323238; color: #a1a1aa; }
.cmp-chip.done { color: #d4d4d8; }
.cmp-chip.now { color: #fff; border-color: #3b82f6; background: #0c2547; box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.15); }
.cmp-gap { display: inline-flex; align-items: center; gap: 4px; font-size: 11px; color: #71717a; }
</style>