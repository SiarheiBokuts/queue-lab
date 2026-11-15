<script setup lang="ts">
/** Shown between the runs of a comparison: which language just finished and which one is next. */
import { computed } from 'vue'
import { CheckCircle2, ChevronRight, LoaderCircle } from '@lucide/vue'
import { langName } from '../langs'
import LangIcon from './LangIcon.vue'

const props = defineProps<{ comparison: { langs: string[]; step: number; total: number; cooldown_left: number } }>()

const finished = computed(() => props.comparison.langs[props.comparison.step - 1] as string)
const next = computed(() => props.comparison.langs[props.comparison.step] as string | undefined)
/** done: finished, waiting: not started yet. The one that just finished is the last done. */
const stateOf = (index: number) => (index < props.comparison.step ? 'done' : 'waiting')
</script>

<template>
  <div class="card cmp-card">
    <div class="flex items-center gap-3 flex-wrap">
      <span class="big"><LangIcon :id="finished" :size="30" /></span>
      <div>
        <div class="flex items-center gap-2">
          <CheckCircle2 :size="20" color="#34d399" />
          <h2>{{ langName(finished) }} finished</h2>
        </div>
        <div v-if="next" class="lbl mt-1 flex items-center gap-2">
          Next: <LangIcon :id="next" :size="16" /><b class="text-zinc-200">{{ langName(next) }}</b>
          <template v-if="comparison.cooldown_left > 0">in {{ comparison.cooldown_left }} s</template>
          <LoaderCircle v-else :size="14" class="spin" />
        </div>
        <div v-else class="lbl mt-1">That was the last language. The comparison opens in a moment.</div>
      </div>
    </div>

    <div class="flex items-center flex-wrap gap-1.5 mt-4">
      <template v-for="(l, i) in comparison.langs" :key="l">
        <span class="chip-lang" :class="[stateOf(i), { next: i === comparison.step }]">
          <CheckCircle2 v-if="stateOf(i) === 'done'" :size="14" color="#34d399" />
          <LangIcon v-else :id="l" :size="14" />
          {{ langName(l) }}
        </span>
        <ChevronRight v-if="i < comparison.langs.length - 1" :size="14" class="gap" />
      </template>
    </div>
    <div class="lbl mt-3">
      The numbers of every run are kept. They appear side by side in the comparison when the last language is done.
    </div>
  </div>
</template>

<style scoped>
.cmp-card {
  border-color: #1d4ed8;
  padding: 22px 24px;
}
.big {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 52px;
  height: 52px;
  border-radius: 14px;
  background: #0c2547;
}
.chip-lang {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 5px 12px;
  border-radius: 999px;
  font-size: 13px;
  background: #1b1b1f;
  border: 1px solid #323238;
  color: #a1a1aa;
}
.chip-lang.done {
  color: #d4d4d8;
  border-color: #065f46;
}
.chip-lang.next {
  color: #fff;
  border-color: #3b82f6;
  background: #0c2547;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.15);
}
.gap {
  color: var(--faint);
}
.spin {
  animation: spin 1s linear infinite;
}
@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
