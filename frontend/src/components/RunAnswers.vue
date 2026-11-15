<script setup lang="ts">
/** The three things to know first about a finished run: did it finish, was anything lost, how fast was it. */
import { computed } from 'vue'
import { AlertTriangle, CheckCircle2, Gauge, XCircle } from '@lucide/vue'
import { fmt } from '../chart'
import { duration } from '../health'

const props = defineProps<{ status: string; summary: any; win: any }>()

const finished = computed(() => {
  if (props.status === 'completed')
    return { icon: CheckCircle2, color: '#10b981', title: 'Finished', text: 'Every message was sent and processed.' }
  if (props.status === 'cancelled')
    return {
      icon: AlertTriangle,
      color: '#f59e0b',
      title: 'Cancelled',
      text: 'Stopped early, so the numbers are partial.',
    }
  return { icon: XCircle, color: '#ef4444', title: 'Failed', text: 'Something went wrong, see the notes below.' }
})
const lost = computed(() => props.summary.lost ?? 0)
const repeated = computed(() => props.summary.duplicates ?? 0)
const clean = computed(() => lost.value === 0 && repeated.value === 0)
const total = computed(() => props.win?.total_s ?? props.summary.duration_s)
</script>

<template>
  <div class="grid gap-3" style="grid-template-columns: repeat(auto-fit, minmax(240px, 1fr))">
    <div class="card answer" :style="{ borderColor: finished.color }">
      <div class="lbl">Did it finish?</div>
      <div class="flex items-center gap-2">
        <component :is="finished.icon" :size="22" :color="finished.color" /><span class="big">{{
          finished.title
        }}</span>
      </div>
      <div class="lbl">{{ finished.text }}</div>
    </div>
    <div class="card answer" :style="{ borderColor: clean ? '#10b981' : '#ef4444' }">
      <div class="lbl">Was anything lost?</div>
      <div class="flex items-center gap-2">
        <component :is="clean ? CheckCircle2 : XCircle" :size="22" :color="clean ? '#10b981' : '#ef4444'" />
        <span class="big">{{ clean ? 'Nothing lost' : fmt(lost) + ' lost' }}</span>
      </div>
      <div class="lbl">
        {{
          clean
            ? 'Every message arrived exactly once.'
            : fmt(lost) + ' lost, ' + fmt(repeated) + ' received more than once.'
        }}
      </div>
    </div>
    <div class="card answer">
      <div class="lbl">How fast?</div>
      <div class="flex items-center gap-2">
        <Gauge :size="22" color="#60a5fa" /><span class="big">{{ fmt(summary.avg_recv_rate) }} msg/s</span>
      </div>
      <div class="lbl">
        on average<template v-if="total"
          >, {{ duration(total) }} from the first message sent to the last one processed</template
        >.
      </div>
    </div>
  </div>
</template>

<style scoped>
.answer {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 16px 18px;
}
.big {
  font-size: 20px;
  font-weight: 700;
}
</style>
