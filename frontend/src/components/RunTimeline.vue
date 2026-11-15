<script setup lang="ts">
import { computed } from 'vue'
import { fmt } from '../chart'

const props = defineProps<{ win: any; sent: number; received: number }>()

const span = computed(() => Math.max(props.win?.last_processed ?? 0, props.win?.send_end ?? 0, 0.001))
const pct = (v: number) => Math.min(100, Math.max(0, (v / span.value) * 100))
const segs = computed(() => {
  const w = props.win || {}
  const out: { label: string; from: number; to: number; color: string }[] = []
  if (w.send_start != null && w.send_end != null) {
    out.push({ label: 'Sending', from: w.send_start, to: w.send_end, color: '#8b5cf6' })
    if (w.last_processed != null && w.last_processed > w.send_end)
      out.push({ label: 'Processing the backlog', from: w.send_end, to: w.last_processed, color: '#10b981' })
  }
  return out
})
/** The moments of the run that are marked on the bar and named below it. */
const marks = computed(() => {
  const w = props.win || {}
  const out: { label: string; at: number; color: string }[] = []
  if (w.send_start != null) out.push({ label: 'First message sent', at: w.send_start, color: '#c4b5fd' })
  if (w.send_end != null) out.push({ label: 'Sending done', at: w.send_end, color: '#a78bfa' })
  if (w.last_processed != null) out.push({ label: 'Last message processed', at: w.last_processed, color: '#6ee7b7' })
  return out
})
const sendRate = computed(() => (props.win?.send_s ? Math.round(props.sent / props.win.send_s) : null))
const e2eRate = computed(() => (props.win?.total_s ? Math.round(props.received / props.win.total_s) : null))
</script>

<template>
  <div class="card" v-if="win && win.send_s">
    <div class="flex items-center justify-between mb-3">
      <h2>Message window</h2>
      <span class="lbl">from the first message sent to the last one processed</span>
    </div>

    <div class="bar-wrap">
      <div class="bar">
        <div
          v-for="s in segs"
          :key="s.label"
          class="seg"
          :style="{ left: pct(s.from) + '%', width: Math.max(0.8, pct(s.to) - pct(s.from)) + '%', background: s.color }"
          :title="s.label + ': ' + (s.to - s.from).toFixed(2) + ' s'"
        />
      </div>
      <!-- A thin line over the bar at every moment named below -->
      <i
        v-for="m in marks"
        :key="m.label"
        class="tick"
        :style="{ left: pct(m.at) + '%', background: m.color }"
        aria-hidden="true"
      />
    </div>

    <!-- The moments, in one row: they never overlap, however close they are on the bar -->
    <div class="moments">
      <div v-for="m in marks" :key="m.label" class="moment">
        <span class="dot" :style="{ background: m.color }" />
        <span class="lbl">{{ m.label }}</span>
        <b>{{ m.at }} s</b>
      </div>
    </div>

    <div class="flex gap-4 mt-3 lbl flex-wrap">
      <span><b class="inline-block w-2 h-2 rounded-sm mr-1" style="background: #8b5cf6" />Sending</span>
      <span
        ><b class="inline-block w-2 h-2 rounded-sm mr-1" style="background: #10b981" />Consumers catching up after
        sending ended</span
      >
    </div>
    <div class="grid gap-2.5 mt-4" style="grid-template-columns: repeat(auto-fit, minmax(160px, 1fr))">
      <div class="tile">
        <div class="lbl">Sending took</div>
        <div class="val">{{ win.send_s }} s</div>
        <div class="lbl">{{ fmt(sendRate) }} msg/s</div>
      </div>
      <div class="tile">
        <div class="lbl">First send to last processed</div>
        <div class="val">{{ win.total_s ?? '-' }} s</div>
        <div class="lbl">{{ fmt(e2eRate) }} msg/s end to end</div>
      </div>
      <div class="tile">
        <div class="lbl">Tail after sending ended</div>
        <div class="val">{{ win.drain_s ?? '-' }} s</div>
        <div class="lbl">{{ (win.drain_s ?? 0) > 1.5 ? 'consumers were behind' : 'consumers kept up' }}</div>
      </div>
      <div class="tile">
        <div class="lbl">First message processed after</div>
        <div class="val">
          {{
            win.first_processed != null && win.send_start != null
              ? (win.first_processed - win.send_start).toFixed(2)
              : '-'
          }}
          s
        </div>
        <div class="lbl">startup latency</div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.bar-wrap {
  position: relative;
}
.bar {
  position: relative;
  height: 40px;
  background: #0f0f12;
  border: 1px solid #27272a;
  border-radius: 10px;
  overflow: hidden;
}
.seg {
  position: absolute;
  top: 0;
  bottom: 0;
  opacity: 0.9;
}
.tick {
  position: absolute;
  top: -5px;
  bottom: -5px;
  width: 2px;
  margin-left: -1px;
  border-radius: 2px;
  box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.35);
}
.moments {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 24px;
  margin-top: 12px;
}
.moment {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 4px 10px;
  border: 1px solid #27272a;
  border-radius: 999px;
  font-size: 12px;
}
.dot {
  width: 8px;
  height: 8px;
  border-radius: 999px;
}
</style>
