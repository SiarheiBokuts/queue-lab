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
const sendRate = computed(() => (props.win?.send_s ? Math.round(props.sent / props.win.send_s) : null))
const e2eRate = computed(() => (props.win?.total_s ? Math.round(props.received / props.win.total_s) : null))
</script>

<template>
  <div class="card" v-if="win && win.send_s">
    <div class="flex items-center justify-between mb-3">
      <h2>Message window</h2>
      <span class="lbl">from the first message sent to the last one processed</span>
    </div>

    <div style="position: relative; height: 38px; background: #0f0f12; border-radius: 8px; overflow: hidden">
      <div
        v-for="s in segs"
        :key="s.label"
        :style="{ position: 'absolute', top: 0, bottom: 0, left: pct(s.from) + '%', width: Math.max(0.8, pct(s.to) - pct(s.from)) + '%', background: s.color, opacity: 0.85 }"
        :title="s.label + ': ' + (s.to - s.from).toFixed(2) + ' s'"
      />
    </div>
    <div class="relative" style="height: 34px; margin-top: 4px">
      <div v-if="win.send_start != null" class="lbl" :style="{ position: 'absolute', left: pct(win.send_start) + '%', transform: 'translateX(-2px)', whiteSpace: 'nowrap' }">
        ▲ first send<br />{{ win.send_start }} s
      </div>
      <div v-if="win.send_end != null" class="lbl" :style="{ position: 'absolute', left: pct(win.send_end) + '%', transform: 'translateX(-50%)', whiteSpace: 'nowrap', textAlign: 'center' }">
        ▲ sending done<br />{{ win.send_end }} s
      </div>
      <div v-if="win.last_processed != null" class="lbl" :style="{ position: 'absolute', right: '0', whiteSpace: 'nowrap', textAlign: 'right' }">
        all processed ▲<br />{{ win.last_processed }} s
      </div>
    </div>
    <div class="flex gap-4 mt-1 lbl flex-wrap">
      <span><b class="inline-block w-2 h-2 rounded-sm mr-1" style="background: #8b5cf6" />Sending</span>
      <span><b class="inline-block w-2 h-2 rounded-sm mr-1" style="background: #10b981" />Consumers catching up after sending ended</span>
    </div>

    <div class="grid gap-2.5 mt-4" style="grid-template-columns: repeat(auto-fit, minmax(160px, 1fr))">
      <div class="tile"><div class="lbl">Sending took</div><div class="val">{{ win.send_s }} s</div><div class="lbl">{{ fmt(sendRate) }} msg/s</div></div>
      <div class="tile"><div class="lbl">First send to last processed</div><div class="val">{{ win.total_s ?? '-' }} s</div><div class="lbl">{{ fmt(e2eRate) }} msg/s end to end</div></div>
      <div class="tile"><div class="lbl">Tail after sending ended</div><div class="val">{{ win.drain_s ?? '-' }} s</div><div class="lbl">{{ (win.drain_s ?? 0) > 1.5 ? 'consumers were behind' : 'consumers kept up' }}</div></div>
      <div class="tile"><div class="lbl">First message processed after</div><div class="val">{{ win.first_processed != null && win.send_start != null ? (win.first_processed - win.send_start).toFixed(2) : '-' }} s</div><div class="lbl">startup latency</div></div>
    </div>
  </div>
</template>
