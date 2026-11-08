<script setup lang="ts">
import { computed } from 'vue'
import { fmt } from '../chart'
import { fmtMs } from '../format'
import Hint from './Hint.vue'

const props = defineProps<{ run: any }>()
const finished = computed(() => !!props.run?.summary && !['pending', 'running'].includes(props.run?.status))
const s = computed(() => props.run?.sample)
const t = computed(() => props.run?.totals)
const sum = computed(() => props.run?.summary)
</script>

<template>
  <div class="grid gap-2.5" style="grid-template-columns: repeat(auto-fit, minmax(140px, 1fr))">
    <div class="tile">
      <div class="lbl">{{ finished ? 'Avg processed / s' : 'Processed / s' }}</div>
      <div class="val">{{ fmt(finished ? sum.avg_recv_rate : (s?.recv_rate ?? 0)) }}</div>
    </div>
    <div class="tile">
      <div class="lbl">{{ finished ? 'Peak sent / s' : 'Sent / s' }}</div>
      <div class="val">{{ fmt(finished ? sum.peak_send_rate : (s?.send_rate ?? 0)) }}</div>
    </div>
    <div class="tile">
      <div class="lbl">{{ finished ? 'Latency p99 (whole run)' : 'Latency p99' }}<Hint text="99% of messages were processed faster than this. It shows how bad the slow tail is, which averages hide." /></div>
      <div class="val">{{ fmtMs(finished ? sum.p99 : s?.p99) }}</div>
    </div>
    <div class="tile">
      <div class="lbl">{{ finished ? 'Most messages waiting' : 'Messages waiting' }}<Hint text="Also called lag: messages that are in Kafka but not yet taken by a worker. If this keeps growing, the workers are slower than the senders." /></div>
      <div class="val">{{ fmt(finished ? sum.peak_lag : (s?.lag_total ?? 0)) }}</div>
    </div>
    <div class="tile">
      <div class="lbl">Processed</div>
      <div class="val">{{ fmt(t?.unique ?? 0) }}<span class="lbl"> / {{ fmt(run?.config?.messages ?? 0) }}</span></div>
    </div>
    <div class="tile">
      <div class="lbl">Lost / duplicates<Hint text="Lost: sent but never processed. Duplicates: processed more than once. Both are detected from per-partition sequence numbers." /></div>
      <div class="val">{{ finished ? sum.lost : '…' }} / {{ t?.duplicates ?? 0 }}</div>
    </div>
  </div>
</template>
