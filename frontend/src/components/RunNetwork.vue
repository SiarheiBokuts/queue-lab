<script setup lang="ts">
/** Network of a run: what the data needed, and what the containers really moved. */
import { computed } from 'vue'

const props = defineProps<{ network: any }>()
const n = computed(() => props.network)
/** What compression saved on the network: the data size minus what the senders really put on the wire. */
const saved = computed(() => {
  const x = n.value
  if (!x || x.compression === 'none' || !(x.needed_mb > 0)) return null
  const mb = x.needed_mb - x.producers_sent_mb
  return { mb: Math.round(mb * 100) / 100, pct: Math.round((mb / x.needed_mb) * 100) }
})
const rows = computed(() => {
  const x = n.value
  if (!x) return []
  const top = Math.max(x.needed_mb, x.producers_sent_mb, x.broker_received_mb, x.broker_sent_mb, x.workers_received_mb, 0.01)
  return [
    { label: 'The data itself', note: `${x.messages.toLocaleString()} messages × ${x.message_size} bytes`, mb: x.needed_mb, color: '#8b5cf6' },
    { label: 'Senders put on the network', note: x.compression === 'none' ? 'no compression' : `compressed with ${x.compression}`, mb: x.producers_sent_mb, color: '#3b82f6' },
    { label: 'Kafka received', note: 'from all senders', mb: x.broker_received_mb, color: '#06b6d4' },
    { label: 'Kafka sent', note: 'to the workers', mb: x.broker_sent_mb, color: '#06b6d4' },
    { label: 'Workers received', note: 'all workers together', mb: x.workers_received_mb, color: '#10b981' },
  ].map((r) => ({ ...r, width: Math.max(1, (r.mb / top) * 100) }))
})
</script>

<template>
  <div class="card" id="network" v-if="n">
    <div class="flex items-center justify-between mb-3 flex-wrap gap-2">
      <h2>Network: needed and actual</h2>
      <span class="lbl">MiB moved between the containers while the messages were sent and processed</span>
    </div>
    <div v-for="r in rows" :key="r.label" class="mb-2">
      <div class="flex justify-between text-sm"><span>{{ r.label }} <span class="lbl">· {{ r.note }}</span></span><b>{{ r.mb }} MiB</b></div>
      <div class="bar"><i :style="{ width: r.width + '%', background: r.color }" /></div>
    </div>
    <div class="grid gap-2.5 mt-3" style="grid-template-columns: repeat(auto-fit, minmax(190px, 1fr))">
      <div class="tile" v-if="n.needed_mbit_s !== null">
        <div class="lbl">While sending, the data needed</div><div class="val">{{ n.needed_mbit_s }} Mbit/s</div>
      </div>
      <div class="tile" v-if="n.actual_mbit_s !== null">
        <div class="lbl">While sending, the wire carried</div><div class="val">{{ n.actual_mbit_s }} Mbit/s</div>
      </div>
      <div class="tile" v-if="saved" :style="{ borderLeft: '3px solid ' + (saved.pct > 0 ? '#10b981' : '#f59e0b') }">
        <div class="lbl">Saved by {{ n.compression }}</div>
        <div class="val">{{ saved.pct > 0 ? saved.pct + '%' : 'nothing' }}</div>
        <div class="lbl">{{ saved.pct > 0 ? saved.mb + ' MiB less on the network' : 'this data does not shrink' }}</div>
      </div>
      <div class="tile" v-if="n.compression_ratio">
        <div class="lbl">Smaller on the wire</div><div class="val">{{ n.compression_ratio }}x</div>
        <div class="lbl">{{ n.compression === 'none' ? 'no compression chosen' : n.compression + ', ' + n.payload + ' data' }}</div>
      </div>
    </div>
    <div class="lbl mt-2">Counters include Kafka protocol overhead and the small progress reports, so they are slightly above the data size without compression.</div>
  </div>
</template>
