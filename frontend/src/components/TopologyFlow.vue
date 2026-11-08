<script setup lang="ts">
/** The live picture of a run: senders → partitions of the queue → workers, with load shown on every box. */
import { computed } from 'vue'
import { VueFlow, Handle, Position } from '@vue-flow/core'
import '@vue-flow/core/dist/style.css'
import { Cog, Columns3, Send } from '@lucide/vue'
import { fmt } from '../chart'
import { containerState } from '../containerState'
import LangIcon from './LangIcon.vue'

const props = defineProps<{ run: any; containers: any[] }>()

const partitions = computed(() => props.run?.config?.partitions ?? 0)
const consumers = computed(() => props.containers.filter((c) => c.role === 'consumer'))
const producers = computed(() => props.containers.filter((c) => c.role === 'producer'))
const lag = computed<number[]>(() => props.run?.sample?.lag ?? [])
const lagMax = computed(() => Math.max(1000, ...lag.value))
const sendRate = computed(() => props.run?.sample?.send_rate ?? 0)
const atOnce = computed(() => props.run?.config?.workers?.concurrency ?? 1)

const ROW = 70
const COL = 310
const owner = computed(() => {
  const m: Record<number, string> = {}
  const asg = props.run?.assignment ?? {}
  for (const [w, ps] of Object.entries<number[]>(asg)) ps.forEach((p) => (m[p] = w))
  return m
})

const rows = computed(() => Math.max(partitions.value, consumers.value.length, producers.value.length, 1))
const height = computed(() => rows.value * ROW + 70)

/** Centre a column of `count` boxes next to the tallest column. */
const top = (count: number) => 40 + ((rows.value - count) * ROW) / 2

const pct = (c: any) => Math.min(100, c.mem_pct || 0)
const nodes = computed(() => {
  const out: any[] = [
    { id: 'h-send', type: 'head', position: { x: 0, y: 0 }, data: { kind: 'producer', text: `Senders (${producers.value.length || props.run?.config?.producer?.count || 0})` } },
    { id: 'h-queue', type: 'head', position: { x: COL, y: 0 }, data: { kind: 'partition', text: `Queue: ${partitions.value} partitions` } },
    { id: 'h-work', type: 'head', position: { x: COL * 2, y: 0 }, data: { kind: 'worker', text: `Workers (${consumers.value.length})` } },
  ]
  producers.value.forEach((c, i) => {
    const st = containerState(c)
    out.push({
      id: c.name, type: 'card', position: { x: 0, y: top(producers.value.length) + i * ROW },
      data: {
        kind: 'producer', title: c.name.replace(/^queuelab-/, ''), lang: c.lang, state: st, meter: pct(c), meterText: `${c.mem_mb}/${c.limit_mb} MB`,
        right: sendRate.value && producers.value.length === 1 ? `${fmt(sendRate.value)}/s` : '', tone: '#8b5cf6', link: '/containers/' + c.name,
      },
    })
  })
  for (let i = 0; i < partitions.value; i++) {
    const l = lag.value[i] ?? 0
    out.push({
      id: `p${i}`, type: 'card', position: { x: COL, y: top(partitions.value) + i * ROW },
      data: {
        kind: 'partition', title: `Partition ${i}`, meter: (l / lagMax.value) * 100, meterText: `${fmt(l)} waiting`,
        tone: l > lagMax.value * 0.5 && l > 5000 ? '#f59e0b' : '#3b82f6', warn: l > 5000, right: '',
      },
    })
  }
  consumers.value.forEach((c, i) => {
    out.push({
      id: c.name, type: 'card', position: { x: COL * 2, y: top(consumers.value.length) + i * ROW },
      data: {
        kind: 'worker', title: c.name.replace(/^queuelab-/, ''), lang: c.lang, state: containerState(c), meter: pct(c), meterText: `${c.mem_mb}/${c.limit_mb} MB`,
        note: atOnce.value > 1 ? `up to ${atOnce.value} at once` : 'one at a time',
        right: c.msgs_s ? `${fmt(c.msgs_s)}/s` : '', tone: c.mem_pct > 90 ? '#f59e0b' : '#10b981', link: '/containers/' + c.name,
      },
    })
  })
  return out
})

const edges = computed(() => {
  const out: any[] = []
  const dur = Math.max(0.25, 2.4 - Math.log10(sendRate.value + 1) * 0.45)
  const many = producers.value.length * partitions.value > 60 // too many lines to animate them all
  for (let i = 0; i < partitions.value; i++) {
    const style = { animationDuration: dur + 's' }
    producers.value.forEach((p) => {
      out.push({ id: `e-${p.name}-${i}`, source: p.name, target: `p${i}`, animated: !many, style: { ...style, stroke: '#8b5cf6' } })
    })
    if (owner.value[i]) out.push({ id: `e-${i}`, source: `p${i}`, target: owner.value[i], animated: true, style: { ...style, stroke: '#10b981' } })
  }
  return out
})

const headIcon: Record<string, any> = { producer: Send, partition: Columns3, worker: Cog }
const headColor: Record<string, string> = { producer: '#a78bfa', partition: '#60a5fa', worker: '#34d399' }
</script>

<template>
  <div class="card" style="padding: 12px 14px">
    <div class="flex items-center justify-between flex-wrap gap-2 px-1 pt-1">
      <h2>Live topology</h2>
      <div class="flex items-center gap-4 lbl">
        <span class="flex items-center gap-1.5"><span class="dot" style="background: #8b5cf6" />senders to the queue</span>
        <span class="flex items-center gap-1.5"><span class="dot" style="background: #10b981" />queue to workers</span>
        <span>lines speed up with the load</span>
      </div>
    </div>
    <div v-if="partitions" :style="{ height: height + 'px' }">
      <VueFlow :nodes="nodes" :edges="edges" :nodes-draggable="false" :nodes-connectable="false" :zoom-on-scroll="false" :pan-on-drag="false" :zoom-on-double-click="false" fit-view-on-init :min-zoom="0.4" :max-zoom="1.2">
        <template #node-head="{ data }">
          <div class="topo-head" :style="{ color: headColor[data.kind] }">
            <component :is="headIcon[data.kind]" :size="15" />{{ data.text }}
          </div>
        </template>
        <template #node-card="{ data }">
          <router-link :to="data.link || '#'" :class="data.link ? '' : 'pointer-events-none'" style="display: block">
            <div class="topo-node" :style="{ borderColor: data.tone }">
              <Handle type="target" :position="Position.Left" />
              <div class="flex items-center gap-2">
                <LangIcon v-if="data.lang" :id="data.lang" :size="16" />
                <component :is="headIcon[data.kind]" v-else :size="15" :color="data.tone" style="flex: none" />
                <span class="topo-title">{{ data.title }}</span>
                <span v-if="data.right" class="topo-right">{{ data.right }}</span>
              </div>
              <div class="flex items-center justify-between gap-2 topo-sub">
                <span>{{ data.meterText }}</span>
                <span v-if="data.note">{{ data.note }}</span>
                <span v-else-if="data.state" :style="{ color: data.state.tone === 'ok' ? '#34d399' : data.state.tone === 'bad' ? '#f87171' : '#a1a1aa' }">{{ data.state.label }}</span>
              </div>
              <div class="topo-meter"><i :style="{ width: Math.max(2, data.meter) + '%', background: data.tone }" /></div>
              <Handle type="source" :position="Position.Right" />
            </div>
          </router-link>
        </template>
      </VueFlow>
    </div>
    <div v-else class="lbl" style="padding: 28px 8px">The topology appears when you start a run.</div>
  </div>
</template>

<style scoped>
.topo-head { display: flex; align-items: center; gap: 7px; font-size: 11px; font-weight: 700; letter-spacing: 0.06em; text-transform: uppercase; white-space: nowrap; }
.topo-node { min-width: 210px; background: linear-gradient(180deg, #1c1c21, #16161a); border: 1px solid; border-radius: 11px; padding: 9px 12px 10px; box-shadow: 0 8px 20px -14px rgba(0, 0, 0, 0.9); transition: transform 0.15s ease, box-shadow 0.15s ease; }
a:hover .topo-node { transform: translateY(-2px); box-shadow: 0 12px 26px -12px rgba(0, 0, 0, 0.95); }
.topo-title { font-weight: 600; font-size: 12px; color: #e4e4e7; }
.topo-right { margin-left: auto; font-size: 11px; color: #e4e4e7; font-variant-numeric: tabular-nums; }
.topo-sub { font-size: 11px; color: #a1a1aa; margin: 4px 0 6px; font-variant-numeric: tabular-nums; }
.topo-meter { height: 4px; border-radius: 2px; background: #27272a; overflow: hidden; }
.topo-meter > i { display: block; height: 100%; border-radius: 2px; transition: width 0.5s cubic-bezier(0.2, 0.8, 0.2, 1); }
</style>
