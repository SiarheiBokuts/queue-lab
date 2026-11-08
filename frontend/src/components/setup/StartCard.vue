<script setup lang="ts">
/** Step 7: what the run needs (memory, cores), warnings about the setup, and the start button. */
import { AlertTriangle, CheckCircle2, Rocket } from '@lucide/vue'
import { langName } from '../../langs'
import { useRunForm } from '../../composables/useRunForm'
import Spinner from '../Spinner.vue'

const {
  compare, compareLangs, randomOrder, cooldown, plan, planErr, capacity, cpuNote,
  runSummary, memSegments, memLeft, busy, submitErr, submit,
} = useRunForm()
</script>

<template>
  <div class="card" :style="{ borderColor: plan && !plan.ok ? '#7f1d1d' : '' }">
    <h2 class="mb-1 flex items-center gap-2"><Rocket :size="17" color="#60a5fa" />7. Check and start</h2>
    <p class="lbl mb-4">{{ runSummary }}</p>

    <template v-if="plan">
      <div class="mem-bar" role="img" :aria-label="'Memory needed: ' + plan.requested_mb + ' MB'">
        <i v-for="s in memSegments" :key="s.label" :style="{ width: s.pct + '%', background: s.color }" />
      </div>
      <div class="flex flex-wrap gap-x-6 gap-y-2 mt-3">
        <div v-for="s in memSegments" :key="s.label" class="flex items-center gap-2 text-sm">
          <span class="dot" :style="{ background: s.color }" />{{ s.label }} <b>{{ s.mb }} MB</b>
        </div>
        <div v-if="!plan.breakdown.broker_mb" class="flex items-center gap-2 text-sm"><span class="dot" style="background: #06b6d4" />Kafka broker <b>already running</b></div>
      </div>
      <div class="grid gap-3 mt-4" style="grid-template-columns: repeat(auto-fit, minmax(180px, 1fr))">
        <div class="tile"><div class="lbl">Memory this run needs</div><div class="val">{{ plan.requested_mb }} MB</div></div>
        <div class="tile"><div class="lbl">Memory budget</div><div class="val">{{ plan.remaining_mb }} MB</div><div class="lbl">free for the lab</div></div>
        <div class="tile"><div class="lbl">Left after start</div><div class="val">{{ memLeft }} MB</div></div>
      </div>
      <div v-if="plan.cpu" class="lbl mt-3">Every worker and sender gets <b>{{ plan.cpu.cpus }} CPU core{{ plan.cpu.cpus === 1 ? '' : 's' }}</b>, the same for every language: this computer has {{ plan.cpu.host_cpus }} cores, one is kept for Kafka and the app, the rest is shared equally by the {{ plan.cpu.containers }} containers. A worker computes {{ plan.cpu.compute_threads }} message{{ plan.cpu.compute_threads === 1 ? '' : 's' }} at a time at most.</div>
      <div class="mt-4">
        <span v-if="plan.ok" class="pill ok"><CheckCircle2 :size="15" />Fits into your budget</span>
        <span v-else class="pill bad"><AlertTriangle :size="15" />{{ plan.reason }}</span>
      </div>
    </template>
    <div v-if="planErr" class="mt-3 text-red-300 text-sm">{{ planErr }}</div>
    <div v-if="cpuNote" class="mt-3 flex items-start gap-2 text-sm" style="color: #fbbf24"><AlertTriangle :size="16" style="flex: none; margin-top: 2px" />{{ cpuNote }}</div>
    <div v-if="capacity" class="mt-3 flex items-start gap-2 text-sm" :style="{ color: capacity.ok ? '#34d399' : '#fbbf24' }">
      <component :is="capacity.ok ? CheckCircle2 : AlertTriangle" :size="16" style="flex: none; margin-top: 2px" />{{ capacity.text }}
    </div>
    <div class="lbl mt-3" v-if="compare">This will run {{ compareLangs.length }} tests one after another ({{ randomOrder ? 'in random order' : compareLangs.map(langName).join(' → ') }}) with {{ cooldown }} s pauses. The first run builds the image of a language if needed, which can take a few minutes.</div>
    <div class="flex items-center gap-3 mt-5 flex-wrap">
      <button class="btn btn-primary inline-flex items-center gap-2" style="padding: 11px 22px; font-size: 14px" :disabled="busy || !plan || !plan.ok || (compare && compareLangs.length < 2)" @click="submit">
        <Spinner v-if="busy" :size="16" /><Rocket v-else :size="16" />
        {{ busy ? 'Starting…' : compare ? 'Run and compare ' + compareLangs.length + ' languages' : 'Create and run' }}
      </button>
      <span class="text-red-300 text-sm">{{ submitErr }}</span>
    </div>
  </div>
</template>
