<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { AlertTriangle, Boxes, CheckCircle2, Cpu, Gauge, MemoryStick, Scale, Timer } from '@lucide/vue'
import { api } from '../api'
import { useLive } from '../stores/live'
import { notify } from '../notify'
import NumberInput from '../components/NumberInput.vue'
import { fairCores } from '../cpu'

const live = useLive()
const router = useRouter()
const gb = (mb: number) => (mb / 1024).toFixed(1).replace('.0', '')

// The buttons, the advised size and the least that works come from the backend (config.py).
const presets = computed(() => live.limits.budget.presets as { mb: number; name: string; note: string }[])
const budget = ref(live.settings.budget_mb || live.limits.budget.recommended_mb)
watch(
  () => live.settings.budget_mb,
  (v) => {
    if (v) budget.value = v
  },
)

const dockerMb = computed(() => live.host.total_mb)
const free = computed(() => live.host.available_mb)
const isPreset = computed(() => presets.value.some((p) => p.mb === budget.value))
const tooMuch = computed(() => budget.value > free.value)
const tooLittle = computed(() => budget.value < live.limits.budget.least_mb)
const small = computed(() => budget.value < live.limits.budget.recommended_mb)
/** Share of Docker's memory the lab may use, for the bar (capped at the whole bar). */
const labPct = computed(() => Math.min(100, Math.round((budget.value / Math.max(1, dockerMb.value)) * 100)))

async function start() {
  try {
    await api.settings({ budget_mb: budget.value, welcomed: true })
    await live.refresh()
    router.push('/setup')
  } catch (e: any) {
    notify(e.message, 'error')
  }
}

const abilities = [
  {
    icon: Boxes,
    title: 'Real containers',
    text: 'Kafka, senders and workers in Python, Node.js, Java, Go and Rust, started for you.',
  },
  {
    icon: MemoryStick,
    title: 'Memory limits',
    text: 'Every container gets a memory limit, so you see what a language really needs.',
  },
  {
    icon: Cpu,
    title: 'Fair processor share',
    text: 'Every container gets the same share of the cores, so the languages compete on equal terms.',
  },
  {
    icon: Gauge,
    title: 'Live numbers',
    text: 'Speed, latency, messages waiting, CPU, memory and network while the test runs.',
  },
  { icon: Scale, title: 'Comparison', text: 'The same test in every language, with every number side by side.' },
  { icon: Timer, title: 'Report', text: 'Every run keeps a report: what was sent, what arrived, what was lost.' },
]
</script>

<template>
  <div class="flex flex-col gap-5" style="max-width: 860px">
    <div>
      <h1>Welcome to queue-lab</h1>
      <p class="lbl mt-1">
        A lab for message queues. You choose how much of your computer it may use, set up a test, and it runs real
        containers and shows what the test cost.
      </p>
    </div>

    <div class="card" :style="{ borderColor: live.dockerOk ? '#065f46' : '#7f1d1d' }">
      <div class="flex items-center gap-2 mb-1">
        <span class="step-no">1</span>
        <h2>Docker</h2>
        <span class="badge" :class="live.dockerOk ? 'ok' : 'bad'">{{ live.dockerOk ? 'found' : 'not reachable' }}</span>
      </div>
      <template v-if="live.dockerOk">
        <p class="lbl mb-3">
          The lab runs everything inside Docker, so it can only use what Docker has. This is what it found:
        </p>
        <div class="grid gap-3" style="grid-template-columns: repeat(auto-fit, minmax(170px, 1fr))">
          <div class="tile">
            <div class="lbl">Processor</div>
            <div class="val">{{ live.host.cpus || '?' }} cores</div>
            <div class="lbl">available to Docker</div>
          </div>
          <div class="tile">
            <div class="lbl">Memory</div>
            <div class="val">{{ gb(dockerMb) }} GB</div>
            <div class="lbl">{{ gb(free) }} GB free now</div>
          </div>
        </div>
        <p class="lbl mt-3">
          These are the limits of Docker, not of your whole computer. To change them, open Docker Desktop, Settings,
          Resources (on Windows with WSL: <span class="mono">.wslconfig</span>).
        </p>
      </template>
      <p v-else class="lbl">
        Start Docker Desktop and reload this page. Nothing can be created until Docker is running.
      </p>
    </div>

    <div class="card flex flex-col gap-3">
      <div>
        <div class="flex items-center gap-2">
          <span class="step-no">2</span>
          <h2>How much memory may the lab use?</h2>
        </div>
        <p class="lbl mt-1">
          Choose the total for Kafka, senders and workers together. The lab refuses to start a container that does not
          fit, so your computer never runs out of memory. You can change it later in Settings.
        </p>
      </div>

      <div
        class="grid gap-3"
        style="grid-template-columns: repeat(auto-fit, minmax(190px, 1fr))"
        role="radiogroup"
        aria-label="Memory for the lab"
      >
        <button
          v-for="p in presets"
          :key="p.mb"
          type="button"
          role="radio"
          :aria-checked="budget === p.mb"
          class="choice"
          :class="{ on: budget === p.mb }"
          @click="budget = p.mb"
        >
          <div class="flex items-center justify-between">
            <span class="lbl">{{ p.name }}<template v-if="p.mb === live.limits.budget.recommended_mb"> · advised</template></span>
            <span v-if="budget === p.mb" class="chosen"><CheckCircle2 :size="15" />Selected</span>
          </div>
          <div class="val">{{ gb(p.mb) }} GB</div>
          <div class="lbl">{{ p.note }}</div>
        </button>
      </div>

      <label class="field" style="max-width: 320px"
        >Or type your own (MB)
        <NumberInput v-model="budget" :min="live.limits.budget.min_mb" :class="{ 'own-on': !isPreset }" />
        <span v-if="!isPreset" class="chosen mt-1"><CheckCircle2 :size="15" />Your own value is selected</span>
      </label>

      <div>
        <div class="flex items-baseline justify-between mb-1.5">
          <span class="text-sm"
            ><b>{{ gb(budget) }} GB</b> for the lab</span
          >
          <span class="lbl">of the {{ gb(dockerMb) }} GB Docker has ({{ labPct }}%)</span>
        </div>
        <div
          class="bar-track"
          role="img"
          :aria-label="'The lab may use ' + gb(budget) + ' GB of ' + gb(dockerMb) + ' GB'"
        >
          <i :style="{ width: labPct + '%', background: tooMuch ? '#f59e0b' : '#3b82f6' }" />
        </div>
        <div class="flex justify-between lbl mt-1">
          <span><i class="dot-lab" />Reserved for the lab: {{ gb(budget) }} GB</span>
          <span
            >Not used by the lab: {{ gb(Math.max(0, dockerMb - budget)) }} GB (stays free for Docker itself and anything
            else running in it)</span
          >
        </div>
      </div>

      <div v-if="tooMuch" class="flex items-start gap-2 text-amber-400 text-xs">
        <AlertTriangle :size="14" style="flex: none; margin-top: 1px" />That is more than the {{ gb(free) }} GB Docker
        has free now. The real free memory is still enforced.
      </div>
      <div v-else-if="tooLittle" class="flex items-start gap-2 text-amber-400 text-xs">
        <AlertTriangle :size="14" style="flex: none; margin-top: 1px" />Below {{ gb(live.limits.budget.least_mb) }} GB one
        sender and one worker do not fit. Kafka takes {{ gb(live.limits.broker_light_mb) }} GB in Light Kafka mode
        (Settings) and {{ gb(live.limits.broker_mb) }} GB without it.
      </div>
      <div v-else-if="small" class="flex items-start gap-2 text-amber-400 text-xs">
        <AlertTriangle :size="14" style="flex: none; margin-top: 1px" />With less than
        {{ gb(live.limits.budget.recommended_mb) }} GB only a small run fits. The form starts with
        {{ live.limits.form.workers }} worker{{ live.limits.form.workers === 1 ? '' : 's' }} and warns when a setup
        does not fit; nothing is refused until you start it.
      </div>
    </div>

    <div class="card flex flex-col gap-3" v-if="live.dockerOk && live.host.cpus">
      <div class="flex items-center gap-2">
        <span class="step-no">3</span>
        <h2>How much processor does the lab get?</h2>
      </div>
      <p class="lbl">
        Nothing to choose here: the lab splits the cores of Docker fairly by itself, so every language competes on equal
        terms.
      </p>
      <div class="grid gap-3" style="grid-template-columns: repeat(auto-fit, minmax(170px, 1fr))">
        <div class="tile">
          <div class="lbl">Cores in Docker</div>
          <div class="val">{{ live.host.cpus }}</div>
          <div class="lbl">the total</div>
        </div>
        <div class="tile">
          <div class="lbl">Kept for Kafka and the app</div>
          <div class="val">{{ live.limits.cpu_reserve }}</div>
          <div class="lbl">so they never starve</div>
        </div>
        <div class="tile">
          <div class="lbl">Shared by senders and workers</div>
          <div class="val">{{ Math.max(1, live.host.cpus - live.limits.cpu_reserve) }}</div>
          <div class="lbl">split equally</div>
        </div>
      </div>
      <table class="tbl">
        <thead>
          <tr>
            <th>Containers in a run (senders + workers)</th>
            <th>Each gets</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="n in [2, 3, 6, 10]" :key="n">
            <td>{{ n }}</td>
            <td>
              <b>{{ fairCores(live.host.cpus, n, live.limits) }}</b> core{{ fairCores(live.host.cpus, n, live.limits) === 1 ? '' : 's' }}
            </td>
          </tr>
        </tbody>
      </table>
      <p class="lbl">
        One core is 100 % in the CPU charts. Nobody gets more than {{ live.limits.cpu_max_per_container }} cores. A worker that
        computes handles as many messages at once as it has whole cores, so a language that uses more threads cannot
        take the cores of another. To get more, give Docker more cores (Docker Desktop, Settings, Resources).
      </p>
    </div>

    <div class="card">
      <h2 class="mb-3">What it can do</h2>
      <div class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(240px, 1fr))">
        <div v-for="a in abilities" :key="a.title" class="flex gap-3">
          <component :is="a.icon" :size="20" style="flex: none; margin-top: 2px; color: #60a5fa" />
          <div>
            <b>{{ a.title }}</b>
            <div class="lbl">{{ a.text }}</div>
          </div>
        </div>
      </div>
    </div>

    <div>
      <button class="btn btn-primary" :disabled="!live.dockerOk" @click="start">Save and set up the first run</button>
    </div>
  </div>
</template>

<style scoped>
.choice {
  text-align: left;
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 14px 16px;
  border-radius: 12px;
  border: 2px solid #3f3f46;
  background: #1b1b1f;
  color: inherit;
  cursor: pointer;
  transition:
    border-color 0.12s,
    background 0.12s;
}
.choice:hover {
  border-color: #52525b;
}
.choice.on {
  border-color: #3b82f6;
  background: #0c2547;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.18);
}
.chosen {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-size: 12px;
  font-weight: 600;
  color: #60a5fa;
}
.bar-track {
  height: 12px;
  border-radius: 999px;
  background: #27272a;
  overflow: hidden;
}
.bar-track i {
  display: block;
  height: 100%;
  border-radius: 999px;
  transition: width 0.2s;
}
.dot-lab {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 999px;
  background: #3b82f6;
  margin-right: 6px;
}
.own-on :deep(input) {
  border-color: #3b82f6;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.18);
}
</style>
