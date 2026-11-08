<script setup lang="ts">
/** Step 6: what the workers do with each message (echo, delay, cpu), with the code of that step. */
import { ref, watch } from 'vue'
import { CheckCircle2, Check, Cpu, Eye, Hourglass, Lightbulb, ListChecks, Zap } from '@lucide/vue'
import { LANGS, langName } from '../../langs'
import { useRunForm } from '../../composables/useRunForm'
import CodeBlock from '../CodeBlock.vue'

const { form, scenarios, current, code, pickScenario } = useRunForm()

/** Icon and accent colour of each scenario card. */
const LOOK: Record<string, { icon: any; color: string }> = {
  echo: { icon: Zap, color: '#f59e0b' },
  delay: { icon: Hourglass, color: '#60a5fa' },
  cpu: { icon: Cpu, color: '#f87171' },
}

const showCode = ref(false)
const codeLang = ref(form.workers.lang)
watch(() => form.workers.lang, (lang) => { codeLang.value = lang })
</script>

<template>
  <div class="card">
    <h2 class="mb-1">6. What should the workers do with each message?</h2>
    <p class="lbl mb-3">Pick the kind of work to simulate. The same setup behaves very differently depending on it, so try them one after another and compare the runs.</p>
    <div class="grid gap-3" style="grid-template-columns: repeat(auto-fit, minmax(240px, 1fr))">
      <button
        v-for="s in scenarios"
        :key="s.id"
        type="button"
        class="text-left"
        :style="{ background: form.scenario.type === s.id ? '#0c2547' : '#1f1f23', border: '1px solid ' + (form.scenario.type === s.id ? '#3b82f6' : '#3f3f46'), borderRadius: '12px', padding: '14px 16px', cursor: 'pointer', color: 'inherit' }"
        @click="pickScenario(s)"
      >
        <div class="flex items-center gap-2.5 mb-2">
          <span class="icon-chip" :style="{ background: (LOOK[s.id]?.color ?? '#a1a1aa') + '24', color: LOOK[s.id]?.color }">
            <component :is="LOOK[s.id]?.icon ?? Zap" :size="17" />
          </span>
          <b>{{ s.title }}</b>
          <CheckCircle2 v-if="form.scenario.type === s.id" :size="17" color="#3b82f6" class="ml-auto" />
        </div>
        <div class="lbl">{{ s.summary }}</div>
      </button>
    </div>

    <div v-if="current" class="mt-5 grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(260px, 1fr))">
      <div class="info-block" style="border-color: #f59e0b">
        <div class="info-head" style="color: #f59e0b"><Lightbulb :size="14" />What it simulates</div>
        <div class="text-sm" style="line-height: 1.55">{{ current.simulates }}</div>
      </div>
      <div class="info-block" style="border-color: #3b82f6">
        <div class="info-head" style="color: #60a5fa"><ListChecks :size="14" />What the worker does</div>
        <ul class="info-list"><li v-for="d in current.does" :key="d"><Check :size="14" color="#60a5fa" />{{ d }}</li></ul>
      </div>
      <div class="info-block" style="border-color: #10b981">
        <div class="info-head" style="color: #34d399"><Eye :size="14" />What you will see in the results</div>
        <ul class="info-list"><li v-for="w in current.watch" :key="w"><Check :size="14" color="#34d399" />{{ w }}</li></ul>
      </div>
    </div>
    <label class="field mt-4" v-if="current && current.param" style="max-width: 300px">{{ current.param.label }}
      <input class="input" type="number" :min="current.param.min" v-model.number="form.scenario.param" />
      <span class="lbl">{{ current.param.hint }}</span>
    </label>

    <div class="mt-4">
      <button class="btn" @click="showCode = !showCode">{{ showCode ? 'Hide the code' : 'Show the code of this step' }}</button>
      <router-link to="/code" class="lbl ml-3 hover:underline">Open the code page</router-link>
    </div>
    <div v-if="showCode && code" class="mt-3">
      <div class="flex gap-2 mb-2 flex-wrap">
        <button v-for="l in LANGS" :key="l.id" class="btn" :class="codeLang === l.id ? 'btn-primary' : ''" @click="codeLang = l.id">{{ l.name }}</button>
      </div>
      <div class="lbl mb-1">The step that handles each message. Only this part changes between scenarios.</div>
      <CodeBlock :code="code[codeLang].scenario" :title="langName(codeLang) + ' · scenario'" />
    </div>
  </div>
</template>
