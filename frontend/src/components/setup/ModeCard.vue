<script setup lang="ts">
/** Step 1: one test, or the same test in several languages. */
import { LANGS } from '../../langs'
import { useRunForm } from '../../composables/useRunForm'
import LangIcon from '../LangIcon.vue'

const { compare, compareLangs, randomOrder, cooldown, producerFollows, toggleLang } = useRunForm()

/** The look of a choice card, selected or not. */
const cardStyle = (selected: boolean) => ({
  background: selected ? '#0c2547' : '#1f1f23',
  border: '1px solid ' + (selected ? '#3b82f6' : '#3f3f46'),
  borderRadius: '10px', padding: '12px 14px', cursor: 'pointer', color: 'inherit',
})
</script>

<template>
  <div class="card">
    <h2 class="mb-2">1. What do you want to do?</h2>
    <div class="grid gap-3" style="grid-template-columns: repeat(auto-fit, minmax(280px, 1fr))">
      <button type="button" class="text-left" :style="cardStyle(!compare)" @click="compare = false">
        <div class="font-semibold mb-1">One test</div>
        <div class="lbl">Run the setup below once, in the languages you choose for the senders and workers.</div>
      </button>
      <button type="button" class="text-left" :style="cardStyle(compare)" @click="compare = true">
        <div class="font-semibold mb-1">Compare languages</div>
        <div class="lbl">Run the same test once per language, one after the other with a pause, then see them side by side.</div>
      </button>
    </div>

    <div v-if="compare" class="mt-4">
      <div class="lbl mb-2">Languages to compare (pick two or more)</div>
      <div class="flex gap-2 flex-wrap mb-3">
        <button
          v-for="l in LANGS"
          :key="l.id"
          type="button"
          class="btn"
          :style="compareLangs.includes(l.id) ? { borderColor: l.color, background: '#1a1a1d' } : {}"
          :title="l.client"
          @click="toggleLang(l.id)"
        >
          <LangIcon :id="l.id" :size="14" style="display: inline-block; vertical-align: -2px; margin-right: 6px" />
          <span class="inline-block w-2 h-2 rounded-full mr-1.5" :style="{ background: l.color }" />{{ l.name }}
          <span v-if="!randomOrder && compareLangs.includes(l.id)" class="lbl"> · #{{ compareLangs.indexOf(l.id) + 1 }}</span>
        </button>
      </div>
      <div class="grid gap-3" style="grid-template-columns: repeat(auto-fit, minmax(240px, 1fr))">
        <label class="field">Pause between the runs (seconds)
          <input class="input" type="number" min="0" max="120" v-model.number="cooldown" />
        </label>
        <label class="flex items-start gap-2 text-sm">
          <input type="checkbox" v-model="randomOrder" class="mt-1" />
          <span>Random order<br /><span class="lbl">The order is drawn when you start, so no language always runs first on a cold machine. Switch off to run them in the order you picked.</span></span>
        </label>
        <label class="flex items-start gap-2 text-sm">
          <input type="checkbox" v-model="producerFollows" class="mt-1" />
          <span>Compare the senders too<br /><span class="lbl">The sender uses the same language as the workers in every run. Off: one sender language for all runs, chosen in step 4.</span></span>
        </label>
      </div>
      <div class="lbl mt-2">All Kafka settings are identical for every language. Only the language and its client library differ.</div>
    </div>
  </div>
</template>
