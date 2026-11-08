<script setup lang="ts">
/**
 * "New run": the form that sets up a test. The state and the rules live in composables/useRunForm.ts; every numbered
 * section is its own component in components/setup/.
 */
import { provideRunForm } from '../composables/useRunForm'
import Glossary from '../components/Glossary.vue'
import PageHeader from '../components/PageHeader.vue'
import LoadCard from '../components/setup/LoadCard.vue'
import ModeCard from '../components/setup/ModeCard.vue'
import QueueCard from '../components/setup/QueueCard.vue'
import ScenarioCard from '../components/setup/ScenarioCard.vue'
import SendersCard from '../components/setup/SendersCard.vue'
import StartCard from '../components/setup/StartCard.vue'
import WorkersCard from '../components/setup/WorkersCard.vue'

const { presets } = provideRunForm()
</script>

<template>
  <PageHeader
    title="New run"
    subtitle="Set up a queue, the senders and the workers, pick what the workers do with each message, and start. You can run one test, or the same test in several languages one after another and compare them."
    :crumbs="[{ label: 'Dashboard', to: '/' }, { label: 'New run' }]"
  />

  <Glossary />

  <div class="card">
    <div class="lbl mb-2">Start from a ready-made setup, then change anything you like.</div>
    <div class="flex gap-3 flex-wrap">
      <button v-for="p in presets" :key="p.name" type="button" class="btn" :title="p.note" @click="p.apply()">{{ p.name }}</button>
    </div>
  </div>

  <ModeCard />
  <div class="grid gap-4" style="grid-template-columns: repeat(auto-fit, minmax(320px, 1fr))">
    <QueueCard />
    <WorkersCard />
    <SendersCard />
  </div>
  <LoadCard />
  <ScenarioCard />
  <StartCard />
</template>
