<script setup lang="ts">
/** Step 3: the workers, the containers that receive the messages. */
import { langName } from '../../langs'
import { useRunForm } from '../../composables/useRunForm'
import Hint from '../Hint.vue'
import LangPicker from '../LangPicker.vue'
import UsersDiagram from '../UsersDiagram.vue'

const { form, compare, compareLangs, plan } = useRunForm()
</script>

<template>
  <div class="card flex flex-col gap-3">
    <h2>3. Workers (they receive the messages)</h2>
    <div v-if="!compare"><div class="lbl mb-1.5">Language</div><LangPicker v-model="form.workers.lang" /></div>
    <div v-else class="lbl">The language changes from run to run: {{ compareLangs.map(langName).join(', ') || 'none selected' }}.</div>
    <div class="grid grid-cols-2 gap-3">
      <label class="field">Count (1-32)
        <input class="input" type="number" min="1" max="32" v-model.number="form.workers.count" />
      </label>
      <label class="field">Memory each (MB)
        <input class="input" type="number" min="128" step="64" v-model.number="form.workers.memory_mb" />
      </label>
      <label class="field">At the same time per worker
        <Hint text="The batch size: a worker reads up to this many messages, groups them by user and handles the groups at the same time. Messages of one user are always handled one after another, like payments of one customer in production; messages of different users run side by side. While one message waits for a database or an API, the others are processed. 1 means strictly one message at a time. The next batch starts when the whole batch is done. Think of each one as a call to a database: 5 workers with 100 each means up to 500 calls at once, while a real service usually keeps 10 to 50 connections to its database (a pool)." />
        <input class="input" type="number" min="1" max="1024" v-model.number="form.workers.concurrency" />
      </label>
    </div>
    <details class="lbl">
      <summary style="cursor: pointer">How workers and users are split</summary>
      <UsersDiagram class="mt-2" />
    </details>
    <div v-if="plan && plan.idle_workers" class="text-amber-400 text-xs">{{ plan.idle_workers }} worker(s) will stay idle: there are more workers than partitions.</div>
  </div>
</template>
