<script setup lang="ts">
/** Step 4: the senders, the containers that put the messages into the queue. */
import { useRunForm } from '../../composables/useRunForm'
import Hint from '../Hint.vue'
import LangPicker from '../LangPicker.vue'

const { form, compare, producerFollows } = useRunForm()
</script>

<template>
  <div class="card flex flex-col gap-3">
    <h2>4. Senders (producers)</h2>
    <div v-if="!(compare && producerFollows)">
      <div class="lbl mb-1.5">Language{{ compare ? ' (the same sender for every run)' : '' }}</div>
      <LangPicker v-model="form.producer.lang" />
    </div>
    <div v-else class="lbl">The sender follows the workers' language.</div>
    <div class="grid grid-cols-2 gap-3">
      <label class="field">How many (1-8)
        <Hint text="Several sender containers share the load. Use more than one when a single sender cannot reach the rate you want." />
        <input class="input" type="number" min="1" max="8" v-model.number="form.producer.count" />
      </label>
      <label class="field">Memory each (MB)
        <input class="input" type="number" min="128" step="64" v-model.number="form.producer.memory_mb" />
      </label>
    </div>
  </div>
</template>
