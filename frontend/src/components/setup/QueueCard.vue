<script setup lang="ts">
/** Step 2: the queue (a Kafka topic) and its partitions. */
import { useRunForm } from '../../composables/useRunForm'
import Hint from '../Hint.vue'

const { form, live } = useRunForm()
</script>

<template>
  <div class="card flex flex-col gap-3">
    <h2>2. Queue</h2>
    <label class="field"
      >Name
      <input class="input" v-model="form.queue" list="existing-queues" />
      <datalist id="existing-queues"><option v-for="q in live.queues" :key="q.name" :value="q.name" /></datalist>
    </label>
    <label class="field"
      >Partitions
      <Hint
        text="A partition is a slice of the queue. Each partition is read by only one worker of the group at a time, so more partitions allow more workers to work in parallel."
      />
      <input class="input" type="number" min="1" v-model.number="form.partitions" />
    </label>
  </div>
</template>
