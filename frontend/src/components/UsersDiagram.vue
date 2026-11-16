<script setup lang="ts">
/** Two workers and eight users: a user is always in one partition, a partition is read by one worker. */
const workers = [
  {
    name: 'Worker 1',
    partitions: [
      { n: 0, users: '0, 4' },
      { n: 1, users: '1, 5' },
    ],
  },
  {
    name: 'Worker 2',
    partitions: [
      { n: 2, users: '2, 6' },
      { n: 3, users: '3, 7' },
    ],
  },
]
</script>

<template>
  <div>
    <div class="grid gap-2" style="grid-template-columns: repeat(auto-fit, minmax(220px, 1fr))">
      <div v-for="w in workers" :key="w.name" class="worker">
        <b>{{ w.name }}</b>
        <div v-for="p in w.partitions" :key="p.n" class="part">
          partition {{ p.n }}<span class="lbl"> · users {{ p.users }}</span>
        </div>
      </div>
    </div>
    <div class="lbl mt-2">
      Example with 4 partitions, 2 workers and 8 users. A user always lands in one partition and a partition is read by
      one worker, so one user is never handled by two workers at once. Inside a worker the messages of one user go one
      after another.
    </div>
  </div>
</template>

<style scoped>
.worker {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px 12px;
  border-radius: 10px;
  border: 1px solid #3f3f46;
  background: #1b1b1f;
}
.part {
  padding: 6px 10px;
  border-radius: 8px;
  background: #0c2547;
  border: 1px solid #1d4ed8;
  font-size: 13px;
}
</style>
