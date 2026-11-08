<script setup lang="ts">
/** "What do these words mean?" for someone who has never used Kafka. Short, in the order a message travels. */
import { BookOpen, Columns3, Cog, Gauge, Hourglass, Layers, Package, Send, Timer, Users } from '@lucide/vue'

const terms = [
  { icon: Layers, color: '#60a5fa', word: 'Queue (topic)', text: 'A named list of messages in Kafka. Senders put messages in, workers take them out.' },
  { icon: Columns3, color: '#a78bfa', word: 'Partition', text: 'A slice of the queue. Each partition is read by one worker at a time, so more partitions allow more workers to work in parallel.' },
  { icon: Send, color: '#34d399', word: 'Sender (producer)', text: 'A program that puts messages into the queue. Here it is a container you can run in Python, Node.js, Java, Go or Rust.' },
  { icon: Cog, color: '#f59e0b', word: 'Worker (consumer)', text: 'A program that takes messages out and does something with them, like saving to a database. Workers share the partitions.' },
  { icon: Users, color: '#f472b6', word: 'User and order', text: 'Every message belongs to a user, like a payment of a customer. Messages of one user stay in one partition and are handled in order.' },
  { icon: Gauge, color: '#10b981', word: 'Throughput', text: 'How many messages per second are really processed. The higher the better.' },
  { icon: Timer, color: '#38bdf8', word: 'Latency (p50, p99)', text: 'How long a message waited from being sent to being processed. p50 is the typical one, p99 is the slow tail: 99 of 100 messages were faster.' },
  { icon: Hourglass, color: '#fb923c', word: 'Messages waiting (lag)', text: 'Messages that are in the queue but not yet taken by a worker. If this keeps growing, the workers are too slow.' },
  { icon: Package, color: '#2dd4bf', word: 'Compression', text: 'Makes messages smaller on the network for a little CPU. It only helps when the data repeats; random data does not shrink.' },
]
</script>

<template>
  <details class="card">
    <summary style="cursor: pointer" class="flex items-center gap-2"><BookOpen :size="16" color="#60a5fa" /><b>New to Kafka? What the words mean</b></summary>
    <dl class="grid gap-3 mt-4" style="grid-template-columns: repeat(auto-fit, minmax(300px, 1fr))">
      <div v-for="t in terms" :key="t.word" class="term" :style="{ borderColor: t.color }">
        <dt class="flex items-center gap-2 text-sm font-semibold">
          <span class="icon-chip" :style="{ background: t.color + '22', color: t.color, width: '26px', height: '26px' }"><component :is="t.icon" :size="15" /></span>{{ t.word }}
        </dt>
        <dd class="lbl" style="line-height: 1.55; margin: 6px 0 0 34px">{{ t.text }}</dd>
      </div>
    </dl>
  </details>
</template>

<style scoped>
.term { background: #151518; border-left: 3px solid; border-radius: 4px 11px 11px 4px; padding: 11px 14px; }
</style>
