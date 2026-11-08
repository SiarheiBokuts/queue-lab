<script setup lang="ts">
/** How a worker handles messages, the same in every language: read a batch, group by user, handle, commit. */
const steps = [
  { title: 'Read a batch', text: 'up to N messages that have already arrived' },
  { title: 'Group by user', text: 'one group per user, in the order sent' },
  { title: 'Handle', text: 'groups run at the same time, a group one message after another' },
  { title: 'Commit', text: 'when every group is done, then the next batch' },
]
</script>

<template>
  <div class="diagram" role="img" aria-label="Read a batch, group by user, handle the groups at the same time, commit, repeat">
    <template v-for="(s, i) in steps" :key="s.title">
      <div class="step"><b>{{ i + 1 }}. {{ s.title }}</b><span class="lbl">{{ s.text }}</span></div>
      <span v-if="i < steps.length - 1" class="arrow">→</span>
    </template>
  </div>
</template>

<style scoped>
.diagram { display: flex; flex-wrap: wrap; align-items: stretch; gap: 8px; }
.step { flex: 1 1 150px; display: flex; flex-direction: column; gap: 4px; padding: 10px 12px; border-radius: 10px; border: 1px solid #3f3f46; background: #1b1b1f; }
.arrow { align-self: center; color: #71717a; }
</style>
