<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import { heatOption } from '../chart'

const props = defineProps<{ rows: number[][]; partitions: number }>()
const height = computed(() => Math.max(90, props.partitions * 22 + 12))
const option = computed(() => heatOption(props.rows, props.partitions))
</script>

<template>
  <VChart v-if="rows.length" :option="option" autoresize :style="{ height: height + 'px', width: '100%' }" />
  <div v-else class="lbl" style="padding: 24px 0">The messages waiting in each partition appear here while a run is in progress.</div>
</template>
