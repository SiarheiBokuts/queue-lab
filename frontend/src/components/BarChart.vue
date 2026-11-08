<script setup lang="ts">
import { computed } from 'vue'
import VChart from 'vue-echarts'
import { barOption } from '../chart'

const props = withDefaults(defineProps<{ labels: string[]; counts: number[]; height?: number; color?: string }>(), { height: 170 })
const option = computed(() => barOption(props.labels, props.counts, props.color))
</script>

<template>
  <VChart :option="option" autoresize :style="{ height: height + 'px', width: '100%' }" />
</template>
