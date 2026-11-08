<script setup lang="ts">
/** All lab containers. The order and the column widths never change, so the numbers can update without the table jumping. */
import { computed } from 'vue'
import { fmt } from '../chart'
import { containerState } from '../containerState'
import { vTooltip } from 'floating-vue'
import LangIcon from './LangIcon.vue'

const props = defineProps<{ containers: any[] }>()
const ROLE_ORDER: Record<string, number> = { broker: 0, producer: 1, consumer: 2 }
const rows = computed(() =>
  [...props.containers].sort((a, b) => (ROLE_ORDER[a.role] ?? 9) - (ROLE_ORDER[b.role] ?? 9) || a.name.localeCompare(b.name, undefined, { numeric: true })))
</script>

<template>
  <div class="card">
    <h2 class="mb-2">Containers</h2>
    <div v-if="rows.length" style="overflow-x: auto">
      <table class="tbl tbl-fixed" style="min-width: 760px">
        <colgroup>
          <col style="width: 21%" /><col style="width: 15%" /><col style="width: 12%" /><col style="width: 8%" />
          <col style="width: 20%" /><col style="width: 9%" /><col style="width: 15%" />
        </colgroup>
        <thead>
          <tr><th>Name</th><th>Role</th><th>State</th><th>CPU</th><th>Memory</th><th>msg/s</th><th>Net in/out</th></tr>
        </thead>
        <tbody>
          <tr v-for="c in rows" :key="c.name">
            <td><router-link :to="'/containers/' + c.name" class="text-blue-400 hover:underline">{{ c.name.replace('queuelab-', '') }}</router-link></td>
            <td><span class="inline-flex items-center gap-1.5"><LangIcon v-if="c.lang" :id="c.lang" :size="14" />{{ c.role }}</span></td>
            <td><span v-tooltip="containerState(c).hint" class="badge" :class="containerState(c).tone">{{ containerState(c).label }}</span></td>
            <td :style="{ color: c.cpu > 80 ? '#fcd34d' : '' }">{{ c.cpu }}%</td>
            <td>
              <div class="bar"><i :style="{ width: Math.min(100, c.mem_pct) + '%', background: c.mem_pct > 90 ? '#f59e0b' : '' }" /></div>
              <span class="lbl">{{ c.mem_mb }} / {{ c.limit_mb }} MB</span>
            </td>
            <td>{{ c.msgs_s ? fmt(c.msgs_s) : '-' }}</td>
            <td class="lbl">{{ Math.round(c.rx_kbs) }} / {{ Math.round(c.tx_kbs) }} KB/s</td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-else class="lbl">No containers yet.</div>
  </div>
</template>
