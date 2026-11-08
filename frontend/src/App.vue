<script setup lang="ts">
import { onMounted } from 'vue'
import { Code, History, LayoutDashboard, Play, Settings } from '@lucide/vue'
import { useLive } from './stores/live'
import ConfirmDialog from './components/ConfirmDialog.vue'
import ToastHost from './components/ToastHost.vue'
import RunStatusBar from './components/RunStatusBar.vue'
import { vTooltip } from 'floating-vue'

const live = useLive()
onMounted(() => live.start())

const nav = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboard, hint: 'Live view of the current run' },
  { to: '/setup', label: 'New run', icon: Play, hint: 'Set up and start a test' },
  { to: '/runs', label: 'Runs', icon: History, hint: 'Finished runs, reports and comparisons' },
  { to: '/code', label: 'Code', icon: Code, hint: 'The code that sends and processes the messages' },
  { to: '/settings', label: 'Settings', icon: Settings, hint: 'Memory budget, queues and cleanup' },
]
</script>

<template>
  <div class="flex min-h-screen">
    <aside class="w-56 shrink-0 border-r border-zinc-800 p-4 flex flex-col gap-1 sticky top-0 self-start h-screen overflow-y-auto">
      <div class="mb-5 px-1 flex items-center gap-2.5">
        <img src="/logo.svg" alt="" width="34" height="34" style="border-radius: 9px" />
        <div>
          <div class="text-lg font-semibold" style="line-height: 1.1">queue-lab</div>
          <div class="lbl">Kafka playground</div>
        </div>
      </div>
      <router-link
        v-for="n in nav"
        :key="n.to"
        :to="n.to"
        v-tooltip="{ content: n.hint, placement: 'right', distance: 10 }"
        class="px-3 py-2.5 rounded-lg text-sm text-zinc-300 hover:bg-zinc-800 flex items-center gap-2.5"
        active-class="!bg-zinc-800 !text-white"
        exact-active-class="!bg-zinc-800 !text-white"
      >
        <component :is="n.icon" :size="17" :stroke-width="2" />
        {{ n.label }}
      </router-link>
      <div class="mt-auto lbl px-1">
        <span class="badge" :class="live.connected ? 'ok' : 'bad'">{{ live.connected ? 'Live' : 'Reconnecting' }}</span>
        <div class="mt-2">Docker: {{ live.dockerOk ? 'connected' : 'not reachable' }}</div>
        <div v-if="live.version" class="mt-1" style="color: #71717a">queue-lab v{{ live.version }}</div>
      </div>
    </aside>
    <main class="flex-1 min-w-0 px-8 py-7 flex flex-col gap-5 mx-auto w-full" style="max-width: 1500px">
      <RunStatusBar />
      <div v-if="!live.dockerOk" class="card" style="border-color: #7f1d1d">
        <b>Docker is not reachable.</b> Start Docker Desktop, then reload this page. Container actions are unavailable until then.
      </div>
      <div v-for="w in live.warnings" :key="w" class="card" style="border-color: #78350f">{{ w }}</div>
      <router-view v-slot="{ Component, route }">
        <div :key="route.path" class="page flex flex-col gap-5">
          <component :is="Component" />
        </div>
      </router-view>
    </main>
  </div>
  <ConfirmDialog />
  <ToastHost />
</template>
