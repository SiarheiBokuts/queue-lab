import { createRouter, createWebHistory } from 'vue-router'
import DashboardPage from './pages/DashboardPage.vue'
import SetupPage from './pages/SetupPage.vue'
import ContainerPage from './pages/ContainerPage.vue'
import RunsPage from './pages/RunsPage.vue'
import RunDetailPage from './pages/RunDetailPage.vue'
import SettingsPage from './pages/SettingsPage.vue'
import CodePage from './pages/CodePage.vue'
import ComparePage from './pages/ComparePage.vue'
import WelcomePage from './pages/WelcomePage.vue'
import { useLive } from './stores/live'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/welcome', component: WelcomePage },
    { path: '/', component: DashboardPage },
    { path: '/setup', component: SetupPage },
    { path: '/containers/:name', component: ContainerPage },
    { path: '/runs', component: RunsPage },
    { path: '/runs/:id', component: RunDetailPage },
    { path: '/code', component: CodePage },
    { path: '/compare/:ids', component: ComparePage },
    { path: '/settings', component: SettingsPage },
  ],
})

// The first visit starts on the welcome page, where the memory budget is chosen.
router.beforeEach(async (to) => {
  const live = useLive()
  if (!live.loaded) await live.refresh()
  if (live.loaded && !live.settings.welcomed && to.path !== '/welcome') return '/welcome'
  return true
})

export default router
