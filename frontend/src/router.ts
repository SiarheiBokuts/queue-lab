import { createRouter, createWebHistory } from 'vue-router'
import DashboardPage from './pages/DashboardPage.vue'
import SetupPage from './pages/SetupPage.vue'
import ContainerPage from './pages/ContainerPage.vue'
import RunsPage from './pages/RunsPage.vue'
import RunDetailPage from './pages/RunDetailPage.vue'
import SettingsPage from './pages/SettingsPage.vue'
import CodePage from './pages/CodePage.vue'
import ComparePage from './pages/ComparePage.vue'

export default createRouter({
  history: createWebHistory(),
  routes: [
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
