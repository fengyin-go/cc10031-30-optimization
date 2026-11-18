import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import ProxyGuardView from '@/views/ProxyGuardView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/proxy_guard', name: 'proxy_guard', component: ProxyGuardView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
