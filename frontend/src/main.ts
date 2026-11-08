import { createApp } from 'vue'
import { createPinia } from 'pinia'
import App from './App.vue'
import router from './router'
import 'floating-vue/dist/style.css'
import './style.css'
import './echarts'

createApp(App).use(createPinia()).use(router).mount('#app')
