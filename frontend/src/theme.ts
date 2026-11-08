/** Dark or light: remembered in the browser, applied as data-theme on the page. */
import { ref } from 'vue'

export type Theme = 'dark' | 'light'
const KEY = 'queuelab-theme'

function read(): Theme {
  try {
    if (localStorage.getItem(KEY) === 'light') return 'light'
  } catch { /* private window or blocked storage: use the default */ }
  return 'dark' // also what an old "system" choice becomes
}

/** The theme shown. Charts read this, so they redraw when it changes. */
export const theme = ref<Theme>(read())

function apply() {
  document.documentElement.dataset.theme = theme.value
}

export function initTheme() {
  apply()
}

/** The other theme. */
export function nextTheme(current: Theme): Theme {
  return current === 'dark' ? 'light' : 'dark'
}

export function toggleTheme() {
  theme.value = nextTheme(theme.value)
  try { localStorage.setItem(KEY, theme.value) } catch { /* not remembered, still applied */ }
  apply()
}
