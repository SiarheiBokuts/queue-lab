/** Short messages in the corner ("Run deleted"), so every action visibly does something. */
import { reactive } from 'vue'

export interface Toast {
  id: number
  text: string
  kind: 'ok' | 'error'
}

export const toasts = reactive<Toast[]>([])
let next = 1

export function notify(text: string, kind: Toast['kind'] = 'ok'): void {
  const id = next++
  toasts.push({ id, text, kind })
  setTimeout(() => {
    const i = toasts.findIndex((t) => t.id === id)
    if (i >= 0) toasts.splice(i, 1)
  }, kind === 'error' ? 8000 : 4000)
}