/**
 * An in-app confirmation dialog. The browser's own confirm() is not used: embedded browsers and some settings disable
 * it, which made delete buttons silently do nothing. Usage: `if (!(await askConfirm({ title: 'Delete?' }))) return`.
 */
import { reactive } from 'vue'

export interface ConfirmOptions {
  title: string
  /** What will happen, in plain words. */
  text?: string
  okLabel?: string
  /** Red button for something that cannot be undone. */
  danger?: boolean
}

export const confirmState = reactive({
  open: false,
  title: '',
  text: '',
  okLabel: 'OK',
  danger: false,
  resolve: null as null | ((ok: boolean) => void),
})

export function askConfirm(options: ConfirmOptions): Promise<boolean> {
  return new Promise((resolve) => {
    confirmState.title = options.title
    confirmState.text = options.text ?? ''
    confirmState.okLabel = options.okLabel ?? 'OK'
    confirmState.danger = options.danger ?? false
    confirmState.resolve = resolve
    confirmState.open = true
  })
}

export function answerConfirm(ok: boolean): void {
  confirmState.open = false
  confirmState.resolve?.(ok)
  confirmState.resolve = null
}