/** Shared number formatting, so the same value looks the same on every screen. */

/** The latency histogram stops here: the highest value it can report, meaning "this long or longer". */
export const LATENCY_CAP_MS = 10_000

const trim = (x: number, digits: number) => String(Number(x.toFixed(digits)))

/**
 * A time given in milliseconds, in the unit that reads best: 0.5 ms, 850 ms, 2.4 s, 1 min 20 s.
 * With `cap` (the default) a value at the measurement limit is shown as "≥ 10 s"; axes pass cap = false.
 */
export function fmtMs(ms: number | null | undefined, cap = true): string {
  if (ms == null || Number.isNaN(ms)) return '-'
  if (cap && ms === LATENCY_CAP_MS) return `≥ ${LATENCY_CAP_MS / 1000} s`
  if (ms < 1) return `${trim(ms, 2)} ms`
  if (ms < 10) return `${trim(ms, 1)} ms`
  if (ms < 1000) return `${Math.round(ms)} ms`
  if (ms < 10_000) return `${trim(ms / 1000, 1)} s`
  if (ms < 60_000) return `${Math.round(ms / 1000)} s`
  const minutes = Math.floor(ms / 60_000)
  const seconds = Math.round((ms - minutes * 60_000) / 1000)
  return seconds ? `${minutes} min ${seconds} s` : `${minutes} min`
}

/** 1234567 -> "1 234 567": groups of three digits separated by a (non-breaking) space, readable at a glance. */
export function groupDigits(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return ''
  return String(Math.trunc(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ' ')
}

/** "1 234 567" or "1,234,567" or "12abc" -> the number made of the digits in it (0 when there are none). */
export function digitsToNumber(text: string): number {
  const digits = text.replace(/\D/g, '')
  return digits ? Number(digits) : 0
}
