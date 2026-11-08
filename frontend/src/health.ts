/** Live indicators for a running test: is it keeping up, how long until done, and what is the bottleneck. */

import { LATENCY_CAP_MS } from './format'

export type Level = 'ok' | 'info' | 'warn' | 'bad'
/** The card always shows the same three slots, so it never changes height while the numbers move. */
export type Slot = 'catchup' | 'trend' | 'limit'
export interface Indicator { slot: Slot; level: Level; title: string; text: string }

const WINDOW = 10 // samples used for trends (about 5 seconds)
/** A backlog smaller than this many seconds of work is normal and not worth a warning. */
const SMALL_BACKLOG_S = 2

/** "2 h 15 min", "40 s": a duration people can read. */
export function duration(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return '-'
  if (seconds < 90) return `${Math.round(seconds)} s`
  if (seconds < 5400) return `${Math.round(seconds / 60)} min`
  const h = Math.floor(seconds / 3600)
  return `${h} h ${Math.round((seconds - h * 3600) / 60)} min`
}

const avg = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : 0)
const n = (x: number) => Math.round(x).toLocaleString()

interface Point { send: number; recv: number; lag: number; p99: number | null }
interface Box { role: string; cpu: number }

/** Build the three indicators from the recent samples and the current containers. Empty until there is data. */
export function liveIndicators(history: Point[], containers: Box[], workers: number): Indicator[] {
  if (history.length < 3) return []
  const recent = history.slice(-WINDOW)
  const last = recent[recent.length - 1] as Point
  const first = recent[0] as Point
  const recv = avg(recent.map((p) => p.recv))
  const send = avg(recent.map((p) => p.send))
  const lagChange = last.lag - first.lag
  const backlogSeconds = recv > 0 ? last.lag / recv : last.lag > 0 ? Infinity : 0
  const small = backlogSeconds < SMALL_BACKLOG_S

  // 1. How long until everything waiting is handled.
  let catchup: Indicator
  if (last.lag <= 0) catchup = { slot: 'catchup', level: 'ok', title: 'Nothing is waiting', text: 'Every message that arrived is already handled.' }
  else if (recv <= 0) catchup = { slot: 'catchup', level: 'bad', title: 'Nothing is being processed', text: `${n(last.lag)} messages wait but the workers handle none right now.` }
  else if (small) catchup = { slot: 'catchup', level: 'ok', title: 'Keeping up', text: `${n(last.lag)} waiting, handled within ${Math.max(1, Math.ceil(backlogSeconds))} s at ${n(recv)} msg/s.` }
  else catchup = { slot: 'catchup', level: backlogSeconds > 300 ? 'bad' : backlogSeconds > 60 ? 'warn' : 'ok', title: `About ${duration(backlogSeconds)} to catch up`, text: `${n(last.lag)} messages wait, the workers handle ${n(recv)} msg/s.` }

  // 2. Is the backlog growing or shrinking (only when it is big enough to matter).
  let trend: Indicator
  if (small) trend = { slot: 'trend', level: 'ok', title: 'Backlog is small', text: 'It stays within a couple of seconds of work.' }
  else if (lagChange > last.lag * 0.05) trend = { slot: 'trend', level: 'warn', title: 'Backlog is growing', text: 'Messages arrive faster than the workers handle them.' }
  else if (lagChange < -last.lag * 0.05) trend = { slot: 'trend', level: 'ok', title: 'Backlog is shrinking', text: 'The workers are catching up.' }
  else trend = { slot: 'trend', level: 'info', title: 'Backlog is steady', text: 'Messages arrive about as fast as they are handled.' }

  // 3. What limits the speed right now (the most important finding wins).
  const cpus = containers.filter((c) => c.role === 'consumer').map((c) => c.cpu)
  const workerCpu = avg(cpus)
  let limit: Indicator = { slot: 'limit', level: 'ok', title: 'Nothing limits the speed', text: 'The workers keep up with the senders.' }
  if (last.p99 != null && last.p99 >= LATENCY_CAP_MS) {
    limit = { slot: 'limit', level: 'bad', title: 'Latency is above the scale', text: `p99 is at the ${LATENCY_CAP_MS / 1000} s limit of the measurement: the real delay is longer.` }
  } else if (workers > 0 && cpus.length > 0 && cpus.length < workers) {
    limit = { slot: 'limit', level: 'warn', title: 'Some workers are not running', text: `${cpus.length} of ${workers} workers are up.` }
  } else if (!small && send > 0 && recv > 0 && send > recv * 2) {
    limit = { slot: 'limit', level: 'warn', title: 'The sender outruns the workers', text: `Sending ${n(send)} msg/s, processing ${n(recv)} msg/s.` }
  } else if (!small && cpus.length && workerCpu > 80) {
    limit = { slot: 'limit', level: 'warn', title: 'Workers are CPU-bound', text: `Workers use ${workerCpu.toFixed(0)}% of a core. Add workers or try a faster language.` }
  } else if (!small && cpus.length && workerCpu < 30) {
    limit = { slot: 'limit', level: 'info', title: 'Workers are waiting, not computing', text: `Workers use only ${workerCpu.toFixed(0)}% of a core: the limit is the processing step (a delay or I/O). More workers or more at the same time per worker will help.` }
  }
  return [catchup, trend, limit]
}
