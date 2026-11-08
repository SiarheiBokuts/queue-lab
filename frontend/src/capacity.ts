/** Can the workers keep up with the senders? A rough estimate shown before the run starts. */
import { duration } from './health'

export interface Capacity { ok: boolean; maxRate: number; text: string }

/**
 * A worker handles `concurrency` messages at the same time, each in its own lane, so while one message waits (a
 * database, an API) the others are processed: about concurrency x 1000 / delay messages per second per worker.
 * With ordering "key" one user always uses one lane, so a worker cannot use more lanes than it has users.
 * Workers beyond the partition count have nothing to do. Returns null when there is no delay to reason about.
 */
export function workerCapacity(
  delayMs: number, workers: number, partitions: number, concurrency: number,
  ordering: string, users: number, messages: number, sendRate: number,
): Capacity | null {
  if (delayMs <= 0 || workers <= 0) return null
  const busy = Math.min(workers, partitions) // a partition is read by one worker, so extra workers have nothing to do
  const lanes = Math.max(1, concurrency)
  const usable = ordering === 'key' ? Math.max(1, Math.min(lanes, Math.floor(users / busy))) : lanes
  const maxRate = (busy * usable * 1000) / delayMs
  const sends = sendRate > 0 ? sendRate : Infinity
  if (maxRate >= sends) return { ok: true, maxRate, text: `The workers can handle up to about ${Math.round(maxRate).toLocaleString()} msg/s.` }
  const drain = messages / maxRate
  const idle = workers > partitions ? ` ${workers - partitions} worker(s) stay idle: there are only ${partitions} partitions.` : ''
  const few = ordering === 'key' && usable < lanes ? ` Only ${users.toLocaleString()} users share ${busy} workers, so a worker can use at most ${usable} lanes: raise "Different users".` : ''
  const how = usable > 1 ? `${usable} messages at the same time per worker, ${delayMs} ms each` : `one message at a time, ${delayMs} ms each`
  return {
    ok: false,
    maxRate,
    text: `The workers handle at most about ${Math.round(maxRate).toLocaleString()} msg/s (${how}), so the queue will grow and processing all ${messages.toLocaleString()} messages takes about ${duration(drain)}.${idle}${few} Use fewer messages, a smaller delay, or raise "At the same time per worker".`,
  }
}
