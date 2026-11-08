/** Can the workers keep up with the senders? A rough estimate shown before the run starts. */
import { duration } from './health'

export interface Capacity { ok: boolean; maxRate: number; text: string }

/**
 * A worker reads a batch of `batch` messages and handles the messages of different users at the same time, so while
 * one message waits (a database, an API) the others are processed: about batch x 1000 / delay messages per second per
 * worker. The messages of one user always go one after another, so a worker cannot handle more at once than it has
 * users. Workers beyond the partition count have nothing to do. Returns null when there is no delay to reason about.
 */
export function workerCapacity(
  delayMs: number, workers: number, partitions: number, batch: number,
  users: number, messages: number, sendRate: number,
): Capacity | null {
  if (delayMs <= 0 || workers <= 0) return null
  const busy = Math.min(workers, partitions) // a partition is read by one worker, so extra workers have nothing to do
  const size = Math.max(1, batch)
  const usable = Math.max(1, Math.min(size, Math.floor(users / busy))) // at most one message per user at a time
  const maxRate = (busy * usable * 1000) / delayMs
  const sends = sendRate > 0 ? sendRate : Infinity
  if (maxRate >= sends) return { ok: true, maxRate, text: `The workers can handle up to about ${Math.round(maxRate).toLocaleString()} msg/s.` }
  const drain = messages / maxRate
  const idle = workers > partitions ? ` ${workers - partitions} worker(s) stay idle: there are only ${partitions} partitions.` : ''
  const few = usable < size ? ` Only ${users.toLocaleString()} users share ${busy} workers, and one user is handled one message at a time, so a worker handles at most ${usable} at once: raise "Different users".` : ''
  const how = usable > 1 ? `${usable} messages at the same time per worker, ${delayMs} ms each` : `one message at a time, ${delayMs} ms each`
  return {
    ok: false,
    maxRate,
    text: `The workers handle at most about ${Math.round(maxRate).toLocaleString()} msg/s (${how}), so the queue will grow and processing all ${messages.toLocaleString()} messages takes about ${duration(drain)}.${idle}${few} Use fewer messages, a smaller delay, or raise "At the same time per worker".`,
  }
}
