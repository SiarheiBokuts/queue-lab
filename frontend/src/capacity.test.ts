import { describe, expect, it } from 'vitest'
import { workerCapacity } from './capacity'

describe('workerCapacity', () => {
  it('has nothing to say without a delay', () => {
    expect(workerCapacity(0, 5, 6, 1, 'key', 1000, 1000, 0)).toBeNull()
  })

  it('one at a time: the ceiling is the busy workers over the delay', () => {
    const c = workerCapacity(15, 6, 10, 1, 'key', 1000, 5_000_000, 0)
    expect(c?.ok).toBe(false)
    expect(Math.round(c?.maxRate ?? 0)).toBe(400)
    expect(c?.text).toContain('3 h')
  })

  it('handling many at once raises the ceiling by that factor', () => {
    expect(Math.round(workerCapacity(15, 6, 10, 100, 'none', 1000, 5_000_000, 0)?.maxRate ?? 0)).toBe(40_000)
    expect(Math.round(workerCapacity(15, 6, 10, 100, 'key', 100_000, 5_000_000, 0)?.maxRate ?? 0)).toBe(40_000)
  })

  it('per user order cannot use more lanes than there are users', () => {
    const c = workerCapacity(15, 5, 5, 100, 'key', 50, 1_000_000, 0) // 10 users per worker
    expect(Math.round(c?.maxRate ?? 0)).toBe(3_333)
    expect(c?.text).toContain('raise "Different users"')
  })

  it('is fine when the senders are slower than the workers', () => {
    expect(workerCapacity(15, 10, 10, 1, 'key', 1000, 1000, 300)?.ok).toBe(true)
  })

  it('counts only as many workers as there are partitions', () => {
    const c = workerCapacity(10, 8, 4, 1, 'key', 1000, 1000, 0)
    expect(Math.round(c?.maxRate ?? 0)).toBe(400)
    expect(c?.text).toContain('4 worker(s) stay idle')
  })
})
