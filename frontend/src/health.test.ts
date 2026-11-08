import { describe, expect, it } from 'vitest'
import { duration, liveIndicators } from './health'

const pt = (lag: number, recv = 100, send = 100, p99: number | null = 10) => ({ lag, recv, send, p99 })
const series = (lags: number[], recv = 100, send = 100, p99: number | null = 10) => lags.map((l) => pt(l, recv, send, p99))
const bySlot = (xs: { slot: string }[], slot: string) => xs.find((x) => x.slot === slot) as { slot: string; level: string; title: string; text: string }

describe('duration', () => {
  it('reads naturally', () => {
    expect(duration(40)).toBe('40 s')
    expect(duration(600)).toBe('10 min')
    expect(duration(8100)).toBe('2 h 15 min')
    expect(duration(-1)).toBe('-')
  })
})

describe('liveIndicators', () => {
  it('says nothing before there is enough data', () => {
    expect(liveIndicators(series([1, 2]), [], 1)).toEqual([])
  })

  it('always returns the same three slots, whatever the numbers are', () => {
    for (const lags of [[0, 0, 0], [5000, 5000, 5000], [1000, 3000, 9000]]) {
      expect(liveIndicators(series(lags), [], 1).map((i) => i.slot)).toEqual(['catchup', 'trend', 'limit'])
    }
  })

  it('a backlog of under two seconds of work is normal: no warnings', () => {
    const r = liveIndicators(series([20_900, 21_000, 20_800], 44_000, 44_000), [{ role: 'consumer', cpu: 15 }], 1)
    expect(bySlot(r, 'catchup').title).toBe('Keeping up')
    expect(bySlot(r, 'trend').title).toBe('Backlog is small')
    expect(bySlot(r, 'limit').level).toBe('ok')
  })

  it('estimates the time to catch up for a big backlog', () => {
    const r = liveIndicators(series([5000, 5000, 5000, 5000]), [], 1)
    expect(bySlot(r, 'catchup').title).toContain('50 s')
  })

  it('flags a growing backlog and a sender that outruns the workers', () => {
    const r = liveIndicators(series([1000, 3000, 6000, 10000], 100, 1000), [], 1)
    expect(bySlot(r, 'trend').title).toBe('Backlog is growing')
    expect(bySlot(r, 'limit').title).toBe('The sender outruns the workers')
  })

  it('blames waiting workers when their CPU is low and the backlog is big', () => {
    const r = liveIndicators(series([5000, 5000, 5000]), [{ role: 'consumer', cpu: 1 }], 1)
    expect(bySlot(r, 'limit').title).toBe('Workers are waiting, not computing')
  })

  it('blames the CPU when workers are busy', () => {
    const r = liveIndicators(series([5000, 5000, 5000]), [{ role: 'consumer', cpu: 95 }], 1)
    expect(bySlot(r, 'limit').title).toBe('Workers are CPU-bound')
  })

  it('warns when latency hits the scale limit', () => {
    const r = liveIndicators(series([10, 10, 10], 100, 100, 10000), [], 1)
    expect(bySlot(r, 'limit').title).toBe('Latency is above the scale')
  })

  it('reports nothing processed while messages wait', () => {
    const r = liveIndicators(series([5000, 5000, 5000], 0, 0), [], 1)
    expect(bySlot(r, 'catchup').title).toBe('Nothing is being processed')
  })
})
