import { describe, expect, it } from 'vitest'
import { containerRows, marksFrom, statsOf, windowRange } from './window'

const win = { send_start: 0.5, send_end: 1.5, last_processed: 3.5 }

describe('windowRange', () => {
  it('pads the window on both sides', () => {
    expect(windowRange(win)).toEqual([0, 4])
    expect(windowRange(win, 0)).toEqual([0.5, 3.5])
  })

  it('never starts before zero', () => {
    expect(windowRange({ send_start: 0.2, last_processed: 1 })?.[0]).toBe(0)
  })

  it('falls back to the end of sending while processing is unknown', () => {
    expect(windowRange({ send_start: 0, send_end: 2 })).toEqual([0, 2.5])
  })

  it('is null when nothing is known', () => {
    expect(windowRange(null)).toBeNull()
    expect(windowRange({})).toBeNull()
  })
})

describe('marksFrom', () => {
  it('names the three moments of the window', () => {
    expect(marksFrom(win).map((m) => [m.at, m.label])).toEqual([
      [0.5, 'send start'],
      [1.5, 'send end'],
      [3.5, 'all processed'],
    ])
  })

  it('skips moments that did not happen', () => {
    expect(marksFrom({ send_start: 1 })).toHaveLength(1)
    expect(marksFrom(undefined)).toEqual([])
  })
})

const sample = (t: number, cpu: number, mem: number, dt = 1) => ({
  t,
  dt,
  containers: [{ name: 'w1', cpu, mem_mb: mem, limit_mb: 300, rx_kbs: 1024, tx_kbs: 512 }],
})

describe('containerRows', () => {
  const all = [sample(0, 10, 50), sample(1, 20, 60), sample(2, 30, 70), sample(9, 99, 90)]

  it('keeps only the container and the range asked for', () => {
    expect(containerRows(all, 'w1', [0.5, 3]).map((r) => r.t)).toEqual([1, 2])
    expect(containerRows(all, 'other')).toEqual([])
    expect(containerRows(all, 'w1')).toHaveLength(4)
  })
})

describe('statsOf', () => {
  it('weights averages by the time each sample covers', () => {
    const rows = containerRows([sample(1, 100, 100, 1), sample(2, 0, 200, 3)], 'w1')
    const s = statsOf(rows, 1000)
    expect(s.cpuAvg).toBe(25)
    expect(s.cpuPeak).toBe(100)
    expect(s.cpuSeconds).toBe(1)
    expect(s.memAvg).toBe(175)
    expect(s.memPeak).toBe(200)
    expect(s.headroom).toBe(100)
    expect(s.costPer1k).toBe(1000) // 1 CPU second for 1000 messages = 1000 ms per 1000 messages
  })

  it('converts network rates to megabytes', () => {
    const s = statsOf(containerRows([sample(1, 0, 0, 2)], 'w1'))
    expect(s.netIn).toBe(2) // 1024 KB/s for 2 s = 2 MB
    expect(s.netOut).toBe(1)
  })

  it('does not divide by zero for an empty selection', () => {
    expect(statsOf([]).cpuAvg).toBe(0)
  })
})
