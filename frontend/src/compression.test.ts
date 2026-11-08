import { describe, expect, it } from 'vitest'
import { estimateNetwork, fmtMiB } from './compression'

describe('estimateNetwork', () => {
  it('without compression the network carries the data as it is', () => {
    const e = estimateNetwork(100_000, 256, 'none', 'compressible')
    expect(e.ratio).toBe(1)
    expect(e.afterMiB).toBeCloseTo(e.beforeMiB)
    expect(e.savedPct).toBe(0)
  })

  it('repeated data shrinks a lot', () => {
    const e = estimateNetwork(1_000_000, 1024, 'lz4', 'compressible')
    expect(e.ratio).toBeGreaterThan(10)
    expect(e.afterMiB).toBeLessThan(e.beforeMiB / 10)
    expect(e.savedPct).toBeGreaterThan(90)
  })

  it('random data does not shrink, whatever the codec', () => {
    for (const codec of ['lz4', 'snappy', 'gzip', 'zstd'] as const) {
      expect(estimateNetwork(1000, 512, codec, 'random').ratio).toBe(1)
    }
  })

  it('tiny messages shrink less than big ones', () => {
    expect(estimateNetwork(1000, 64, 'zstd', 'compressible').ratio).toBeLessThan(estimateNetwork(1000, 1024, 'zstd', 'compressible').ratio)
  })
})

describe('fmtMiB', () => {
  it('picks a readable unit', () => {
    expect(fmtMiB(292.97)).toBe('293 MiB')
    expect(fmtMiB(9.22)).toBe('9.2 MiB')
    expect(fmtMiB(0.5)).toBe('512 KiB')
  })
})
