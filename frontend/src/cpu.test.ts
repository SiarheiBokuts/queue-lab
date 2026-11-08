import { describe, expect, it } from 'vitest'
import { fairCores } from './cpu'

describe('fairCores', () => {
  it('shares the cores minus one equally', () => {
    expect(fairCores(8, 3)).toBeCloseTo(2.3, 1) // (8 - 1) / 3, rounded down to a twentieth
    expect(fairCores(8, 7)).toBe(1)
  })

  it('never gives more than four cores or less than a quarter', () => {
    expect(fairCores(32, 2)).toBe(4)
    expect(fairCores(2, 20)).toBe(0.25)
  })

  it('a small machine still gives one core', () => {
    expect(fairCores(1, 2)).toBe(0.5)
  })
})
