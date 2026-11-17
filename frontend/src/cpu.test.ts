import { describe, expect, it } from 'vitest'
import { fairCores } from './cpu'

// The numbers of the backend (config.CPU_RESERVE and config.CPU_MAX_PER_CONTAINER); the rule is what is tested here.
const limits = { cpu_reserve: 1, cpu_max_per_container: 4 }

describe('fairCores', () => {
  it('shares the cores minus the reserve equally', () => {
    expect(fairCores(8, 3, limits)).toBeCloseTo(2.3, 1) // (8 - 1) / 3, rounded down to a twentieth
    expect(fairCores(8, 7, limits)).toBe(1)
  })

  it('never gives more than the maximum or less than a quarter', () => {
    expect(fairCores(32, 2, limits)).toBe(4)
    expect(fairCores(2, 20, limits)).toBe(0.25)
  })

  it('a small machine still gives one core', () => {
    expect(fairCores(1, 2, limits)).toBe(0.5)
  })

  it('follows the numbers it is given', () => {
    expect(fairCores(8, 2, { cpu_reserve: 2, cpu_max_per_container: 2 })).toBe(2) // 6 / 2 = 3, cut to the maximum 2
    expect(fairCores(8, 4, { cpu_reserve: 0, cpu_max_per_container: 4 })).toBe(2)
  })
})
