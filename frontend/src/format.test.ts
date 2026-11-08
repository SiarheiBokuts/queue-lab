import { describe, expect, it } from 'vitest'
import { fmtMs } from './format'

describe('fmtMs', () => {
  it('picks the unit that reads best', () => {
    expect(fmtMs(0.5)).toBe('0.5 ms')
    expect(fmtMs(7.25)).toBe('7.3 ms')
    expect(fmtMs(850)).toBe('850 ms')
    expect(fmtMs(2400)).toBe('2.4 s')
    expect(fmtMs(2000)).toBe('2 s')
    expect(fmtMs(45_000)).toBe('45 s')
    expect(fmtMs(80_000)).toBe('1 min 20 s')
    expect(fmtMs(120_000)).toBe('2 min')
  })

  it('shows the measurement limit as "at least"', () => {
    expect(fmtMs(10_000)).toBe('≥ 10 s')
    expect(fmtMs(10_000, false)).toBe('10 s')
  })

  it('shows a dash for missing values', () => {
    expect(fmtMs(null)).toBe('-')
    expect(fmtMs(undefined)).toBe('-')
  })
})

describe('number grouping', () => {
  it('groups thousands', async () => {
    const { groupDigits } = await import('./format')
    expect(groupDigits(1234567)).toBe('1\u00A0234\u00A0567')
    expect(groupDigits(1000000)).toBe('1\u00A0000\u00A0000')
    expect(groupDigits(999)).toBe('999')
    expect(groupDigits(null)).toBe('')
  })

  it('reads the number back from text with separators or stray letters', async () => {
    const { digitsToNumber } = await import('./format')
    expect(digitsToNumber('1,234,567')).toBe(1234567)
    expect(digitsToNumber('1 000 000')).toBe(1000000)
    expect(digitsToNumber('12abc')).toBe(12)
    expect(digitsToNumber('')).toBe(0)
  })
})
