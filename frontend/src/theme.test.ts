import { describe, expect, it } from 'vitest'
import { nextTheme } from './theme'

describe('nextTheme', () => {
  it('switches between dark and light', () => {
    expect(nextTheme('dark')).toBe('light')
    expect(nextTheme('light')).toBe('dark')
  })
})
