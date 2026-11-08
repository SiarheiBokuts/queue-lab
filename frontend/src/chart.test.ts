import { describe, expect, it } from 'vitest'
import { barOption, fmt, heatOption, lineOption } from './chart'

describe('fmt', () => {
  it('shortens big numbers and rounds small ones', () => {
    expect(fmt(0)).toBe('0')
    expect(fmt(999.6)).toBe('1000')
    expect(fmt(15_300)).toBe('15.3k')
    expect(fmt(2_500_000)).toBe('2.50M')
  })

  it('shows a dash for missing values', () => {
    expect(fmt(null)).toBe('-')
    expect(fmt(undefined)).toBe('-')
  })
})

describe('lineOption', () => {
  const x = [0, 1, 2]
  const series = [{ name: 'a', color: '#fff', data: [1, 2, 3] }]

  it('plots points as [time, value] pairs on a numeric axis', () => {
    const o = lineOption(x, series)
    expect(o.xAxis.type).toBe('value')
    expect(o.series[0]?.data).toEqual([[0, 1], [1, 2], [2, 3]])
  })

  it('puts markers and the shaded window on the first series only', () => {
    const two = [...series, { name: 'b', color: '#000', data: [3, 2, 1] }]
    const o = lineOption(x, two, { marks: [{ at: 1, label: 'send end' }], shade: [0.5, 1.5] })
    expect(o.series[0]?.markLine?.data).toHaveLength(1)
    expect(o.series[0]?.markArea).toBeDefined()
    expect(o.series[1]?.markLine).toBeUndefined()
  })

  it('zooms the axis when a range is given', () => {
    const o = lineOption(x, series, { xmin: 0.5, xmax: 1.5 })
    expect(o.xAxis.min).toBe(0.5)
    expect(o.xAxis.max).toBe(1.5)
  })
})

describe('lineOption with a time axis per series', () => {
  it('lets two runs of different length share a chart', () => {
    const o = lineOption([], [
      { name: 'a', color: '#fff', data: [1, 2, 3], x: [0, 1, 2] },
      { name: 'b', color: '#000', data: [5, 6], x: [0.5, 1.5] },
    ])
    expect(o.series[0]?.data).toEqual([[0, 1], [1, 2], [2, 3]])
    expect(o.series[1]?.data).toEqual([[0.5, 5], [1.5, 6]])
  })
})

describe('heatOption', () => {
  it('scales the colors to the largest lag', () => {
    const o = heatOption([[0, 10], [200, 5]], 2)
    expect(o.visualMap.max).toBe(200)
    expect(o.series[0]?.data).toHaveLength(4)
    expect(o.yAxis.data).toEqual(['P0', 'P1'])
  })
})

describe('barOption', () => {
  it('uses the labels and counts as given', () => {
    const o = barOption(['≤1', '≤2'], [5, 7])
    expect(o.xAxis.data).toEqual(['≤1', '≤2'])
    expect(o.series[0]?.data).toEqual([5, 7])
  })
})
