import { fmtMs } from './format'

export const C = {
  blue: '#3b82f6', amber: '#f59e0b', green: '#10b981', red: '#ef4444', violet: '#8b5cf6', grid: '#27272a', text: '#a1a1aa',
}

export interface Series {
  name: string
  color: string
  data: (number | null)[]
  dashed?: boolean
  /** Own time axis for this series (comparing two runs); defaults to the shared one. */
  x?: number[]
}

export interface Mark {
  at: number
  label: string
  color?: string
}

interface LineOpts {
  area?: boolean
  max?: number
  marks?: Mark[]
  shade?: [number, number] | null
  xmin?: number
  xmax?: number
  /** The values are latencies in milliseconds: label them in seconds or minutes when that reads better. */
  latency?: boolean
  /** The big view: zoom with the wheel or a slider, and a button that saves the picture. */
  zoom?: boolean
}

const tooltip = {
  backgroundColor: '#18181b',
  borderColor: '#3f3f46',
  textStyle: { color: '#e4e4e7', fontSize: 12 },
}

/** Line chart on a numeric time axis (seconds), with optional vertical markers and a shaded window. */
export function lineOption(x: number[], series: Series[], opts: LineOpts = {}) {
  return {
    backgroundColor: 'transparent',
    animation: false,
    textStyle: { color: C.text },
    grid: { left: 52, right: 14, top: opts.zoom ? 44 : 28, bottom: opts.zoom ? 64 : 24 },
    ...(opts.zoom
      ? {
          dataZoom: [
            { type: 'inside', xAxisIndex: 0 },
            { type: 'slider', xAxisIndex: 0, height: 18, bottom: 12, borderColor: '#3f3f46', textStyle: { color: C.text }, fillerColor: 'rgba(59,130,246,0.18)' },
          ],
          toolbox: { right: 4, top: 0, iconStyle: { borderColor: C.text }, feature: { saveAsImage: { title: 'Save as image', backgroundColor: '#18181b' }, restore: { title: 'Reset zoom' } } },
        }
      : {}),
    legend: { top: 0, right: opts.zoom ? 90 : 0, textStyle: { color: C.text, fontSize: 11 }, itemWidth: 10, itemHeight: 3, icon: 'rect' },
    tooltip: { trigger: 'axis', ...tooltip, valueFormatter: opts.latency ? (v: number) => fmtMs(v) : undefined },
    xAxis: {
      type: 'value',
      min: opts.xmin ?? 'dataMin',
      max: opts.xmax ?? 'dataMax',
      axisLine: { lineStyle: { color: '#3f3f46' } },
      axisLabel: { color: C.text, fontSize: 10, formatter: (v: number) => `${Math.round(v * 10) / 10}s` },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value',
      max: opts.max,
      axisLabel: { color: C.text, fontSize: 10, formatter: (v: number) => (opts.latency ? fmtMs(v, false) : v >= 1000 ? `${Math.round(v / 100) / 10}k` : String(v)) },
      splitLine: { lineStyle: { color: C.grid } },
    },
    series: series.map((s, i) => ({
      name: s.name,
      type: 'line',
      data: s.data.map((v, k) => [(s.x ?? x)[k], v]),
      showSymbol: x.length < 40,
      symbolSize: 5,
      smooth: x.length > 6,
      lineStyle: { width: 2, color: s.color, type: s.dashed ? 'dashed' : 'solid' },
      itemStyle: { color: s.color },
      areaStyle: opts.area ? { color: s.color, opacity: 0.1 } : undefined,
      markLine:
        i === 0 && opts.marks?.length
          ? {
              silent: true,
              symbol: 'none',
              label: { color: C.text, fontSize: 10, formatter: '{b}', position: 'insideEndTop' },
              data: opts.marks.map((m) => ({ xAxis: m.at, name: m.label, lineStyle: { color: m.color || '#71717a', type: 'dashed', width: 1 } })),
            }
          : undefined,
      markArea:
        i === 0 && opts.shade
          ? { silent: true, itemStyle: { color: 'rgba(59,130,246,0.07)' }, data: [[{ xAxis: opts.shade[0] }, { xAxis: opts.shade[1] }]] }
          : undefined,
    })),
  }
}

export function barOption(labels: string[], counts: number[], color = C.blue) {
  return {
    backgroundColor: 'transparent',
    animation: false,
    grid: { left: 52, right: 12, top: 12, bottom: 34 },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, ...tooltip },
    xAxis: { type: 'category', data: labels, axisLabel: { color: C.text, fontSize: 10 }, axisLine: { lineStyle: { color: '#3f3f46' } } },
    yAxis: {
      type: 'value',
      axisLabel: { color: C.text, fontSize: 10, formatter: (v: number) => (v >= 1000 ? `${Math.round(v / 100) / 10}k` : String(v)) },
      splitLine: { lineStyle: { color: C.grid } },
    },
    series: [{ type: 'bar', data: counts, itemStyle: { color, borderRadius: [3, 3, 0, 0] }, barMaxWidth: 28 }],
  }
}

export function heatOption(rows: number[][], partitions: number) {
  const data: number[][] = []
  let max = 1
  rows.forEach((row, xi) =>
    row.forEach((v, yi) => {
      data.push([xi, yi, v])
      if (v > max) max = v
    }),
  )
  return {
    backgroundColor: 'transparent',
    animation: false,
    grid: { left: 36, right: 8, top: 6, bottom: 6 },
    tooltip: { ...tooltip, formatter: (p: any) => `Partition ${p.value[1]}: ${p.value[2]} messages waiting` },
    xAxis: { type: 'category', data: rows.map((_, i) => i), show: false },
    yAxis: {
      type: 'category',
      data: Array.from({ length: partitions }, (_, i) => `P${i}`),
      inverse: true,
      axisLabel: { color: C.text, fontSize: 10 },
      axisLine: { show: false },
      axisTick: { show: false },
    },
    visualMap: { min: 0, max, show: false, inRange: { color: ['#18181b', '#1d4ed8', '#f59e0b', '#ef4444'] } },
    series: [{ type: 'heatmap', data, itemStyle: { borderColor: '#09090b', borderWidth: 1 }, progressive: 0 }],
  }
}

export const fmt = (n: number | null | undefined) =>
  n === null || n === undefined ? '-' : n >= 1_000_000 ? `${(n / 1e6).toFixed(2)}M` : n >= 10_000 ? `${(n / 1000).toFixed(1)}k` : String(Math.round(n))
