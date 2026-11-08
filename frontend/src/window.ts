import type { Mark } from './chart'

/** Time range of the message window (send start to last processed), padded a little. Seconds from producer start. */
export function windowRange(win: any, pad = 0.5): [number, number] | null {
  if (!win) return null
  const hi = win.last_processed ?? win.send_end
  if (hi === undefined || hi === null) return null
  return [Math.max(0, (win.send_start ?? 0) - pad), hi + pad]
}

export function marksFrom(win: any): Mark[] {
  const out: Mark[] = []
  if (win?.send_start != null) out.push({ at: win.send_start, label: 'send start', color: '#3b82f6' })
  if (win?.send_end != null) out.push({ at: win.send_end, label: 'send end', color: '#8b5cf6' })
  if (win?.last_processed != null) out.push({ at: win.last_processed, label: 'all processed', color: '#10b981' })
  return out
}

export interface Row {
  t: number
  dt: number
  c: any
}

export function containerRows(containerSamples: any[], name: string, range?: [number, number] | null): Row[] {
  return containerSamples
    .map((e) => ({ t: e.t, dt: e.dt ?? 1, c: (e.containers || []).find((c: any) => c.name === name) }))
    .filter((r) => r.c && (!range || (r.t >= range[0] && r.t <= range[1])))
}

/** Time-weighted resource statistics of one container over the given rows. */
export function statsOf(rows: Row[], messages?: number) {
  const secs = rows.reduce((a, r) => a + r.dt, 0) || 1
  const cpuS = rows.reduce((a, r) => a + (r.c.cpu / 100) * r.dt, 0)
  const limit = rows.length ? rows[rows.length - 1].c.limit_mb : 0
  const memPeak = Math.max(0, ...rows.map((r) => r.c.mem_mb))
  return {
    samples: rows.length,
    cpuAvg: Math.round((cpuS / secs) * 1000) / 10,
    cpuPeak: Math.max(0, ...rows.map((r) => r.c.cpu)),
    cpuSeconds: Math.round(cpuS * 100) / 100,
    memAvg: Math.round((rows.reduce((a, r) => a + r.c.mem_mb * r.dt, 0) / secs) * 10) / 10,
    memPeak,
    limit,
    headroom: limit ? Math.round((limit - memPeak) * 10) / 10 : null,
    netIn: Math.round((rows.reduce((a, r) => a + r.c.rx_kbs * r.dt, 0) / 1024) * 100) / 100,
    netOut: Math.round((rows.reduce((a, r) => a + r.c.tx_kbs * r.dt, 0) / 1024) * 100) / 100,
    costPer1k: messages ? Math.round(((cpuS * 1000) / (messages / 1000)) * 10) / 10 : null,
  }
}
