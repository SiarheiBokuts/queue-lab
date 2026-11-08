/** A rough idea of what compression does to the network, shown before a run. The report shows the real numbers. */

export type Codec = 'none' | 'lz4' | 'snappy' | 'gzip' | 'zstd'

/**
 * Ratios (size before / size after) measured with real runs of this lab: 512-byte messages of the repeated filler the
 * lab sends by default, 20000 messages per second. Real data differs: text and JSON usually shrink 3-10x, random or
 * encrypted data not at all.
 */
const FILLER_RATIO_AT_512: Record<Codec, number> = { none: 1, lz4: 20, snappy: 11, gzip: 25, zstd: 35 }

/**
 * The bigger the message, the better it shrinks, because every message carries a little data that cannot shrink.
 * Measured with lz4: 64 B gave 2.6x, 512 B gave 20x, 4096 B gave 97x, which is roughly proportional to the size.
 */
function sizeFactor(messageSize: number): number {
  return Math.min(6, Math.max(0.1, Math.pow(messageSize / 512, 0.9)))
}
export interface NetworkEstimate { beforeMiB: number; afterMiB: number; ratio: number; savedPct: number }

/** Approximate data size before and after compression for `messages` messages of `messageSize` bytes. */
export function estimateNetwork(messages: number, messageSize: number, codec: Codec, payload: 'compressible' | 'random'): NetworkEstimate {
  const beforeMiB = (messages * messageSize) / (1024 * 1024)
  const ratio = codec === 'none' || payload === 'random' ? 1 : Math.max(1, FILLER_RATIO_AT_512[codec] * sizeFactor(messageSize))
  const afterMiB = beforeMiB / ratio
  return { beforeMiB, afterMiB, ratio, savedPct: Math.round((1 - 1 / ratio) * 100) }
}

/** "24 MiB", "0.9 MiB", "850 KiB": the unit that reads best. */
export function fmtMiB(mib: number): string {
  if (mib >= 100) return `${Math.round(mib)} MiB`
  if (mib >= 1) return `${mib.toFixed(1)} MiB`
  if (mib >= 0.1) return `${Math.round(mib * 1024)} KiB`
  return `${Math.max(1, Math.round(mib * 1024))} KiB`
}
