/**
 * Pure helpers of the Node.js worker: no Kafka, no network, no environment. Easy to unit test.
 *
 * Message format, the same in every language:
 *
 *     <run>|<producer>|<partition>|<sequence>|<sentAtMs>|xxxxxxxx...
 *
 * run: id of the run, so leftovers of earlier runs are ignored. producer: which sender. partition: where it was sent.
 * sequence: counts from 0 per sender and partition; a gap means loss, a repeat a duplicate. sentAtMs: the sender's clock,
 * subtracted from "now" to get the latency. The rest is padding up to the message size chosen in the form.
 */

/** The header is far shorter than this; we never decode the (possibly 64 KB) padding just to read it. */
export const HEADER_MAX_BYTES = 96;

/** Parse the HIST_EDGES environment value, e.g. "0.5,1,2,5", into bucket edges in milliseconds. */
export function parseEdges(raw: string): number[] {
  return raw
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part !== '')
    .map(Number);
}

/**
 * Index of the histogram bucket a latency falls into.
 *
 * Bucket i holds latencies <= edges[i]; the last bucket (index edges.length) holds everything above the last edge.
 * Counting messages per bucket keeps reports small even at hundreds of thousands of messages per second.
 */
export function bucket(latencyMs: number, edges: readonly number[]): number {
  for (let i = 0; i < edges.length; i++) {
    if (latencyMs <= (edges[i] as number)) return i;
  }
  return edges.length;
}

/** Header of a message; the producer appends padding after it. */
export function buildHeader(run: string, producer: number, partition: number, seq: number, sentMs: number): string {
  return `${run}|${producer}|${partition}|${seq}|${sentMs.toFixed(3)}|`;
}

export interface Header {
  producer: number;
  partition: number;
  seq: number;
  sentMs: number;
}

/** Parse the start of a message. Returns null if it is malformed or not part of `expectedRun`. */
export function parseHeader(head: string, expectedRun: string): Header | null {
  const parts = head.split('|', 5);
  if (parts.length < 5 || parts[0] !== expectedRun) return null;
  const [, producer, partition, seq, sentMs] = parts as [string, string, string, string, string];
  if (![producer, partition, seq].every((n) => /^\d+$/.test(n)) || !/^\d+(\.\d+)?$/.test(sentMs)) return null;
  return { producer: Number(producer), partition: Number(partition), seq: Number(seq), sentMs: Number(sentMs) };
}

const BAR = 0x7c; // "|"
const DOT = 0x2e; // "."
let cursor = 0; // reading position shared by the two readers below (no allocation per message)

/** Read digits up to the next "|" from the shared cursor; -1 if the bytes are not a plain number. */
function readUInt(buf: Buffer, limit: number): number {
  let n = 0;
  let digits = 0;
  while (cursor < limit) {
    const c = buf[cursor] as number;
    cursor++;
    if (c === BAR) return digits > 0 ? n : -1;
    if (c < 48 || c > 57) return -1;
    n = n * 10 + (c - 48);
    digits++;
  }
  return -1;
}

/** Like readUInt, but allows one decimal point ("1794170000123.456"). */
function readDecimal(buf: Buffer, limit: number): number {
  let n = 0;
  let digits = 0;
  let scale = 0;
  while (cursor < limit) {
    const c = buf[cursor] as number;
    cursor++;
    if (c === BAR) return digits > 0 ? n : -1;
    if (c === DOT && scale === 0) {
      scale = 0.1;
      continue;
    }
    if (c < 48 || c > 57) return -1;
    if (scale === 0) n = n * 10 + (c - 48);
    else {
      n += (c - 48) * scale;
      scale /= 10;
    }
    digits++;
  }
  return -1;
}

/**
 * A parser for the header of a message that works on the raw bytes: no string, no split, no regular expression per
 * message. It gives the same result as parseHeader, which stays as the readable reference (and is tested against it).
 */
export function makeHeaderParser(expectedRun: string): (value: Buffer) => Header | null {
  const prefix = Buffer.from(`${expectedRun}|`, 'latin1');
  return (value) => {
    if (value.length <= prefix.length || value.compare(prefix, 0, prefix.length, 0, prefix.length) !== 0) return null;
    const limit = Math.min(value.length, HEADER_MAX_BYTES);
    cursor = prefix.length;
    const producer = readUInt(value, limit);
    const partition = readUInt(value, limit);
    const seq = readUInt(value, limit);
    const sentMs = readDecimal(value, limit);
    if (producer < 0 || partition < 0 || seq < 0 || sentMs < 0) return null;
    return { producer, partition, seq, sentMs };
  };
}

export type SeqVerdict = 'ok' | 'gap' | 'duplicate';

/**
 * Detects lost and duplicated messages from per-partition sequence numbers.
 *
 * Within one partition Kafka preserves order, so after sequence N of a stream (one producer, one partition) the next
 * message of that stream must be N + 1.
 * A higher number means messages were skipped (gap); a number we already saw (or lower) means a duplicate.
 * The first message seen on a stream only sets the baseline, because a worker may join mid-run after a rebalance.
 */
export class SeqTracker {
  duplicates = 0;
  /** Total number of skipped sequence numbers. */
  gaps = 0;
  private readonly last = new Map<string | number, number>();

  /** Record one message of a stream (one producer, one partition): any key that is the same for the same stream. */
  observe(stream: string | number, seq: number): SeqVerdict {
    const last = this.last.get(stream);
    if (last === undefined || seq === last + 1) {
      this.last.set(stream, seq);
      return 'ok';
    }
    if (seq <= last) {
      this.duplicates += 1;
      return 'duplicate';
    }
    this.gaps += seq - last - 1;
    this.last.set(stream, seq);
    return 'gap';
  }
}

/**
 * Wall-clock time in milliseconds with sub-millisecond precision (`Date.now()` is whole milliseconds only).
 * Producer and consumer both use it, so latencies below 1 ms are measured the same way in both languages.
 */
export function nowMs(): number {
  return performance.timeOrigin + performance.now();
}

/**
 * Seconds a producer must wait so that `sent` messages take at least sent / rate seconds. 0 means "go on".
 *
 * This is how a producer holds a steady target rate: it compares the time it should have taken with the time it did
 * take. A rate of 0 means "as fast as possible", and a producer that is behind schedule never waits.
 */
export function pacingDelay(sent: number, rate: number, elapsedSeconds: number): number {
  if (rate <= 0) return 0;
  return Math.max(0, sent / rate - elapsedSeconds);
}

/** How many messages to send between two pacing checks: about 10 ms worth, so the rate stays smooth. */
export function pacingChunk(rate: number, maxChunk: number): number {
  if (rate <= 0) return maxChunk;
  return Math.max(1, Math.min(maxChunk, Math.floor(rate / 100)));
}

/**
 * Message key of the user a message belongs to, like a payment of one user.
 * A user always lands in the same partition (user number % partitions === partition), so Kafka keeps that user's
 * messages in order. Within a partition the users take turns.
 */
export function userKey(part: number, i: number, nparts: number, users: number): string {
  const perPartition = Math.max(1, Math.floor(users / nparts));
  return `user-${part + nparts * (Math.floor(i / nparts) % perPartition)}`;
}

/** The lane (0 .. lanes-1) that handles messages with this key. The same key always gets the same lane (FNV-1a hash). */
export function laneFor(key: string, lanes: number): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < key.length; i++) hash = Math.imul(hash ^ key.charCodeAt(i), 0x01000193) >>> 0;
  return hash % lanes;
}
