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

/** Ranges kept per stream; beyond this the worker only counts (the report says how many it did not list). */
export const MAX_RANGES = 1000;

/**
 * Remembers which message numbers arrived, as ranges, so the lab can count lost and repeated messages exactly.
 *
 * Every message has a number per stream (one sender, one partition). Numbers that arrive one after another make one
 * range (`0-4999`), so a run without losses is one range per stream. A number that is already inside a range is a
 * repeat. The lab merges the ranges of all workers: what was sent and is in nobody's ranges is lost, what is counted
 * more than once is repeated (for example after a worker restarts and its batch is read again).
 */
export class SeqRanges {
  repeats = 0; // numbers this worker saw twice
  overflow = 0; // numbers not listed because a stream hit MAX_RANGES
  private readonly streams = new Map<string, [number, number][]>();

  /** Remember one message number. Returns false when this worker has seen it before. */
  observe(producer: number, partition: number, seq: number): boolean {
    const key = `${producer}:${partition}`;
    let ranges = this.streams.get(key);
    if (!ranges) {
      ranges = [];
      this.streams.set(key, ranges);
    }
    const last = ranges[ranges.length - 1];
    if (last && seq === last[1] + 1) {
      last[1] = seq; // the usual case: the next number
      return true;
    }
    if (ranges.some(([start, end]) => start <= seq && seq <= end)) {
      this.repeats += 1;
      return false;
    }
    if (ranges.length >= MAX_RANGES) {
      this.overflow += 1;
      return true;
    }
    ranges.push([seq, seq]);
    return true;
  }

  /** The ranges per stream, sorted and merged, keyed "producer:partition". */
  report(): Record<string, [number, number][]> {
    const out: Record<string, [number, number][]> = {};
    for (const [key, ranges] of this.streams) {
      const merged: [number, number][] = [];
      for (const [start, end] of [...ranges].sort((x, y) => x[0] - y[0])) {
        const previous = merged[merged.length - 1];
        if (previous && start <= previous[1] + 1) previous[1] = Math.max(previous[1], end);
        else merged.push([start, end]);
      }
      out[key] = merged;
    }
    return out;
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

/**
 * Split the limit of unconfirmed messages into batches: how many messages go in one send() call and how many such calls may wait
 * for the broker at once. There are always at least two batches in the air, so the connection is not idle.
 */
export function batchPlan(limit: number, chunk: number): { chunk: number; inFlight: number } {
  const size = Math.max(1, Math.min(chunk, Math.floor(limit / 2)));
  return { chunk: size, inFlight: Math.max(1, Math.floor(limit / size)) };
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

/**
 * How long a batch took, and how long of that some groups were done while others still worked (milliseconds).
 * `started` and `done` are clock readings in milliseconds. A batch without groups (all its messages belonged to
 * another run) took no time and waited for nobody.
 */
export function batchTiming(started: number, done: readonly number[]): { busyMs: number; waitMs: number } {
  if (done.length === 0) return { busyMs: 0, waitMs: 0 };
  const last = Math.max(...done);
  return { busyMs: last - started, waitMs: last - Math.min(...done) };
}

/** What the handling step needs to know about a message: whose it is and which one it is. */
export interface Item {
  key: Buffer | null; // the user
  producer: number; // which sender
  seq: number; // the number of the message in its stream
}

/**
 * Checks that the messages of one user are handled in the order they were sent.
 *
 * For every (user, sender) it remembers the number of the message handled last. A message with a lower number than that
 * was handled too late: it is counted as an order violation. A well built consumer never has one.
 */
export class OrderCheck {
  violations = 0;
  private readonly last = new Map<string, number>();

  /** Remember that `item` is handled now. Returns false when it came after a later message of the same user. */
  check(item: Item): boolean {
    const key = `${item.key === null ? '' : item.key.toString('latin1')}|${item.producer}`;
    const last = this.last.get(key);
    if (last !== undefined && item.seq < last) {
      this.violations += 1;
      return false;
    }
    this.last.set(key, item.seq);
    return true;
  }
}

/** Group messages by their key (the user). The order inside a group is the order they were sent in. */
export function groupByUser<T extends { key: Buffer | null }>(messages: readonly T[]): T[][] {
  const groups = new Map<string, T[]>();
  for (const message of messages) {
    const key = message.key === null ? '' : message.key.toString('latin1');
    const group = groups.get(key);
    if (group) group.push(message);
    else groups.set(key, [message]);
  }
  return [...groups.values()];
}
