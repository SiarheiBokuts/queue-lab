/** What a consumer counts about the messages it reads, and the report it sends once per second. */
import { SeqRanges, bucket, type Header } from './lib.js';

export class ConsumerStats {
  received = 0;
  errors = 0;
  firstAt: number | null = null; // seconds since the epoch
  lastAt: number | null = null;
  maxLatencyMs = 0;
  /** The next offset to read in each partition: the lab uses it to work out the messages waiting. */
  readonly offsets: Record<number, number> = {};
  private hist: number[];
  /** Which message numbers arrived: the lab counts lost and repeated ones from it. */
  private readonly seen = new SeqRanges();

  constructor(private readonly edges: readonly number[]) {
    this.hist = this.emptyHistogram();
  }

  private emptyHistogram(): number[] {
    return new Array<number>(this.edges.length + 1).fill(0);
  }

  /** Count one message that arrived at `arrivedMs` (milliseconds) in `partition`. */
  record(header: Header, partition: number, arrivedMs: number): void {
    this.received += 1;
    if (this.firstAt === null) this.firstAt = arrivedMs / 1000;
    this.seen.observe(header.producer, partition, header.seq);
    const latency = arrivedMs - header.sentMs; // from "sent" to "received"
    this.hist[bucket(latency, this.edges)] += 1;
    if (latency > this.maxLatencyMs) this.maxLatencyMs = latency;
  }

  /** The report for the lab. The histogram is sent as the change since the last report, then reset. */
  report(base: { runId: string; worker: string }, assigned: number[], final: boolean): Record<string, unknown> {
    const payload = {
      ...base, role: 'consumer', lang: 'node', ts: Date.now() / 1000,
      ready: assigned.length > 0, assigned, received: this.received, duplicates: this.seen.repeats,
      ranges: this.seen.report(), overflow: this.seen.overflow, errors: this.errors, offsets: this.offsets, hist: this.hist, final,
      firstAt: this.firstAt, lastAt: this.lastAt, maxLatencyMs: this.maxLatencyMs,
    };
    this.hist = this.emptyHistogram();
    return payload;
  }
}