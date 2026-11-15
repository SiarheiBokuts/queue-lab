/** Builds the messages a producer sends: partition, user key, and one buffer holding the header and the padding. */
import { randomBytes } from 'node:crypto';
import { buildHeader, nowMs, userKey } from './lib.js';

/** Random padding is cut from one pre-generated megabyte, so generating it costs almost nothing. */
const POOL_BYTES = 1 << 20;

export interface OutMessage {
  key: string;
  value: Buffer;
  partition: number;
}

export interface MessageOptions {
  run: string;
  producerIndex: number; // which producer this is (0, 1, ...)
  partitions: number;
  size: number; // the message size chosen in the form, in bytes
  users: number; // how many different users (message keys) the messages are spread over
  randomPayload: boolean; // incompressible data instead of a repeated filler
}

export class MessageFactory {
  /** Each partition has its own sequence counter for this producer, starting at 0. */
  private readonly seqs: number[];
  private readonly pool: Buffer;

  constructor(private readonly o: MessageOptions) {
    this.seqs = new Array<number>(o.partitions).fill(0);
    this.pool = o.randomPayload ? randomBytes(POOL_BYTES + o.size) : Buffer.alloc(0);
  }

  /** Message number `i` of this producer. */
  make(i: number): OutMessage {
    const { run, producerIndex, partitions, size, users, randomPayload } = this.o;
    const partition = (i + producerIndex) % partitions; // round robin; producers start on different partitions
    const seq = this.seqs[partition] as number;
    this.seqs[partition] = seq + 1;
    const header = buildHeader(run, producerIndex, partition, seq, nowMs());
    const value = Buffer.allocUnsafe(Math.max(size, header.length)); // header and padding in ONE buffer
    value.write(header, 0, 'latin1');
    if (randomPayload) {
      const offset = (i * 997) % POOL_BYTES;
      this.pool.copy(value, header.length, offset, offset + value.length - header.length);
    } else value.fill(120, header.length);
    return { key: userKey(partition, i, partitions, users), value, partition };
  }

  /** `count` messages starting at message number `from`. */
  batch(from: number, count: number): OutMessage[] {
    return Array.from({ length: count }, (_, j) => this.make(from + j));
  }
}
