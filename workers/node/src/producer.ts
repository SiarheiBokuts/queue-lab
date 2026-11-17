/**
 * The producer: sends its share of the messages over all partitions, at the requested rate, then reports "done".
 *
 * The file reads top to bottom in the same groups as the producer of every other language:
 *  1. settings      what this producer was asked to do
 *  2. counters      what the report shows, shared by the loop, the delivery handling and the reporter
 *  3. client        the Kafka producer and its delivery reports
 *  4. message       how one message is built and handed to the client
 *  5. run           the steps in order (the sending loop is the part shown on the Code page)
 */
import { randomBytes } from 'node:crypto';
import type { KafkaJS } from '@confluentinc/kafka-javascript';
import { kafka } from './kafka.js';
import { buildHeader, nowMs, pacingChunk, pacingDelay, userKey } from './lib.js';
import * as reporting from './reporting.js';
import * as settings from './settings.js';

/** Random padding is cut from one pre-generated megabyte, so generating it costs almost nothing. */
const POOL_BYTES = 1 << 20;

/** Most messages per send() call, and how many send() calls may be in flight at once (pipelining). */
const MAX_CHUNK = 1000;
const MAX_IN_FLIGHT = 8;

/** The time in seconds since 1970, as the report wants it. */
function nowSeconds(): number {
  return Date.now() / 1000;
}

// ---- 1. settings -------------------------------------------------------------------------------------------------

interface ProducerSettings {
  total: number; // this producer's share of the messages
  size: number; // bytes per message
  partitions: number; // partitions of the queue
  index: number; // which producer this is (0, 1, ...) when several share the load
  rate: number; // this producer's target messages per second; 0 = as fast as possible
  randomPayload: boolean; // incompressible data instead of a repeated filler
  users: number; // how many different users (message keys) the messages are spread over
}

function readSettings(): ProducerSettings {
  return {
    total: parseInt(settings.required('MESSAGES'), 10),
    size: parseInt(settings.required('MSG_SIZE'), 10),
    partitions: parseInt(settings.required('PARTITIONS'), 10),
    index: parseInt(settings.required('PRODUCER_INDEX'), 10),
    rate: parseFloat(settings.required('RATE')),
    randomPayload: settings.required('PAYLOAD') === 'random',
    users: parseInt(settings.required('USERS'), 10),
  };
}

// ---- 2. counters -------------------------------------------------------------------------------------------------

class Counters {
  sent = 0;
  delivered = 0;
  errors = 0;
  done = false;
  started: number | null = null; // seconds since 1970; null = not yet
  finished: number | null = null;

  setSent(n: number): void {
    this.sent = n;
  }

  /** Called once the broker confirmed or rejected a batch of `count` messages. */
  confirmed(failed: boolean, count: number): void {
    if (failed) this.errors += count;
    else this.delivered += count;
  }

  start(): void {
    this.started = nowSeconds();
  }

  finish(): void {
    this.finished = nowSeconds();
    this.done = true;
  }

  report(): Promise<void> {
    return reporting.post({
      runId: settings.RUN,
      worker: settings.WID,
      role: 'producer',
      lang: 'node',
      ts: nowSeconds(),
      sent: this.sent,
      delivered: this.delivered,
      errors: this.errors,
      done: this.done,
      started: this.started,
      finished: this.finished,
    });
  }
}

// ---- 3. client ---------------------------------------------------------------------------------------------------

interface OutMessage {
  key: string;
  value: Buffer;
  partition: number;
}

/**
 * The producer settings the backend sent (librdkafka properties as JSON): batching, acks, queue size, compression.
 * The one cast of this file: the JSON is only checked at run time, so the compiler has to be told its type.
 */
function producerConfig(): KafkaJS.ProducerConstructorConfig {
  return { kafkaJS: {}, ...settings.kafkaConf('PRODUCER_CONF') } as KafkaJS.ProducerConstructorConfig;
}

/** The Kafka producer. `send` settles once the broker confirmed or rejected the batch, and never throws. */
class KafkaClient {
  private readonly producer: KafkaJS.Producer;
  private readonly counters: Counters;

  private constructor(producer: KafkaJS.Producer, counters: Counters) {
    this.producer = producer;
    this.counters = counters;
  }

  static async connect(counters: Counters): Promise<KafkaClient> {
    const producer = kafka.producer(producerConfig());
    await producer.connect();
    return new KafkaClient(producer, counters);
  }

  /** Sends the batch and counts the whole batch as confirmed or failed (the delivery report). */
  async send(messages: OutMessage[]): Promise<void> {
    try {
      await this.producer.send({ topic: settings.TOPIC, messages });
      this.counters.confirmed(false, messages.length);
    } catch (error) {
      this.counters.confirmed(true, messages.length);
      console.log('send failed:', (error as Error).message);
    }
  }

  disconnect(): Promise<void> {
    return this.producer.disconnect();
  }
}

// ---- 4. message --------------------------------------------------------------------------------------------------

/**
 * Builds message number i of this producer: which partition, which user, and the bytes to send.
 * It owns what has to be remembered between messages: the sequence counter of every partition.
 */
class MessageMaker {
  private readonly cfg: ProducerSettings;
  private readonly seqs: number[]; // each partition has its own sequence counter for this producer, starting at 0
  private readonly pool: Buffer; // the data random padding is cut from (empty when the padding is a repeated filler)

  constructor(cfg: ProducerSettings) {
    this.cfg = cfg;
    this.seqs = new Array<number>(cfg.partitions).fill(0);
    this.pool = cfg.randomPayload ? randomBytes(POOL_BYTES + cfg.size) : Buffer.alloc(0);
  }

  make(i: number): OutMessage {
    const part = (i + this.cfg.index) % this.cfg.partitions; // round robin; producers start on different partitions so the load stays even
    const seq = this.seqs[part] ?? 0;
    this.seqs[part] = seq + 1;
    const header = buildHeader(settings.RUN, this.cfg.index, part, seq, nowMs());
    return {
      key: userKey(part, i, this.cfg.partitions, this.cfg.users),
      value: this.pad(header, i),
      partition: part,
    };
  }

  /** The messages numbered `first` up to `first + count - 1`. */
  batch(first: number, count: number): OutMessage[] {
    const messages: OutMessage[] = [];
    for (let i = first; i < first + count; i++) messages.push(this.make(i));
    return messages;
  }

  /** The header (who sent it, where, which number, when) padded up to the requested message size. */
  private pad(header: string, i: number): Buffer {
    const value = Buffer.allocUnsafe(Math.max(this.cfg.size, header.length)); // header and padding in ONE buffer
    value.write(header, 0, 'latin1');
    if (this.cfg.randomPayload) {
      const offset = (i * 997) % POOL_BYTES;
      this.pool.copy(value, header.length, offset, offset + value.length - header.length);
    } else {
      value.fill('x', header.length);
    }
    return value;
  }
}

/** Hands a batch to the client and remembers it in `inFlight` until the broker has answered. */
function produce(client: KafkaClient, inFlight: Set<Promise<void>>, messages: OutMessage[]): void {
  const send: Promise<void> = client.send(messages).then(() => {
    inFlight.delete(send);
  });
  inFlight.add(send);
}

// ---- 5. run ------------------------------------------------------------------------------------------------------

export async function run(): Promise<void> {
  const cfg = readSettings();
  const counters = new Counters();
  const client = await KafkaClient.connect(counters);
  const maker = new MessageMaker(cfg);
  const stopReporter = reporting.startReporter(() => counters.report());
  process.on('SIGTERM', () => {
    reporting.control.stopping = true;
  });

  // @producer-start
  // Messages go out in batches: building and sending them one by one would cost too much in Node.js.
  // Each round: build a batch, send it, keep at most MAX_IN_FLIGHT unconfirmed batches, and wait if we are too fast.
  const begin = nowSeconds();
  counters.start();
  const chunk = pacingChunk(cfg.rate, MAX_CHUNK); // batch size: about 10 ms of the target rate, at most MAX_CHUNK
  const inFlight = new Set<Promise<void>>();
  for (let first = 0; first < cfg.total && !reporting.control.stopping; first += chunk) {
    const count = Math.min(chunk, cfg.total - first);
    const batch = maker.batch(first, count); // messages number first .. first + count - 1
    produce(client, inFlight, batch);
    counters.setSent(first + count);
    if (inFlight.size >= MAX_IN_FLIGHT) await Promise.race(inFlight); // do not run ahead of the broker
    const delay = pacingDelay(first + count, cfg.rate, nowSeconds() - begin); // ahead of the target rate? then wait
    if (delay > 0) await reporting.sleep(delay * 1000);
  }
  // @producer-end
  await Promise.all(inFlight); // wait until every message is confirmed by the broker
  counters.finish();
  stopReporter();
  await counters.report();
  await client.disconnect();
  process.exit(0);
}
