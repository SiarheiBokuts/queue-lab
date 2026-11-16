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

/** The Kafka producer. `send` settles once the broker confirmed or rejected the batch, and never throws. */
interface Client {
  send(messages: OutMessage[]): Promise<void>;
  disconnect(): Promise<void>;
}

async function newKafkaProducer(counters: Counters): Promise<Client> {
  // Batching, acks, queue size and compression come from the backend (the same values for every language).
  // The client accepts librdkafka properties next to its own `kafkaJS` block; its typings do not list them.
  const producer = kafka.producer({ kafkaJS: {}, ...settings.kafkaConf('PRODUCER_CONF') } as never);
  await producer.connect();
  return {
    // delivery report: the broker confirmed or rejected the batch
    async send(messages) {
      try {
        await producer.send({ topic: settings.TOPIC, messages });
        counters.confirmed(false, messages.length);
      } catch (error) {
        counters.confirmed(true, messages.length);
        console.log('send failed:', (error as Error).message);
      }
    },
    disconnect: () => producer.disconnect(),
  };
}

// ---- 4. message --------------------------------------------------------------------------------------------------

/** The data random padding is cut from (empty when the padding is a repeated filler). */
function randomPool(cfg: ProducerSettings): Buffer {
  return cfg.randomPayload ? randomBytes(POOL_BYTES + cfg.size) : Buffer.alloc(0);
}

/** The header (who sent it, where, which number, when) padded up to the requested message size. */
function buildMessage(header: string, i: number, cfg: ProducerSettings, pool: Buffer): Buffer {
  const value = Buffer.allocUnsafe(Math.max(cfg.size, header.length)); // header and padding in ONE buffer
  value.write(header, 0, 'latin1');
  if (cfg.randomPayload) {
    const offset = (i * 997) % POOL_BYTES;
    pool.copy(value, header.length, offset, offset + value.length - header.length);
  } else {
    value.fill('x', header.length);
  }
  return value;
}

/** Hands a batch to the client and remembers it in `inFlight` until the broker has answered. */
function produce(client: Client, inFlight: Set<Promise<void>>, messages: OutMessage[]): void {
  const send: Promise<void> = client.send(messages).then(() => {
    inFlight.delete(send);
  });
  inFlight.add(send);
}

// ---- 5. run ------------------------------------------------------------------------------------------------------

export async function run(): Promise<void> {
  const cfg = readSettings();
  const counters = new Counters();
  const client = await newKafkaProducer(counters);
  const pool = randomPool(cfg);
  const stopReporter = reporting.startReporter(() => counters.report());
  process.on('SIGTERM', () => {
    reporting.control.stopping = true;
  });

  // @producer-start
  // Messages go out in batches: building and sending them one by one would cost too much in Node.js.
  // Each round: build a batch, send it, keep at most MAX_IN_FLIGHT unconfirmed batches, and wait if we are too fast.
  const seqs = new Array<number>(cfg.partitions).fill(0); // each partition has its own sequence counter for this producer, starting at 0
  const begin = nowSeconds();
  counters.start();
  const chunk = pacingChunk(cfg.rate, MAX_CHUNK); // batch size: about 10 ms of the target rate, at most MAX_CHUNK
  const inFlight = new Set<Promise<void>>();
  for (let i = 0; i < cfg.total && !reporting.control.stopping; i += chunk) {
    const n = Math.min(chunk, cfg.total - i);
    const batch: OutMessage[] = [];
    for (let k = i; k < i + n; k++) {
      const part = (k + cfg.index) % cfg.partitions; // round robin; producers start on different partitions so the load stays even
      const seq = seqs[part] ?? 0;
      seqs[part] = seq + 1;
      const header = buildHeader(settings.RUN, cfg.index, part, seq, nowMs());
      batch.push({
        key: userKey(part, k, cfg.partitions, cfg.users),
        value: buildMessage(header, k, cfg, pool),
        partition: part,
      });
    }
    produce(client, inFlight, batch);
    counters.setSent(i + n);
    if (inFlight.size >= MAX_IN_FLIGHT) await Promise.race(inFlight); // do not run ahead of the broker
    const delay = pacingDelay(i + n, cfg.rate, nowSeconds() - begin); // ahead of the target rate? then wait
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
