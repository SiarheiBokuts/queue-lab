/** The producer: sends its share of the messages over all partitions, at the requested rate, then reports "done". */
import { kafka } from './kafka.js';
import { pacingChunk, pacingDelay } from './lib.js';
import { MessageFactory } from './message.js';
import * as reporting from './reporting.js';
import * as settings from './settings.js';

/** Most messages per send() call, and how many send() calls may be in flight at once (pipelining). */
const MAX_CHUNK = 1000;
const MAX_IN_FLIGHT = 8;

export async function run(): Promise<void> {
  const total = parseInt(settings.required('MESSAGES'), 10); // this producer's share of the messages
  const size = parseInt(settings.required('MSG_SIZE'), 10);
  const nparts = parseInt(settings.required('PARTITIONS'), 10);
  const index = parseInt(settings.required('PRODUCER_INDEX'), 10); // which producer this is (0, 1, ...)
  const rate = parseFloat(settings.required('RATE')); // this producer's target messages per second; 0 = as fast as possible
  const randomPayload = settings.required('PAYLOAD') === 'random'; // incompressible data instead of a repeated filler
  const users = parseInt(settings.required('USERS'), 10); // how many different users (message keys) the messages are spread over
  const state = {
    sent: 0,
    delivered: 0,
    errors: 0,
    done: false,
    started: null as number | null,
    finished: null as number | null,
  };

  // Batching, acks, queue size and compression come from the backend (the same values for every language).
  // The client accepts librdkafka properties next to its own `kafkaJS` block; its typings do not list them.
  const producer = kafka.producer({ kafkaJS: {}, ...settings.kafkaConf('PRODUCER_CONF') } as never);
  await producer.connect();

  const report = (): Promise<void> =>
    reporting.post({
      runId: settings.RUN,
      worker: settings.WID,
      role: 'producer',
      lang: 'node',
      ts: Date.now() / 1000,
      sent: state.sent,
      delivered: state.delivered,
      errors: state.errors,
      done: state.done,
      started: state.started,
      finished: state.finished,
    });
  const stopReporter = reporting.startReporter(report);
  process.on('SIGTERM', () => {
    reporting.control.stopping = true;
  });

  // @producer-start
  // Messages go out in batches: building and sending them one by one would cost too much in Node.js.
  // Each round: build a batch, send it, keep at most MAX_IN_FLIGHT unconfirmed batches, and wait if we are too fast.
  const factory = new MessageFactory({
    run: settings.RUN,
    producerIndex: index,
    partitions: nparts,
    size,
    users,
    randomPayload,
  });
  const started = Date.now() / 1000;
  state.started = started;
  const chunk = pacingChunk(rate, MAX_CHUNK); // batch size: about 10 ms of the target rate, at most MAX_CHUNK
  const inFlight = new Set<Promise<void>>();
  for (let i = 0; i < total && !reporting.control.stopping; i += chunk) {
    const n = Math.min(chunk, total - i);
    const send: Promise<void> = producer
      .send({ topic: settings.TOPIC, messages: factory.batch(i, n) })
      .then(() => {
        state.delivered += n;
      })
      .catch((error: Error) => {
        state.errors += n;
        console.log('send failed:', error.message);
      })
      .finally(() => {
        inFlight.delete(send);
      });
    inFlight.add(send);
    state.sent += n;
    if (inFlight.size >= MAX_IN_FLIGHT) await Promise.race(inFlight); // do not run ahead of the broker
    const delay = pacingDelay(state.sent, rate, Date.now() / 1000 - started); // ahead of the target rate? then wait
    if (delay > 0) await reporting.sleep(delay * 1000);
  }
  await Promise.all(inFlight); // wait until every message is confirmed by the broker  // @producer-end
  state.finished = Date.now() / 1000;
  state.done = true;
  stopReporter();
  await report();
  await producer.disconnect();
  process.exit(0);
}
