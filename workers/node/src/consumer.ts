/** The consumer (the "handler"): reads messages in a consumer group, verifies them, runs the chosen scenario. */
import { HashPool } from './cpu.js';
import { kafka } from './kafka.js';
import { SeqTracker, bucket, laneFor, makeHeaderParser, nowMs } from './lib.js';
import * as reporting from './reporting.js';
import * as settings from './settings.js';

/** Messages that may wait in front of one lane before the reading loop pauses. */
const LANE_QUEUE = 8;

export async function run(): Promise<void> {
  const scenario = process.env.SCENARIO ?? 'echo';
  const param = parseInt(process.env.SCENARIO_PARAM ?? '0', 10) || 0;
  const tracker = new SeqTracker(); // detects gaps and duplicates per partition
  const parse = makeHeaderParser(settings.RUN); // reads the header straight from the bytes of a message
  const emptyHistogram = (): number[] => new Array<number>(settings.EDGES.length + 1).fill(0);
  const state = {
    received: 0,
    errors: 0,
    hist: emptyHistogram(),
    offsets: {} as Record<number, number>,
    firstAt: null as number | null,
    lastAt: null as number | null,
    maxLatencyMs: 0,
  };

  // Offset reset, commit and timeout settings come from the backend (the same values for every language).
  // The client accepts librdkafka properties next to its own `kafkaJS` block; its typings do not list them.
  const consumer = kafka.consumer({
    kafkaJS: { groupId: settings.required('GROUP_ID') },
    ...settings.kafkaConf('CONSUMER_CONF'),
  } as never);
  await consumer.connect();
  await consumer.subscribe({ topics: [settings.TOPIC] });

  /** Send cumulative counters. The histogram is sent as a delta and reset after each report. */
  const sendReport = async (final: boolean): Promise<void> => {
    let assigned: number[] = [];
    try {
      assigned = consumer.assignment().map((a) => a.partition).sort((a, b) => a - b);
    } catch {
      // not assigned yet
    }
    const payload = {
      runId: settings.RUN, worker: settings.WID, role: 'consumer', lang: 'node', ts: Date.now() / 1000,
      ready: assigned.length > 0, assigned, received: state.received, duplicates: tracker.duplicates,
      gaps: tracker.gaps, errors: state.errors, offsets: state.offsets, hist: state.hist, final,
      firstAt: state.firstAt, lastAt: state.lastAt, maxLatencyMs: state.maxLatencyMs,
    };
    state.hist = emptyHistogram();
    await reporting.post(payload);
  };
  const stopReporter = reporting.startReporter(() => sendReport(false));

  const cpuPool = new HashPool(scenario === 'cpu' ? undefined : 0); // threads only when there is something to compute
  // @scenario-start
  /** What the worker does with one message. Several of these run at the same time when CONCURRENCY is above 1. */
  const handle = async (): Promise<void> => {
    if (scenario === 'delay' && param > 0) await reporting.sleep(param); // pretend to wait for a database or an API call
    else if (scenario === 'cpu' && param > 0) await cpuPool.run(param); // computing happens in a pool of worker threads
    // echo: do nothing, just count the message
  };
  // @scenario-end

  // Production style: ONE loop reads from Kafka and hands every message to a "lane". There are CONCURRENCY lanes, each
  // handles its messages one after another, so up to CONCURRENCY messages are handled at the same time (while one lane
  const laneCount = settings.CONCURRENCY;
  const useLanes = laneCount > 1 && scenario !== 'echo';
  const lanes = Array.from({ length: useLanes ? laneCount : 0 }, () => ({ tail: Promise.resolve(), waiting: 0 }));
  let nextLane = 0;
  const dispatch = async (key: string): Promise<void> => {
    if (!useLanes) return handle(); // concurrency 1 (or nothing to wait for): one message at a time
    const lane = lanes[settings.ORDERING === 'key' ? laneFor(key, laneCount) : nextLane++ % laneCount] as (typeof lanes)[number];
    if (lane.waiting >= LANE_QUEUE) await lane.tail.catch(() => undefined); // that lane is full: wait
    lane.waiting += 1;
    lane.tail = lane.tail.catch(() => undefined).then(handle).finally(() => { // a failed message must not stop the ones behind it
      lane.waiting -= 1;
    });
  };
  await consumer.run({
    eachBatch: async ({ batch }) => {
      const arrivedMs = nowMs();
      const nowSeconds = Date.now() / 1000;
      const partition = batch.partition;
      let lastOffset: string | null = null;
      for (const m of batch.messages) {
        if (reporting.control.stopping) break;
        const value = m.value;
        if (value === null) continue;
        const header = parse(value);
        if (header === null) continue; // a leftover from another run: ignore it
        state.received += 1;
        if (state.firstAt === null) state.firstAt = arrivedMs / 1000;
        tracker.observe(header.producer * 4096 + partition, header.seq); // counts gaps (lost) and duplicates: one key per stream
        lastOffset = m.offset;
        const latency = arrivedMs - header.sentMs; // time from "sent" to "received", in milliseconds
        state.hist[bucket(latency, settings.EDGES)] += 1;
        if (latency > state.maxLatencyMs) state.maxLatencyMs = latency;
        // Echo has nothing to do per message, so it does not even wait a tick; the others hand the message on.
        if (useLanes) await dispatch(m.key ? m.key.toString('latin1') : '');
        else if (scenario !== 'echo') await handle();
      }
      state.lastAt = nowSeconds;
      if (lastOffset !== null) state.offsets[partition] = parseInt(lastOffset, 10) + 1; // the next offset to read, used for lag
    },
  });
  /** Leave the group cleanly so the partitions are reassigned at once. */
  const shutdown = async (): Promise<void> => {
    reporting.control.stopping = true;
    stopReporter();
    await Promise.allSettled(lanes.map((lane) => lane.tail)); // let the messages already taken finish before the final report
    await cpuPool.close();
    await sendReport(true);
    try {
      await consumer.disconnect();
    } catch {
      // ignore
    }
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}
