/**
 * The consumer (the "handler"): reads messages in batches, verifies them and runs the chosen scenario.
 *
 * The same loop is written in every language of the lab, with the same steps and names:
 *
 *   readBatch -> groupByUser -> runBatch (handleGroup for every user at the same time) -> commit
 *
 * A batch is at most CONCURRENCY messages. The messages of one user are always handled one after another, like in
 * production; the messages of different users are handled at the same time. The next batch is read when the whole
 * batch is done: the client has already fetched the next messages in the background, and starting early could let a
 * message of a busy user overtake an older one.
 */
import type { KafkaMessage } from '@confluentinc/kafka-javascript/types/kafkajs.js';
import { HashPool } from './cpu.js';
import { kafka } from './kafka.js';
import { groupByUser, makeHeaderParser, nowMs } from './lib.js';
import * as reporting from './reporting.js';
import * as settings from './settings.js';
import { ConsumerStats } from './stats.js';

const SCENARIO = process.env.SCENARIO ?? 'echo';
const PARAM = parseInt(process.env.SCENARIO_PARAM ?? '0', 10) || 0;

/** The scenario for one message. */
function makeHandle(cpuPool: HashPool): () => Promise<void> {
  // @scenario-start
  /**
   * What the worker does with one message. The same three scenarios in every language:
   * echo = nothing, the message is only counted; delay = wait, like a call to a database;
   * json = parse a JSON document and write it back, like a service reading a request.
   */
  return async (): Promise<void> => {
    if (SCENARIO === 'delay' && PARAM > 0) await reporting.sleep(PARAM); // wait PARAM milliseconds and do nothing else
    else if (SCENARIO === 'json' && PARAM > 0) {
      // Parse the document and write it back PARAM times. It runs in another thread, so this one stays free to read.
      await cpuPool.run(settings.JSON_DOC, PARAM);
    }
    // echo: nothing to do
  };
  // @scenario-end
}

/** Handle the messages of one user one after another. */
async function handleGroup(group: KafkaMessage[], handle: () => Promise<void>): Promise<void> {
  for (let i = 0; i < group.length; i++) await handle();
}

/** Handle all groups at the same time and wait until every one of them is done. */
async function runBatch(groups: KafkaMessage[][], handle: () => Promise<void>): Promise<void> {
  await Promise.all(groups.map((group) => handleGroup(group, handle)));
}

export async function run(): Promise<void> {
  const parse = makeHeaderParser(settings.RUN); // reads the header straight from the bytes of a message
  const stats = new ConsumerStats(settings.EDGES);

  // Offset reset, commit and timeout settings come from the backend (the same values for every language).
  // The client accepts librdkafka properties next to its own `kafkaJS` block; its typings do not list them.
  // `js.consumer.max.batch.size` is the batch size: the client hands over up to that many messages at a time.
  const consumer = kafka.consumer({
    kafkaJS: { groupId: settings.required('GROUP_ID') },
    'js.consumer.max.batch.size': settings.CONCURRENCY,
    ...settings.kafkaConf('CONSUMER_CONF'),
  } as never);
  await consumer.connect();
  await consumer.subscribe({ topics: [settings.TOPIC] });

  const sendReport = async (final: boolean): Promise<void> => {
    let assigned: number[] = [];
    try {
      assigned = consumer.assignment().map((a) => a.partition).sort((a, b) => a - b);
    } catch {
      // not assigned yet
    }
    await reporting.post(stats.report({ runId: settings.RUN, worker: settings.WID }, assigned, final));
  };
  const stopReporter = reporting.startReporter(() => sendReport(false));

  const cpuPool = new HashPool(SCENARIO === 'json' ? undefined : 0); // threads only when there is something to compute
  const handle = makeHandle(cpuPool);

  await consumer.run({
    // readBatch: the client gives up to CONCURRENCY messages of one partition and waits at most BATCH_WAIT_MS for them.
    eachBatch: async ({ batch }) => {
      const arrivedMs = nowMs();
      const mine: KafkaMessage[] = [];
      for (const m of batch.messages) {
        if (reporting.control.stopping) break;
        const header = m.value === null ? null : parse(m.value);
        if (header === null) continue; // empty, or a leftover from another run: ignore it
        stats.record(header, batch.partition, arrivedMs);
        mine.push(m);
      }
      stats.lastAt = Date.now() / 1000;
      const next = mine.length > 0 ? parseInt(mine[mine.length - 1]!.offset, 10) + 1 : null; // the next offset to read
      if (next !== null) stats.offsets[batch.partition] = next; // also used for the lag
      await runBatch(groupByUser(mine), handle);
      // commit: tell Kafka that everything read so far is handled. A crash before this repeats the batch.
      if (next !== null) await consumer.commitOffsets([{ topic: settings.TOPIC, partition: batch.partition, offset: String(next) }]);
    },
  });

  /** Finish what was taken, report, and leave the group cleanly so the partitions are reassigned at once. */
  const shutdown = async (): Promise<void> => {
    reporting.control.stopping = true;
    stopReporter();
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
