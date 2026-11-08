/**
 * Everything a worker reads from its environment, parsed once at start-up.
 *
 * The environment is the "worker contract" defined in the backend (backend/app/contract.py). Reading it all here keeps
 * the rest of the worker free of `process.env`. Kafka client tuning (batching, acks, timeouts) is NOT decided here: the
 * backend sends the same values to the Python and the Node.js workers (PRODUCER_CONF / CONSUMER_CONF), so comparisons
 * between the two languages are fair.
 */
import { parseEdges } from './lib.js';

/** Read a required environment variable or fail fast with a clear message. */
export function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === '') throw new Error(`Missing environment variable ${name}`);
  return value;
}

export const ROLE = required('ROLE'); // "producer" or "consumer"
export const BOOT = required('KAFKA_BOOTSTRAP'); // broker address
export const TOPIC = required('TOPIC'); // the queue
export const RUN = required('RUN_ID'); // id of this run; messages of other runs are ignored
export const WID = process.env.WORKER_ID ?? 'worker'; // container name, identifies this worker in reports
export const REPORT = required('REPORT_URL'); // where the once-per-second report is POSTed
// Latency histogram: instead of sending every latency we count messages per bucket. EDGES are the upper edge of each
// bucket in milliseconds (for example 0.5, 1, 2, 5 ... 10000); the backend sends them so they are defined only once.
export const EDGES = parseEdges(required('HIST_EDGES'));
export const CONCURRENCY = Math.max(1, Number.parseInt(process.env.CONCURRENCY ?? '1', 10) || 1); // messages one worker handles at the same time = lanes (consumer only)
export const ORDERING = process.env.ORDERING ?? 'key'; // 'key': one user stays in one lane (in order); 'none': any lane (consumer only)

/** Kafka client properties (librdkafka names) sent by the backend as JSON, e.g. {"property.name": value}. */
export function kafkaConf(variable: string): Record<string, unknown> {
  return JSON.parse(required(variable)) as Record<string, unknown>;
}