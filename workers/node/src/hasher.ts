/** Runs inside a worker thread: burns CPU for the "cpu" scenario so the main thread can keep reading from Kafka. */
import { parentPort } from 'node:worker_threads';

const SEED = 2463534242; // starting value of the xorshift chain, the same in every language

parentPort?.on('message', (task: { id: number; rounds: number }) => {
  let x = SEED;
  for (let i = 0; i < task.rounds; i++) {
    x ^= x << 13; // plain integer code in the language itself, no library does the work
    x ^= x >>> 17;
    x ^= x << 5;
    x >>>= 0; // keep it an unsigned 32-bit number
  }
  parentPort?.postMessage(task.id);
});
