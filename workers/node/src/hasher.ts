/** Runs inside a worker thread: burns CPU for the "cpu" scenario so the main thread can keep reading from Kafka. */
import { hash } from 'node:crypto';
import { parentPort } from 'node:worker_threads';

const SEED = Buffer.from('queuelab'); // starting value of the hash chain, the same in every language

parentPort?.on('message', (task: { id: number; rounds: number }) => {
  let h: Buffer = SEED;
  for (let i = 0; i < task.rounds; i++) h = hash('sha256', h, 'buffer'); // one-shot hash, no object per round
  parentPort?.postMessage(task.id);
});
