/** Runs inside a worker thread: the JSON work of the "json" scenario, so the main thread can keep reading from Kafka. */
import { parentPort } from 'node:worker_threads';

parentPort?.on('message', (task: { id: number; rounds: number; document: string }) => {
  let text = task.document;
  for (let i = 0; i < task.rounds; i++) text = JSON.stringify(JSON.parse(text)); // parse the document, write it back
  parentPort?.postMessage(task.id);
});
