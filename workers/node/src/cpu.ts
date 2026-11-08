/**
 * A pool of worker threads for CPU-bound work. One Node.js thread can use one core only; a service that computes keeps
 * the reading thread free and sends the computing to a pool, which is what this does. The size is the number of
 * cores, at most CPU_THREADS: the same budget in every language.
 */
import { availableParallelism } from 'node:os';
import path from 'node:path';
import { Worker } from 'node:worker_threads';
import { CPU_THREADS } from './settings.js';


export class HashPool {
  private readonly threads: Worker[];
  private readonly waiting = new Map<number, () => void>();
  private nextId = 0;
  private nextThread = 0;

  constructor(size: number = Math.min(CPU_THREADS, availableParallelism())) {
    this.threads = Array.from({ length: size }, () => {
      const thread = new Worker(path.join(__dirname, 'hasher.js'));
      thread.on('message', (id: number) => {
        this.waiting.get(id)?.();
        this.waiting.delete(id);
      });
      thread.unref(); // an idle pool must not keep the process alive
      return thread;
    });
  }

  get size(): number {
    return this.threads.length;
  }

  /** Parse and write back the JSON document `rounds` times on one of the threads (in turn). Resolves when done. */
  run(document: string, rounds: number): Promise<void> {
    const id = this.nextId++;
    const thread = this.threads[this.nextThread++ % this.threads.length] as Worker;
    return new Promise((resolve) => {
      this.waiting.set(id, resolve);
      thread.postMessage({ id, rounds, document });
    });
  }

  async close(): Promise<void> {
    await Promise.all(this.threads.map((t) => t.terminate()));
  }
}
