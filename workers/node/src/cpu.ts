/**
 * A pool of worker threads for CPU-bound work. One Node.js thread can use one core only; a service that computes keeps
 * the reading thread free and sends the computing to a pool, which is what this does. The size is the number of
 * cores, at most MAX_THREADS (every thread costs memory, and a worker container has a memory limit).
 */
import { availableParallelism } from 'node:os';
import path from 'node:path';
import { Worker } from 'node:worker_threads';

const MAX_THREADS = 4;

export class HashPool {
  private readonly threads: Worker[];
  private readonly waiting = new Map<number, () => void>();
  private nextId = 0;
  private nextThread = 0;

  constructor(size: number = Math.min(MAX_THREADS, availableParallelism())) {
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

  /** Compute `rounds` hashes on one of the threads (in turn). Resolves when it is done. */
  run(rounds: number): Promise<void> {
    const id = this.nextId++;
    const thread = this.threads[this.nextThread++ % this.threads.length] as Worker;
    return new Promise((resolve) => {
      this.waiting.set(id, resolve);
      thread.postMessage({ id, rounds });
    });
  }

  async close(): Promise<void> {
    await Promise.all(this.threads.map((t) => t.terminate()));
  }
}
