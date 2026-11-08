/** Progress reporting shared by the producer and the consumer: the stop flag and the HTTP report. */
import * as settings from './settings.js';

/** Set on SIGTERM so loops can finish and send a final report. */
export const control = { stopping: false };

export const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** POST a progress report to the backend. A failed report is logged and skipped, never fatal. */
export async function post(payload: object): Promise<void> {
  try {
    await fetch(settings.REPORT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(3000),
    });
  } catch (error) {
    console.log('report failed:', (error as Error).message);
  }
}

/** Call `send` once per second until the returned function is called. */
export function startReporter(send: () => Promise<void>): () => void {
  const timer = setInterval(() => void send(), 1000);
  return () => clearInterval(timer);
}