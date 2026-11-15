/** Helpers shared by the tests: the API of the lab and the state a test starts from. */
import { expect, type APIRequestContext, type Page } from '@playwright/test';

/** The setup of a small run: the Go sender, five partitions, 2 workers of the chosen language. */
export function smallRun(queue: string, lang: string, messages = 20_000, scenario = { type: 'delay', param: 5 }) {
  return {
    queue,
    partitions: 5,
    messages,
    message_size: 256,
    rate: 0,
    users: 1000,
    compression: 'none',
    payload: 'compressible',
    producer: { count: 1, lang: 'go', memory_mb: 384 },
    workers: { lang, count: 2, memory_mb: 400, concurrency: 100 },
    scenario,
  };
}

/** The tests start from a lab that has been visited before (the welcome page is only for the first visit). */
export async function visited(request: APIRequestContext) {
  const res = await request.put('/api/settings', { data: { welcomed: true } });
  expect(res.ok()).toBeTruthy();
}

/** Wait until no run is going, so a test never starts while another one is still running. */
export async function waitUntilIdle(request: APIRequestContext, seconds = 180) {
  for (let i = 0; i < seconds / 3; i++) {
    const state = await (await request.get('/api/state')).json();
    const status = state.snapshot?.run?.status;
    if (!status || !['pending', 'running'].includes(status)) return;
    await new Promise((r) => setTimeout(r, 3000));
  }
  throw new Error('A run is still going after waiting');
}

/** The heading of the page and no error in the console: what "the page works" means in these tests. */
export async function opensCleanly(page: Page, path: string, heading: string | RegExp) {
  const errors: string[] = [];
  page.on('pageerror', (e) => errors.push(e.message));
  await page.goto(path);
  await expect(page.getByRole('heading', { name: heading, level: 1 })).toBeVisible();
  expect(errors).toEqual([]);
}

/** Wait until the run with this id is over (the state of the app lags a second behind a start, so wait for the id). */
export async function waitForRun(request: APIRequestContext, id: string, seconds = 300) {
  for (let i = 0; i < seconds / 3; i++) {
    const runs: { id: string; status: string }[] = await (await request.get('/api/runs')).json();
    const run = runs.find((r) => r.id === id);
    if (run && !['pending', 'running'].includes(run.status)) return run.status;
    await new Promise((r) => setTimeout(r, 3000));
  }
  throw new Error(`The run ${id} did not finish in ${seconds} s`);
}
