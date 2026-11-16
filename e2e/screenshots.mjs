// Takes the screenshots of the README from the running app: `npm run screenshots` (see CONTRIBUTING.md).
// It runs a comparison of the five languages (a few minutes) and a run that is going, so the screens show real numbers.
import { chromium } from '@playwright/test';

const base = process.env.BASE_URL ?? 'http://queuelab-app:8000';
const out = process.env.SCREENSHOTS_DIR ?? '/screenshots';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(path, method = 'GET', body) {
  const res = await fetch(base + path, {
    method,
    headers: body ? { 'content-type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`${method} ${path}: ${res.status} ${await res.text()}`);
  return res.json();
}
const state = async () => (await api('/api/state')).snapshot;

async function waitUntilIdle(tries = 120) {
  for (let i = 0; i < tries; i++) {
    const s = await state();
    if (!s?.run || !['pending', 'running'].includes(s.run.status)) return;
    await sleep(3000);
  }
}

const setup = (queue, lang, messages, scenario) => ({
  queue,
  partitions: 5,
  messages,
  message_size: 256,
  rate: 0,
  users: 1000,
  compression: 'none',
  payload: 'compressible',
  producer: { count: 1, lang: 'go', memory_mb: 384 },
  workers: { lang, count: 5, memory_mb: 400, concurrency: 100 },
  scenario,
});

await waitUntilIdle();
await api('/api/settings', 'PUT', { welcomed: true });

console.log('A comparison of the five languages (JSON scenario)...');
await api('/api/comparisons', 'POST', {
  setup: setup('screens-compare', 'go', 300_000, { type: 'json', param: 5 }),
  langs: ['python', 'node', 'java', 'go', 'rust'],
  cooldown_s: 3,
  producer_follows: false,
  random_order: false,
});
for (let i = 0; i < 200; i++) {
  await sleep(5000);
  const c = (await state())?.comparison;
  if (c && c.status !== 'running') break;
}
const cmp = (await state()).comparison;
console.log('comparison:', cmp.status);

const browser = await chromium.launch();
const page = await (await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: 'dark' })).newPage();

async function shot(name, url, height, wait = 2500) {
  await page.goto(base + url, { waitUntil: 'networkidle' });
  await sleep(wait);
  await page.screenshot({ path: `${out}/${name}.png`, clip: { x: 0, y: 0, width: 1440, height } });
  console.log('saved', name);
}

await api('/api/settings', 'PUT', { welcomed: false }); // the first visit
await shot('welcome', '/welcome', 1500);
await api('/api/settings', 'PUT', { welcomed: true });

await shot('compare', `/compare/${cmp.run_ids.join(',')}`, 1700);
await shot('report', `/runs/${cmp.run_ids[3]}`, 1700);
await shot('runs', '/runs', 900);
await shot('new-run', '/setup', 1500);
await shot('code', '/code', 1400);
await shot('settings', '/settings', 900);

console.log('A run that is going (a clean lab, so no stopped containers show)...');
await api('/api/reset', 'POST');
await sleep(3000);
await api('/api/setups', 'POST', setup('screens-live', 'go', 600_000, { type: 'delay', param: 20 }));
for (let i = 0; i < 90; i++) {
  await sleep(2000);
  const s = await state();
  if (s?.run?.status === 'running' && (s.run.sample?.recv_rate ?? 0) > 1000) break;
}
await sleep(12_000);
await shot('dashboard', '/', 1750, 1500);
await waitUntilIdle();

await browser.close();
console.log('done');