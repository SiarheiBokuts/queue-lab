import { expect, test } from '@playwright/test';
import { smallRun, visited, waitForRun, waitUntilIdle } from './lab';

/** A real run: Kafka, a sender and workers are started as containers, so this takes a minute or two. */

test('a run in each of two languages finishes without losing a message, and the two can be compared', async ({
  page,
  request,
}) => {
  await visited(request);
  await waitUntilIdle(request);

  const ids: string[] = [];
  for (const lang of ['go', 'python']) {
    const started = await request.post('/api/setups', { data: smallRun(`e2e-${lang}`, lang, 20_000) });
    expect(started.ok(), await started.text()).toBeTruthy();
    const body = await started.json();
    const id = body.id ?? body.run_id;
    ids.push(id);
    expect(await waitForRun(request, id)).toBe('completed');
  }

  const runs: { id: string; status: string; summary: { sent: number; received: number; lost: number; duplicates: number } }[] =
    await (await request.get('/api/runs')).json();
  for (const id of ids) {
    const run = runs.find((r) => r.id === id);
    expect(run?.status).toBe('completed');
    expect(run?.summary.sent).toBe(20_000);
    expect(run?.summary.lost).toBe(0);
    expect(run?.summary.duplicates).toBe(0);
  }

  // The report answers the three questions first.
  await page.goto(`/runs/${ids[0]}`);
  await expect(page.getByText('Finished', { exact: true })).toBeVisible();
  await expect(page.getByText('Nothing lost')).toBeVisible();
  await expect(page.getByText(/msg\/s/).first()).toBeVisible();
  await expect(page.getByText('Handled out of order')).toBeVisible();

  // The comparison shows numbers side by side and names no winner.
  await page.goto(`/compare/${ids.join(',')}`);
  await expect(page.getByText('All numbers side by side')).toBeVisible();
  await expect(page.getByText('These are numbers, not grades.')).toBeVisible();
  await expect(page.getByText(/winner/i)).toHaveCount(0);
});
