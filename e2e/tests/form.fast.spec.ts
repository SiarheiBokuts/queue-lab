import { expect, test } from '@playwright/test';
import { smallRun, visited } from './lab';

test.beforeEach(async ({ request }) => visited(request));

test('a ready-made setup fills the form', async ({ page }) => {
  await page.goto('/setup');
  await page.getByRole('button', { name: 'Slow service (waiting)' }).click();
  // The waiting scenario is selected, with the delay of the preset in its field.
  await expect(page.getByRole('spinbutton', { name: /Delay per message/ })).toHaveValue('20');
  await expect(page.getByText(/5 × Node\.js workers/)).toBeVisible();
});

test('a huge number is accepted and the form says what to expect', async ({ page }) => {
  await page.goto('/setup');
  const messages = page.getByLabel('Messages in total');
  await messages.fill('200000000');
  await messages.blur();
  await expect(page.getByText('Good to know before you start')).toBeVisible();
  await expect(page.getByText(/will pass through Kafka/)).toBeVisible();
  // Nothing is refused because of the size of a number: the start button stays usable.
  await expect(page.getByRole('button', { name: 'Create and run' })).toBeEnabled();
});

test('the plan API warns about big numbers and refuses nothing', async ({ request }) => {
  const res = await request.post('/api/plan', {
    data: { ...smallRun('big', 'go'), partitions: 300, messages: 200_000_000, message_size: 2_000_000, rate: 2000, users: 5 },
  });
  expect(res.ok()).toBeTruthy();
  const plan = await res.json();
  expect(plan.warnings.length).toBeGreaterThanOrEqual(4);
  expect(plan.warnings.join(' ')).toMatch(/refuse it/); // a message above the broker limit
});

test('only what a worker cannot read is refused', async ({ request }) => {
  const res = await request.post('/api/plan', { data: { ...smallRun('too-big', 'go'), partitions: 2 ** 31 } });
  expect(res.status()).toBe(422);
});

test('compare mode starts with every language selected', async ({ page }) => {
  await page.goto('/setup');
  await page.getByRole('button', { name: /Compare languages/ }).first().click();
  for (const name of ['Python', 'Node.js', 'Java', 'Go', 'Rust']) {
    await expect(page.getByRole('button', { name: new RegExp(name) }).first()).toBeVisible();
  }
  await expect(page.getByRole('button', { name: /Run and compare 5 languages/ })).toBeVisible();
});
