import { expect, test } from '@playwright/test';
import { opensCleanly, visited } from './lab';

test.beforeEach(async ({ request }) => visited(request));

test.describe('every page opens', () => {
  const pages: [string, string | RegExp][] = [
    ['/', 'Dashboard'],
    ['/setup', 'New run'],
    ['/runs', 'Runs'],
    ['/code', 'Code'],
    ['/settings', 'Settings'],
    ['/welcome', 'Welcome to queue-lab'],
  ];
  for (const [path, heading] of pages) {
    test(`${path} shows "${heading}"`, async ({ page }) => opensCleanly(page, path, heading));
  }
});

test('the welcome page shows Docker and lets you pick the memory', async ({ page }) => {
  await page.goto('/welcome');
  await expect(page.getByText('found', { exact: true })).toBeVisible();
  await page.getByRole('radio', { name: /Recommended/ }).click();
  await expect(page.getByRole('radio', { name: /Recommended/ })).toHaveAttribute('aria-checked', 'true');
  await expect(page.getByText('Selected', { exact: true })).toBeVisible();
  await expect(page.getByText('How much processor does the lab get?')).toBeVisible();
});

test('the theme switches between dark and light and is remembered', async ({ page }) => {
  await page.goto('/');
  const html = page.locator('html');
  await expect(html).toHaveAttribute('data-theme', 'dark');
  await page.getByRole('button', { name: /Dark|Light/ }).click();
  await expect(html).toHaveAttribute('data-theme', 'light');
  await page.reload();
  await expect(html).toHaveAttribute('data-theme', 'light');
  await page.getByRole('button', { name: /Dark|Light/ }).click(); // leave it as it was
  await expect(html).toHaveAttribute('data-theme', 'dark');
});

test('the code is shown with colours, one set of steps for every language', async ({ page }) => {
  await page.goto('/code');
  await expect(page.getByRole('heading', { name: 'The same steps in every language' })).toBeVisible();
  const colored = page.locator('.code-line span[style*="color"]');
  await expect(colored.first()).toBeVisible(); // Shiki has coloured the tokens
  for (const name of ['Python', 'Java', 'Go', 'Rust']) {
    await page.getByRole('button', { name: new RegExp(`^${name}`) }).first().click();
    await expect(page.locator('.code-block').first()).toContainText(/\w/);
  }
});
