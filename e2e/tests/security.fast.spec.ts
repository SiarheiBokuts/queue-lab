import { expect, test } from '@playwright/test';

/** The app drives the Docker Engine, so it must not answer to a foreign host or to a page of another site. */

test('a request that names another host is refused', async ({ request }) => {
  const res = await request.get('/api/state', { headers: { Host: 'evil.example.com' } });
  expect(res.status()).toBe(400);
});

test('a change coming from a page of another site is refused', async ({ request }) => {
  const res = await request.put('/api/settings', {
    data: { welcomed: true },
    headers: { Origin: 'https://evil.example.com' },
  });
  expect(res.status()).toBe(403);
});

test('the pages carry the security headers', async ({ request }) => {
  const res = await request.get('/');
  const headers = res.headers();
  expect(headers['content-security-policy']).toContain("default-src 'self'");
  expect(headers['x-content-type-options']).toBe('nosniff');
});

test('a worker report that is far too big is refused', async ({ request }) => {
  const res = await request.post('/internal/report', { data: { padding: 'x'.repeat(1_200_000) } });
  expect(res.status()).toBe(413);
});

test('the log stream of a container that is not the lab\'s is refused', async ({ page }) => {
  await page.goto('/');
  const result = await page.evaluate(
    () =>
      new Promise<{ opened: boolean }>((resolve) => {
        const ws = new WebSocket(`ws://${location.host}/ws/logs/some-other-container`);
        let opened = false;
        ws.onopen = () => {
          opened = true;
        };
        ws.onclose = () => resolve({ opened });
        ws.onerror = () => undefined;
      }),
  );
  expect(result.opened).toBe(false); // the server refuses the handshake, so the socket never opens
});
