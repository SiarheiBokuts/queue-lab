import { defineConfig } from '@playwright/test';

/**
 * End-to-end tests of the running lab. They need the app up (`docker compose up`) and run in a container on the
 * sandbox network, because the app only answers to localhost and to its own alias (see backend/app/security.py).
 * See compose.e2e.yaml and CONTRIBUTING.md.
 */
export default defineConfig({
  testDir: './tests',
  fullyParallel: false, // the tests share one lab: one Kafka, one run at a time
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  reporter: [['list']],
  use: {
    baseURL: process.env.BASE_URL ?? 'http://queuelab-app:8000',
    viewport: { width: 1440, height: 900 },
    colorScheme: 'dark',
    trace: 'retain-on-failure',
  },
  projects: [
    // Pages and the form: a few seconds each, nothing is started.
    { name: 'fast', testMatch: /.*\.fast\.spec\.ts/, timeout: 30_000 },
    // A real run with real containers: it starts Kafka and the workers, so it takes a minute or two.
    { name: 'slow', testMatch: /.*\.slow\.spec\.ts/, timeout: 360_000 },
  ],
});
