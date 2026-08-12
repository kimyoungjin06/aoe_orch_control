import { defineConfig, devices } from '@playwright/test';

const previewPort = Number(process.env.FORAGER_PLAYWRIGHT_PORT ?? 4322);
const previewOrigin = `http://127.0.0.1:${previewPort}`;

export default defineConfig({
  testDir: './tests/visual',
  outputDir: './test-results',
  fullyParallel: true,
  workers: Number(process.env.FORAGER_PLAYWRIGHT_WORKERS ?? (process.env.CI ? '2' : '8')),
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: previewOrigin,
    trace: 'retain-on-failure',
  },
  webServer: {
    command: `npm run preview -- --host 127.0.0.1 --port ${previewPort}`,
    url: `${previewOrigin}/forager-cli/review/`,
    env: {
      // Astro 7 backgrounds preview servers in detected agent environments.
      // Playwright needs to own a foreground process for reliable teardown.
      ASTRO_PREVIEW_BACKGROUND: '0',
    },
    reuseExistingServer: false,
    timeout: 30_000,
  },
  projects: [
    {
      name: 'chromium-desktop',
      use: {
        ...devices['Desktop Chrome'],
        viewport: { width: 1440, height: 1000 },
      },
    },
    {
      name: 'chromium-mobile',
      use: {
        ...devices['Pixel 5'],
      },
    },
  ],
});
