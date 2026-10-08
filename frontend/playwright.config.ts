import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './tests',
  webServer: process.env.PL_STATIC_PREVIEW ? {
    command: 'npm run preview -- --host 127.0.0.1 --port 5174',
    url: 'http://127.0.0.1:5174', reuseExistingServer: false, timeout: 30000,
  } : undefined,
  fullyParallel: false,
  workers: 1,
  timeout: 45_000,
  expect: { timeout: 10_000 },
  use: { baseURL: process.env.PL_DEMO_URL || 'http://127.0.0.1:5174',
    trace: 'retain-on-failure', screenshot: 'only-on-failure' },
  reporter: [['list'], ['json', { outputFile: 'test-results/browser-results.json' }]],
  projects: [
    { name: 'desktop', use: { viewport: { width: 1440, height: 960 } } },
    { name: 'mobile', use: { viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true } },
  ],
})
