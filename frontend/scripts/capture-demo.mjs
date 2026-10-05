import { chromium, expect } from '@playwright/test'
import { mkdir } from 'node:fs/promises'

const url = process.env.PL_DEMO_URL || 'http://127.0.0.1:8010'
const destination = new URL('../../docs/images/', import.meta.url).pathname
await mkdir(destination, { recursive: true })
const browser = await chromium.launch()
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 960 }, deviceScaleFactor: 1 })
  await page.goto(url)
  await expect(page.getByText(/NODES.*EDGES/)).toBeVisible()
  await page.getByLabel('Filter fixtures').fill('Liverpool')
  await page.getByRole('button', { name: /Liverpool versus Bournemouth/ }).first().click()
  await expect(page.getByText(/NODES.*EDGES/)).toBeVisible()
  await page.waitForTimeout(2500) // Allow the graph simulation to settle for capture.
  await page.mouse.move(870, 500)
  await page.mouse.wheel(0, -800)
  await page.waitForTimeout(600)
  await page.screenshot({ path: destination + 'demo-match.png', fullPage: true })
  await page.getByRole('button', { name: 'Evidence Chat', exact: true }).click()
  await page.getByLabel('Football statistics question').fill("Compare Aston Villa and Liverpool's season statistics.")
  await page.getByRole('button', { name: 'Send', exact: true }).click()
  await expect(page.getByText('65 points', { exact: false })).toBeVisible()
  await page.waitForTimeout(700)
  await page.screenshot({ path: destination + 'demo-analysis.png', fullPage: true })
  const mobile = await browser.newPage({ viewport: { width: 390, height: 844 }, deviceScaleFactor: 1 })
  await mobile.goto(url)
  await mobile.getByRole('button', { name: 'Navigation', exact: true }).click()
  await mobile.getByRole('button', { name: 'Evidence Chat', exact: true }).click()
  await mobile.getByLabel('Football statistics question').fill("Compare Aston Villa and Liverpool's season statistics.")
  await mobile.getByRole('button', { name: 'Send', exact: true }).click()
  await expect(mobile.getByText('65 points', { exact: false })).toBeVisible()
  await mobile.waitForTimeout(700)
  await mobile.screenshot({ path: destination + 'demo-mobile.png', fullPage: true })
} finally {
  await browser.close()
}
