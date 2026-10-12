import { chromium, expect } from '@playwright/test'
const url = process.env.PL_DEMO_URL || 'http://127.0.0.1:8010'
const destination = new URL('../../docs/images/', import.meta.url).pathname
const browser = await chromium.launch()
try {
  for (const width of [1440, 390]) {
    const page = await browser.newPage({ viewport: { width, height: width === 1440 ? 960 : 844 } })
    await page.goto(url)
    const menu = page.getByRole('button', { name: 'Navigation', exact: true })
    if (await menu.isVisible()) await menu.click()
    await page.getByRole('button', { name: 'Evidence Chat', exact: true }).click()
    await page.getByRole('button', { name: 'Predictions', exact: true }).click()
    await expect(page.getByLabel('Prediction fixture')).toBeEnabled({ timeout: 120000 })
    const option = page.getByLabel('Prediction fixture').locator('option').filter({ hasText: 'Liverpool vs Bournemouth' }).first()
    const identifier = await option.getAttribute('value')
    if (!identifier) throw Error('Expected real Liverpool/Bournemouth fixture absent')
    await page.getByLabel('Prediction fixture').selectOption(identifier)
    await page.getByRole('button', { name: 'Replay prediction', exact: true }).click()
    await expect(page.getByRole('heading', { name: 'Liverpool vs Bournemouth', exact: true })).toBeVisible({ timeout: 120000 })
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
    await page.screenshot({ path: destination + (width === 1440 ? 'prediction.png' : 'prediction-mobile.png'), fullPage: true })
    await page.getByText(/Inspect \d+ earlier result records/).click()
    await expect(page.getByRole('link', { name: /Original CSV/ }).first()).toBeVisible()
    await page.getByLabel('Prediction season').selectOption('2024-25')
    await expect(page.getByRole('heading', { name: 'Liverpool vs Bournemouth', exact: true })).toHaveCount(0)
    console.log(`${width}px real forecast, original source, season reset and no overflow passed`)
    await page.close()
  }
} finally { await browser.close() }
