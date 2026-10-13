import { chromium, expect } from '@playwright/test'

const url = process.env.PL_DEMO_URL || 'http://127.0.0.1:8010'
const destination = new URL('../../docs/images/', import.meta.url).pathname
const browser = await chromium.launch()
try {
  for (const width of [1440, 390]) {
    const page = await browser.newPage({ viewport: { width, height: width === 1440 ? 960 : 844 } })
    await page.goto(url + '/#statistics')
    const input = page.getByLabel('Football statistics question')
    for (const [index, [question, answer]] of [
      ['Liverpool points', '60 points'],
      ['What about Villa?', '65 points'],
    ].entries()) {
      await input.fill(question)
      await page.getByRole('button', { name: 'Send', exact: true }).click()
      await expect(page.getByText(answer, { exact: false })).toBeVisible({ timeout: 90000 })
      if (width === 390 && index === 0) {
        await page.screenshot({ path: destination + 'conversation-mobile.png', animations: 'disabled' })
      }
    }
    if (width === 1440) {
      await page.screenshot({ path: destination + 'conversation.png', animations: 'disabled' })
    }
    await input.fill('How did they do at home?')
    await page.getByRole('button', { name: 'Send', exact: true }).click()
    await expect(page.getByText('38 points in 19 home matches', { exact: false })).toBeVisible({ timeout: 90000 })
    await page.getByRole('button', { name: /source/ }).last().click()
    await expect(page.getByRole('link', { name: 'View source' }).last()).toHaveAttribute('href', /^https:\/\//)
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
    console.log(`${width}px verified three-turn chat, source link and no overflow`)
    await page.close()
  }
} finally { await browser.close() }
