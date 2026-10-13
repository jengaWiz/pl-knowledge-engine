import { test, expect, type Page } from '@playwright/test'

async function open(page: Page) {
  await page.goto('/#statistics')
  await expect(page.locator('.conversation-bar').getByText('Football analyst', { exact: true })).toBeVisible()
}

test('follow-up carries the previous question and offers cited fixture choices', async ({ page }) => {
  await page.route('**/api/matches', r => r.fulfill({ json: [] }))
  await page.route('**/api/chat', r => {
    const body = r.request().postDataJSON()
    const result = body.message === 'Liverpool points'
      ? { reply: 'Liverpool: **65 points** from verified matches.', sources: [{ type: 'statistics', summary: 'Original results', url: 'https://example.org/season.csv', record_ids: ['m1'] }], season: body.season, status: 'ok' }
      : body.message === 'What about Villa?'
        ? { reply: 'Aston Villa: **63 points** from verified matches.', sources: [{ type: 'statistics', summary: 'Original results', url: 'https://example.org/season.csv', record_ids: ['m2'] }], season: body.season, status: 'ok' }
        : { reply: 'Which fixture date?', sources: [], suggestions: ['Liverpool vs Bournemouth on 2025-08-15'], season: body.season, status: 'insufficient_evidence' }
    if (body.message === 'What about Villa?') expect(body.history.at(-2)?.content).toBe('Liverpool points')
    return r.fulfill({ json: result })
  })
  await open(page)
  const input = page.getByLabel('Football statistics question')
  await input.fill('Liverpool points')
  await page.getByRole('button', { name: 'Send', exact: true }).click()
  await expect(page.getByText('65 points', { exact: false })).toBeVisible()
  await page.getByRole('button', { name: '1 source' }).click()
  await expect(page.getByRole('link', { name: 'View source' })).toHaveAttribute('href', 'https://example.org/season.csv')
  await input.fill('What about Villa?')
  await page.getByRole('button', { name: 'Send', exact: true }).click()
  await expect(page.getByText('63 points', { exact: false })).toBeVisible()
  await page.reload()
  await expect(page.getByText('63 points', { exact: false })).toBeVisible()
  await input.fill('Who won Liverpool vs Bournemouth?')
  await page.getByRole('button', { name: 'Send', exact: true }).click()
  await expect(page.getByRole('button', { name: /Liverpool vs Bournemouth on 2025-08-15/ })).toBeVisible()
  await page.getByRole('button', { name: 'New conversation' }).click()
  await expect(page.getByText('63 points', { exact: false })).toHaveCount(0)
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
})

test('season switch clears context and failed request keeps the draft', async ({ page }) => {
  await page.route('**/api/matches', r => r.fulfill({ json: [] }))
  await page.route('**/api/chat', r => r.fulfill({ status: 503, json: { detail: 'private-path-secret' } }))
  await open(page)
  await page.getByLabel('Conversation season').selectOption('2024-25')
  const input = page.getByLabel('Football statistics question')
  await input.fill('Liverpool points')
  await page.getByRole('button', { name: 'Send', exact: true }).click()
  await expect(page.getByRole('alert')).toContainText('unavailable')
  await expect(input).toHaveValue('Liverpool points')
  await expect(page.getByText('private-path-secret')).toHaveCount(0)
})
