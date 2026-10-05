import { test, expect, type Page } from '@playwright/test'

async function navigate(page: Page, name: string) {
  const menu = page.getByRole('button', { name: 'Navigation', exact: true })
  if (await menu.isVisible()) await menu.click()
  await page.getByRole('button', { name, exact: true }).click()
}

test('populated graph, player search and fixture navigation', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText(/NODES.*EDGES/)).toBeVisible()
  await page.getByLabel('Search graph player').fill('Salah')
  const playerResponse = page.waitForResponse(r => r.url().includes('/api/graph/player/'))
  await page.getByRole('button', { name: 'Search', exact: true }).click()
  expect((await playerResponse).status()).toBe(200)
  await expect(page.getByText(/NODES.*EDGES/)).toBeVisible()
  await expect(page.getByText(/is unavailable in the verified corpus/)).toHaveCount(0)
  const menu = page.getByRole('button', { name: 'Navigation', exact: true })
  if (await menu.isVisible()) await menu.click()
  await page.getByLabel('Filter fixtures').fill('Liverpool')
  const fixture = page.getByRole('button', { name: /Liverpool versus Bournemouth/ }).first()
  await expect(fixture).toBeVisible()
  await expect(fixture).toContainText('15 Aug')
  const matchResponse = page.waitForResponse(r => r.url().includes('/api/graph/match/'))
  await fixture.focus()
  await fixture.press('Enter')
  expect((await matchResponse).status()).toBe(200)
  await expect(page.getByText(/NODES.*EDGES/)).toBeVisible()
  await page.getByRole('button', { name: 'Overview', exact: true }).click()
  await expect(page.getByText(/NODES.*EDGES/)).toBeVisible()
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
})

test('deduction and source opening', async ({ page }) => {
  await page.goto('/')
  await navigate(page, 'Evidence Chat')
  await page.getByRole('button', { name: "Who are Liverpool's top scorers this season?", exact: true }).click()
  await expect(page.getByText('Hugo Ekitiké:', { exact: false })).toBeVisible()
  await expect(page.getByText('11 goals', { exact: false })).toBeVisible()
  await page.getByRole('button', { name: /\d+ sources?/ }).click()
  const source = page.getByRole('link', { name: 'View source' }).first()
  await expect(source).toHaveAttribute('href', /^https:\/\//)
  const target = await source.getAttribute('href')
  // Opening is checked locally; an external publisher outage must not fail the demo.
  await page.context().route(target!, route => route.fulfill({ body: 'Source opened' }))
  const popupPromise = page.waitForEvent('popup')
  await source.click()
  const popup = await popupPromise
  await expect(popup).toHaveURL(target!)
  await popup.close()
  await page.getByLabel('Football statistics question').fill('Will Liverpool win next season?')
  await page.getByRole('button', { name: 'Send', exact: true }).click()
  await expect(page.getByText('I cannot establish that', { exact: false })).toBeVisible()
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
})

test('missing fixtures can be retried', async ({ page }) => {
  let fail = true
  await page.route('**/api/matches', route => fail
    ? route.fulfill({ status: 503, json: { detail: 'Dataset unavailable' } }) : route.continue())
  await page.goto('/')
  const menu = page.getByRole('button', { name: 'Navigation', exact: true })
  if (await menu.isVisible()) await menu.click()
  await expect(page.getByRole('alert')).toContainText('Fixtures are unavailable')
  fail = false
  await page.getByRole('button', { name: 'Retry fixtures' }).click()
  await expect(page.getByText('380 MATCHES', { exact: true })).toBeVisible()
  await page.getByLabel('Filter fixtures').fill('no-such-club')
  await expect(page.getByText('No matches found')).toBeVisible()
})

test('failed chat preserves a retryable input', async ({ page }) => {
  await page.route('**/api/chat', route => route.fulfill({ status: 503, json: { detail: 'Unavailable' } }))
  await page.goto('/')
  await navigate(page, 'Evidence Chat')
  await page.getByLabel('Football statistics question').fill('Liverpool points')
  await page.getByRole('button', { name: 'Send', exact: true }).click()
  await expect(page.getByText('The local evidence service is unavailable', { exact: false })).toBeVisible()
  await expect(page.getByLabel('Football statistics question')).toBeEnabled()
})
