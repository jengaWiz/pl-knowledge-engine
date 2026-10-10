import { test, expect } from '@playwright/test'

test.beforeEach(async ({ page }) => {
  await page.route('**/api/matches', route => route.fulfill({ json: [] }))
  await page.route('**/api/graph/overview', route => route.fulfill({ json: { nodes: [
    { id: 'alisson', name: 'A.Becker', full_name: 'Alisson Becker', type: 'Player' },
  ], edges: [] } }))
})

test('closest matches are selectable and resolve a canonical player identity', async ({ page }) => {
  const requested: string[] = []
  await page.route('**/api/graph/player/**', route => {
    const key = decodeURIComponent(route.request().url().split('/').pop()!)
    requested.push(key)
    if (key === 'allison') return route.fulfill({ status: 409, json: { detail: {
      message: 'Choose a player', suggestions: [{ id: 'canonical-alisson', name: 'A.Becker', full_name: 'Alisson Becker' }],
    } } })
    return route.fulfill({ json: { nodes: [{ id: 'alisson', name: 'A.Becker', full_name: 'Alisson Becker', type: 'Player' }], edges: [] } })
  })
  await page.goto('/')
  await page.getByLabel('Search graph player').fill('allison')
  await page.getByRole('button', { name: 'Search', exact: true }).click()
  await expect(page.getByRole('alert')).toContainText('closest roster matches')
  const suggestion = page.getByRole('button', { name: /Alisson Becker.*A.Becker/ })
  await suggestion.focus()
  await suggestion.press('Enter')
  await expect(page.getByText('Showing Alisson Becker', { exact: true })).toBeVisible()
  expect(requested).toEqual(['allison', 'canonical-alisson'])
  await expect(page.getByLabel('Search graph player')).toHaveValue('Alisson Becker')
  await expect(page.getByRole('alert')).toHaveCount(0)
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
})

test('missing coverage and service outages have different messages', async ({ page }) => {
  let status = 404
  await page.route('**/api/graph/player/**', route => route.fulfill({ status, json: { detail: 'private-error' } }))
  await page.goto('/')
  await page.getByLabel('Search graph player').fill('Luis Diaz')
  await page.getByRole('button', { name: 'Search', exact: true }).click()
  await expect(page.getByRole('alert')).toContainText('No stored player matches')
  await expect(page.getByRole('alert')).toContainText('pinned 2025–26 Aston Villa and Liverpool roster')
  status = 503
  await page.getByRole('button', { name: 'Search', exact: true }).click()
  await expect(page.getByRole('alert')).toContainText('search is unavailable')
  await expect(page.getByText('private-error')).toHaveCount(0)
})
