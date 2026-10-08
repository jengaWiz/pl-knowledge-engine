import { test, expect, type Page } from '@playwright/test'

test.beforeEach(async ({ page }) => {
  await page.route('**/api/matches', route => route.fulfill({ json: [] }))
  await page.route('**/api/graph/overview', route => route.fulfill({ json: { nodes: [], edges: [] } }))
})

const ready = {
  status: 'ready', primary_season: '2025-26', seasons: ['2022-23', '2025-26'],
  index: { documents: 2682, records_by_season: { '2022-23': 380, '2025-26': 1542 } },
}

async function openEvidence(page: Page) {
  await page.goto('/')
  const menu = page.getByRole('button', { name: 'Navigation', exact: true })
  if (await menu.isVisible()) await menu.click()
  await page.getByRole('button', { name: 'Evidence Chat', exact: true }).click()
  await page.getByRole('button', { name: 'Source evidence', exact: true }).click()
}

test('explicit season, fixture clarification and traceable source cards', async ({ page }) => {
  await page.route('**/api/evidence/status', route => route.fulfill({ json: ready }))
  await page.route('**/api/evidence/retrieve', route => {
    const body = route.request().postDataJSON()
    expect(body.evidence_season).toBe('2022-23')
    const ambiguous = body.query === 'Liverpool vs Bournemouth'
    return route.fulfill({ json: {
      route: { status: ambiguous ? 'clarification' : 'resolved', season: '2022-23',
        reason: 'Specify the home club, away club or ISO fixture date.',
        candidates: ambiguous ? [{ canonical_id: 'match', date: '2022-08-27',
          home_team: 'Liverpool', away_team: 'Bournemouth' }] : undefined },
      hits: ambiguous ? [] : [{ id: 'document-checksum', text: 'Verified match evidence.', graph: {
        match_id: 'match', date: '2022-08-27', home_team: 'Liverpool', away_team: 'Bournemouth',
        home_score: 9, away_score: 0,
        source_refs: [{ id: 'matches', row: 22, revision: 'pinned-revision', url: 'https://example.org/source.csv' }],
      } }],
    } })
  })
  await openEvidence(page)
  await expect(page.getByText('2,682 verified documents', { exact: false })).toBeVisible()
  await page.getByLabel('Evidence season').selectOption('2022-23')
  await page.getByLabel('Evidence question').fill('Liverpool vs Bournemouth')
  await page.getByRole('button', { name: 'Find evidence', exact: true }).click()
  await expect(page.getByText('Clarification needed', { exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Liverpool vs Bournemouth · 2022-08-27' }).click()
  await expect(page.getByRole('heading', { name: 'Liverpool 9–0 Bournemouth', exact: true })).toBeVisible()
  await page.getByText('1 source references', { exact: true }).click()
  await expect(page.getByText('matches · row 22', { exact: true })).toBeVisible()
  await expect(page.getByText('Revision: pinned-revision', { exact: true })).toBeVisible()
  await expect(page.getByRole('link', { name: 'View original source' })).toHaveAttribute('href', 'https://example.org/source.csv')
  await page.getByLabel('Evidence season').selectOption('2025-26')
  await expect(page.getByRole('heading', { name: 'Liverpool 9–0 Bournemouth', exact: true })).toHaveCount(0)
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
})

test('optional evidence not ready can be rechecked without hiding statistics', async ({ page }) => {
  let available = false
  await page.route('**/api/evidence/status', route => route.fulfill({
    status: available ? 200 : 503,
    json: available ? ready : { status: 'not_ready', primary_season: '2025-26', seasons: [] },
  }))
  await openEvidence(page)
  await expect(page.getByText('Structured evidence is not ready.', { exact: false })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Find evidence', exact: true })).toBeDisabled()
  available = true
  await page.getByRole('button', { name: 'Recheck evidence', exact: true }).click()
  await expect(page.getByLabel('Evidence question')).toBeEnabled()
  await page.getByRole('button', { name: 'Season statistics', exact: true }).click()
  await expect(page.getByRole('button', { name: "Who are Liverpool's top scorers this season?", exact: true })).toBeVisible()
})

test('busy and unavailable requests preserve input and provide retry', async ({ page }) => {
  await page.route('**/api/evidence/status', route => route.fulfill({ json: ready }))
  let status = 429
  await page.route('**/api/evidence/retrieve', route => route.fulfill({ status,
    json: { detail: 'secret-provider-token' } }))
  await openEvidence(page)
  const input = page.getByLabel('Evidence question')
  await expect(input).toBeEnabled()
  await input.fill('Digne appearances against Liverpool')
  await page.getByRole('button', { name: 'Find evidence', exact: true }).click()
  await expect(page.getByRole('alert')).toContainText('Another evidence check is running')
  await expect(input).toHaveValue('Digne appearances against Liverpool')
  status = 503
  await page.getByRole('button', { name: 'Find evidence', exact: true }).click()
  await expect(page.getByRole('alert')).toContainText('Recheck evidence and retry')
  await expect(page.getByText('secret-provider-token')).toHaveCount(0)
  await page.getByRole('button', { name: 'Recheck evidence', exact: true }).click()
  await expect(input).toBeEnabled()
  await expect(input).toHaveValue('Digne appearances against Liverpool')
})
