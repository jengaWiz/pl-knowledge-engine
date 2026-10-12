import { test, expect, type Page } from '@playwright/test'
const fixture = { id: 'match-25', season: '2025-26', date: '2025-08-15', home_team: 'Liverpool', away_team: 'Bournemouth' }
const status = { status: 'ready', fixtures: [fixture, { ...fixture, id: 'match-24', season: '2024-25' }], evaluations: { '2024-25': { rolling: { matches: 380, accuracy: .513, log_loss: .992, brier: .592 }, league: { log_loss: 1.082, brier: .656 }, uncertainty: { intervals: { accuracy: { lower: .46, upper: .56 } } } } } }
const result = { fixture, probabilities: { home: .6, draw: .25, away: .15 }, expected_goals: { home: 2.1, away: .9 }, feature_cutoff_exclusive: fixture.date, history_matches: 1140, model_version: 'prematch-poisson-v1', selection_season: '2023-24', window: 20, explanation: 'Uses strictly earlier results.', evidence_scope: 'Earlier team records only.', history_sha256: 'history-hash', limitations: ['Historical replay; no publication timestamps.'], evidence: [{ id: 'earlier', date: '2025-05-01', home_team: 'Liverpool', away_team: 'Aston Villa', home_score: 2, away_score: 1, source: { url: 'https://example.org/source.csv', row: 12, revision: 'pinned-revision' } }] }
async function open(page: Page) {
  await page.route('**/api/matches', r => r.fulfill({ json: [] }))
  await page.route('**/api/graph/overview', r => r.fulfill({ json: { nodes: [], edges: [] } }))
  await page.goto('/')
  const menu = page.getByRole('button', { name: 'Navigation', exact: true })
  if (await menu.isVisible()) await menu.click()
  await page.getByRole('button', { name: 'Evidence Chat', exact: true }).click()
  await page.getByRole('button', { name: 'Predictions', exact: true }).click()
}
test('historical probabilities, cutoff, original source and selection reset', async ({ page }) => {
  await page.route('**/api/prediction/status', r => r.fulfill({ json: status }))
  await page.route('**/api/analyst/prediction', r => {
    expect(r.request().postDataJSON()).toEqual({ match_id: fixture.id })
    return r.fulfill({ json: result })
  })
  await open(page)
  await page.getByLabel('Prediction fixture').selectOption(fixture.id)
  await page.getByRole('button', { name: 'Replay prediction', exact: true }).click()
  await expect(page.getByText('60.0%', { exact: true })).toBeVisible()
  await expect(page.getByText('Results dated before 2025-08-15', { exact: false })).toBeVisible()
  await page.getByText('Inspect 1 earlier result records', { exact: true }).click()
  await expect(page.getByRole('link', { name: 'Original CSV · row 12' })).toHaveAttribute('href', 'https://example.org/source.csv')
  await expect(page.getByText('Revision: pinned-revision')).toBeVisible()
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
  await page.getByLabel('Prediction season').selectOption('2024-25')
  await expect(page.getByText('60.0%', { exact: true })).toHaveCount(0)
  await expect(page.getByRole('button', { name: 'Replay prediction', exact: true })).toBeDisabled()
})
test('unavailable sources can be rechecked and busy forecast preserves selection', async ({ page }) => {
  let available = false
  await page.route('**/api/prediction/status', r => r.fulfill({ status: available ? 200 : 503, json: status }))
  let code = 429
  await page.route('**/api/analyst/prediction', r => r.fulfill({ status: code, json: { detail: 'private-secret' } }))
  await open(page)
  await expect(page.getByRole('alert')).toContainText('unavailable or busy')
  available = true
  await page.getByRole('button', { name: 'Recheck predictions' }).click()
  await page.getByLabel('Prediction fixture').selectOption(fixture.id)
  await page.getByRole('button', { name: 'Replay prediction', exact: true }).click()
  await expect(page.getByRole('alert')).toContainText('Retry shortly')
  await expect(page.getByLabel('Prediction fixture')).toHaveValue(fixture.id)
  code = 503
  await page.getByRole('button', { name: 'Replay prediction', exact: true }).click()
  await expect(page.getByRole('alert')).toContainText('Recheck sources and retry')
  await expect(page.getByText('private-secret')).toHaveCount(0)
  available = false
  await page.getByRole('button', { name: 'Recheck predictions' }).click()
  await expect(page.getByRole('alert')).toContainText('unavailable or busy')
  await expect(page.getByRole('button', { name: 'Replay prediction', exact: true })).toBeDisabled()
})
