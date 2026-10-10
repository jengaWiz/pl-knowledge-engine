import { test, expect } from '@playwright/test'

const clubs = ['Liverpool', 'Aston Villa', ...Array.from({ length: 18 }, (_, i) => `Club ${i + 1}`)]
const nodes = [
  { id: 'season', name: '2025–26', type: 'Season' },
  ...clubs.map((name, i) => ({ id: `club${i}`, name, type: 'Team' })),
  ...Array.from({ length: 15 }, (_, i) => ({ id: `p${i}`, name: `Player ${i}`, type: 'Player', goals: i })),
  ...Array.from({ length: 12 }, (_, i) => ({ id: `m${i}`, name: `fixture${i}`, type: 'Match', home: 'Liverpool', away: `Opponent ${i}`, date: `2026-05-${String(i + 1).padStart(2, '0')}`, home_score: i, away_score: 0 })),
]
const edges = [
  ...clubs.map((_, i) => ({ source: `club${i}`, target: 'season', type: 'IN_SEASON' })),
  ...Array.from({ length: 15 }, (_, i) => ({ source: `p${i}`, target: 'club0', type: 'PLAYS_FOR' })),
  ...Array.from({ length: 12 }, (_, i) => ({ source: 'club0', target: `m${i}`, type: 'HOME_TEAM' })),
]

test.beforeEach(async ({ page }) => {
  await page.route('**/api/matches', route => route.fulfill({ json: [] }))
  await page.route('**/api/graph/overview', route => route.fulfill({ json: { nodes, edges } }))
})

test('club-first map bounds detail, supports keyboard inspection and resets', async ({ page }) => {
  await page.goto('/')
  await expect(page.getByText('21 NODES · 20 EDGES')).toBeVisible()
  await page.getByLabel('Explore club').selectOption('club0')
  await expect(page.getByText('19 NODES · 18 EDGES')).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Liverpool', exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Map', exact: true }).click()
  const player = page.getByRole('button', { name: 'Inspect Player Player 14', exact: true })
  await player.focus()
  await player.press('Enter')
  const inspector = page.getByRole('complementary', { name: 'Record inspector' })
  await expect(inspector.getByRole('heading', { name: 'Player 14', exact: true })).toBeVisible()
  await expect(inspector.getByText('→ plays for', { exact: true })).toBeVisible()
  await inspector.getByRole('button', { name: /plays for.*Liverpool/ }).click()
  await expect(inspector.getByRole('heading', { name: 'Liverpool', exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'List', exact: true }).click()
  await expect(page.locator('.graph-record-list button')).toHaveCount(19)
  await page.getByRole('button', { name: 'Overview', exact: true }).click()
  await expect(page.getByText('21 NODES · 20 EDGES')).toBeVisible()
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
})

test('player routes show eight recent appearances with their real links', async ({ page }) => {
  const apps = Array.from({ length: 10 }, (_, i) => ({ id: `a${i}`, name: 'Appearance', type: 'PlayerAppearance', date: `2026-05-${String(i + 1).padStart(2, '0')}`, minutes: 90 }))
  await page.route('**/api/graph/player/**', route => route.fulfill({ json: {
    nodes: [nodes[1], nodes[21], ...nodes.filter(n => n.type === 'Match').slice(0, 10), ...apps],
    edges: [edges[20], ...apps.flatMap((a, i) => [
      { source: 'p0', target: a.id, type: 'HAD_APPEARANCE' },
      { source: a.id, target: `m${i}`, type: 'IN_MATCH' },
    ])],
  } }))
  await page.goto('/')
  await page.getByLabel('Search graph player').fill('Player 0')
  await page.getByRole('button', { name: 'Search', exact: true }).click()
  await expect(page.getByText('18 NODES · 17 EDGES')).toBeVisible()
  await page.getByRole('button', { name: 'List', exact: true }).click()
  await expect(page.locator('.graph-record-list button')).toHaveCount(18)
  await expect(page.locator('.graph-record-list').getByText('Appearance', { exact: true })).toHaveCount(8)
  await expect(page.getByText('Showing up to 8 recent appearance records', { exact: false })).toBeVisible()
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
})

test('fixture paths identify each player appearance without squad-link clutter', async ({ page }) => {
  await page.route('**/api/matches', route => route.fulfill({ json: [{ id: 'fixture', date: '2026-05-12', home_team: 'Liverpool', away_team: 'Aston Villa', home_score: 2, away_score: 0, gameweek: 38 }] }))
  const fixtureNodes = [nodes[1], nodes[2], nodes[21], nodes[22], nodes[36],
    { id: 'a0', name: 'Liverpool', type: 'PlayerAppearance', minutes: 90, date: '2026-05-12' },
    { id: 'a1', name: 'Liverpool', type: 'PlayerAppearance', minutes: 81, date: '2026-05-12' }]
  const fixtureEdges = [
    { source: 'club0', target: 'm0', type: 'HOME_TEAM' },
    { source: 'club1', target: 'm0', type: 'AWAY_TEAM' },
    ...[0, 1].flatMap(i => [
      { source: `p${i}`, target: 'club0', type: 'PLAYS_FOR' },
      { source: `p${i}`, target: `a${i}`, type: 'HAD_APPEARANCE' },
      { source: `a${i}`, target: 'm0', type: 'IN_MATCH' },
    ]),
  ]
  await page.route('**/api/graph/match/fixture', route => route.fulfill({ json: { nodes: fixtureNodes, edges: fixtureEdges } }))
  await page.goto('/')
  const menu = page.getByRole('button', { name: 'Navigation', exact: true })
  if (await menu.isVisible()) await menu.click()
  await page.getByRole('button', { name: /Liverpool versus Aston Villa/ }).click()
  await expect(page.getByText('7 NODES · 6 EDGES')).toBeVisible()
  await page.getByRole('button', { name: 'List', exact: true }).click()
  await page.getByRole('button', { name: /Appearance Player 1 · 81 min/ }).click()
  const inspector = page.getByRole('complementary', { name: 'Record inspector' })
  await expect(inspector.getByRole('heading', { name: 'Player 1 · 81 min' })).toBeVisible()
  await expect(inspector.getByText('← recorded appearance', { exact: true })).toBeVisible()
  await expect(inspector.getByText('→ in fixture', { exact: true })).toBeVisible()
})
