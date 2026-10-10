import type { GraphData, GraphNode } from '../types'

export function title(node: GraphNode): string {
  if (node.type === 'Match') return `${node.home} ${node.home_score ?? '–'} · ${node.away_score ?? '–'} ${node.away}`
  if (node.type === 'PlayerAppearance') return `${node.displayName || node.name} · ${node.minutes ?? '–'} min`
  return node.name
}

export function project(data: GraphData, clubId: string, focused: boolean): GraphData {
  let nodes: GraphNode[]
  if (!focused && !clubId) {
    nodes = data.nodes.filter(n => n.type === 'Team' || n.type === 'Season')
  } else if (!focused) {
    const adjacent = new Set(data.edges.filter(e => e.source === clubId || e.target === clubId)
      .flatMap(e => [e.source, e.target]))
    const players = data.nodes.filter(n => adjacent.has(n.id) && n.type === 'Player')
      .sort((a, b) => (b.goals ?? 0) - (a.goals ?? 0) || a.name.localeCompare(b.name)).slice(0, 12)
    const matches = data.nodes.filter(n => adjacent.has(n.id) && n.type === 'Match')
      .sort((a, b) => (b.date || '').localeCompare(a.date || '') || a.id.localeCompare(b.id)).slice(0, 6)
    nodes = [...data.nodes.filter(n => n.id === clubId), ...players, ...matches]
  } else {
    const appearances = data.nodes.filter(n => n.type === 'PlayerAppearance')
      .sort((a, b) => (b.date || '').localeCompare(a.date || '') || (b.minutes ?? 0) - (a.minutes ?? 0) || a.id.localeCompare(b.id))
      .slice(0, 8)
    const adjacent = new Set(data.edges.filter(e => appearances.some(a => a.id === e.source || a.id === e.target))
      .flatMap(e => [e.source, e.target]))
    nodes = data.nodes.filter(n => n.type === 'Team' || (n.type === 'Match' && (adjacent.has(n.id) || !appearances.length))
      || (n.type === 'Player' && (adjacent.has(n.id) || !appearances.length)) || appearances.some(a => a.id === n.id))
  }
  nodes = nodes.map(node => {
    if (node.type !== 'PlayerAppearance') return node
    const link = data.edges.find(e => e.type === 'HAD_APPEARANCE' && e.target === node.id)
    return { ...node, displayName: data.nodes.find(n => n.id === link?.source)?.name || node.name }
  })
  const ids = new Set(nodes.map(n => n.id))
  const fixtureView = focused && nodes.filter(n => n.type === 'Player').length > 1
  return { nodes, edges: data.edges.filter(e => ids.has(e.source) && ids.has(e.target)
    && (!fixtureView || e.type !== 'PLAYS_FOR')) }
}

export function layout(data: GraphData, overview: boolean) {
  const positions = new Map<string, { x: number; y: number }>()
  if (overview) {
    const clubs = data.nodes.filter(n => n.type === 'Team').sort((a, b) => a.name.localeCompare(b.name))
    clubs.forEach((n, i) => positions.set(n.id, { x: i < 10 ? 200 : 900, y: 65 + (i % 10) * 55 }))
    data.nodes.filter(n => n.type === 'Season').forEach(n => positions.set(n.id, { x: 550, y: 315 }))
  } else {
    const appearances = data.nodes.filter(n => n.type === 'PlayerAppearance')
    if (appearances.length && data.nodes.filter(n => n.type === 'Player').length > 1) {
      data.nodes.filter(n => n.type === 'Team').forEach((n, i) => positions.set(n.id, { x: 180 + i * 740, y: 55 }))
      data.nodes.filter(n => n.type === 'Match').forEach(n => positions.set(n.id, { x: 920, y: 340 }))
      appearances.forEach((node, i) => {
        const y = 145 + i * 55
        positions.set(node.id, { x: 550, y })
        const link = data.edges.find(e => e.type === 'HAD_APPEARANCE' && e.target === node.id)
        if (link) positions.set(link.source, { x: 180, y })
      })
      return positions
    }
    const lanes = [['Team', 'Player'], ['PlayerAppearance'], ['Match']]
    // Club views have no appearance nodes: spread their roster and fixtures into two lanes.
    const club = data.nodes.filter(n => n.type === 'Team')
    const clubView = !data.nodes.some(n => n.type === 'PlayerAppearance') && club.length === 1
    const groups = clubView ? [club, data.nodes.filter(n => n.type === 'Player'), data.nodes.filter(n => n.type === 'Match')]
      : lanes.map(types => data.nodes.filter(n => types.includes(n.type)))
    groups.forEach((group, lane) => group.forEach((n, i) => positions.set(n.id, {
      x: clubView ? [550, 180, 920][lane] : 180 + lane * 370, y: 315 + (i - (group.length - 1) / 2) * Math.min(55, 550 / Math.max(group.length, 1)),
    })))
  }
  return positions
}
