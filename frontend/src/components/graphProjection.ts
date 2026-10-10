import type { GraphData, GraphNode } from '../types'

export function title(node: GraphNode): string {
  if (node.type === 'Match') return `${node.home} ${node.home_score ?? '–'} · ${node.away_score ?? '–'} ${node.away}`
  if (node.type === 'PlayerAppearance') return `${node.minutes ?? '–'} min · ${node.date?.slice(5) || node.name}`
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
  const ids = new Set(nodes.map(n => n.id))
  return { nodes, edges: data.edges.filter(e => ids.has(e.source) && ids.has(e.target)) }
}

export function layout(data: GraphData, overview: boolean) {
  const positions = new Map<string, { x: number; y: number }>()
  if (overview) {
    const clubs = data.nodes.filter(n => n.type === 'Team').sort((a, b) => a.name.localeCompare(b.name))
    clubs.forEach((n, i) => positions.set(n.id, { x: i < 10 ? 200 : 900, y: 65 + (i % 10) * 55 }))
    data.nodes.filter(n => n.type === 'Season').forEach(n => positions.set(n.id, { x: 550, y: 315 }))
  } else {
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
