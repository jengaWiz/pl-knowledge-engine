import { useEffect, useRef, useState, useCallback, useMemo, type FormEvent } from 'react'
import { fetchOverviewGraph, fetchPlayerGraph } from '../api'
import type { GraphData, GraphNode } from '../types'
import { project, layout, title } from './graphProjection'

const COLORS: Record<string, string> = { Season: '#8e7a9c', Team: '#739a92', Player: '#7797b9', Match: '#b69d75', PlayerAppearance: '#a08dab' }
const TYPE: Record<string, string> = { Season: 'Season', Team: 'Club', Player: 'Player', Match: 'Fixture', PlayerAppearance: 'Appearance' }
const RELATIONS: Record<string, string> = { IN_SEASON: 'in season', PLAYS_FOR: 'plays for', HOME_TEAM: 'home club', AWAY_TEAM: 'away club', HAD_APPEARANCE: 'recorded appearance', IN_MATCH: 'in fixture', FOR_TEAM: 'for club' }
interface Props { overrideGraph: GraphData | null; onClearOverride: () => void }

export default function GraphView({ overrideGraph, onClearOverride }: Props) {
  const [data, setData] = useState<GraphData>({ nodes: [], edges: [] })
  const [loading, setLoading] = useState(true)
  const [search, setSearch] = useState('')
  const [selected, setSelected] = useState<GraphNode | null>(null)
  const [clubId, setClubId] = useState('')
  const [focused, setFocused] = useState(false)
  const [focusKind, setFocusKind] = useState<'player' | 'fixture'>('fixture')
  const [error, setError] = useState('')
  const [suggestions, setSuggestions] = useState<{id: string; name: string; full_name: string}[]>([])
  const [roster, setRoster] = useState<GraphNode[]>([])
  const [coverage, setCoverage] = useState({ clubs: 0, players: 0, fixtures: 0 })
  const [mode, setMode] = useState<'map' | 'list'>(() => window.matchMedia('(max-width: 800px)').matches ? 'list' : 'map')
  const requestId = useRef(0)
  const load = useCallback((graph: GraphData, focus = false) => {
    setSuggestions([]); if (!focus) { setRoster(graph.nodes.filter(n => n.type === 'Player')); setCoverage({ clubs: graph.nodes.filter(n => n.type === 'Team').length, players: graph.nodes.filter(n => n.type === 'Player').length, fixtures: graph.nodes.filter(n => n.type === 'Match').length }) }; setData(graph); setSelected(null); setClubId(''); setFocused(focus); setLoading(false); setError('')
  }, [])
  const overview = useCallback(() => {
    const id = ++requestId.current
    setLoading(true); setError('')
    fetchOverviewGraph().then(graph => { if (id === requestId.current) load(graph) })
      .catch(() => { if (id === requestId.current) { setError('The graph is unavailable. Retry Overview.'); setLoading(false) } })
  }, [load])
  useEffect(() => { overview(); return () => { requestId.current += 1 } }, [overview])
  useEffect(() => { if (overrideGraph) { requestId.current += 1; load(overrideGraph, true); setFocusKind('fixture') } }, [overrideGraph, load])
  const findPlayer = (value: string, label = value) => {
    if (!value.trim()) return
    const id = ++requestId.current
    setLoading(true); setError(''); setSuggestions([])
    fetchPlayerGraph(value.trim()).then(graph => { if (id === requestId.current) { load(graph, true); setSearch(label); setFocusKind('player'); onClearOverride() } })
      .catch(err => { if (id === requestId.current) {
        const detail = err.response?.data?.detail
        setSuggestions(Array.isArray(detail?.suggestions) ? detail.suggestions : [])
        setError(err.response?.status === 409 ? 'Choose a player from the closest roster matches below.'
          : err.response?.status === 404 ? `No stored player matches “${label}”. Coverage: pinned 2025–26 Aston Villa and Liverpool roster. A player may be outside this snapshot.`
          : 'Player search is unavailable. Please retry shortly.')
        setLoading(false)
      } })
  }
  const handleSearch = (event: FormEvent) => { event.preventDefault(); findPlayer(search) }
  const visible = useMemo(() => project(data, clubId, focused), [data, clubId, focused])
  const positions = useMemo(() => layout(visible, !focused && !clubId), [visible, focused, clubId])
  const clubs = data.nodes.filter(n => n.type === 'Team').sort((a, b) => a.name.localeCompare(b.name))
  const neighbors = new Set(selected ? visible.edges.filter(e => e.source === selected.id || e.target === selected.id).flatMap(e => [e.source, e.target]) : [])
  const inspect = (node: GraphNode) => {
    setSelected(node)
    if (node.type === 'Team' && !focused && !clubId) { setClubId(node.id); setSelected(node) }
  }
  const heading = focused ? (focusKind === 'player' ? 'Player connections' : 'Fixture connections') : clubId ? clubs.find(n => n.id === clubId)?.name : 'Start with a club.'
  return <section className="graph-workspace">
    <div className="graph-intro">
      <div><p className="workspace-eyebrow">THE EXPLORER / 2025–26</p><h1>{heading}</h1>
        {focused && focusKind === 'player' && <p className="player-match">Showing {data.nodes.find(n => n.type === 'Player')?.full_name || data.nodes.find(n => n.type === 'Player')?.name}</p>}
        <p>{focused ? 'Follow the recorded links between players, appearances and fixtures.' : clubId ? 'A focused look at the available squad and six most recent fixtures.' : 'Twenty clubs. One season. Follow a connection to find the story.'}</p></div>
      <div className="workspace-stamp"><span className="status-dot" />LOCAL DATA<span>Verified season records</span>
        <div className="coverage-metrics"><div><strong>{coverage.clubs || '—'}</strong><small>CLUBS</small></div><div><strong>{coverage.players || '—'}</strong><small>PLAYERS</small></div><div><strong>{coverage.fixtures || '—'}</strong><small>FIXTURES</small></div></div></div>
    </div>
    <div className="explorer-toolbar">
      <form onSubmit={handleSearch}><label className="sr-only" htmlFor="graph-search">Search graph player</label>
        <input id="graph-search" list="player-roster" maxLength={120} value={search} onChange={e => setSearch(e.target.value)} placeholder="Find a player, e.g. Salah" />
        <button className="primary-action" type="submit" disabled={loading}>Search</button>
        <datalist id="player-roster">{roster.map(player => <option key={player.id} value={player.full_name || player.name}>{player.name}</option>)}</datalist></form>
      <button onClick={() => { onClearOverride(); overview() }}>Overview</button>
      {!focused && <select aria-label="Explore club" value={clubId} onChange={e => { setClubId(e.target.value); setSelected(null) }}>
        <option value="">All clubs</option>{clubs.map(n => <option key={n.id} value={n.id}>{n.name}</option>)}
      </select>}
      <div className="map-switch" aria-label="Graph presentation"><button aria-pressed={mode === 'map'} onClick={() => setMode('map')}>Map</button><button aria-pressed={mode === 'list'} onClick={() => setMode('list')}>List</button></div>
    </div>
    {error && <div className="graph-notice"><p role="alert">{error}</p>
      {suggestions.length > 0 && <div className="player-suggestions" aria-label="Closest player matches">{suggestions.map(player => <button key={player.id} onClick={() => findPlayer(player.id, player.full_name || player.name)}><span>{player.full_name || player.name}</span><small>{player.name} ↗</small></button>)}</div>}
    </div>}
    <div className="explorer-body">
      <div className="graph-stage">
        <div className="graph-caption"><span>{focused ? 'RECORDED CONNECTIONS' : clubId ? 'CLUB NETWORK' : 'LEAGUE MAP'}</span><span>{visible.nodes.length} NODES · {visible.edges.length} EDGES</span></div>
        {loading ? <div className="graph-empty" role="status">Loading graph…</div> : !visible.nodes.length ? <div className="graph-empty">No graph records available. Retry Overview after loading the local stores.</div> : mode === 'map' ?
          <div className="graph-scroll"><svg className="relationship-map" viewBox="0 0 1100 640" aria-label="Interactive football relationship map">
            {clubId && !focused && <g className="map-lane-label"><text x="43" y="22">AVAILABLE PLAYERS</text><text x="413" y="22">CLUB</text><text x="783" y="22">RECENT FIXTURES</text></g>}
            {visible.edges.map((edge, i) => {
              const a = positions.get(edge.source), b = positions.get(edge.target)
              if (!a || !b) return null
              const active = selected && (edge.source === selected.id || edge.target === selected.id)
              return <path key={i} className={`map-edge ${active ? 'active' : ''}`} opacity={selected && !active ? .12 : 1}
                d={`M ${a.x} ${a.y} C ${(a.x + b.x) / 2} ${a.y}, ${(a.x + b.x) / 2} ${b.y}, ${b.x} ${b.y}`}><title>{RELATIONS[edge.type] || edge.type.toLowerCase().replace(/_/g, ' ')}</title></path>
            })}
            {visible.nodes.map(node => {
              const point = positions.get(node.id)
              if (!point) return null
              const active = selected?.id === node.id
              return <g key={node.id} role="button" tabIndex={0} aria-label={`Inspect ${TYPE[node.type] || node.type} ${title(node)}`} aria-pressed={active}
                data-kind={node.type} className={`map-node ${active ? 'selected' : ''}`} transform={`translate(${point.x},${point.y})`}
                opacity={selected && !active && !neighbors.has(node.id) ? .35 : 1}
                onClick={() => inspect(node)} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); inspect(node) } }}>
                <title>{title(node)}{node.date ? ` · ${node.date}` : ''}</title>
                <rect x="-137" y="-21" width="274" height="42" rx="9" />
                {node.type === 'Team' ? <><circle cx="-119" r="11" fill="#f1edf5" stroke="#d8cfe1" /><text className="club-monogram" x="-119" y="3">{node.name.split(' ').map(word => word[0]).join('').slice(0, 3)}</text></> : <circle cx="-119" r="4" fill={COLORS[node.type] || '#aab6c8'} />}
                <text x={node.type === 'Team' ? -99 : -106} y="-1" className="map-name">{title(node).length > 31 ? `${title(node).slice(0, 30)}…` : title(node)}</text>
                <text x={node.type === 'Team' ? -99 : -106} y="13" className="map-kind">{TYPE[node.type]}{node.date ? ` · ${node.date}` : ''}</text>
              </g>
            })}
          </svg></div> : <div className="graph-record-list">{visible.nodes.map(node => <button key={node.id} data-kind={node.type} aria-pressed={selected?.id === node.id} onClick={() => inspect(node)}>
            <span className="record-dot" style={{ background: COLORS[node.type] }} /><span><small>{TYPE[node.type]}</small><strong>{title(node)}</strong>{node.date && <small>{node.date}</small>}</span><span aria-hidden="true">↗</span>
          </button>)}</div>}
        <div className="map-footer"><span>{Object.entries(COLORS).filter(([kind]) => visible.nodes.some(n => n.type === kind)).map(([kind, color]) => <span key={kind}><i style={{ background: color }} />{TYPE[kind]}</span>)}</span><span>Select a record to inspect</span></div>
      </div>
      <aside className="graph-inspector" aria-label="Record inspector">
        <p className="workspace-eyebrow">{selected ? 'SELECTED RECORD' : 'YOUR STARTING POINT'}</p>
        {selected ? <><h2>{title(selected)}</h2><span className="inspector-type">{TYPE[selected.type]}</span>
          <dl>{Object.entries(selected).filter(([key, value]) => !['id', 'name', 'type', 'displayName'].includes(key) && value !== null && value !== undefined).map(([key, value]) => <div key={key}><dt>{key.replace(/_/g, ' ')}</dt><dd>{String(value)}</dd></div>)}</dl>
          <h3>Visible connections</h3><div className="inspector-connections">{visible.edges.filter(e => e.source === selected.id || e.target === selected.id).map((edge, i) => {
            const outgoing = edge.source === selected.id
            const other = visible.nodes.find(n => n.id === (outgoing ? edge.target : edge.source))!
            return <button key={i} onClick={() => setSelected(other)}><small>{outgoing ? '→' : '←'} {RELATIONS[edge.type] || edge.type.toLowerCase().replace(/_/g, ' ')}</small><strong>{title(other)}</strong></button>
          })}</div><button className="clear-selection" onClick={() => setSelected(null)}>Clear selection</button></> : <>
          <div className="inspector-art" aria-hidden="true"><span>01</span><svg viewBox="0 0 220 110"><path d="M30 55H90M90 55C125 55 125 20 185 20M90 55C125 55 125 90 185 90" /><circle cx="30" cy="55" r="9" /><circle cx="90" cy="55" r="6" /><circle cx="185" cy="20" r="9" /><circle cx="185" cy="90" r="9" /></svg></div>
          <h2>Explore a connection</h2><p>Choose a club to reveal its players and recent fixtures. Select any record to read its details and follow its connections.</p>
          {!focused && <h3>Try a focused route</h3>}{!focused && ['Liverpool', 'Aston Villa'].map(name => <button className="club-shortcut" key={name} disabled={!clubs.some(n => n.name === name)} onClick={() => { setClubId(clubs.find(n => n.name === name)!.id); setFocused(false) }}>{name}<span>Explore club ↗</span></button>)}
        </>}
        <p className="inspector-scope">{focused ? 'Showing up to 8 recent appearance records and their connected entities. Fixture views omit squad membership links to keep appearance paths clear.' : clubId ? 'Up to 12 available players, ordered by goals, and 6 recent fixtures. Player coverage is limited to Aston Villa and Liverpool.' : 'The map starts with clubs and the season. Fixtures and players appear when you drill in.'} All links shown come from the stored graph. This explorer covers the 2025–26 MVP.</p>
      </aside>
    </div>
  </section>
}
