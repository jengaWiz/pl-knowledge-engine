import { useEffect, useRef, useState, useCallback, type FormEvent } from 'react'
import ForceGraph2D from 'react-force-graph-2d'
import { fetchOverviewGraph, fetchPlayerGraph } from '../api'
import type { GraphData, GraphNode } from '../types'

const NODE_COLORS: Record<string, string> = {
  Season:           '#f59e0b',
  Team:             '#a855f7',
  Stadium:          '#22c55e',
  Player:           '#3b82f6',
  Gameweek:         '#6b7280',
  Match:            '#f97316',
  PlayerAppearance: '#ec4899',
}

const NODE_SIZES: Record<string, number> = {
  Season:           14,
  Team:             12,
  Stadium:          9,
  Player:           7,
  Gameweek:         5,
  Match:            8,
  PlayerAppearance: 4,
}

interface Props {
  overrideGraph: GraphData | null
  onClearOverride: () => void
}

export default function GraphView({ overrideGraph, onClearOverride }: Props) {
  const [graphData, setGraphData] = useState<{ nodes: GraphNode[]; links: object[] }>({ nodes: [], links: [] })
  const [loading, setLoading] = useState(true)
  const [search, setSearch] = useState('')
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null)
  const [error, setError] = useState('')
  const containerRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({ w: 800, h: 600 })

  const loadGraph = useCallback((data: GraphData) => {
    const links = data.edges.map(e => ({ source: e.source, target: e.target, type: e.type }))
    setGraphData({ nodes: data.nodes, links })
    setLoading(false)
    setError('')
  }, [])

  useEffect(() => {
    if (!containerRef.current) return
    const obs = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect
      setDims({ w: width, h: height })
    })
    obs.observe(containerRef.current)
    return () => obs.disconnect()
  }, [])

  useEffect(() => {
    setLoading(true)
    fetchOverviewGraph()
      .then(loadGraph)
      .catch(err => { setError('Failed to load graph: ' + err.message); setLoading(false) })
  }, [loadGraph])

  useEffect(() => { if (overrideGraph) loadGraph(overrideGraph) }, [overrideGraph, loadGraph])

  const handleSearch = (e: FormEvent) => {
    e.preventDefault()
    if (!search.trim()) return
    setLoading(true)
    fetchPlayerGraph(search.trim())
      .then(data => { loadGraph(data); onClearOverride() })
      .catch(() => { setError(`Player "${search}" not found`); setLoading(false) })
  }

  return (
    <div
      ref={containerRef}
      style={{ position: 'relative', width: '100%', height: '100%', background: '#050508' }}
    >
      {/* ── Canvas depth layers (pointer-events: none) ── */}
      {/* Radial center glow — gives the cluster area energy */}
      <div style={{
        position: 'absolute', inset: 0, zIndex: 1, pointerEvents: 'none',
        background: 'radial-gradient(ellipse 55% 45% at 50% 52%, rgba(103,14,54,0.07) 0%, transparent 65%)',
      }} />
      {/* Edge vignette — darkens the corners for depth */}
      <div style={{
        position: 'absolute', inset: 0, zIndex: 1, pointerEvents: 'none',
        background: 'radial-gradient(ellipse at center, transparent 35%, rgba(5,5,8,0.72) 100%)',
      }} />

      {/* ── Controls ── */}
      <div style={{ position: 'absolute', top: 16, left: 16, zIndex: 10, display: 'flex', gap: 8, alignItems: 'center' }}>
        <form onSubmit={handleSearch} style={{ display: 'flex' }}>
          <div style={glass({ padding: '0 4px 0 12px', display: 'flex', alignItems: 'center', gap: 8 })}>
            <SearchIcon />
            <input
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Search player (e.g. Salah)…"
              style={{
                background: 'transparent', border: 'none', outline: 'none',
                padding: '8px 0', color: 'var(--t-1)', fontSize: 13, width: 210,
              }}
            />
            <button type="submit" style={{
              background: 'var(--villa)', border: 'none', borderRadius: 8,
              padding: '6px 14px', margin: '4px',
              color: '#fff', fontSize: 12, fontWeight: 700, cursor: 'pointer',
              boxShadow: '0 2px 10px rgba(103,14,54,0.5)',
              letterSpacing: '-0.01em',
              transition: 'background 0.15s',
            }}>
              Search
            </button>
          </div>
        </form>
        <button
          onClick={() => { onClearOverride(); setLoading(true); fetchOverviewGraph().then(loadGraph) }}
          style={{
            ...glass({ padding: '8px 14px' }),
            color: 'var(--t-2)', fontSize: 12, fontWeight: 500,
            cursor: 'pointer', border: '1px solid rgba(33,38,45,0.85)',
            transition: 'color 0.15s, border-color 0.15s',
          } as React.CSSProperties}
          onMouseEnter={e => {
            const el = e.currentTarget as HTMLElement
            el.style.color = 'var(--t-1)'; el.style.borderColor = 'rgba(103,14,54,0.5)'
          }}
          onMouseLeave={e => {
            const el = e.currentTarget as HTMLElement
            el.style.color = 'var(--t-2)'; el.style.borderColor = 'rgba(33,38,45,0.85)'
          }}
        >
          Overview
        </button>
      </div>

      {/* ── Legend ── */}
      <div style={{ ...glass({ padding: '12px 15px' }), position: 'absolute', top: 16, right: 16, zIndex: 10 }}>
        <div style={{ fontSize: 9, fontWeight: 800, color: 'var(--t-3)', letterSpacing: '0.1em', marginBottom: 10 }}>
          NODE TYPES
        </div>
        {Object.entries(NODE_COLORS).map(([type, color]) => (
          <div key={type} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
            <div style={{
              width: 8, height: 8, borderRadius: '50%', background: color, flexShrink: 0,
              boxShadow: `0 0 7px ${color}`,
            }} />
            <span style={{ fontSize: 11, color: 'var(--t-2)', letterSpacing: '-0.01em' }}>{type}</span>
          </div>
        ))}
      </div>

      {/* ── Node detail panel ── */}
      {selectedNode && (
        <div
          className="fade-in-up"
          style={{
            ...glass({ padding: '14px 16px' }),
            position: 'absolute', bottom: 48, left: 16, zIndex: 10,
            minWidth: 240, maxWidth: 310,
            borderLeft: `3px solid ${NODE_COLORS[selectedNode.type] || 'var(--villa)'}`,
            boxShadow: `0 10px 40px rgba(0,0,0,0.6), 0 0 0 1px rgba(255,255,255,0.04)`,
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
            <span style={{
              fontSize: 9, fontWeight: 800, letterSpacing: '0.1em',
              color: NODE_COLORS[selectedNode.type] || '#fff',
              background: `${NODE_COLORS[selectedNode.type] || '#670E36'}20`,
              border: `1px solid ${NODE_COLORS[selectedNode.type] || '#670E36'}45`,
              padding: '3px 8px', borderRadius: 5,
            }}>
              {selectedNode.type.toUpperCase()}
            </span>
            <button
              onClick={() => setSelectedNode(null)}
              style={{
                background: 'none', border: 'none', color: 'var(--t-3)',
                cursor: 'pointer', fontSize: 18, lineHeight: 1, padding: '2px 5px',
                transition: 'color 0.15s',
              }}
              onMouseEnter={e => (e.currentTarget.style.color = 'var(--t-1)')}
              onMouseLeave={e => (e.currentTarget.style.color = 'var(--t-3)')}
            >
              ×
            </button>
          </div>
          <div style={{
            fontSize: 16, fontWeight: 800, color: 'var(--t-1)',
            letterSpacing: '-0.02em', marginBottom: 10,
          }}>
            {selectedNode.name}
          </div>
          {Object.entries(selectedNode)
            .filter(([k]) => !['id', 'name', 'type', 'x', 'y', 'vx', 'vy', 'index', '__indexColor'].includes(k))
            .filter(([, v]) => v !== undefined && v !== null)
            .map(([k, v]) => (
              <div key={k} style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4, gap: 12 }}>
                <span style={{ fontSize: 12, color: 'var(--t-3)', textTransform: 'capitalize' }}>{k}</span>
                <span style={{ fontSize: 12, color: 'var(--t-2)', fontWeight: 600, textAlign: 'right' }}>{String(v)}</span>
              </div>
            ))
          }
        </div>
      )}

      {/* ── Loading spinner ── */}
      {loading && (
        <div style={{
          position: 'absolute', inset: 0, zIndex: 5,
          display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 16,
          background: 'rgba(5,5,8,0.8)', backdropFilter: 'blur(6px)',
        }}>
          <div style={{
            width: 40, height: 40, borderRadius: '50%',
            border: '2px solid rgba(103,14,54,0.2)',
            borderTop: '2px solid var(--villa)',
            animation: 'spin 0.75s linear infinite',
          }} />
          <span style={{ fontSize: 13, color: 'var(--t-3)', letterSpacing: '0.05em', fontWeight: 500 }}>
            Loading graph…
          </span>
        </div>
      )}

      {/* ── Error ── */}
      {error && (
        <div className="fade-in" style={{
          position: 'absolute', top: 68, left: '50%', transform: 'translateX(-50%)', zIndex: 20,
          background: 'rgba(127,29,29,0.92)', border: '1px solid rgba(153,27,27,0.8)',
          borderRadius: 10, padding: '9px 18px', fontSize: 13, color: '#fca5a5',
          backdropFilter: 'blur(8px)', boxShadow: '0 4px 20px rgba(0,0,0,0.5)',
        }}>
          {error}
        </div>
      )}

      {/* ── Force graph canvas ── */}
      {!loading && (
        <ForceGraph2D
          width={dims.w}
          height={dims.h - 30}        /* leave room for status bar */
          graphData={graphData}
          nodeId="id"
          linkSource="source"
          linkTarget="target"
          nodeColor={(n: GraphNode) => NODE_COLORS[n.type] || '#9ca3af'}
          nodeVal={(n: GraphNode) => NODE_SIZES[n.type] || 5}
          nodeLabel={(n: GraphNode) => `${n.type}: ${n.name}`}
          linkColor={() => 'rgba(48,54,61,0.7)'}
          linkWidth={1.5}
          linkDirectionalArrowLength={5}
          linkDirectionalArrowRelPos={1}
          backgroundColor="#050508"
          onNodeClick={(n) => setSelectedNode(n as GraphNode)}
          nodeCanvasObject={(node, ctx, globalScale) => {
            const n = node as GraphNode & { x: number; y: number }
            const size = (NODE_SIZES[n.type] || 5) / 2
            const color = NODE_COLORS[n.type] || '#9ca3af'

            // Outer glow ring
            ctx.shadowBlur = 14
            ctx.shadowColor = color
            ctx.beginPath()
            ctx.arc(n.x, n.y, size, 0, 2 * Math.PI)
            ctx.fillStyle = color
            ctx.fill()
            ctx.shadowBlur = 0

            // Inner specular highlight
            ctx.beginPath()
            ctx.arc(n.x - size * 0.3, n.y - size * 0.32, size * 0.35, 0, 2 * Math.PI)
            ctx.fillStyle = 'rgba(255,255,255,0.3)'
            ctx.fill()

            // Label at zoom
            if (globalScale > 1.5) {
              const label = n.name?.length > 16 ? n.name.slice(0, 16) + '…' : (n.name || '')
              ctx.shadowBlur = 0
              ctx.font = `${Math.max(4, 6 / globalScale)}px Inter, sans-serif`
              ctx.fillStyle = 'rgba(240,246,252,0.85)'
              ctx.textAlign = 'center'
              ctx.fillText(label, n.x, n.y + size + 5)
            }
          }}
          nodeCanvasObjectMode={() => 'replace'}
        />
      )}

      {/* ── Status bar ── */}
      {!loading && (
        <div style={{
          position: 'absolute', bottom: 0, left: 0, right: 0, height: 30, zIndex: 10,
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '0 16px',
          background: 'rgba(13,17,23,0.85)',
          borderTop: '1px solid rgba(33,38,45,0.7)',
          backdropFilter: 'blur(10px)',
          fontSize: 10, color: 'var(--t-3)', fontWeight: 600, letterSpacing: '0.06em',
        }}>
          <span>
            {graphData.nodes.length} NODES&nbsp;&nbsp;·&nbsp;&nbsp;{graphData.links.length} EDGES
          </span>
          <span>Click node to inspect · Scroll to zoom · Drag to pan</span>
        </div>
      )}
    </div>
  )
}

/* ── Shared glassmorphism style helper ─────────────────────── */
function glass(extra: React.CSSProperties = {}): React.CSSProperties {
  return {
    background: 'rgba(13,17,23,0.9)',
    border: '1px solid rgba(33,38,45,0.85)',
    borderRadius: 11,
    backdropFilter: 'blur(18px)',
    boxShadow: '0 4px 24px rgba(0,0,0,0.5)',
    ...extra,
  }
}

/* ── Icon ─────────────────────────────────────────────────── */
function SearchIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none"
      stroke="var(--t-3)" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="11" cy="11" r="8" />
      <line x1="21" y1="21" x2="16.65" y2="16.65" />
    </svg>
  )
}
