import React, { useState, useEffect, useRef } from 'react'
import GraphView from './components/GraphView'
import ChatView from './components/ChatView'
import MatchList from './components/MatchList'
import { fetchMatches, fetchMatchGraph } from './api'
import type { Match, GraphData } from './types'

type View = 'graph' | 'chat'

export default function App() {
  const [view, setView] = useState<View>('graph')
  const [matches, setMatches] = useState<Match[]>([])
  const [overrideGraph, setOverrideGraph] = useState<GraphData | null>(null)

  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [matchesLoading, setMatchesLoading] = useState(true)
  const [matchesError, setMatchesError] = useState('')
  const [selectionError, setSelectionError] = useState('')
  const requestId = useRef(0)
  const loadMatches = () => {
    setMatchesLoading(true)
    setMatchesError('')
    fetchMatches().then(setMatches)
      .catch(() => setMatchesError('Fixtures are unavailable. Load the local dataset and retry.'))
      .finally(() => setMatchesLoading(false))
  }
  useEffect(loadMatches, [])
  const selectView = (next: View) => {
    requestId.current += 1
    setView(next)
    setSidebarOpen(false)
    setSelectionError('')
  }

  const handleMatchClick = (match: Match) => {
    setView('graph')
    setSidebarOpen(false)
    setSelectionError('')
    const id = ++requestId.current
    fetchMatchGraph(match.id).then(data => { if (id === requestId.current) setOverrideGraph(data) })
      .catch(() => { if (id === requestId.current) setSelectionError('This match graph is unavailable. Try another fixture.') })
  }

  return (
    <div className="app-shell" style={{ display: 'flex', height: '100dvh', overflow: 'hidden', background: 'var(--bg-0)' }}>

      {/* ════════════════════ SIDEBAR ════════════════════ */}
      <aside className="app-sidebar" data-open={sidebarOpen} style={{
        width: 276, flexShrink: 0,
        background: 'var(--bg-1)',
        borderRight: '1px solid var(--bg-3)',
        display: 'flex', flexDirection: 'column', overflow: 'hidden',
      }}>

        <button className="sidebar-close" onClick={() => setSidebarOpen(false)}>Close navigation</button>
        {/* ── Header with ambient glow ── */}
        <div style={{
          padding: '20px 16px 16px',
          borderBottom: '1px solid var(--bg-3)',
          position: 'relative', overflow: 'hidden',
        }}>
          {/* Content */}
          <div style={{ position: 'relative', zIndex: 1 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 13 }}>
              <div style={{
                width: 42, height: 42, borderRadius: 13, flexShrink: 0,
                background: '#766185',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                border: '1px solid #887595',
              }}>
                <NetworkIcon />
              </div>
              <div>
                <div style={{ fontSize: 14, fontWeight: 800, color: 'var(--t-1)', letterSpacing: '-0.03em', lineHeight: 1.2 }}>
                  PL Knowledge Engine
                </div>
                <div style={{ fontSize: 11, color: 'var(--t-3)', marginTop: 3, letterSpacing: '0.01em' }}>
                  PREMIER LEAGUE / 2025–26
                </div>
              </div>
            </div>

            {/* Team pills — full names */}
            <div style={{ display: 'flex', gap: 7 }}>
              <FullTeamPill team="villa" />
              <FullTeamPill team="lfc" />
            </div>
          </div>
        </div>

        {/* ── View toggle ── */}
        <div style={{ padding: '11px 13px', borderBottom: '1px solid var(--bg-3)' }}>
          <div style={{
            display: 'flex',
            background: 'var(--bg-0)',
            border: '1px solid var(--bg-3)',
            borderRadius: 10, padding: 3,
          }}>
            <ViewTab active={view === 'graph'} onClick={() => selectView('graph')}>
              <GraphTabIcon />
              Graph View
            </ViewTab>
            <ViewTab active={view === 'chat'} onClick={() => selectView('chat')}>
              <ChatTabIcon />
              Evidence Chat
            </ViewTab>
          </div>
        </div>

        {matchesLoading ? <p className="state-notice" role="status">Loading fixtures…</p>
          : matchesError ? <div className="state-notice" role="alert">{matchesError}
              <button onClick={loadMatches}>Retry fixtures</button></div>
          : <MatchList matches={matches} onSelect={handleMatchClick} />}

      </aside>

      {/* ════════════════════ MAIN ════════════════════ */}
      {sidebarOpen && <button className="nav-backdrop" aria-label="Close navigation" onClick={() => setSidebarOpen(false)} />}
      <main style={{ minWidth: 0, flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <header className="app-header" style={{
          height: 52, flexShrink: 0,
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '0 22px',
          background: 'var(--bg-1)',
          borderBottom: '1px solid var(--bg-3)',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <button className="mobile-menu" aria-expanded={sidebarOpen}
              onClick={() => setSidebarOpen(true)}>Navigation</button>
            <span style={{
              width: 8, height: 8, borderRadius: '50%', display: 'block', flexShrink: 0,
              background: '#779b87',
              boxShadow: 'none',
              transition: 'all 0.3s ease',
            }} />
            <span style={{ fontSize: 14, fontWeight: 700, color: 'var(--t-1)', letterSpacing: '-0.025em' }}>
              {view === 'graph' ? 'Knowledge Graph' : 'Evidence Analyst'}
            </span>
          </div>
          <div className="header-meta" style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
            <span style={{ fontSize: 11, color: 'var(--t-3)', marginRight: 4 }}>
              {matchesLoading ? 'Loading…' : matchesError ? 'Dataset unavailable' : `${matches.length} matches indexed`}
            </span>
            <HeaderBadge team="villa" />
            <HeaderBadge team="lfc" />
          </div>
        </header>

        {selectionError && <p className="state-notice" role="alert">{selectionError}</p>}
        <div style={{ flex: 1, minHeight: 0, overflow: 'hidden' }}>
          {view === 'graph'
            ? <GraphView overrideGraph={overrideGraph} onClearOverride={() => setOverrideGraph(null)} />
            : <ChatView />
          }
        </div>
      </main>
    </div>
  )
}

/* ── View tab ─────────────────────────────────────────────── */
function ViewTab({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      style={{
        flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
        padding: '8px 0', borderRadius: 8, border: 'none', cursor: 'pointer',
        fontSize: 13, fontWeight: 600, letterSpacing: '-0.01em',
        background: active ? '#eee8f4' : 'transparent',
        color: active ? '#655176' : 'var(--t-3)',
        transition: 'all 0.18s ease',
        boxShadow: 'none',
      }}
      onMouseEnter={e => { if (!active) (e.currentTarget as HTMLElement).style.color = 'var(--t-2)' }}
      onMouseLeave={e => { if (!active) (e.currentTarget as HTMLElement).style.color = 'var(--t-3)' }}
    >
      {children}
    </button>
  )
}

/* ── Team pills ───────────────────────────────────────────── */

/**
 * Aston Villa = claret (#670E36) + sky blue (#6BAED8)
 * Liverpool FC = red (#C8102E) + gold (#F6C94E)
 */
function FullTeamPill({ team }: { team: 'villa' | 'lfc' }) {
  if (team === 'villa') {
    return (
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8,
        padding: '5px 11px 5px 7px', borderRadius: 8,
        background: '#f6f0f3',
        border: '1px solid rgba(103,14,54,0.42)',
      }}>
        {/* Claret + sky-blue kit swatch */}
        <div style={{ display: 'flex', borderRadius: 2, overflow: 'hidden', flexShrink: 0, boxShadow: 'none' }}>
          <span style={{ display: 'block', width: 5, height: 18, background: '#670E36' }} />
          <span style={{ display: 'block', width: 4, height: 18, background: '#6BAED8' }} />
        </div>
        <span style={{ fontSize: 11, fontWeight: 700, color: '#845d70', letterSpacing: '-0.01em' }}>
          Aston Villa
        </span>
      </div>
    )
  }
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8,
      padding: '5px 11px 5px 7px', borderRadius: 8,
      background: '#faf1f1',
      border: '1px solid rgba(200,16,46,0.42)',
    }}>
      {/* Red + gold kit swatch */}
      <div style={{ display: 'flex', borderRadius: 2, overflow: 'hidden', flexShrink: 0, boxShadow: 'none' }}>
        <span style={{ display: 'block', width: 6, height: 18, background: '#C8102E' }} />
        <span style={{ display: 'block', width: 3, height: 18, background: '#F6C94E' }} />
      </div>
      <span style={{ fontSize: 11, fontWeight: 700, color: '#995e68', letterSpacing: '-0.01em' }}>
        Liverpool FC
      </span>
    </div>
  )
}

function HeaderBadge({ team }: { team: 'villa' | 'lfc' }) {
  if (team === 'villa') {
    return (
      <div style={{
        display: 'flex', alignItems: 'center', gap: 5,
        padding: '3px 8px 3px 5px', borderRadius: 5,
        background: 'rgba(103,14,54,0.18)', border: '1px solid rgba(103,14,54,0.38)',
      }}>
        <div style={{ display: 'flex', borderRadius: 1, overflow: 'hidden' }}>
          <span style={{ display: 'block', width: 3, height: 11, background: '#670E36' }} />
          <span style={{ display: 'block', width: 2, height: 11, background: '#6BAED8' }} />
        </div>
        <span style={{ fontSize: 9, fontWeight: 800, color: '#845d70', letterSpacing: '0.06em' }}>AVFC</span>
      </div>
    )
  }
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 5,
      padding: '3px 8px 3px 5px', borderRadius: 5,
      background: '#faf1f1', border: '1px solid rgba(200,16,46,0.38)',
    }}>
      <div style={{ display: 'flex', borderRadius: 1, overflow: 'hidden' }}>
        <span style={{ display: 'block', width: 4, height: 11, background: '#C8102E' }} />
        <span style={{ display: 'block', width: 2, height: 11, background: '#F6C94E' }} />
      </div>
      <span style={{ fontSize: 9, fontWeight: 800, color: '#995e68', letterSpacing: '0.06em' }}>LFC</span>
    </div>
  )
}

/* ── Icons ────────────────────────────────────────────────── */
function NetworkIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none"
      stroke="rgba(255,255,255,0.9)" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="5"  r="2.5" fill="rgba(255,255,255,0.2)" />
      <circle cx="5"  cy="19" r="2.5" fill="rgba(255,255,255,0.2)" />
      <circle cx="19" cy="19" r="2.5" fill="rgba(255,255,255,0.2)" />
      <line x1="12" y1="7.5" x2="5.8"  y2="16.7" />
      <line x1="12" y1="7.5" x2="18.2" y2="16.7" />
      <line x1="6.8" y1="18.5" x2="17.2" y2="18.5" />
    </svg>
  )
}

function GraphTabIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="5"  r="2.5" />
      <circle cx="5"  cy="19" r="2.5" />
      <circle cx="19" cy="19" r="2.5" />
      <line x1="12" y1="7.5" x2="5"  y2="16.5" />
      <line x1="12" y1="7.5" x2="19" y2="16.5" />
    </svg>
  )
}

function ChatTabIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
    </svg>
  )
}
