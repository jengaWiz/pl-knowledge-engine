import React, { useState, useEffect } from 'react'
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

  useEffect(() => {
    fetchMatches().then(setMatches).catch(console.error)
  }, [])

  const handleMatchClick = (match: Match) => {
    setView('graph')
    fetchMatchGraph(match.id).then(setOverrideGraph).catch(console.error)
  }

  return (
    <div style={{ display: 'flex', height: '100vh', overflow: 'hidden', background: 'var(--bg-0)' }}>

      {/* ════════════════════ SIDEBAR ════════════════════ */}
      <aside style={{
        width: 300, flexShrink: 0,
        background: 'var(--bg-1)',
        borderRight: '1px solid var(--bg-3)',
        display: 'flex', flexDirection: 'column', overflow: 'hidden',
      }}>

        {/* ── Header with ambient glow ── */}
        <div style={{
          padding: '20px 16px 16px',
          borderBottom: '1px solid var(--bg-3)',
          position: 'relative', overflow: 'hidden',
        }}>
          {/* Ambient glow orbs */}
          <div style={{
            position: 'absolute', top: -40, left: -25, width: 140, height: 140,
            borderRadius: '50%', background: 'rgba(103,14,54,0.55)', filter: 'blur(50px)',
            pointerEvents: 'none',
          }} />
          <div style={{
            position: 'absolute', top: -10, right: -20, width: 110, height: 110,
            borderRadius: '50%', background: 'rgba(200,16,46,0.45)', filter: 'blur(40px)',
            pointerEvents: 'none',
          }} />

          {/* Content */}
          <div style={{ position: 'relative', zIndex: 1 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 13 }}>
              <div style={{
                width: 42, height: 42, borderRadius: 13, flexShrink: 0,
                background: 'linear-gradient(135deg, #670E36 0%, #a81545 100%)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                boxShadow: '0 3px 18px rgba(103,14,54,0.65), 0 0 0 1px rgba(255,255,255,0.09)',
              }}>
                <NetworkIcon />
              </div>
              <div>
                <div style={{ fontSize: 14, fontWeight: 800, color: 'var(--t-1)', letterSpacing: '-0.03em', lineHeight: 1.2 }}>
                  PL Knowledge Engine
                </div>
                <div style={{ fontSize: 11, color: 'var(--t-3)', marginTop: 3, letterSpacing: '0.01em' }}>
                  2025–26 Premier League
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
            <ViewTab active={view === 'graph'} onClick={() => setView('graph')}>
              <GraphTabIcon />
              Graph View
            </ViewTab>
            <ViewTab active={view === 'chat'} onClick={() => setView('chat')}>
              <ChatTabIcon />
              AI Chat
            </ViewTab>
          </div>
        </div>

        <MatchList matches={matches} onSelect={handleMatchClick} />
      </aside>

      {/* ════════════════════ MAIN ════════════════════ */}
      <main style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <header style={{
          height: 52, flexShrink: 0,
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '0 22px',
          background: 'var(--bg-1)',
          borderBottom: '1px solid var(--bg-3)',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span style={{
              width: 8, height: 8, borderRadius: '50%', display: 'block', flexShrink: 0,
              background: view === 'graph' ? '#f59e0b' : 'var(--villa)',
              boxShadow: view === 'graph' ? '0 0 10px #f59e0b' : '0 0 10px rgba(103,14,54,0.95)',
              transition: 'all 0.3s ease',
            }} />
            <span style={{ fontSize: 14, fontWeight: 700, color: 'var(--t-1)', letterSpacing: '-0.025em' }}>
              {view === 'graph' ? 'Knowledge Graph' : 'AI Analyst'}
            </span>
          </div>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
            <span style={{ fontSize: 11, color: 'var(--t-3)', marginRight: 4 }}>
              {matches.length > 0 ? `${matches.length} matches indexed` : 'Loading…'}
            </span>
            <HeaderBadge team="villa" />
            <HeaderBadge team="lfc" />
          </div>
        </header>

        <div style={{ flex: 1, overflow: 'hidden' }}>
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
        background: active ? 'var(--villa)' : 'transparent',
        color: active ? '#fff' : 'var(--t-3)',
        transition: 'all 0.18s ease',
        boxShadow: active ? '0 2px 12px rgba(103,14,54,0.5)' : 'none',
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
        background: 'rgba(103,14,54,0.2)',
        border: '1px solid rgba(103,14,54,0.42)',
      }}>
        {/* Claret + sky-blue kit swatch */}
        <div style={{ display: 'flex', borderRadius: 2, overflow: 'hidden', flexShrink: 0, boxShadow: '0 0 5px rgba(103,14,54,0.6)' }}>
          <span style={{ display: 'block', width: 5, height: 18, background: '#670E36' }} />
          <span style={{ display: 'block', width: 4, height: 18, background: '#6BAED8' }} />
        </div>
        <span style={{ fontSize: 11, fontWeight: 700, color: '#d08898', letterSpacing: '-0.01em' }}>
          Aston Villa
        </span>
      </div>
    )
  }
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8,
      padding: '5px 11px 5px 7px', borderRadius: 8,
      background: 'rgba(200,16,46,0.18)',
      border: '1px solid rgba(200,16,46,0.42)',
    }}>
      {/* Red + gold kit swatch */}
      <div style={{ display: 'flex', borderRadius: 2, overflow: 'hidden', flexShrink: 0, boxShadow: '0 0 5px rgba(200,16,46,0.55)' }}>
        <span style={{ display: 'block', width: 6, height: 18, background: '#C8102E' }} />
        <span style={{ display: 'block', width: 3, height: 18, background: '#F6C94E' }} />
      </div>
      <span style={{ fontSize: 11, fontWeight: 700, color: '#f07888', letterSpacing: '-0.01em' }}>
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
        <span style={{ fontSize: 9, fontWeight: 800, color: '#d08898', letterSpacing: '0.06em' }}>AVFC</span>
      </div>
    )
  }
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 5,
      padding: '3px 8px 3px 5px', borderRadius: 5,
      background: 'rgba(200,16,46,0.18)', border: '1px solid rgba(200,16,46,0.38)',
    }}>
      <div style={{ display: 'flex', borderRadius: 1, overflow: 'hidden' }}>
        <span style={{ display: 'block', width: 4, height: 11, background: '#C8102E' }} />
        <span style={{ display: 'block', width: 2, height: 11, background: '#F6C94E' }} />
      </div>
      <span style={{ fontSize: 9, fontWeight: 800, color: '#f07888', letterSpacing: '0.06em' }}>LFC</span>
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
