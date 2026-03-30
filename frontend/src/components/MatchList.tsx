import { useState } from 'react'
import type { Match } from '../types'

function teamColor(name: string): string {
  if (name === 'Aston Villa') return '#670E36'
  if (name === 'Liverpool')   return '#C8102E'
  return ''
}

function formatDate(d: string) {
  return new Date(d).toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })
}

interface Props {
  matches: Match[]
  onSelect: (m: Match) => void
}

export default function MatchList({ matches, onSelect }: Props) {
  const [selected, setSelected] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [focused, setFocused] = useState(false)

  const filtered = matches.filter(m =>
    !search ||
    m.home_team.toLowerCase().includes(search.toLowerCase()) ||
    m.away_team.toLowerCase().includes(search.toLowerCase()),
  )

  // Group by gameweek
  const grouped: Record<number, Match[]> = {}
  filtered.forEach(m => {
    if (!grouped[m.gameweek]) grouped[m.gameweek] = []
    grouped[m.gameweek].push(m)
  })
  const gameweeks = Object.keys(grouped).map(Number).sort((a, b) => a - b)

  const handleClick = (m: Match) => { setSelected(m.id); onSelect(m) }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', flex: 1, overflow: 'hidden' }}>
      {/* ── Search ── */}
      <div style={{ padding: '10px 12px', borderBottom: '1px solid var(--bg-3)' }}>
        <div style={{
          display: 'flex', alignItems: 'center', gap: 8,
          background: 'var(--bg-0)',
          border: `1px solid ${focused ? 'rgba(103,14,54,0.5)' : 'var(--bg-3)'}`,
          borderRadius: 8, padding: '7px 10px',
          transition: 'border-color 0.18s',
        }}>
          <SearchIcon />
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            onFocus={() => setFocused(true)}
            onBlur={() => setFocused(false)}
            placeholder="Filter matches…"
            style={{ flex: 1, background: 'transparent', border: 'none', color: 'var(--t-1)', fontSize: 13 }}
          />
          {search && (
            <button onClick={() => setSearch('')} style={{
              background: 'none', border: 'none', color: 'var(--t-3)', cursor: 'pointer', padding: 0, fontSize: 16, lineHeight: 1,
            }}>×</button>
          )}
        </div>
        <div style={{ fontSize: 10, color: 'var(--t-3)', marginTop: 6, letterSpacing: '0.07em', fontWeight: 700 }}>
          {filtered.length} MATCH{filtered.length !== 1 ? 'ES' : ''}
          {search ? ` · filtered` : ''}
        </div>
      </div>

      {/* ── Grouped list ── */}
      <div style={{ overflowY: 'auto', flex: 1 }}>
        {filtered.length === 0 ? (
          <div style={{ padding: '28px 16px', textAlign: 'center', color: 'var(--t-3)', fontSize: 13 }}>
            No matches found
          </div>
        ) : gameweeks.map(gw => (
          <div key={gw}>
            {/* GW section header */}
            <div style={{
              padding: '9px 14px 6px',
              display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              background: 'var(--bg-0)',
              borderBottom: '1px solid var(--bg-3)',
              position: 'sticky', top: 0, zIndex: 2,
            }}>
              <span style={{
                fontSize: 10, fontWeight: 800, color: 'var(--t-3)', letterSpacing: '0.1em',
              }}>
                GAMEWEEK {gw}
              </span>
              <span style={{
                fontSize: 9, color: 'var(--t-3)',
                background: 'var(--bg-2)', border: '1px solid var(--bg-3)',
                borderRadius: 4, padding: '1px 6px', fontWeight: 600, letterSpacing: '0.04em',
              }}>
                {grouped[gw].length} match{grouped[gw].length !== 1 ? 'es' : ''}
              </span>
            </div>

            {/* Match cards for this GW */}
            {grouped[gw].map(m => (
              <MatchCard key={m.id} match={m} isSelected={m.id === selected} onClick={() => handleClick(m)} />
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}

/* ── Match card ───────────────────────────────────────────── */
function MatchCard({ match: m, isSelected, onClick }: {
  match: Match
  isSelected: boolean
  onClick: () => void
}) {
  const [hovered, setHovered] = useState(false)
  const finished = m.home_score !== null && m.away_score !== null
  const homeColor = teamColor(m.home_team)
  const awayColor = teamColor(m.away_team)
  const active = isSelected || hovered

  return (
    <div
      onClick={onClick}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        padding: '12px 14px 12px 11px',
        cursor: 'pointer',
        borderBottom: '1px solid rgba(33,38,45,0.55)',
        borderLeft: `3px solid ${isSelected ? '#670E36' : hovered ? 'rgba(103,14,54,0.45)' : 'transparent'}`,
        background: isSelected
          ? 'linear-gradient(90deg, rgba(103,14,54,0.16) 0%, rgba(103,14,54,0.04) 100%)'
          : hovered ? 'var(--bg-2)' : 'transparent',
        transition: 'background 0.15s, border-color 0.15s',
        display: 'flex', alignItems: 'center', gap: 12,
      }}
    >
      {/* Team + date info */}
      <div style={{ flex: 1, minWidth: 0 }}>
        {/* Date */}
        <div style={{
          fontSize: 10, color: active ? 'rgba(200,120,150,0.75)' : 'var(--t-3)',
          fontWeight: 600, letterSpacing: '0.04em', marginBottom: 7,
          transition: 'color 0.15s',
        }}>
          {formatDate(m.date)}
        </div>

        {/* Home */}
        <TeamRow name={m.home_team} color={homeColor} side="H" />
        <div style={{ height: 5 }} />
        {/* Away */}
        <TeamRow name={m.away_team} color={awayColor} side="A" />
      </div>

      {/* Score */}
      <div style={{ flexShrink: 0, textAlign: 'center', minWidth: 36 }}>
        {finished ? (
          <>
            <div style={{
              fontSize: 17, fontWeight: 900, color: 'var(--green)', lineHeight: 1,
              letterSpacing: '-0.02em',
              textShadow: '0 0 12px rgba(63,185,80,0.5)',
            }}>
              {m.home_score}–{m.away_score}
            </div>
            <div style={{
              fontSize: 8, color: 'var(--green)', fontWeight: 700,
              letterSpacing: '0.08em', marginTop: 3, opacity: 0.7,
            }}>
              FT
            </div>
          </>
        ) : (
          <div style={{
            fontSize: 10, color: 'var(--t-3)', fontWeight: 700,
            background: 'var(--bg-2)', border: '1px solid var(--bg-3)',
            borderRadius: 5, padding: '4px 6px', letterSpacing: '0.03em',
          }}>
            TBD
          </div>
        )}
      </div>
    </div>
  )
}

function TeamRow({ name, color, side }: { name: string; color: string; side: string }) {
  const isVilla = name === 'Aston Villa'
  const isLFC   = name === 'Liverpool'

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
      <span style={{
        fontSize: 9, fontWeight: 700, color: 'var(--t-3)', width: 9,
        textAlign: 'right', letterSpacing: '0.03em', flexShrink: 0,
      }}>
        {side}
      </span>

      {/* Kit swatch — dual-color for Villa, red+gold for Liverpool, gray for others */}
      {isVilla ? (
        <div style={{
          display: 'flex', borderRadius: 2, overflow: 'hidden', flexShrink: 0,
          boxShadow: '0 0 5px rgba(103,14,54,0.65)',
        }}>
          <span style={{ display: 'block', width: 3, height: 16, background: '#670E36' }} />
          <span style={{ display: 'block', width: 2, height: 16, background: '#6BAED8' }} />
        </div>
      ) : isLFC ? (
        <div style={{
          display: 'flex', borderRadius: 2, overflow: 'hidden', flexShrink: 0,
          boxShadow: '0 0 5px rgba(200,16,46,0.65)',
        }}>
          <span style={{ display: 'block', width: 3, height: 16, background: '#C8102E' }} />
          <span style={{ display: 'block', width: 2, height: 16, background: '#F6C94E' }} />
        </div>
      ) : (
        <span style={{ display: 'block', width: 3, height: 16, borderRadius: 2, flexShrink: 0, background: 'var(--bg-4)' }} />
      )}

      <span style={{
        fontSize: 13, fontWeight: 600,
        color: (isVilla || isLFC) ? 'var(--t-1)' : 'var(--t-2)',
        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        letterSpacing: '-0.01em',
      }}>
        {name}
      </span>
    </div>
  )
}

/* ── Icon ─────────────────────────────────────────────────── */
function SearchIcon() {
  return (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none"
      stroke="var(--t-3)" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="11" cy="11" r="8" />
      <line x1="21" y1="21" x2="16.65" y2="16.65" />
    </svg>
  )
}
