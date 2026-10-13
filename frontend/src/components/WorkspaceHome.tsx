import { useState } from 'react'
import type { Match } from '../types'

export type WorkspacePage = 'home' | 'graph' | 'statistics' | 'evidence' | 'prediction'
export const destinations: { id: WorkspacePage; label: string; detail: string; symbol: string }[] = [
  { id: 'home', label: 'Discover', detail: 'Your starting point', symbol: '◈' },
  { id: 'graph', label: 'Graph View', detail: 'Connect clubs, players and matches', symbol: '⌘' },
  { id: 'statistics', label: 'Football analyst', detail: 'Ask about matches, players and form', symbol: '▥' },
  { id: 'evidence', label: 'Source evidence', detail: 'Find the original match records', symbol: '≡' },
  { id: 'prediction', label: 'Predictions', detail: 'Replay historical match probabilities', symbol: '↗' },
]

export default function WorkspaceHome({ matches, loading, error, onNavigate, onMatch, onRetry }: {
  matches: Match[]; loading: boolean; error: string; onNavigate: (page: WorkspacePage) => void;
  onMatch: (match: Match) => void; onRetry: () => void;
}) {
  const [club, setClub] = useState('Liverpool')
  const [index, setIndex] = useState(0)
  const fixtures = matches.filter(m => m.home_team === club || m.away_team === club)
  const fixture = fixtures[index % (fixtures.length || 1)]
  return <div className="discover-page">
    <section className="discover-hero">
      <div className="hero-copy">
        <p className="section-kicker"><span /> A DIFFERENT VIEW OF THE GAME</p>
        <h1>Football.<br /><span>Connected.</span></h1>
        <p className="hero-description">Follow the players. Find the patterns.<br />See the records behind every result.</p>
        <button className="hero-action" onClick={() => onNavigate('graph')}>Explore the league <span aria-hidden="true">↗</span></button>
        <div className="hero-footnote">Premier League research workspace <span>2025–26</span></div>
      </div>
      <div className="spotlight" aria-label="Match spotlight">
        <div className="spotlight-top"><span>MATCH SPOTLIGHT</span><span className="archive-tag">Season archive</span></div>
        <div className="club-switch" aria-label="Spotlight club">
          {['Liverpool', 'Aston Villa'].map(name => <button key={name} aria-pressed={club === name} onClick={() => { setClub(name); setIndex(0) }}>{name}</button>)}
        </div>
        <div className="pitch-art" aria-hidden="true"><div className="pitch-center" /><div className="pitch-box left" /><div className="pitch-box right" /><i className="pitch-node one" /><i className="pitch-node two" /><i className="pitch-node three" /></div>
        {loading ? <div className="spotlight-empty" role="status">Loading the season…</div>
          : error ? <div className="spotlight-empty">Matches couldn’t load.<button onClick={onRetry}>Try again</button></div>
          : fixture ? <div className="fixture-feature" key={fixture.id}>
            <p>{new Date(fixture.date).toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric', timeZone: 'UTC' })} · GW {fixture.gameweek}</p>
            <div className="fixture-teams"><div><span className="team-disc">{fixture.home_team.slice(0, 3).toUpperCase()}</span><strong>{fixture.home_team}</strong><small>HOME</small></div>
              <div className="fixture-score">{fixture.home_score ?? '–'}<span>:</span>{fixture.away_score ?? '–'}<small>{fixture.home_score === null || fixture.away_score === null ? 'UNRECORDED' : 'FULL TIME'}</small></div>
              <div><span className="team-disc away">{fixture.away_team.slice(0, 3).toUpperCase()}</span><strong>{fixture.away_team}</strong><small>AWAY</small></div></div>
            <button className="spotlight-link" onClick={() => onMatch(fixture)}>Explore this match <span aria-hidden="true">↗</span></button>
          </div> : <div className="spotlight-empty">No stored fixtures for this club yet.</div>}
        <div className="spotlight-bottom"><span>{fixtures.length ? `${index % fixtures.length + 1} / ${fixtures.length} fixtures` : 'Stored match records'}</span><div>
          <button aria-label="Previous spotlight match" disabled={fixtures.length < 2} onClick={() => setIndex(i => (i - 1 + fixtures.length) % fixtures.length)}>←</button>
          <button aria-label="Next spotlight match" disabled={fixtures.length < 2} onClick={() => setIndex(i => (i + 1) % fixtures.length)}>→</button>
        </div></div>
      </div>
    </section>
    <section className="tool-section" aria-labelledby="tools-heading"><div className="section-heading"><div><p className="section-kicker">YOUR NEXT MOVE</p><h2 id="tools-heading">A question. A place to start.</h2></div><span>Four ways to explore</span></div>
      <div className="tool-grid">{destinations.slice(1).map((item, i) => <button key={item.id} className={`tool-card tool-${item.id}`} onClick={() => onNavigate(item.id)}>
        <div className="tool-card-top"><span className="tool-symbol" aria-hidden="true">{item.symbol}</span><span className="tool-index">0{i + 1}</span></div>
        <h3>{item.label === 'Graph View' ? 'Explore connections' : item.label}</h3><p>{item.detail}.</p>
        <span className="tool-bottom">{['Start with a club', 'Try a suggested question', 'Choose a season', 'Choose a past fixture'][i]}<span aria-hidden="true">↗</span></span>
      </button>)}</div>
    </section>
    <footer className="discover-footer"><span><i /> Built around traceable football records</span><span>Historical data · Independent project</span></footer>
  </div>
}
