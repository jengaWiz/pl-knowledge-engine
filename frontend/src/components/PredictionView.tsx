import { useEffect, useRef, useState } from 'react'
import axios from 'axios'
import { fetchPredictionStatus, predictFixture } from '../api'
import type { PredictionResult, PredictionStatus } from '../types'

function safeURL(url: string): string | undefined {
  try { const parsed = new URL(url); return ['http:', 'https:'].includes(parsed.protocol) ? parsed.href : undefined }
  catch { return undefined }
}

export default function PredictionView() {
  const [catalogue, setCatalogue] = useState<PredictionStatus | null>(null)
  const [season, setSeason] = useState('2025-26')
  const [fixture, setFixture] = useState('')
  const [result, setResult] = useState<PredictionResult | null>(null)
  const [checking, setChecking] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const controller = useRef<AbortController | null>(null)

  const check = async () => {
    controller.current?.abort()
    const request = new AbortController(); controller.current = request
    setChecking(true); setError(''); setResult(null); setCatalogue(null)
    try {
      const next = await fetchPredictionStatus(request.signal)
      if (!request.signal.aborted) { setCatalogue(next); setFixture('') }
    } catch {
      if (!request.signal.aborted) setError('Prediction sources are unavailable or busy. Prepare the historical sources and recheck.')
    } finally { if (!request.signal.aborted) setChecking(false) }
  }
  useEffect(() => { void check(); return () => controller.current?.abort() }, [])

  const send = async () => {
    if (!fixture || busy || checking || !catalogue) return
    const request = new AbortController(); controller.current = request
    setBusy(true); setError(''); setResult(null)
    try {
      const next = await predictFixture(fixture, request.signal)
      if (!request.signal.aborted) setResult(next)
    } catch (err) {
      if (!request.signal.aborted) setError(axios.isAxiosError(err) && err.response?.status === 429
        ? 'Another prediction check is running. Retry shortly.'
        : axios.isAxiosError(err) && err.response?.status === 404
          ? 'This fixture is no longer available. Recheck the fixture catalogue.'
          : 'Verified prediction results are unavailable. Recheck sources and retry.')
    } finally { if (!request.signal.aborted) setBusy(false) }
  }
  const fixtures = catalogue?.fixtures.filter(row => row.season === season) || []
  return <section className="evidence-view prediction-view" aria-label="Historical prediction workspace">
    <div className="evidence-heading">
      <div><p className="evidence-eyebrow">MATCH OUTCOMES / HISTORICAL REPLAY</p>
        <h2>Replay the forecast before the result</h2>
        <p>Choose a recorded fixture to estimate home, draw and away probabilities using only results from earlier dates. This is a historical model replay, not a live forecast.</p></div>
      <button onClick={() => void check()} disabled={busy || checking}>Recheck predictions</button>
    </div>
    {checking && <p role="status">Verifying historical results and model selection…</p>}
    {error && <p className="evidence-notice" role="alert">{error}</p>}
    <form className="prediction-form" onSubmit={e => { e.preventDefault(); void send() }}>
      <label>Season<select aria-label="Prediction season" value={season} disabled={!catalogue || busy || checking}
        onChange={e => { setSeason(e.target.value); setFixture(''); setResult(null); setError('') }}>
        <option value="2025-26">2025–26</option><option value="2024-25">2024–25</option>
      </select></label>
      <label className="prediction-fixture">Fixture<select aria-label="Prediction fixture" value={fixture} disabled={!catalogue || busy || checking}
        onChange={e => { setFixture(e.target.value); setResult(null); setError('') }}>
        <option value="">Choose a historical fixture</option>
        {fixtures.map(row => <option key={row.id} value={row.id}>{row.date} · {row.home_team} vs {row.away_team}</option>)}
      </select></label>
      <button type="submit" disabled={!fixture || busy || checking}>{busy ? 'Calculating…' : 'Replay prediction'}</button>
    </form>
    {busy && <p role="status">Checking sources and calculating date-bounded probabilities…</p>}
    {result && <div className="prediction-results" aria-live="polite">
      <p className="prediction-cutoff">Results dated before {result.feature_cutoff_exclusive} · {result.history_matches.toLocaleString()} eligible fixtures</p>
      <h3>{result.fixture.home_team} vs {result.fixture.away_team}</h3>
      <p className="evidence-scope">{result.fixture.season} · {result.fixture.date} · Actual result excluded</p>
      <div className="outcome-grid">{(['home', 'draw', 'away'] as const).map(kind =>
        <div className={`outcome-card outcome-${kind}`} key={kind}>
          <span>{kind === 'home' ? result.fixture.home_team : kind === 'away' ? result.fixture.away_team : 'Draw'}</span>
          <strong>{(result.probabilities[kind] * 100).toFixed(1)}%</strong>
          <div className="outcome-track" aria-hidden="true"><i style={{ width: `${result.probabilities[kind] * 100}%` }} /></div>
        </div>)}</div>
      <p className="prediction-goals">Model mean goals: {result.fixture.home_team} {result.expected_goals.home.toFixed(2)} · {result.fixture.away_team} {result.expected_goals.away.toFixed(2)}. These are goal-rate estimates, not a predicted scoreline.</p>
      <article className="prediction-explanation"><h4>What drives this estimate</h4><p>{result.explanation}</p></article>
      <details className="prediction-evidence"><summary>Inspect {result.evidence.length} earlier result records</summary>
        <p className="evidence-scope">{result.evidence_scope}</p>
        {result.evidence.map(row => <article key={row.id}>
          <small>{row.date}</small><strong>{row.home_team} {row.home_score}–{row.away_score} {row.away_team}</strong>
          {safeURL(row.source.url) && <a href={safeURL(row.source.url)} target="_blank" rel="noopener noreferrer">Original CSV · row {row.source.row}</a>}
          <code>Revision: {row.source.revision}</code>
        </article>)}
        <code>History SHA-256: {result.history_sha256}</code>
      </details>
      <details className="prediction-evidence"><summary>Model and limitations</summary>
        <p>{result.model_version} · {result.window}-match window · selected on {result.selection_season}</p>
        <ul>{result.limitations.map(text => <li key={text}>{text}</li>)}</ul>
      </details>
    </div>}
    {catalogue && <aside className="prediction-evaluation"><h4>How this model was evaluated</h4>
      {Object.entries(catalogue.evaluations).map(([key, evaluation]) => <div key={key} className="evaluation-season">
        <strong>{key} · {evaluation.rolling.matches} historical fixtures</strong>
        <p>Accuracy {(evaluation.rolling.accuracy * 100).toFixed(1)}% · log loss {evaluation.rolling.log_loss.toFixed(3)} · Brier {evaluation.rolling.brier.toFixed(3)}.</p>
        <p>League baseline: log loss {evaluation.league.log_loss.toFixed(3)} · Brier {evaluation.league.brier.toFixed(3)}. Lower log loss and Brier are better.</p>
        <p>Approximate 95% accuracy interval: {(evaluation.uncertainty.intervals.accuracy.lower * 100).toFixed(1)}–{(evaluation.uncertainty.intervals.accuracy.upper * 100).toFixed(1)}%. This measures score uncertainty within the season, not uncertainty for this fixture.</p>
      </div>)}
      <p>These retrospective results do not establish live forecasting skill. Date-block intervals omit some serial dependence and season drift. Calibration bins are documented in the repository; probabilities have not been recalibrated.</p>
      <p>Source publication times are unavailable, and later corrections may exist. No injury, lineup, odds, tactical or generated commentary enters the model.</p>
    </aside>}
  </section>
}
