import { useEffect, useRef, useState } from 'react'
import axios from 'axios'
import { fetchEvidenceStatus, retrieveEvidence } from '../api'
import type { EvidenceResult, EvidenceStatus } from '../types'

const STATUS_LABELS = {
  resolved: 'Verified records', clarification: 'Clarification needed',
  unsupported: 'Outside current scope', unavailable: 'No matching evidence',
}

function sourceURL(value?: string): string | undefined {
  try {
    const parsed = new URL(value || '')
    return ['https:', 'http:'].includes(parsed.protocol) ? parsed.href : undefined
  } catch { return undefined }
}

export default function EvidenceView() {
  const [status, setStatus] = useState<EvidenceStatus | null>(null)
  const [checking, setChecking] = useState(false)
  const [season, setSeason] = useState('')
  const [query, setQuery] = useState('')
  const [submitted, setSubmitted] = useState('')
  const [result, setResult] = useState<EvidenceResult | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const controller = useRef<AbortController | null>(null)

  const check = async () => {
    controller.current?.abort()
    const request = new AbortController()
    controller.current = request
    setChecking(true)
    setError('')
    setResult(null)
    setStatus(null)
    try {
      const next = await fetchEvidenceStatus(request.signal)
      if (request.signal.aborted) return
      setStatus(next)
      setSeason(previous => next.seasons.includes(previous) ? previous
        : next.seasons.includes(next.primary_season) ? next.primary_season : next.seasons[0] || '')
    } catch {
      if (!request.signal.aborted) setError('Evidence verification is unavailable or busy. Retry shortly.')
    } finally { if (!request.signal.aborted) setChecking(false) }
  }

  useEffect(() => { void check(); return () => controller.current?.abort() }, [])

  const send = async (text: string) => {
    if (!text.trim() || !season || busy || checking || status?.status !== 'ready') return
    const request = new AbortController()
    controller.current = request
    setBusy(true)
    setError('')
    setResult(null)
    setSubmitted(text)
    setQuery(text)
    try {
      const next = await retrieveEvidence(text, season, request.signal)
      if (!request.signal.aborted) setResult(next)
    } catch (err) {
      if (!request.signal.aborted) {
        const unavailable = axios.isAxiosError(err) && err.response?.status === 503
        if (unavailable) setStatus(null)
        setError(axios.isAxiosError(err) && err.response?.status === 429
          ? 'Another evidence check is running. Retry shortly.'
          : 'Verified retrieval is unavailable. Recheck evidence and retry.')
      }
    } finally { if (!request.signal.aborted) setBusy(false) }
  }

  const ready = status?.status === 'ready' && !checking
  return <section className="evidence-view" aria-label="Structured evidence search">
    <div className="evidence-heading">
      <div><p className="evidence-eyebrow">SOURCE EVIDENCE</p><h2>Find the record behind the question</h2>
        <p>Search recorded fixtures across four seasons, or player appearances in 2025–26. Specify home, away or a fixture date.</p></div>
      <button type="button" onClick={() => void check()} disabled={busy || checking}>Recheck evidence</button>
    </div>
    {checking && <p role="status">Verifying sources and local stores…</p>}
    {status?.status === 'not_ready' && <div className="evidence-notice" role="status">
      Structured evidence is not ready. Season statistics remain available in the other mode.
      <p>Prepare the accepted corpus, evidence index and evidence graph, then recheck.</p>
    </div>}
    {error && <p className="evidence-notice" role="alert">{error}</p>}
    {ready && <p className="evidence-coverage">{status.index?.documents.toLocaleString()} verified documents · {status.seasons.length} seasons</p>}
    <form onSubmit={event => { event.preventDefault(); void send(query) }} className="evidence-form">
      <label>Evidence season<select aria-label="Evidence season" value={season} disabled={!ready || busy}
        onChange={event => { setSeason(event.target.value); setResult(null); setError('') }}>
        {!season && <option value="">Select a verified season</option>}
        {status?.seasons.map(item => <option key={item} value={item}>{item}</option>)}
      </select></label>
      <label className="evidence-question">Question<textarea aria-label="Evidence question" value={query}
        maxLength={4000} rows={2} disabled={!ready || busy} onChange={event => setQuery(event.target.value)}
        placeholder="Liverpool home match against Bournemouth" /></label>
      <button type="submit" disabled={!ready || busy || !query.trim()}>{busy ? 'Verifying…' : 'Find evidence'}</button>
    </form>
    <div className="evidence-examples">
      {['Liverpool home match against Bournemouth', ...(season === '2025-26' ? ['Digne appearances against Liverpool'] : [])].map(text =>
        <button key={text} disabled={!ready || busy} onClick={() => void send(text)}>{text}</button>)}
    </div>
    <p className="evidence-scope">Each question stands alone. Results show recorded evidence, not predictions. Player search considers at most 50 recent matching appearances.</p>
    {busy && <p role="status">Checking graph connections, ranking eligible documents and verifying source references…</p>}
    {result && <div className="evidence-results" aria-live="polite">
      <div className="evidence-result-header"><span className="evidence-status">{STATUS_LABELS[result.route.status]}</span>
        <span>{result.route.season}</span></div>
      <h3>{submitted}</h3>
      {result.route.status !== 'resolved' && <p>{result.route.reason}</p>}
      {result.route.candidates?.map(candidate => <button key={candidate.canonical_id} disabled={busy}
        className="evidence-candidate" onClick={() => void send(`${candidate.home_team} home match against ${candidate.away_team} on ${candidate.date}`)}>
        {candidate.home_team} vs {candidate.away_team} · {candidate.date}</button>)}
      {result.hits.map(hit => <article key={hit.id} className="evidence-card">
        <div className="evidence-result-header"><span>{hit.graph.date}</span><span>{result.route.season}</span></div>
        <h3>{hit.graph.home_team
          ? `${hit.graph.home_team} ${hit.graph.home_score}–${hit.graph.away_score} ${hit.graph.away_team}`
          : `${hit.graph.team} vs ${hit.graph.opponent} · ${hit.graph.minutes} minutes`}</h3>
        <p>{hit.text}</p>
        <details><summary>{hit.graph.source_refs.length} source references</summary>
          {hit.graph.source_refs.map((source, index) => <div className="evidence-source" key={`${source.asset_id}-${source.row}-${index}`}>
            <span>{source.id || 'Original source'}{source.row !== undefined ? ` · row ${source.row}` : ''}</span>
            {source.revision && <code>Revision: {source.revision}</code>}
            {sourceURL(source.url) && <a href={sourceURL(source.url)} target="_blank" rel="noopener noreferrer">View original source</a>}
          </div>)}
          <code className="evidence-document">Document: {hit.id}</code>
        </details>
      </article>)}
    </div>}
  </section>
}
