import { useState, useEffect, useRef } from 'react'
import GraphView from './components/GraphView'
import ChatView from './components/ChatView'
import MatchList from './components/MatchList'
import WorkspaceHome, { destinations, type WorkspacePage } from './components/WorkspaceHome'
import { fetchMatches, fetchMatchGraph } from './api'
import type { Match, GraphData } from './types'

function readPage(): WorkspacePage {
  const id = window.location.hash.slice(1)
  return destinations.find(item => item.id === id)?.id ?? 'home'
}

export default function App() {
  const [view, setView] = useState<WorkspacePage>(readPage)
  const [matches, setMatches] = useState<Match[]>([])
  const [overrideGraph, setOverrideGraph] = useState<GraphData | null>(null)
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [matchesLoading, setMatchesLoading] = useState(true)
  const [matchesError, setMatchesError] = useState('')
  const [selectionError, setSelectionError] = useState('')
  const [selecting, setSelecting] = useState(false)
  const [command, setCommand] = useState('')
  const dialog = useRef<HTMLDialogElement>(null)
  const commandInput = useRef<HTMLInputElement>(null)
  const menuButton = useRef<HTMLButtonElement>(null)
  const sidebar = useRef<HTMLElement>(null)
  const main = useRef<HTMLElement>(null)
  const requestId = useRef(0)
  const wasSidebarOpen = useRef(false)
  const loadMatches = () => {
    setMatchesLoading(true); setMatchesError('')
    fetchMatches().then(setMatches)
      .catch(() => setMatchesError('Fixtures are unavailable. Load the local dataset and retry.'))
      .finally(() => setMatchesLoading(false))
  }
  useEffect(loadMatches, [])
  useEffect(() => {
    const change = () => { requestId.current += 1; setView(readPage()); setSidebarOpen(false); setSelectionError(''); setSelecting(false) }
    window.addEventListener('hashchange', change)
    return () => window.removeEventListener('hashchange', change)
  }, [])
  const openCommand = () => { setCommand(''); dialog.current?.showModal(); commandInput.current?.focus() }
  useEffect(() => {
    const key = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') { event.preventDefault(); openCommand() }
      if (event.key === 'Escape' && !dialog.current?.open && sidebarOpen) { setSidebarOpen(false); menuButton.current?.focus() }
    }
    window.addEventListener('keydown', key)
    return () => window.removeEventListener('keydown', key)
  }, [sidebarOpen])
  useEffect(() => {
    const media = window.matchMedia('(max-width: 800px)')
    const sync = () => {
      if (sidebar.current) sidebar.current.inert = media.matches && !sidebarOpen
      if (main.current) main.current.inert = media.matches && sidebarOpen
    }
    sync()
    if (wasSidebarOpen.current && !sidebarOpen) menuButton.current?.focus()
    wasSidebarOpen.current = sidebarOpen
    media.addEventListener('change', sync)
    return () => media.removeEventListener('change', sync)
  }, [sidebarOpen])
  useEffect(() => {
    if (!sidebarOpen) return
    sidebar.current?.querySelector<HTMLButtonElement>('button')?.focus()
    const trap = (event: KeyboardEvent) => {
      if (event.key !== 'Tab' || !sidebar.current || dialog.current?.open) return
      const elements = Array.from(sidebar.current.querySelectorAll<HTMLElement>('button:not(:disabled), input, a')).filter(el => el.getClientRects().length)
      const first = elements[0], last = elements[elements.length - 1]
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus() }
      if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus() }
    }
    document.addEventListener('keydown', trap)
    return () => document.removeEventListener('keydown', trap)
  }, [sidebarOpen])
  const selectView = (next: WorkspacePage) => {
    requestId.current += 1; setView(next); setSidebarOpen(false); setSelectionError(''); setSelecting(false)
    // pushState preserves in-flight fixture selection and supports browser back/forward.
    if (window.location.hash !== `#${next}`) window.history.pushState(null, '', `#${next}`)
    main.current?.focus()
  }
  const handleMatchClick = (match: Match) => {
    selectView('graph'); setSelecting(true)
    const id = ++requestId.current
    fetchMatchGraph(match.id).then(data => { if (id === requestId.current) setOverrideGraph(data) })
      .catch(() => { if (id === requestId.current) setSelectionError('This match graph is unavailable. Try another fixture.') })
      .finally(() => { if (id === requestId.current) setSelecting(false) })
  }
  const active = destinations.find(item => item.id === view)!
  const filtered = destinations.filter(item => `${item.label} ${item.detail}`.toLowerCase().includes(command.toLowerCase()))
  return <div className="app-shell refreshed-shell">
    <a className="skip-link" href="#workspace-main" onClick={e => { e.preventDefault(); main.current?.focus() }}>Skip to content</a>
    <aside ref={sidebar} className="app-sidebar" data-open={sidebarOpen} aria-label="Workspace navigation">
      <button className="sidebar-close" onClick={() => setSidebarOpen(false)}>Close navigation</button>
      <button className="brand" onClick={() => selectView('home')} aria-label="PL Knowledge Engine home"><span className="brand-mark">pl<span>↗</span></span><span>Knowledge<br /><strong>Engine</strong></span></button>
      <button className="quick-switch" onClick={openCommand}><span>⌕</span> Quick switch <kbd>⌘ K</kbd></button>
      <p className="nav-caption">WORKSPACE</p>
      <nav className="workspace-nav">{destinations.map(item => <button key={item.id} aria-current={view === item.id ? 'page' : undefined} onClick={() => selectView(item.id)}><span aria-hidden="true">{item.symbol}</span>{item.label}<i /></button>)}</nav>
      <div className="fixture-nav-heading"><p className="nav-caption">SEASON FIXTURES</p><span>25 / 26</span></div>
      {matchesLoading ? <p className="state-notice" role="status">Loading fixtures…</p> : matchesError ? <div className="state-notice" role="alert">{matchesError}<button onClick={loadMatches}>Retry fixtures</button></div> : <MatchList matches={matches} onSelect={handleMatchClick} />}
      <div className="sidebar-footer"><span className="local-dot" /><div>Local workspace<small>Historical records · $0 hosting</small></div></div>
    </aside>
    {sidebarOpen && <button className="nav-backdrop" aria-label="Close navigation" onClick={() => setSidebarOpen(false)} />}
    <main ref={main} id="workspace-main" tabIndex={-1} className="workspace-main">
      <header className="app-header"><div className="header-location"><button ref={menuButton} className="mobile-menu" aria-expanded={sidebarOpen} onClick={() => setSidebarOpen(true)}>Navigation</button><span className="header-breadcrumb">Workspace <span>/</span></span><strong>{active.label}</strong></div>
        <div className="header-actions"><span className="header-meta">PREMIER LEAGUE</span><button aria-label="Open quick switch" onClick={openCommand}>⌕<span> Jump to</span></button></div></header>
      {selecting && <p className="state-notice" role="status">Opening match connections…</p>}
      {selectionError && <p className="state-notice" role="alert">{selectionError}</p>}
      <div className="workspace-content" key={view}>
        {view === 'home' ? <WorkspaceHome matches={matches} loading={matchesLoading} error={matchesError} onNavigate={selectView} onMatch={handleMatchClick} onRetry={loadMatches} />
          : view === 'graph' ? <GraphView overrideGraph={overrideGraph} onClearOverride={() => setOverrideGraph(null)} />
          : <ChatView mode={view} />}
      </div>
    </main>
    <dialog ref={dialog} className="command-dialog" onClick={event => { if (event.target === event.currentTarget) dialog.current?.close() }}>
      <div className="command-heading"><label htmlFor="command-search">Jump to your next question</label><button aria-label="Close quick switch" onClick={() => dialog.current?.close()}>Esc</button></div>
      <input ref={commandInput} id="command-search" placeholder="Find a tool: players, records, predictions…" value={command} onChange={event => setCommand(event.target.value)} onKeyDown={event => { if (event.key === 'Enter' && filtered[0]) { dialog.current?.close(); selectView(filtered[0].id) } }} />
      <div className="command-results">{filtered.map(item => <button key={item.id} onClick={() => { dialog.current?.close(); selectView(item.id) }}><span aria-hidden="true">{item.symbol}</span><div><strong>{item.label}</strong><small>{item.detail}</small></div><span aria-hidden="true">↵</span></button>)}{!filtered.length && <p>No tools match. Try “players”, “records” or “predictions”.</p>}</div>
      <p className="command-hint">Search workspace tools · Tab to move · Enter to open</p>
    </dialog>
  </div>
}
