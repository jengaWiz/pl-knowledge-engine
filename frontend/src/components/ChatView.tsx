import { useState, useRef, useEffect } from 'react'
import ReactMarkdown from 'react-markdown'
import { sendChat } from '../api'
import type { ChatMessage } from '../types'

const STARTERS = [
  "Who are Liverpool's top scorers this season?",
  'How has Aston Villa performed in their last 5 games?',
  'Compare Watkins and Díaz this season',
  'Which players have the most assists across both teams?',
]

export default function ChatView() {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const bottomRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  const send = async (text: string) => {
    if (!text.trim() || loading) return
    const userMsg: ChatMessage = { role: 'user', content: text }
    setMessages(prev => [...prev, userMsg])
    setInput('')
    setLoading(true)
    try {
      const { reply, sources } = await sendChat(text, [...messages, userMsg])
      setMessages(prev => [...prev, { role: 'assistant', content: reply, sources }])
    } catch {
      setMessages(prev => [...prev, { role: 'assistant', content: 'Sorry, something went wrong. Please try again.' }])
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', background: 'var(--bg-0)' }}>
      <div style={{ flex: 1, overflowY: 'auto', padding: '32px 0 12px' }}>
        {messages.length === 0
          ? <WelcomeScreen onSelect={send} />
          : (
            <div style={{ maxWidth: 760, margin: '0 auto', padding: '0 24px', display: 'flex', flexDirection: 'column', gap: 20 }}>
              {messages.map((m, i) => <MessageBubble key={i} msg={m} idx={i} />)}
              {loading && <TypingIndicator />}
              <div ref={bottomRef} />
            </div>
          )
        }
      </div>
      <InputBar value={input} onChange={setInput} onSend={() => send(input)} loading={loading} />
    </div>
  )
}

/* ── Welcome screen ───────────────────────────────────────── */
function WelcomeScreen({ onSelect }: { onSelect: (s: string) => void }) {
  return (
    <div className="fade-in" style={{ maxWidth: 620, margin: '0 auto', padding: '20px 24px 40px' }}>
      {/* Hero */}
      <div style={{ textAlign: 'center', marginBottom: 36 }}>
        {/* Icon */}
        <div style={{
          width: 72, height: 72, borderRadius: 20, margin: '0 auto 22px',
          background: 'linear-gradient(135deg, rgba(103,14,54,0.3) 0%, rgba(200,16,46,0.15) 100%)',
          border: '1px solid rgba(103,14,54,0.4)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          boxShadow: '0 6px 32px rgba(103,14,54,0.25)',
          position: 'relative',
        }}>
          <AnalystIcon />
          {/* Subtle glow halo */}
          <div style={{
            position: 'absolute', inset: -1, borderRadius: 21,
            background: 'linear-gradient(135deg, rgba(103,14,54,0.15), transparent)',
            pointerEvents: 'none',
          }} />
        </div>

        <div style={{ fontSize: 26, fontWeight: 800, color: 'var(--t-1)', letterSpacing: '-0.04em', marginBottom: 9 }}>
          PL AI Analyst
        </div>
        <div style={{ fontSize: 14, color: 'var(--t-2)', lineHeight: 1.65, maxWidth: 380, margin: '0 auto' }}>
          Ask anything about Aston Villa &amp; Liverpool's<br />2025–26 Premier League season
        </div>
      </div>

      {/* Starter cards */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--t-3)', letterSpacing: '0.1em', marginBottom: 12, textAlign: 'center' }}>
          SUGGESTED QUESTIONS
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
          {STARTERS.map(s => <StarterCard key={s} text={s} onClick={() => onSelect(s)} />)}
        </div>
      </div>
    </div>
  )
}

function StarterCard({ text, onClick }: { text: string; onClick: () => void }) {
  const [hovered, setHovered] = useState(false)
  return (
    <button
      onClick={onClick}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        background: hovered ? 'var(--bg-2)' : 'var(--bg-1)',
        border: `1px solid ${hovered ? 'rgba(103,14,54,0.45)' : 'var(--bg-3)'}`,
        borderLeft: `3px solid ${hovered ? 'var(--villa)' : 'var(--bg-4)'}`,
        borderRadius: 11, padding: '14px 16px',
        color: hovered ? 'var(--t-1)' : 'var(--t-2)',
        fontSize: 13, textAlign: 'left', cursor: 'pointer', lineHeight: 1.55,
        transition: 'all 0.18s ease',
        boxShadow: hovered ? '0 2px 12px rgba(103,14,54,0.12)' : 'none',
      }}
    >
      {text}
    </button>
  )
}

/* ── Message bubbles ──────────────────────────────────────── */
function MessageBubble({ msg, idx }: { msg: ChatMessage; idx: number }) {
  const [sourcesOpen, setSourcesOpen] = useState(false)
  const isUser = msg.role === 'user'

  return (
    <div
      className="fade-in-up"
      style={{
        display: 'flex', gap: 12,
        flexDirection: isUser ? 'row-reverse' : 'row',
        animationDelay: `${idx * 0.02}s`,
      }}
    >
      <Avatar assistant={!isUser} />
      <div style={{ maxWidth: '78%', display: 'flex', flexDirection: 'column', gap: 6 }}>
        <div style={{
          background: isUser
            ? 'linear-gradient(135deg, rgba(26,58,95,0.9) 0%, rgba(37,99,235,0.2) 100%)'
            : 'var(--bg-1)',
          border: `1px solid ${isUser ? 'rgba(37,99,235,0.3)' : 'var(--bg-3)'}`,
          borderRadius: isUser ? '14px 3px 14px 14px' : '3px 14px 14px 14px',
          padding: '13px 17px',
          fontSize: 14, lineHeight: 1.7, color: 'var(--t-1)',
          boxShadow: isUser ? '0 2px 12px rgba(37,99,235,0.12)' : 'none',
        }}>
          {isUser ? msg.content : (
            <div className="markdown">
              <ReactMarkdown>{msg.content}</ReactMarkdown>
            </div>
          )}
        </div>

        {!isUser && msg.sources && msg.sources.length > 0 && (
          <div>
            <button
              onClick={() => setSourcesOpen(o => !o)}
              style={{
                background: 'none', border: 'none', cursor: 'pointer', padding: '2px 0',
                display: 'flex', alignItems: 'center', gap: 5,
                color: 'var(--t-3)', fontSize: 12, transition: 'color 0.15s',
              }}
              onMouseEnter={e => (e.currentTarget.style.color = 'var(--t-2)')}
              onMouseLeave={e => (e.currentTarget.style.color = 'var(--t-3)')}
            >
              <ChevronIcon open={sourcesOpen} />
              {msg.sources.length} source{msg.sources.length > 1 ? 's' : ''}
            </button>
            {sourcesOpen && (
              <div className="fade-in" style={{ display: 'flex', flexDirection: 'column', gap: 5, marginTop: 6 }}>
                {msg.sources.map((s, i) => (
                  <div key={i} style={{
                    background: 'var(--bg-0)', border: '1px solid var(--bg-3)',
                    borderLeft: '2px solid rgba(103,14,54,0.6)',
                    borderRadius: 7, padding: '7px 12px', fontSize: 12, color: 'var(--t-2)',
                  }}>
                    <span style={{ color: '#c87898', fontWeight: 700, marginRight: 7 }}>[{s.type}]</span>
                    {s.summary}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function TypingIndicator() {
  return (
    <div className="fade-in" style={{ display: 'flex', gap: 12 }}>
      <Avatar assistant />
      <div style={{
        background: 'var(--bg-1)', border: '1px solid var(--bg-3)',
        borderRadius: '3px 14px 14px 14px',
        padding: '15px 18px', display: 'flex', gap: 6, alignItems: 'center',
      }}>
        {[0, 1, 2].map(i => (
          <div key={i} style={{
            width: 6, height: 6, borderRadius: '50%', background: 'var(--villa)',
            animation: `pulseDot 1.2s ease-in-out ${i * 0.18}s infinite`,
          }} />
        ))}
      </div>
    </div>
  )
}

/* ── Input bar ────────────────────────────────────────────── */
function InputBar({ value, onChange, onSend, loading }: {
  value: string
  onChange: (v: string) => void
  onSend: () => void
  loading: boolean
}) {
  const [focused, setFocused] = useState(false)
  const canSend = !loading && !!value.trim()

  return (
    <div style={{
      borderTop: '1px solid var(--bg-3)',
      padding: '14px 24px 16px',
      background: 'var(--bg-1)',
    }}>
      <div style={{ maxWidth: 760, margin: '0 auto' }}>
        <div style={{
          display: 'flex', gap: 10,
          background: 'var(--bg-0)',
          border: `1px solid ${focused ? 'rgba(103,14,54,0.55)' : 'var(--bg-3)'}`,
          borderRadius: 14, padding: '4px 4px 4px 18px',
          transition: 'border-color 0.2s, box-shadow 0.2s',
          boxShadow: focused ? '0 0 0 3px rgba(103,14,54,0.12)' : 'none',
        }}>
          <input
            value={value}
            onChange={e => onChange(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); onSend() } }}
            onFocus={() => setFocused(true)}
            onBlur={() => setFocused(false)}
            placeholder="Ask about players, matches, stats…"
            disabled={loading}
            style={{
              flex: 1, background: 'transparent', border: 'none',
              padding: '10px 0', color: 'var(--t-1)', fontSize: 14, lineHeight: 1.5,
              opacity: loading ? 0.5 : 1,
            }}
          />
          <button
            onClick={onSend}
            disabled={!canSend}
            style={{
              background: canSend ? 'var(--villa)' : 'var(--bg-2)',
              border: 'none', borderRadius: 11, padding: '10px 18px', margin: 3,
              color: canSend ? '#fff' : 'var(--t-3)',
              fontWeight: 700, fontSize: 13,
              cursor: canSend ? 'pointer' : 'not-allowed',
              display: 'flex', alignItems: 'center', gap: 6,
              transition: 'all 0.18s ease',
              letterSpacing: '-0.01em',
              boxShadow: canSend ? '0 2px 12px rgba(103,14,54,0.45)' : 'none',
            }}
          >
            <SendIcon />
            Send
          </button>
        </div>
        <div style={{ fontSize: 11, color: 'var(--t-3)', marginTop: 8, textAlign: 'center' }}>
          Press Enter to send · Powered by Claude AI
        </div>
      </div>
    </div>
  )
}

/* ── Avatar ───────────────────────────────────────────────── */
function Avatar({ assistant }: { assistant?: boolean }) {
  return (
    <div style={{
      width: 34, height: 34, borderRadius: '50%', flexShrink: 0, marginTop: 2,
      background: assistant
        ? 'linear-gradient(135deg, #670E36 0%, #9b1540 100%)'
        : 'linear-gradient(135deg, #1e3a6e 0%, #1d4ed8 100%)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      fontSize: 11, fontWeight: 800, color: 'rgba(255,255,255,0.9)', letterSpacing: '-0.02em',
      boxShadow: assistant ? '0 2px 10px rgba(103,14,54,0.45)' : '0 2px 10px rgba(29,78,216,0.35)',
    }}>
      {assistant ? 'AI' : 'U'}
    </div>
  )
}

/* ── Icons ────────────────────────────────────────────────── */
function AnalystIcon() {
  return (
    <svg width="32" height="32" viewBox="0 0 24 24" fill="none"
      stroke="rgba(212,128,154,0.9)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z" />
      <polyline points="3.27 6.96 12 12.01 20.73 6.96" />
      <line x1="12" y1="22.08" x2="12" y2="12" />
    </svg>
  )
}

function SendIcon() {
  return (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <line x1="22" y1="2" x2="11" y2="13" />
      <polygon points="22 2 15 22 11 13 2 9 22 2" />
    </svg>
  )
}

function ChevronIcon({ open }: { open: boolean }) {
  return (
    <svg width="11" height="11" viewBox="0 0 24 24" fill="none"
      stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"
      style={{ transform: open ? 'rotate(90deg)' : 'none', transition: 'transform 0.15s' }}>
      <polyline points="9 18 15 12 9 6" />
    </svg>
  )
}
