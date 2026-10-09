import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useRef, useState, type FormEvent, type KeyboardEvent } from 'react'
import { Link, useNavigate, useParams } from 'react-router'
import { api } from '../api/endpoints'
import { streamChat } from '../api/sse'
import { ConfirmationCard } from '../components/chat/ConfirmationCard'
import { ToolCard } from '../components/chat/ToolCard'
import { TrainOptions } from '../components/chat/TrainOptions'
import { isTrainOptions } from '../lib/trainOptions'
import { ErrorBox, Spinner } from '../components/ui'
import { formatInstantDate } from '../lib/format'
import { applyEvent, type Item } from '../lib/transcript'

const SUGGESTIONS = [
  'Which trains go from Dubai to Abu Dhabi tomorrow?',
  'Book me a business seat on the next train to Fujairah',
  'Can I bring a bicycle on board?',
  'What is the status of my latest booking?',
]

export function AssistantPage() {
  const { conversationId } = useParams()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const conversations = useQuery({ queryKey: ['conversations'], queryFn: api.conversations })
  const [items, setItems] = useState<Item[]>([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [loadError, setLoadError] = useState<unknown>(null)
  const abort = useRef<AbortController | null>(null)
  /** The conversation this page's stream created — navigating to it must not reload its history. */
  const streamed = useRef<string | null>(null)
  const bottom = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (conversationId && conversationId === streamed.current) return
    abort.current?.abort()
    setItems([])
    setLoadError(null)
    if (!conversationId) return
    let alive = true
    api.messages(conversationId)
      .then((msgs) => {
        if (!alive) return
        setItems(msgs.filter((m) => m.content).map((m) => (m.role === 'USER'
          ? { kind: 'user', text: m.content } : { kind: 'assistant', text: m.content })))
      })
      .catch((e) => alive && setLoadError(e))
    return () => { alive = false }
  }, [conversationId])

  useEffect(() => { bottom.current?.scrollIntoView({ behavior: 'smooth', block: 'end' }) }, [items])
  useEffect(() => () => abort.current?.abort(), [])

  const send = async (text: string) => {
    const message = text.trim()
    if (!message || busy) return
    setInput('')
    setBusy(true)
    setItems((xs) => [...xs, { kind: 'user', text: message }])
    const controller = new AbortController()
    abort.current = controller
    try {
      await streamChat(message, conversationId ?? null, (e) => {
        if (e.type === 'conversation' && e.conversationId !== conversationId) {
          streamed.current = e.conversationId
          navigate(`/assistant/${e.conversationId}`, { replace: !conversationId })
        }
        setItems((xs) => applyEvent(xs, e))
      }, controller.signal)
    } catch (e) {
      if ((e as Error).name !== 'AbortError') {
        setItems((xs) => [...xs, { kind: 'notice', tone: 'error', text: (e as Error).message }])
      }
    } finally {
      setBusy(false)
      void queryClient.invalidateQueries({ queryKey: ['conversations'] })
    }
  }

  const submit = (e: FormEvent) => {
    e.preventDefault()
    void send(input)
  }
  const onKey = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault()
      void send(input)
    }
  }

  const last = items[items.length - 1]
  const thinking = busy && !(last?.kind === 'assistant' && last.text.trim())

  return (
    <div className="assistant">
      <aside className="conversations">
        <button className="btn btn-primary btn-block" onClick={() => { streamed.current = null; navigate('/assistant') }} disabled={busy}>
          New chat
        </button>
        {conversations.isPending && <Spinner />}
        <ul>
          {conversations.data?.map((c) => (
            <li key={c.conversationId}>
              <Link to={`/assistant/${c.conversationId}`} className={c.conversationId === conversationId ? 'active' : ''}
                onClick={() => { streamed.current = null }}>
                <span className="conv-title">{c.title || 'Untitled chat'}</span>
                <span className="muted small">{formatInstantDate(c.updatedAt)}</span>
              </Link>
            </li>
          ))}
        </ul>
      </aside>

      <section className="chat">
        <div className="transcript" aria-live="polite">
          {loadError ? <ErrorBox error={loadError} /> : null}
          {items.length === 0 && !loadError && (
            <div className="chat-empty">
              <h2>How can I help with your trip?</h2>
              <p className="muted">I search real schedules and hold real seats. You always confirm before anything is booked.</p>
              <div className="suggestions">
                {SUGGESTIONS.map((s) => (
                  <button key={s} className="chip" onClick={() => void send(s)} disabled={busy}>{s}</button>
                ))}
              </div>
            </div>
          )}
          {items.map((item, i) => <Message key={i} item={item} />)}
          {thinking && <div className="bubble assistant typing" aria-label="Assistant is thinking"><span /><span /><span /></div>}
          <div ref={bottom} />
        </div>

        <form className="composer" onSubmit={submit}>
          <textarea value={input} onChange={(e) => setInput(e.target.value)} onKeyDown={onKey} rows={1} maxLength={2000}
            placeholder="Ask about trains, seats or your bookings…" aria-label="Message" disabled={busy} />
          {busy ? (
            <button type="button" className="btn" onClick={() => abort.current?.abort()}>Stop</button>
          ) : (
            <button className="btn btn-primary" disabled={!input.trim()}>Send</button>
          )}
        </form>
        <p className="muted small disclaimer">Answers come from a local model and can be wrong. Prices and seats on cards come straight from the booking system.</p>
      </section>
    </div>
  )
}

function Message({ item }: { item: Item }) {
  switch (item.kind) {
    case 'user':
      return <div className="bubble user">{item.text}</div>
    case 'assistant':
      return item.text.trim() ? <div className="bubble assistant">{item.text.trim()}</div> : null
    case 'tool':
      return <ToolCard tool={item.tool} outcome={item.outcome} result={item.result} />
    case 'confirmation':
      return <ConfirmationCard event={item.event} />
    case 'notice':
      return <div className={`alert alert-${item.tone === 'warn' ? 'warn' : 'error'}`}>{item.text}</div>
    case 'fallback':
      return (
        <div className="tool-card">
          <p className="small">{item.text}</p>
          {isTrainOptions(item.results) ? <TrainOptions options={item.results} />
            : <Link className="btn btn-small btn-primary" to="/">Open the booking form</Link>}
        </div>
      )
  }
}
