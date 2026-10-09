import { config } from '../config'
import { authHeader, toApiError } from './http'

/** One SSE frame: `event:` name plus `data:` lines joined with newlines. */
export interface SseFrame {
  event: string
  data: string
}

/**
 * Incremental text/event-stream parser. EventSource cannot POST or send an Authorization header, so
 * /chat is read with fetch and parsed here. Handles CRLF, multi-line data and frames split across chunks.
 */
export class SseParser {
  private buffer = ''

  push(chunk: string): SseFrame[] {
    this.buffer += chunk.replace(/\r\n?/g, '\n')
    const frames: SseFrame[] = []
    let end: number
    while ((end = this.buffer.indexOf('\n\n')) >= 0) {
      const block = this.buffer.slice(0, end)
      this.buffer = this.buffer.slice(end + 2)
      const frame = parseBlock(block)
      if (frame) frames.push(frame)
    }
    return frames
  }
}

function parseBlock(block: string): SseFrame | null {
  let event = 'message'
  const data: string[] = []
  for (const line of block.split('\n')) {
    if (line === '' || line.startsWith(':')) continue
    const colon = line.indexOf(':')
    const field = colon < 0 ? line : line.slice(0, colon)
    let value = colon < 0 ? '' : line.slice(colon + 1)
    if (value.startsWith(' ')) value = value.slice(1)
    if (field === 'event') event = value
    else if (field === 'data') data.push(value)
  }
  return data.length ? { event, data: data.join('\n') } : null
}

// ---- the assistant's events (ai-assistant-service ChatEvent) -----------------------------------------

export interface ConfirmationEvent {
  action: 'CONFIRM_BOOKING' | 'CANCEL_BOOKING' | string
  method: string
  path: string
  idempotencyKey: string
  details: Record<string, unknown>
}

export type ChatEvent =
  | { type: 'conversation'; conversationId: string }
  | { type: 'token'; text: string }
  | { type: 'tool'; tool: string; outcome: string; result: unknown }
  | ({ type: 'confirmation' } & ConfirmationEvent)
  | { type: 'warning'; code: string; message: string }
  | { type: 'fallback'; tier: number; message: string; results: unknown }
  | { type: 'error'; code: string; message: string }
  | { type: 'done'; conversationId: string }

const KNOWN = new Set(['conversation', 'token', 'tool', 'confirmation', 'warning', 'fallback', 'error', 'done'])

export function toChatEvent(frame: SseFrame): ChatEvent | null {
  if (!KNOWN.has(frame.event)) return null // forward compatible: ignore events we don't know
  try {
    return { type: frame.event, ...JSON.parse(frame.data) } as ChatEvent
  } catch {
    return null
  }
}

/** Streams one chat turn. Resolves when the stream ends; abort with the signal. */
export async function streamChat(
  message: string,
  conversationId: string | null,
  onEvent: (e: ChatEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  const res = await fetch(`${config.apiBase}/api/assistant/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'text/event-stream', ...(await authHeader(true)) },
    body: JSON.stringify({ conversationId, message }),
    signal,
  })
  if (!res.ok || !res.body) throw await toApiError(res)
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader()
  const parser = new SseParser()
  for (;;) {
    const { value, done } = await reader.read()
    if (done) break
    for (const frame of parser.push(value)) {
      const event = toChatEvent(frame)
      if (event) onEvent(event)
    }
  }
}
