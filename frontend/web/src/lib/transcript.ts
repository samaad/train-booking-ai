import type { ChatEvent, ConfirmationEvent } from '../api/sse'

export type Item =
  | { kind: 'user'; text: string }
  | { kind: 'assistant'; text: string }
  | { kind: 'tool'; tool: string; outcome: string; result: unknown }
  | { kind: 'confirmation'; event: ConfirmationEvent }
  | { kind: 'notice'; tone: 'warn' | 'error'; text: string }
  | { kind: 'fallback'; tier: number; text: string; results: unknown }

/** Folds one streamed event into the transcript. Pure, so it is tested without a server. */
export function applyEvent(items: Item[], e: ChatEvent): Item[] {
  switch (e.type) {
    case 'token': {
      const last = items[items.length - 1]
      if (last?.kind === 'assistant') return [...items.slice(0, -1), { ...last, text: last.text + e.text }]
      return [...items, { kind: 'assistant', text: e.text }]
    }
    case 'tool':
      return [...items, { kind: 'tool', tool: e.tool, outcome: e.outcome, result: e.result }]
    case 'confirmation': {
      const { type: _type, ...event } = e
      return [...items, { kind: 'confirmation', event }]
    }
    case 'warning':
      return [...items, { kind: 'notice', tone: 'warn', text: e.message }]
    case 'error':
      return [...items, { kind: 'notice', tone: 'error', text: e.message }]
    case 'fallback':
      return [...items, { kind: 'fallback', tier: e.tier, text: e.message, results: e.results }]
    default:
      return items
  }
}
