import { describe, expect, it } from 'vitest'
import { applyEvent, type Item } from './transcript'

describe('applyEvent', () => {
  it('appends tokens to the current reply and starts a new one after a tool', () => {
    let items: Item[] = [{ kind: 'user', text: 'hi' }]
    items = applyEvent(items, { type: 'token', text: 'Hel' })
    items = applyEvent(items, { type: 'token', text: 'lo' })
    items = applyEvent(items, { type: 'tool', tool: 'searchTrains', outcome: 'success', result: [] })
    items = applyEvent(items, { type: 'token', text: 'Done' })
    expect(items).toEqual([
      { kind: 'user', text: 'hi' },
      { kind: 'assistant', text: 'Hello' },
      { kind: 'tool', tool: 'searchTrains', outcome: 'success', result: [] },
      { kind: 'assistant', text: 'Done' },
    ])
  })

  it('keeps the confirmation payload without the event type', () => {
    const items = applyEvent([], {
      type: 'confirmation', action: 'CONFIRM_BOOKING', method: 'POST', path: '/api/booking/bookings', idempotencyKey: 'k', details: {},
    })
    expect(items[0]).toEqual({
      kind: 'confirmation',
      event: { action: 'CONFIRM_BOOKING', method: 'POST', path: '/api/booking/bookings', idempotencyKey: 'k', details: {} },
    })
  })
})
