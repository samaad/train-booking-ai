import { describe, expect, it } from 'vitest'
import { addDays, formatTime, railwayDate, railwayToday } from './format'

describe('railway-local formatting', () => {
  it('shows UTC instants in Dubai time', () => {
    expect(formatTime('2026-10-17T03:55:00Z')).toBe('07:55')
  })

  it('computes the railway date, not the browser date', () => {
    expect(railwayToday(new Date('2026-10-16T21:30:00Z'))).toBe('2026-10-17')
    expect(railwayDate('2026-10-17T00:30:00+04:00')).toBe('2026-10-17')
  })

  it('adds days across month ends', () => {
    expect(addDays('2026-10-31', 1)).toBe('2026-11-01')
  })
})
