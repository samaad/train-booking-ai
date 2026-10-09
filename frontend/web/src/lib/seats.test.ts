import { describe, expect, it } from 'vitest'
import { isWindow, seatGrid } from './seats'

describe('seatGrid', () => {
  const cls = { fareClass: 'BUSINESS', total: 4, available: 2, availableSeats: ['B1', 'B3'] }

  it('rebuilds the full layout from the total and marks free seats', () => {
    expect(seatGrid(cls).map((s) => `${s.seatNo}:${s.state}`)).toEqual(['B1:free', 'B2:taken', 'B3:free', 'B4:taken'])
  })

  it('lets the live map override the snapshot', () => {
    expect(seatGrid(cls, { B1: 'HELD', B3: 'COMMITTED' }).map((s) => s.state)).toEqual(['held', 'taken', 'taken', 'taken'])
  })

  it('falls back to the class initial when nothing is free', () => {
    expect(seatGrid({ fareClass: 'ECONOMY', total: 2, available: 0, availableSeats: [] }).map((s) => s.seatNo)).toEqual(['E1', 'E2'])
  })

  it('knows window seats in a 2+2 row', () => {
    expect(['E1', 'E2', 'E3', 'E4', 'E5'].map(isWindow)).toEqual([true, false, false, true, true])
  })
})
