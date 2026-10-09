import type { ClassAvailability } from '../api/types'

export type SeatState = 'free' | 'held' | 'taken'

export interface SeatCell {
  seatNo: string
  state: SeatState
}

/**
 * Inventory names seats `<prefix><n>` for n = 1..total (e.g. B1…B12) and reports only the free ones, so the
 * full layout is rebuilt from the total. The live map (streams) marks seats held or committed since the
 * snapshot; either source saying "not free" wins.
 */
export function seatGrid(cls: ClassAvailability, live: Record<string, 'HELD' | 'COMMITTED'> = {}): SeatCell[] {
  const free = new Set(cls.availableSeats)
  const prefix = seatPrefix(cls)
  return Array.from({ length: cls.total }, (_, i) => {
    const seatNo = `${prefix}${i + 1}`
    const liveState = live[seatNo]
    const state: SeatState = liveState === 'COMMITTED' ? 'taken'
      : liveState === 'HELD' ? 'held'
      : free.has(seatNo) ? 'free' : 'taken'
    return { seatNo, state }
  })
}

export function seatPrefix(cls: ClassAvailability): string {
  const sample = cls.availableSeats[0]
  return sample ? sample.replace(/\d+$/, '') : cls.fareClass.charAt(0)
}

/** Rows of four, 2+2 with an aisle: positions 1 and 4 are windows. */
export function isWindow(seatNo: string): boolean {
  const n = Number(seatNo.replace(/^\D+/, ''))
  const pos = (n - 1) % 4
  return pos === 0 || pos === 3
}
