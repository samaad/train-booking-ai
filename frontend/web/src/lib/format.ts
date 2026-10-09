import { config } from '../config'

const zone = config.railwayZone

const timeFmt = new Intl.DateTimeFormat('en-GB', { timeZone: zone, hour: '2-digit', minute: '2-digit', hour12: false })
const dateFmt = new Intl.DateTimeFormat('en-GB', { timeZone: zone, weekday: 'short', day: 'numeric', month: 'short', year: 'numeric' })
const dateTimeFmt = new Intl.DateTimeFormat('en-GB', {
  timeZone: zone, day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', hour12: false,
})

/** Departure/arrival in the railway's zone — the API sends UTC and a 07:55 train must not read "03:55". */
export const formatTime = (iso: string) => timeFmt.format(new Date(iso))
export const formatDateTime = (iso: string) => dateTimeFmt.format(new Date(iso))

/** A travel date (yyyy-MM-dd) is a calendar date, not an instant — format it without shifting zones. */
export function formatTravelDate(date: string): string {
  const [y, m, d] = date.split('-').map(Number)
  return new Intl.DateTimeFormat('en-GB', { timeZone: 'UTC', weekday: 'short', day: 'numeric', month: 'short', year: 'numeric' })
    .format(new Date(Date.UTC(y, m - 1, d)))
}

export const formatInstantDate = (iso: string) => dateFmt.format(new Date(iso))

export function formatDuration(minutes: number): string {
  const h = Math.floor(minutes / 60)
  const m = minutes % 60
  return h ? `${h}h ${m.toString().padStart(2, '0')}m` : `${m}m`
}

export function formatMoney(amount: number, currency: string): string {
  try {
    return new Intl.NumberFormat('en-AE', { style: 'currency', currency }).format(amount)
  } catch {
    return `${amount.toFixed(2)} ${currency}`
  }
}

/** Today in the railway's zone, yyyy-MM-dd — the inventory's booking window is computed there too. */
export function railwayToday(now = new Date()): string {
  return new Intl.DateTimeFormat('en-CA', { timeZone: zone, year: 'numeric', month: '2-digit', day: '2-digit' }).format(now)
}

export function addDays(date: string, days: number): string {
  const [y, m, d] = date.split('-').map(Number)
  return new Date(Date.UTC(y, m - 1, d + days)).toISOString().slice(0, 10)
}

export const titleCase = (s: string) => s.charAt(0) + s.slice(1).toLowerCase().replace(/_/g, ' ')

/** The railway-local calendar date of an instant (a 00:30 departure in Dubai is still "that" day there). */
export const railwayDate = (iso: string) => railwayToday(new Date(iso))
