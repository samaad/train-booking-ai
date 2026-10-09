import type { ConfirmationEvent } from '../api/sse'

const UUID = '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'

/**
 * The assistant only *proposes* irreversible actions; the browser performs them on an explicit click.
 * Before doing so, check the proposal is one of the two known actions aimed at the exact endpoint it
 * should be — a confirmation card must never become a way to make the user's browser call anything else.
 */
export function isAllowedConfirmation(c: ConfirmationEvent): boolean {
  if (c.method !== 'POST' || !/^[A-Za-z0-9._:-]{1,200}$/.test(c.idempotencyKey ?? '')) return false
  switch (c.action) {
    case 'CONFIRM_BOOKING':
      return c.path === '/api/booking/bookings' && typeof c.details?.holdId === 'string'
    case 'CANCEL_BOOKING':
      return new RegExp(`^/api/booking/bookings/${UUID}/cancel$`).test(c.path)
        && c.path.includes(String(c.details?.bookingId))
    default:
      return false
  }
}
