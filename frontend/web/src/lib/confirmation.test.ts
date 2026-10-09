import { describe, expect, it } from 'vitest'
import { isAllowedConfirmation } from './confirmation'

const HOLD = '01929b8e-7a1c-7c3e-9b5a-1f2e3d4c5b6a'
const BOOKING = '01929b8e-7a1c-7c3e-9b5a-aaaaaaaaaaaa'

describe('isAllowedConfirmation', () => {
  const book = { action: 'CONFIRM_BOOKING', method: 'POST', path: '/api/booking/bookings', idempotencyKey: 'abc123', details: { holdId: HOLD } }
  const cancel = { action: 'CANCEL_BOOKING', method: 'POST', path: `/api/booking/bookings/${BOOKING}/cancel`, idempotencyKey: 'k', details: { bookingId: BOOKING } }

  it('accepts the two known actions on their exact endpoints', () => {
    expect(isAllowedConfirmation(book)).toBe(true)
    expect(isAllowedConfirmation(cancel)).toBe(true)
  })

  it('rejects anything aimed elsewhere', () => {
    expect(isAllowedConfirmation({ ...book, path: '/api/payment/payments' })).toBe(false)
    expect(isAllowedConfirmation({ ...book, path: 'https://evil.example/api/booking/bookings' })).toBe(false)
    expect(isAllowedConfirmation({ ...book, method: 'DELETE' })).toBe(false)
    expect(isAllowedConfirmation({ ...book, action: 'REFUND' })).toBe(false)
    expect(isAllowedConfirmation({ ...book, idempotencyKey: 'bad key\n' })).toBe(false)
    expect(isAllowedConfirmation({ ...cancel, path: `/api/booking/bookings/${BOOKING}/cancel/../../x` })).toBe(false)
    // the card would show one booking but cancel another
    expect(isAllowedConfirmation({ ...cancel, details: { bookingId: HOLD } })).toBe(false)
  })
})
