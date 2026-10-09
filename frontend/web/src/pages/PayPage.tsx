import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link, useParams } from 'react-router'
import { api } from '../api/endpoints'
import { ApiError } from '../api/http'
import type { Booking, Payment } from '../api/types'
import { HoldTimer } from '../components/HoldTimer'
import { TripSummary } from '../components/TripSummary'
import { Card, ErrorBox, Spinner, StatusBadge } from '../components/ui'
import { formatMoney } from '../lib/format'
import { forgetIntent, intentKey } from '../lib/idempotency'
import { Steps } from './CheckoutPage'

/**
 * payment-service takes a PSP token, never card data. Locally the PSP is a stub whose behaviour is
 * chosen by the token's prefix, so the "card" here is a choice of test outcome.
 */
const TEST_METHODS = [
  { token: 'tok_visa_4242', label: 'Test card — approves', hint: 'Payment succeeds' },
  { token: 'tok_decline_card', label: 'Test card — declines', hint: 'Card declined; the seat is released' },
  { token: 'tok_unavailable_psp', label: 'Test card — PSP outage', hint: 'Outcome unknown; retry replays safely' },
]

/**
 * payment-service learns a booking's price from booking.created, which arrives through the outbox a moment
 * after the booking is made. Until then it answers PAYMENT_NOT_READY — nothing was attempted, so waiting and
 * replaying the same key is safe. Any other failure is surfaced to the user.
 */
async function payWhenReady(pay: () => Promise<Payment>, attempts = 15): Promise<Payment> {
  for (let i = 1; ; i++) {
    try {
      return await pay()
    } catch (e) {
      if (!(e instanceof ApiError && e.code === 'PAYMENT_NOT_READY') || i >= attempts) throw e
      await new Promise((r) => setTimeout(r, Math.min(2000, 300 * i)))
    }
  }
}

const settled = (b: Booking) => b.status === 'CANCELLED' || b.sagaState === 'TICKETED'

export function PayPage() {
  const { bookingId = '' } = useParams()
  const queryClient = useQueryClient()
  const [method, setMethod] = useState(TEST_METHODS[0].token)
  const [paid, setPaid] = useState(false)

  const booking = useQuery({
    queryKey: ['booking', bookingId],
    queryFn: () => api.booking(bookingId),
    // After paying, follow the saga (payment → seats committed → ticketed) until it settles.
    refetchInterval: (q) => {
      const b = q.state.data
      return b && !settled(b) && (paid || b.sagaState !== 'PAYMENT_PENDING') ? 1500 : false
    },
  })
  const hold = useQuery({
    queryKey: ['hold', booking.data?.holdId],
    queryFn: () => api.getHold(booking.data!.holdId),
    enabled: !!booking.data && booking.data.status === 'PENDING_PAYMENT',
  })

  const intent = `pay:${bookingId}`
  const pay = useMutation({
    mutationFn: (b: Booking) => payWhenReady(() =>
      api.pay({ bookingId: b.bookingId, amount: b.amount, currency: b.currency, paymentToken: method }, intentKey(intent))),
    // The key is kept after success: a reload that shows this form again replays the same payment.
    onSuccess: () => {
      setPaid(true)
      void queryClient.invalidateQueries({ queryKey: ['booking', bookingId] })
    },
    onError: (e) => {
      // 503 = the PSP's answer is unknown: keep the key so "Try again" re-drives the same charge.
      if (!(e instanceof ApiError && e.outcomeUnknown)) forgetIntent(intent)
    },
  })

  if (booking.isPending) return <Spinner label="Loading booking…" />
  if (booking.error) return <ErrorBox error={booking.error} onRetry={() => booking.refetch()} />
  const b = booking.data
  const unknown = pay.error instanceof ApiError && pay.error.outcomeUnknown

  return (
    <div className="checkout">
      <Steps step={b.sagaState === 'TICKETED' ? 4 : 3} />
      <div className="checkout-grid">
        <Card>
          {b.status === 'PENDING_PAYMENT' && !pay.data && (
            <>
              <h2>Payment</h2>
              <p className="amount">{formatMoney(b.amount, b.currency)}</p>
              <p className="muted small">Fare quoted by the booking service for {b.fareClass.toLowerCase()} {b.fromStation} → {b.toStation}.</p>
              <fieldset className="methods">
                <legend>Payment method <span className="badge badge-neutral">Test mode</span></legend>
                {TEST_METHODS.map((m) => (
                  <label key={m.token} className={`method ${method === m.token ? 'active' : ''}`}>
                    <input type="radio" name="method" value={m.token} checked={method === m.token}
                      onChange={() => setMethod(m.token)} disabled={pay.isPending || unknown} />
                    <span>
                      <strong>{m.label}</strong>
                      <span className="muted small"> — {m.hint}</span>
                    </span>
                  </label>
                ))}
              </fieldset>
              {pay.error && (
                <ErrorBox error={unknown
                  ? new Error('We could not confirm the payment outcome. Retrying is safe — you will not be charged twice.')
                  : pay.error} />
              )}
              <button className="btn btn-primary btn-block" disabled={pay.isPending} onClick={() => pay.mutate(b)}>
                {pay.isPending ? 'Processing…' : unknown ? 'Try again' : `Pay ${formatMoney(b.amount, b.currency)}`}
              </button>
            </>
          )}

          {pay.data?.status === 'FAILED' && (
            <div className="result">
              <h2>Payment declined</h2>
              <p className="muted">{pay.data.failureReason ?? 'Your card was declined.'} Your seat is being released.</p>
              <Link className="btn btn-primary" to="/">Search again</Link>
            </div>
          )}

          {(pay.data?.status === 'AUTHORIZED' || b.status !== 'PENDING_PAYMENT') && pay.data?.status !== 'FAILED' && (
            <SagaProgress booking={b} />
          )}
        </Card>

        <Card className="checkout-side">
          <div className="row-between">
            <h3>Booking <span className="pnr">{b.pnr}</span></h3>
            {hold.data?.status === 'ACTIVE' && b.status === 'PENDING_PAYMENT' ? <HoldTimer expiresAt={hold.data.expiresAt} />
              : <StatusBadge status={b.status} />}
          </div>
          <TripSummary {...b} />
          <p className="muted small">Passenger: {b.passengerName}</p>
        </Card>
      </div>
    </div>
  )
}

function SagaProgress({ booking: b }: { booking: Booking }) {
  if (b.status === 'CANCELLED') {
    return (
      <div className="result">
        <h2>Booking cancelled</h2>
        <p className="muted">{b.cancelReason ?? 'This booking was cancelled.'} Any payment is refunded automatically.</p>
        <Link className="btn btn-primary" to="/">Search again</Link>
      </div>
    )
  }
  const stages = [
    { label: 'Payment authorised', done: b.sagaState !== 'PAYMENT_PENDING' || b.status === 'CONFIRMED' },
    { label: 'Seat confirmed', done: b.sagaState === 'SEATS_COMMITTED' || b.sagaState === 'TICKETED' },
    { label: 'Ticket issued', done: b.sagaState === 'TICKETED' },
  ]
  const finished = b.sagaState === 'TICKETED'
  return (
    <div className="result">
      <h2>{finished ? 'You’re booked!' : 'Confirming your booking…'}</h2>
      <ul className="progress">
        {stages.map((s, i) => {
          const current = !s.done && (i === 0 || stages[i - 1].done)
          return (
            <li key={s.label} className={s.done ? 'done' : current ? 'current' : ''}>{s.label}</li>
          )
        })}
      </ul>
      {finished ? (
        <Link className="btn btn-primary" to={`/trips/${b.bookingId}`}>View ticket</Link>
      ) : (
        <p className="muted small">This usually takes a few seconds. You can leave this page — the booking continues.</p>
      )}
    </div>
  )
}
