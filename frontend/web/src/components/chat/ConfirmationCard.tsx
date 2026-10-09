import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link } from 'react-router'
import { api } from '../../api/endpoints'
import { request } from '../../api/http'
import type { ConfirmationEvent } from '../../api/sse'
import type { Booking } from '../../api/types'
import { useAuth } from '../../auth/session'
import { isAllowedConfirmation } from '../../lib/confirmation'
import { HoldTimer } from '../HoldTimer'
import { TripSummary } from '../TripSummary'
import { ErrorBox } from '../ui'

/**
 * The human half of "the LLM proposes; the user disposes". Nothing has happened when this renders.
 * Only a click here sends the request, with the server-derived idempotency key from the event — so a
 * double click, or the model proposing the same thing twice, still yields one booking.
 */
export function ConfirmationCard({ event }: { event: ConfirmationEvent }) {
  const allowed = isAllowedConfirmation(event)
  const [dismissed, setDismissed] = useState(false)
  if (!allowed) {
    return <div className="alert alert-warn">The assistant proposed an action this app does not recognise, so it was ignored.</div>
  }
  if (dismissed) return <div className="muted small confirm-dismissed">You declined this action. Nothing was changed.</div>
  return event.action === 'CONFIRM_BOOKING'
    ? <ConfirmBooking event={event} onDismiss={() => setDismissed(true)} />
    : <CancelBooking event={event} onDismiss={() => setDismissed(true)} />
}

function ConfirmBooking({ event, onDismiss }: { event: ConfirmationEvent; onDismiss: () => void }) {
  const d = event.details as Record<string, string>
  const auth = useAuth()
  const profile = useQuery({ queryKey: ['profile'], queryFn: api.profile, retry: false })
  // Prefilled from the profile (or the token) until the user types their own.
  const [nameEdit, setName] = useState<string | null>(null)
  const [emailEdit, setEmail] = useState<string | null>(null)
  const name = nameEdit ?? profile.data?.displayName ?? auth.displayName ?? ''
  const email = emailEdit ?? profile.data?.email ?? auth.email ?? ''

  const confirm = useMutation({
    mutationFn: () => request<Booking>(event.path, {
      method: 'POST',
      idempotencyKey: event.idempotencyKey,
      body: { holdId: d.holdId, fromStation: d.fromStation, toStation: d.toStation, passengerName: name.trim(), contactEmail: email.trim() },
    }),
  })

  if (confirm.data) {
    return (
      <div className="confirm-card done">
        <strong>Booking {confirm.data.pnr} created.</strong> Seat {confirm.data.seatNo} is yours once paid.
        <Link className="btn btn-primary btn-small" to={`/trips/${confirm.data.bookingId}/pay`}>Pay now</Link>
      </div>
    )
  }
  return (
    <div className="confirm-card">
      <div className="row-between">
        <strong>Confirm this booking?</strong>
        {d.holdExpiresAt && <HoldTimer expiresAt={d.holdExpiresAt} />}
      </div>
      <TripSummary trainNumber={d.trainNumber} travelDate={d.travelDate} fareClass={d.fareClass} seatNo={d.seatNo}
        fromStation={d.fromStation} toStation={d.toStation} />
      <div className="form two-col">
        <label><span>Passenger name</span>
          <input value={name} onChange={(e) => setName(e.target.value)} required maxLength={200} />
        </label>
        <label><span>Email</span>
          <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required />
        </label>
      </div>
      <p className="muted small">The fare is shown before you pay. Nothing is charged until then.</p>
      {confirm.error && <ErrorBox error={confirm.error} />}
      <div className="row">
        <button className="btn btn-primary" disabled={confirm.isPending || !name.trim() || !email.includes('@')}
          onClick={() => confirm.mutate()}>
          {confirm.isPending ? 'Booking…' : 'Confirm booking'}
        </button>
        <button className="btn" onClick={onDismiss} disabled={confirm.isPending}>Not now</button>
      </div>
    </div>
  )
}

function CancelBooking({ event, onDismiss }: { event: ConfirmationEvent; onDismiss: () => void }) {
  const d = event.details as Record<string, string | null>
  const queryClient = useQueryClient()
  const cancel = useMutation({
    mutationFn: () => request<Booking>(event.path, {
      method: 'POST', idempotencyKey: event.idempotencyKey, body: { reason: d.reason ?? null },
    }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['bookings'] }),
  })
  if (cancel.data) {
    return <div className="confirm-card done"><strong>Booking {cancel.data.pnr} cancelled.</strong> Any payment will be refunded.</div>
  }
  return (
    <div className="confirm-card danger">
      <strong>Cancel booking <span className="pnr">{d.pnr}</span>?</strong>
      <p className="small">{d.trainNumber} on {d.travelDate}{d.reason ? ` — “${d.reason}”` : ''}. This cannot be undone.</p>
      {cancel.error && <ErrorBox error={cancel.error} />}
      <div className="row">
        <button className="btn btn-danger" disabled={cancel.isPending} onClick={() => cancel.mutate()}>
          {cancel.isPending ? 'Cancelling…' : 'Yes, cancel it'}
        </button>
        <button className="btn" onClick={onDismiss} disabled={cancel.isPending}>Keep it</button>
      </div>
    </div>
  )
}
