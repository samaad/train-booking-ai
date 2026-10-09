import { useMutation, useQuery } from '@tanstack/react-query'
import { useState, type FormEvent } from 'react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router'
import { api } from '../api/endpoints'
import { ApiError } from '../api/http'
import { useAuth } from '../auth/session'
import { HoldTimer } from '../components/HoldTimer'
import { TripSummary } from '../components/TripSummary'
import { Card, ErrorBox, Spinner, StatusBadge } from '../components/ui'
import { useCountdown } from '../hooks/useCountdown'
import { forgetIntent, intentKey } from '../lib/idempotency'

/** Step 2: who is travelling. Creates the booking — priced by booking-service, never by this page. */
export function CheckoutPage() {
  const { holdId = '' } = useParams()
  const [params] = useSearchParams()
  const from = params.get('from') ?? ''
  const to = params.get('to') ?? ''
  const navigate = useNavigate()
  const auth = useAuth()

  const hold = useQuery({ queryKey: ['hold', holdId], queryFn: () => api.getHold(holdId) })
  const profile = useQuery({ queryKey: ['profile'], queryFn: api.profile, retry: false })
  const travellers = useQuery({ queryKey: ['travellers'], queryFn: api.travellers, retry: false })
  const left = useCountdown(hold.data?.expiresAt)

  // Prefilled from the profile (or the token) until the user types their own.
  const [nameEdit, setName] = useState<string | null>(null)
  const [emailEdit, setEmail] = useState<string | null>(null)
  const name = nameEdit ?? profile.data?.displayName ?? auth.displayName ?? ''
  const email = emailEdit ?? profile.data?.email ?? auth.email ?? ''

  const book = useMutation({
    mutationFn: () => {
      const intent = `book:${holdId}`
      return api.createBooking({ holdId, fromStation: from, toStation: to, passengerName: name.trim(), contactEmail: email.trim() },
        intentKey(intent))
        .then((b) => { forgetIntent(intent); return b })
        .catch((e) => {
          if (!(e instanceof ApiError && e.outcomeUnknown)) forgetIntent(intent)
          throw e
        })
    },
    onSuccess: (b) => navigate(`/trips/${b.bookingId}/pay`, { replace: true }),
  })

  const release = useMutation({
    mutationFn: () => api.releaseHold(holdId),
    onSuccess: () => navigate(-1),
  })

  if (hold.isPending) return <Spinner label="Loading your hold…" />
  if (hold.error) return <ErrorBox error={hold.error} onRetry={() => hold.refetch()} />
  const h = hold.data
  const active = h.status === 'ACTIVE' && left > 0

  const submit = (e: FormEvent) => {
    e.preventDefault()
    book.mutate()
  }

  return (
    <div className="checkout">
      <Steps step={2} />
      <div className="checkout-grid">
        <Card>
          <h2>Passenger details</h2>
          {!active ? (
            <div className="alert alert-warn">
              This hold is {h.status === 'ACTIVE' ? 'expired' : h.status.toLowerCase()}.{' '}
              <Link to={`/trains/${h.trainNumber}/${h.travelDate}?${new URLSearchParams({ from, to, class: h.fareClass })}`}>
                Pick a seat again
              </Link>
            </div>
          ) : (
            <form className="form" onSubmit={submit}>
              {travellers.data && travellers.data.length > 0 && (
                <div className="chips" aria-label="Saved travellers">
                  {[auth.displayName, ...travellers.data.map((t) => t.fullName)].filter(Boolean).map((n) => (
                    <button key={n} type="button" className={`chip ${name === n ? 'active' : ''}`} onClick={() => setName(n!)}>
                      {n}
                    </button>
                  ))}
                </div>
              )}
              <label>
                <span>Full name (as on ID)</span>
                <input value={name} onChange={(e) => setName(e.target.value)} required maxLength={200} autoComplete="name" />
              </label>
              <label>
                <span>Email for your ticket</span>
                <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required autoComplete="email" />
              </label>
              {book.error && <ErrorBox error={book.error} />}
              <button className="btn btn-primary btn-block" disabled={book.isPending || !from || !to}>
                {book.isPending ? 'Creating booking…' : 'Continue to payment'}
              </button>
              {(!from || !to) && <p className="alert alert-warn">Missing stations — start again from a search.</p>}
            </form>
          )}
        </Card>
        <Card className="checkout-side">
          <div className="row-between">
            <h3>Your trip</h3>
            {active ? <HoldTimer expiresAt={h.expiresAt} /> : <StatusBadge status={left === 0 ? 'EXPIRED' : h.status} />}
          </div>
          <TripSummary {...h} fromStation={from} toStation={to} />
          <p className="muted small">You will see the exact fare before paying.</p>
          {active && (
            <button className="btn btn-link" disabled={release.isPending} onClick={() => release.mutate()}>
              Release this seat
            </button>
          )}
          {release.error && <ErrorBox error={release.error} />}
        </Card>
      </div>
    </div>
  )
}

export function Steps({ step }: { step: 1 | 2 | 3 | 4 }) {
  const labels = ['Seat', 'Passenger', 'Payment', 'Ticket']
  return (
    <ol className="steps">
      {labels.map((l, i) => (
        <li key={l} className={i + 1 < step ? 'done' : i + 1 === step ? 'current' : ''}>{l}</li>
      ))}
    </ol>
  )
}
