import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link, useParams } from 'react-router'
import { api } from '../api/endpoints'
import type { Booking } from '../api/types'
import { TripSummary } from '../components/TripSummary'
import { Card, ErrorBox, Spinner, StatusBadge } from '../components/ui'
import { formatDateTime, formatMoney } from '../lib/format'

export function TripDetailPage() {
  const { bookingId = '' } = useParams()
  const booking = useQuery({
    queryKey: ['booking', bookingId],
    queryFn: () => api.booking(bookingId),
    // A confirmed booking is ticketed moments later by the saga; follow it until it settles.
    refetchInterval: (q) => (q.state.data?.status === 'CONFIRMED' && q.state.data.sagaState !== 'TICKETED' ? 2000 : false),
  })

  if (booking.isPending) return <Spinner label="Loading booking…" />
  if (booking.error) return <ErrorBox error={booking.error} onRetry={() => booking.refetch()} />
  const b = booking.data

  return (
    <div className="stack">
      <header className="page-head">
        <div>
          <Link to="/trips" className="muted small">← My trips</Link>
          <h1>Booking <span className="pnr">{b.pnr}</span></h1>
        </div>
        <StatusBadge status={b.status} />
      </header>
      <div className="checkout-grid">
        <div className="stack">
          {b.sagaState === 'TICKETED' && b.status === 'CONFIRMED' && <TicketCard pnr={b.pnr} />}
          {b.status === 'PENDING_PAYMENT' && (
            <Card>
              <h2>Awaiting payment</h2>
              <p className="muted">Pay before the seat hold runs out, or the booking is released.</p>
              <Link className="btn btn-primary" to={`/trips/${b.bookingId}/pay`}>Pay {formatMoney(b.amount, b.currency)}</Link>
            </Card>
          )}
          {b.status === 'CONFIRMED' && b.sagaState !== 'TICKETED' && (
            <Card><Spinner label="Issuing your ticket…" /></Card>
          )}
          {b.status === 'CANCELLED' && (
            <Card>
              <h2>Cancelled</h2>
              <p className="muted">{b.cancelReason ?? 'This booking was cancelled.'} Payments are refunded automatically.</p>
            </Card>
          )}
          {b.status !== 'CANCELLED' && <CancelCard booking={b} />}
        </div>
        <Card className="checkout-side">
          <h3>Trip</h3>
          <TripSummary {...b} />
          <dl className="summary">
            <div><dt>Passenger</dt><dd>{b.passengerName}</dd></div>
            <div><dt>Fare</dt><dd>{formatMoney(b.amount, b.currency)}</dd></div>
            <div><dt>Booked</dt><dd>{formatDateTime(b.createdAt)}</dd></div>
          </dl>
        </Card>
      </div>
    </div>
  )
}

function TicketCard({ pnr }: { pnr: string }) {
  const ticket = useQuery({ queryKey: ['ticket', pnr], queryFn: () => api.ticket(pnr) })
  const qr = useImage(['ticket-qr', pnr], () => api.ticketQr(pnr), ticket.data?.status === 'ISSUED')
  const pdf = useMutation({
    mutationFn: () => api.ticketPdf(pnr),
    onSuccess: (blob) => {
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `ticket-${pnr}.pdf`
      a.click()
      setTimeout(() => URL.revokeObjectURL(url), 10_000)
    },
  })

  if (ticket.isPending) return <Card><Spinner label="Loading ticket…" /></Card>
  if (ticket.error) return <ErrorBox error={ticket.error} onRetry={() => ticket.refetch()} />
  const t = ticket.data
  return (
    <Card className="ticket">
      <div className="ticket-body">
        <div>
          <div className="row-between">
            <h2>Your ticket</h2>
            <StatusBadge status={t.status} />
          </div>
          <p className="ticket-route">{t.fromStation} → {t.toStation}</p>
          <p className="muted">{t.passengerName} · {t.trainNumber} · Seat <strong>{t.seatNo}</strong></p>
          <p className="muted small">Issued {formatDateTime(t.issuedAt)}</p>
          <button className="btn" onClick={() => pdf.mutate()} disabled={pdf.isPending || t.status !== 'ISSUED'}>
            {pdf.isPending ? 'Preparing…' : 'Download PDF'}
          </button>
          {pdf.error && <ErrorBox error={pdf.error} />}
        </div>
        <div className="qr">
          {t.status !== 'ISSUED' ? <span className="muted small">Voided</span>
            : qr.url ? <img src={qr.url} alt={`Boarding QR code for ${pnr}`} width={168} height={168} />
            : qr.error ? <span className="muted small">QR unavailable</span>
            : <Spinner label="" />}
        </div>
      </div>
    </Card>
  )
}

/** Binary endpoints need the bearer token, so the QR is fetched and inlined as a data URL. */
function useImage(key: unknown[], load: () => Promise<Blob>, enabled: boolean) {
  const query = useQuery({ queryKey: key, queryFn: async () => blobToDataUrl(await load()), enabled, staleTime: Infinity, gcTime: 0 })
  return { url: query.data ?? null, error: query.error }
}

function blobToDataUrl(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => resolve(reader.result as string)
    reader.onerror = () => reject(reader.error)
    reader.readAsDataURL(blob)
  })
}

function CancelCard({ booking: b }: { booking: Booking }) {
  const queryClient = useQueryClient()
  const [open, setOpen] = useState(false)
  const [reason, setReason] = useState('')
  const cancel = useMutation({
    mutationFn: () => api.cancelBooking(b.bookingId, reason.trim()),
    onSuccess: (updated) => {
      queryClient.setQueryData(['booking', b.bookingId], updated)
      void queryClient.invalidateQueries({ queryKey: ['bookings'] })
      setOpen(false)
    },
  })
  if (!open) {
    return (
      <button className="btn btn-danger-outline" onClick={() => setOpen(true)}>Cancel booking</button>
    )
  }
  return (
    <Card className="danger-zone">
      <h3>Cancel booking {b.pnr}?</h3>
      <p className="muted small">Your seat is released and any payment refunded. This cannot be undone.</p>
      <label className="form">
        <span>Reason (optional)</span>
        <input value={reason} maxLength={500} onChange={(e) => setReason(e.target.value)} placeholder="e.g. Plans changed" />
      </label>
      {cancel.error && <ErrorBox error={cancel.error} />}
      <div className="row">
        <button className="btn btn-danger" disabled={cancel.isPending} onClick={() => cancel.mutate()}>
          {cancel.isPending ? 'Cancelling…' : 'Yes, cancel it'}
        </button>
        <button className="btn" onClick={() => setOpen(false)} disabled={cancel.isPending}>Keep booking</button>
      </div>
    </Card>
  )
}
