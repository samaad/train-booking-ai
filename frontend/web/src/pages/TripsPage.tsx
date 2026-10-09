import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router'
import { api } from '../api/endpoints'
import type { Booking } from '../api/types'
import { Empty, ErrorBox, Spinner, StatusBadge } from '../components/ui'
import { useStations } from '../hooks/useStations'
import { formatMoney, formatTime, formatTravelDate, railwayToday } from '../lib/format'

export function TripsPage() {
  const bookings = useQuery({ queryKey: ['bookings'], queryFn: api.myBookings })
  if (bookings.isPending) return <Spinner label="Loading your trips…" />
  if (bookings.error) return <ErrorBox error={bookings.error} onRetry={() => bookings.refetch()} />

  const today = railwayToday()
  const upcoming = bookings.data.filter((b) => b.status !== 'CANCELLED' && b.travelDate >= today)
    .sort((a, b) => a.departure.localeCompare(b.departure))
  const past = bookings.data.filter((b) => !upcoming.includes(b))

  return (
    <div className="stack">
      <h1>My trips</h1>
      {bookings.data.length === 0 && (
        <Empty title="No trips yet">
          <Link className="btn btn-primary" to="/">Find a train</Link>
        </Empty>
      )}
      {upcoming.length > 0 && <TripList title="Upcoming" trips={upcoming} />}
      {past.length > 0 && <TripList title="Past and cancelled" trips={past} />}
    </div>
  )
}

function TripList({ title, trips }: { title: string; trips: Booking[] }) {
  const stations = useStations()
  return (
    <section className="stack-sm">
      <h2>{title}</h2>
      <ul className="trip-list">
        {trips.map((b) => (
          <li key={b.bookingId}>
            <Link to={`/trips/${b.bookingId}`} className={`card trip ${b.status === 'CANCELLED' ? 'faded' : ''}`}>
              <div className="trip-date">
                <strong>{formatTravelDate(b.travelDate)}</strong>
                <span className="muted small">{formatTime(b.departure)}</span>
              </div>
              <div className="trip-main">
                <div>{stations.name(b.fromStation)} → {stations.name(b.toStation)}</div>
                <div className="muted small">{b.trainNumber} · Seat {b.seatNo} · <span className="pnr">{b.pnr}</span></div>
              </div>
              <div className="trip-side">
                <StatusBadge status={b.status === 'CONFIRMED' && b.sagaState === 'TICKETED' ? 'TICKETED' : b.status} />
                <span className="muted small">{formatMoney(b.amount, b.currency)}</span>
              </div>
            </Link>
          </li>
        ))}
      </ul>
    </section>
  )
}
