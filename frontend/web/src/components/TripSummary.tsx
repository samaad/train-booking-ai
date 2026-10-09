import { formatTime, formatTravelDate, titleCase } from '../lib/format'
import { useStations } from '../hooks/useStations'

interface Props {
  trainNumber: string
  travelDate: string
  fareClass: string
  seatNo: string
  fromStation?: string | null
  toStation?: string | null
  departure?: string | null
  arrival?: string | null
}

export function TripSummary(p: Props) {
  const stations = useStations()
  return (
    <dl className="summary">
      {p.fromStation && p.toStation && (
        <div className="summary-route">
          <dt>Route</dt>
          <dd>
            {stations.name(p.fromStation)} → {stations.name(p.toStation)}
          </dd>
        </div>
      )}
      <div><dt>Train</dt><dd>{p.trainNumber}</dd></div>
      <div><dt>Date</dt><dd>{formatTravelDate(p.travelDate)}</dd></div>
      {p.departure && (
        <div><dt>Departs</dt><dd>{formatTime(p.departure)}{p.arrival && ` → ${formatTime(p.arrival)}`}</dd></div>
      )}
      <div><dt>Class</dt><dd>{titleCase(p.fareClass)}</dd></div>
      <div><dt>Seat</dt><dd className="seat-tag">{p.seatNo}</dd></div>
    </dl>
  )
}
