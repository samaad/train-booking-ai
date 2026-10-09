import { Link } from 'react-router'
import { formatDuration, formatTime, railwayDate, titleCase } from '../../lib/format'
import type { TrainOptionLike } from '../../lib/trainOptions'

/** Trains rendered from the tool's own result — the facts, not the model's retelling of them. */
export function TrainOptions({ options }: { options: TrainOptionLike[] }) {
  return (
    <ul className="mini-trains">
      {options.map((o) => {
        const date = railwayDate(o.departure)
        return (
          <li key={o.trainNumber + o.departure}>
            <span className="train-no">{o.trainNumber}</span>
            <span>{formatTime(o.departure)} → {formatTime(o.arrival)}</span>
            <span className="muted small">{formatDuration(o.durationMinutes)} · {o.fareClasses.map(titleCase).join(' / ')}</span>
            <Link className="btn btn-small" to={`/trains/${o.trainNumber}/${date}?${new URLSearchParams({ from: o.from, to: o.to })}`}>
              Seats
            </Link>
          </li>
        )
      })}
    </ul>
  )
}
