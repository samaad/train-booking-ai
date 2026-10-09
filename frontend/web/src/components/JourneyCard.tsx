import type { ReactNode } from 'react'
import type { JourneyOption } from '../api/types'
import { formatDuration, formatTime } from '../lib/format'

export function JourneyTimes({ journey, stationName }: { journey: JourneyOption; stationName: (c: string) => string }) {
  return (
    <div className="journey-times">
      <div>
        <div className="time">{formatTime(journey.departure)}</div>
        <div className="station">{stationName(journey.from)}</div>
      </div>
      <div className="journey-line" aria-hidden>
        <span>{formatDuration(journey.durationMinutes)}</span>
      </div>
      <div className="right">
        <div className="time">{formatTime(journey.arrival)}</div>
        <div className="station">{stationName(journey.to)}</div>
      </div>
    </div>
  )
}

export function JourneyCard({
  journey, stationName, children,
}: { journey: JourneyOption; stationName: (c: string) => string; children?: ReactNode }) {
  return (
    <article className="card journey">
      <header className="journey-head">
        <span className="train-no">{journey.trainNumber}</span>
        <span className="muted">{journey.trainName}</span>
        <span className="muted small push">{journey.distanceKm} km</span>
      </header>
      <JourneyTimes journey={journey} stationName={stationName} />
      {children}
    </article>
  )
}
