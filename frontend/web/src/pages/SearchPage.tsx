import { useQuery } from '@tanstack/react-query'
import { useState, type FormEvent } from 'react'
import { Link, useSearchParams } from 'react-router'
import { api } from '../api/endpoints'
import type { JourneyOption } from '../api/types'
import { JourneyCard } from '../components/JourneyCard'
import { Empty, ErrorBox, Spinner } from '../components/ui'
import { useStations } from '../hooks/useStations'
import { addDays, formatTravelDate, railwayToday, titleCase } from '../lib/format'

/** inventory-service accepts travel dates from today up to its 90-day booking horizon. */
const HORIZON_DAYS = 90

export function SearchPage() {
  const [params, setParams] = useSearchParams()
  const stations = useStations()
  const today = railwayToday()
  const [from, setFrom] = useState(params.get('from') ?? '')
  const [to, setTo] = useState(params.get('to') ?? '')
  const [date, setDate] = useState(params.get('date') ?? addDays(today, 1))

  const search = { from: params.get('from'), to: params.get('to'), date: params.get('date') }
  const ready = !!(search.from && search.to && search.date)
  const results = useQuery({
    queryKey: ['search', search.from, search.to, search.date],
    queryFn: () => api.search(search.from!, search.to!, search.date!),
    enabled: ready,
  })

  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (from && to && date) setParams({ from, to, date })
  }

  const swap = () => {
    setFrom(to)
    setTo(from)
  }

  return (
    <div className="stack">
      <section className="hero">
        <h1>Where are you going?</h1>
        <p className="muted">Real schedules, live seats. Or just ask the <Link to="/assistant">assistant</Link>.</p>
        <form className="search-form card" onSubmit={submit}>
          <label>
            <span>From</span>
            <select value={from} onChange={(e) => setFrom(e.target.value)} required>
              <option value="">Select station</option>
              {stations.data?.map((s) => (
                <option key={s.code} value={s.code} disabled={s.code === to}>
                  {s.name} ({s.code})
                </option>
              ))}
            </select>
          </label>
          <button type="button" className="btn btn-icon swap" onClick={swap} aria-label="Swap stations" title="Swap">
            ⇄
          </button>
          <label>
            <span>To</span>
            <select value={to} onChange={(e) => setTo(e.target.value)} required>
              <option value="">Select station</option>
              {stations.data?.map((s) => (
                <option key={s.code} value={s.code} disabled={s.code === from}>
                  {s.name} ({s.code})
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>Date</span>
            <input type="date" value={date} min={today} max={addDays(today, HORIZON_DAYS)}
              onChange={(e) => setDate(e.target.value)} required />
          </label>
          <button className="btn btn-primary" type="submit" disabled={!from || !to || from === to}>
            Search trains
          </button>
        </form>
        {stations.error && <ErrorBox error={stations.error} onRetry={() => stations.refetch()} />}
      </section>

      {ready && (
        <section className="stack">
          <h2>
            {stations.name(search.from!)} → {stations.name(search.to!)}
            <span className="muted small"> · {formatTravelDate(search.date!)}</span>
          </h2>
          {results.isPending && <Spinner label="Searching trains…" />}
          {results.error && <ErrorBox error={results.error} onRetry={() => results.refetch()} />}
          {results.data?.length === 0 && (
            <Empty title="No trains on this date">
              <p className="muted">Try another day — not every route runs daily.</p>
            </Empty>
          )}
          {results.data?.map((j) => (
            <JourneyCard key={j.trainNumber + j.departure} journey={j} stationName={stations.name}>
              <FareClasses journey={j} date={search.date!} />
            </JourneyCard>
          ))}
        </section>
      )}
    </div>
  )
}

function FareClasses({ journey, date }: { journey: JourneyOption; date: string }) {
  const availability = useQuery({
    queryKey: ['availability', journey.trainNumber, date],
    queryFn: () => api.availability(journey.trainNumber, date),
    staleTime: 15_000,
  })
  const counts = new Map(availability.data?.classes.map((c) => [c.fareClass, c]))
  return (
    <div className="fare-classes">
      {journey.fareClasses.map((fc) => {
        const c = counts.get(fc)
        const soldOut = c?.available === 0
        const link = `/trains/${journey.trainNumber}/${date}?${new URLSearchParams({ from: journey.from, to: journey.to, class: fc })}`
        return (
          <div key={fc} className={`fare-class ${soldOut ? 'sold-out' : ''}`}>
            <div>
              <div className="fare-name">{titleCase(fc)}</div>
              <div className="muted small">
                {availability.isPending ? 'Checking seats…'
                  : availability.error ? 'Seats unknown'
                  : soldOut ? 'Sold out' : `${c?.available ?? 0} of ${c?.total ?? 0} seats free`}
              </div>
            </div>
            {soldOut ? (
              <span className="btn btn-small" aria-disabled>Sold out</span>
            ) : (
              <Link className="btn btn-small btn-primary" to={link}>Choose seat</Link>
            )}
          </div>
        )
      })}
    </div>
  )
}
