import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useCallback, useMemo, useRef, useState } from 'react'
import { useNavigate, useParams, useSearchParams } from 'react-router'
import { api } from '../api/endpoints'
import { ApiError } from '../api/http'
import { JourneyTimes } from '../components/JourneyCard'
import { ErrorBox, Spinner } from '../components/ui'
import { useAuth } from '../auth/session'
import { useSeatMap } from '../hooks/useSeatMap'
import { useStations } from '../hooks/useStations'
import { forgetIntent, intentKey } from '../lib/idempotency'
import { formatTravelDate, titleCase } from '../lib/format'
import { isWindow, seatGrid } from '../lib/seats'

export function SeatsPage() {
  const { train = '', date = '' } = useParams()
  const [params, setParams] = useSearchParams()
  const from = params.get('from') ?? ''
  const to = params.get('to') ?? ''
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const stations = useStations()
  const { user, login } = useAuth()
  const [selected, setSelected] = useState<string | null>(null)

  const availability = useQuery({
    queryKey: ['availability', train, date],
    queryFn: () => api.availability(train, date),
  })
  const journey = useQuery({
    queryKey: ['search', from, to, date],
    queryFn: () => api.search(from, to, date),
    enabled: !!(from && to),
    select: (list) => list.find((j) => j.trainNumber === train),
  })

  // A live change means some seat moved; re-read the authoritative snapshot, at most every 2 s.
  const lastRefresh = useRef(0)
  const refresh = useCallback(() => {
    if (Date.now() - lastRefresh.current < 2000) return
    lastRefresh.current = Date.now()
    void queryClient.invalidateQueries({ queryKey: ['availability', train, date] })
  }, [queryClient, train, date])
  const live = useSeatMap(train, date, refresh)

  const classes = availability.data?.classes ?? []
  const fareClass = params.get('class') ?? classes[0]?.fareClass ?? ''
  const current = classes.find((c) => c.fareClass === fareClass)
  const seats = useMemo(() => (current ? seatGrid(current, live.occupancy?.seats) : []), [current, live.occupancy])
  const selectedState = seats.find((s) => s.seatNo === selected)?.state
  const selectedGone = !!selected && selectedState !== 'free'

  const hold = useMutation({
    mutationFn: (seatNo: string | null) => {
      const intent = `hold:${train}:${date}:${fareClass}:${seatNo ?? 'any'}`
      return api.hold({ trainNumber: train, travelDate: date, fareClass, seatNo: seatNo ?? undefined }, intentKey(intent))
        .then((h) => { forgetIntent(intent); return h })
        .catch((e) => {
          // A definite rejection is settled; only an unknown outcome keeps the key for a replay.
          if (!(e instanceof ApiError && e.outcomeUnknown)) forgetIntent(intent)
          throw e
        })
    },
    onSuccess: (h) => navigate(`/checkout/${h.holdId}?${new URLSearchParams({ from, to })}`),
    onError: () => refresh(),
  })

  const chooseClass = (fc: string) => {
    setSelected(null)
    const next = new URLSearchParams(params)
    next.set('class', fc)
    setParams(next, { replace: true })
  }

  if (availability.isPending) return <Spinner label="Loading seats…" />
  if (availability.error) return <ErrorBox error={availability.error} onRetry={() => availability.refetch()} />

  return (
    <div className="stack">
      <header className="page-head">
        <div>
          <h1>{train} <span className="muted">· {formatTravelDate(date)}</span></h1>
          {journey.data && <p className="muted">{journey.data.trainName}</p>}
        </div>
        <span className={`live live-${live.status}`} title="Seat changes stream in live">
          {live.status === 'live' ? 'Live' : live.status === 'connecting' ? 'Connecting…' : 'Live updates paused'}
        </span>
      </header>

      {journey.data && (
        <div className="card"><JourneyTimes journey={journey.data} stationName={stations.name} /></div>
      )}

      <div className="tabs" role="tablist">
        {classes.map((c) => (
          <button key={c.fareClass} role="tab" aria-selected={c.fareClass === fareClass}
            className={`tab ${c.fareClass === fareClass ? 'active' : ''}`} onClick={() => chooseClass(c.fareClass)}>
            {titleCase(c.fareClass)} <span className="muted small">{c.available} free</span>
          </button>
        ))}
      </div>

      <div className="seat-layout">
        <div className="card seat-map-card">
          <div className="seat-legend">
            <span><i className="seat-dot free" /> Free</span>
            <span><i className="seat-dot held" /> On hold</span>
            <span><i className="seat-dot taken" /> Taken</span>
            <span><i className="seat-dot mine" /> Your pick</span>
          </div>
          <div className="coach">
            <div className="coach-front muted small">Front of train</div>
            <div className="seat-grid">
              {seats.map((s, i) => (
                <button key={s.seatNo} type="button"
                  className={`seat ${s.state} ${selected === s.seatNo ? 'mine' : ''} ${i % 4 === 2 ? 'after-aisle' : ''}`}
                  disabled={s.state !== 'free'}
                  aria-pressed={selected === s.seatNo}
                  aria-label={`Seat ${s.seatNo}, ${isWindow(s.seatNo) ? 'window' : 'aisle'}, ${s.state}`}
                  onClick={() => setSelected(selected === s.seatNo ? null : s.seatNo)}>
                  {s.seatNo}
                </button>
              ))}
            </div>
          </div>
        </div>

        <aside className="card checkout-side">
          <h3>Your seat</h3>
          {selected ? (
            <p className="big">{selected} <span className="muted small">{isWindow(selected) ? 'Window' : 'Aisle'}</span></p>
          ) : (
            <p className="muted">Pick a seat on the map, or let us choose one.</p>
          )}
          {selectedGone && <div className="alert alert-warn">Someone just took {selected}. Please pick another.</div>}
          {hold.error && <ErrorBox error={hold.error} />}
          {!user ? (
            <button className="btn btn-primary btn-block" onClick={() => login()}>Sign in to hold a seat</button>
          ) : (
            <>
              <button className="btn btn-primary btn-block" disabled={!selected || selectedGone || hold.isPending || !from || !to}
                onClick={() => hold.mutate(selected)}>
                {hold.isPending ? 'Holding…' : selected ? `Hold seat ${selected}` : 'Hold seat'}
              </button>
              <button className="btn btn-block" disabled={hold.isPending || current?.available === 0 || !from || !to}
                onClick={() => hold.mutate(null)}>
                Any {titleCase(fareClass).toLowerCase()} seat
              </button>
            </>
          )}
          <p className="muted small">A hold keeps the seat for you for a few minutes while you check out. Nothing is charged yet.</p>
          {(!from || !to) && <div className="alert alert-warn">Open this train from a search so we know your stations.</div>}
        </aside>
      </div>
    </div>
  )
}
