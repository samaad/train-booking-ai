import { HoldTimer } from '../HoldTimer'
import { formatTravelDate, titleCase } from '../../lib/format'
import { isTrainOptions } from '../../lib/trainOptions'
import { TrainOptions } from './TrainOptions'

const LABELS: Record<string, string> = {
  searchTrains: 'Searched trains',
  checkAvailability: 'Checked seats',
  getBookingStatus: 'Looked up booking',
  holdSeat: 'Held a seat',
  confirmBooking: 'Prepared booking',
  cancelBooking: 'Prepared cancellation',
}

type Obj = Record<string, unknown>
const isObj = (v: unknown): v is Obj => !!v && typeof v === 'object' && !Array.isArray(v)

export function ToolCard({ tool, outcome, result }: { tool: string; outcome: string; result: unknown }) {
  const failed = outcome !== 'success'
  return (
    <div className={`tool-card ${failed ? 'failed' : ''}`}>
      <div className="tool-head">
        <span className="tool-icon" aria-hidden>{failed ? '!' : '✓'}</span>
        <span>{LABELS[tool] ?? tool}</span>
        {failed && <span className="muted small push">{outcome.replace(/_/g, ' ')}</span>}
      </div>
      <ToolBody tool={tool} result={result} failed={failed} />
    </div>
  )
}

function ToolBody({ tool, result, failed }: { tool: string; result: unknown; failed: boolean }) {
  if (failed && isObj(result)) return <p className="small">{String(result.message ?? result.error)}</p>
  if (isTrainOptions(result)) return <TrainOptions options={result} />
  if (Array.isArray(result) && result.length === 0) return <p className="muted small">No results.</p>
  if (tool === 'checkAvailability' && isObj(result)) {
    return (
      <p className="small">
        {String(result.trainNumber)} · {formatTravelDate(String(result.travelDate))} · {titleCase(String(result.fareClass))}:{' '}
        <strong>{String(result.available)}</strong> of {String(result.total)} seats free
      </p>
    )
  }
  if (tool === 'holdSeat' && isObj(result) && result.holdId) {
    return (
      <p className="small">
        Seat <strong>{String(result.seatNo)}</strong> on {String(result.trainNumber)}, {formatTravelDate(String(result.travelDate))}{' '}
        <HoldTimer expiresAt={String(result.expiresAt)} />
      </p>
    )
  }
  if (tool === 'getBookingStatus' && isObj(result) && result.pnr) {
    return (
      <p className="small">
        <span className="pnr">{String(result.pnr)}</span> · {titleCase(String(result.status))} · {String(result.trainNumber)} seat {String(result.seatNo)}
      </p>
    )
  }
  if (result == null || (isObj(result) && result.status === 'AWAITING_USER_CONFIRMATION')) return null
  return (
    <details className="small">
      <summary className="muted">Details</summary>
      <pre>{JSON.stringify(result, null, 2)}</pre>
    </details>
  )
}
