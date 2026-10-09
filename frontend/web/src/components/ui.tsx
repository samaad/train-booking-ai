import type { ReactNode } from 'react'
import { ApiError } from '../api/http'
import { titleCase } from '../lib/format'

export function Spinner({ label = 'Loading…' }: { label?: string }) {
  return (
    <div className="spinner" role="status">
      <span className="spinner-dot" aria-hidden />
      {label}
    </div>
  )
}

export function ErrorBox({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  if (!error) return null
  const message = error instanceof Error ? error.message : String(error)
  const code = error instanceof ApiError ? error.code : null
  return (
    <div className="alert alert-error" role="alert">
      <div>
        {message}
        {code && <span className="muted small"> ({code})</span>}
      </div>
      {onRetry && (
        <button type="button" className="btn btn-small" onClick={onRetry}>
          Try again
        </button>
      )}
    </div>
  )
}

const TONE: Record<string, string> = {
  CONFIRMED: 'good', TICKETED: 'good', ISSUED: 'good', AUTHORIZED: 'good', ACTIVE: 'good',
  PENDING_PAYMENT: 'warn', PAYMENT_PENDING: 'warn', SEATS_COMMITTED: 'warn', PENDING: 'warn',
  CANCELLED: 'bad', VOIDED: 'bad', FAILED: 'bad', EXPIRED: 'bad', RELEASED: 'bad', REFUNDED: 'neutral',
}

export function StatusBadge({ status }: { status: string }) {
  return <span className={`badge badge-${TONE[status] ?? 'neutral'}`}>{titleCase(status)}</span>
}

export function Card({ children, className = '' }: { children: ReactNode; className?: string }) {
  return <section className={`card ${className}`}>{children}</section>
}

export function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="empty">
      <p className="empty-title">{title}</p>
      {children}
    </div>
  )
}
