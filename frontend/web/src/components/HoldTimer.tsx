import { mmss, useCountdown } from '../hooks/useCountdown'

export function HoldTimer({ expiresAt }: { expiresAt: string }) {
  const left = useCountdown(expiresAt)
  const tone = left === 0 ? 'bad' : left < 120 ? 'warn' : 'good'
  return (
    <span className={`timer timer-${tone}`} role="timer" aria-live="off">
      {left === 0 ? 'Hold expired' : `Seat held for ${mmss(left)}`}
    </span>
  )
}
