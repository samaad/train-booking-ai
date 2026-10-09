import { useEffect, useState } from 'react'

/** Seconds left until `iso` (never negative), ticking once a second. */
export function useCountdown(iso: string | null | undefined): number {
  const target = iso ? new Date(iso).getTime() : 0
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    if (!target) return
    const t = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(t)
  }, [target])
  return target ? Math.max(0, Math.floor((target - now) / 1000)) : 0
}

export const mmss = (s: number) => `${Math.floor(s / 60)}:${(s % 60).toString().padStart(2, '0')}`
