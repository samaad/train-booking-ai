import { useEffect, useRef, useState } from 'react'
import type { Occupancy } from '../api/types'
import { config } from '../config'

export type LiveStatus = 'connecting' | 'live' | 'offline'

/**
 * Subscribes to seat-availability-streams' WebSocket (via Kong) for one train-date. Every message is the
 * full occupancy, so a dropped message costs nothing; reconnects with backoff. `onChange` lets the caller
 * refresh the authoritative inventory snapshot (the live map is a projection and may lag).
 */
export function useSeatMap(trainNumber: string, travelDate: string, onChange?: () => void) {
  const [occupancy, setOccupancy] = useState<Occupancy | null>(null)
  const [status, setStatus] = useState<LiveStatus>('connecting')
  const onChangeRef = useRef(onChange)
  useEffect(() => {
    onChangeRef.current = onChange
  }, [onChange])

  useEffect(() => {
    let socket: WebSocket | null = null
    let retry: ReturnType<typeof setTimeout> | undefined
    let attempts = 0
    let closed = false
    const url = new URL('/api/availability/ws/seat-map', config.apiBase)
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
    url.search = new URLSearchParams({ train: trainNumber, date: travelDate }).toString()

    const connect = () => {
      setStatus('connecting')
      socket = new WebSocket(url)
      socket.onopen = () => {
        attempts = 0
        setStatus('live')
        // The socket only pushes on change; read the current state once so existing holds show now.
        fetch(new URL(`/api/availability/occupancy/${encodeURIComponent(trainNumber)}/${travelDate}`, config.apiBase))
          .then((res) => (res.ok ? res.json() : null))
          .then((o: Occupancy | null) => {
            if (o && !closed) setOccupancy((current) => current ?? o)
          })
          .catch(() => {})
      }
      socket.onmessage = (msg) => {
        try {
          setOccupancy(JSON.parse(msg.data as string) as Occupancy)
          onChangeRef.current?.()
        } catch {
          // malformed frame: wait for the next full snapshot
        }
      }
      socket.onclose = () => {
        if (closed) return
        setStatus('offline')
        retry = setTimeout(connect, Math.min(30_000, 1000 * 2 ** attempts++))
      }
    }
    connect()
    return () => {
      closed = true
      clearTimeout(retry)
      socket?.close()
    }
  }, [trainNumber, travelDate])

  return { occupancy, status }
}
