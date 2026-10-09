import { useEffect, type ReactNode } from 'react'
import { useLocation } from 'react-router'
import { useAuth } from '../auth/session'
import { Spinner } from './ui'

/** Sends signed-out visitors to Keycloak, returning them here afterwards. */
export function RequireAuth({ children }: { children: ReactNode }) {
  const { user, loading, login } = useAuth()
  const location = useLocation()
  useEffect(() => {
    if (!loading && !user) void login(location.pathname + location.search)
  }, [loading, user, login, location])
  if (!user) return <Spinner label="Signing you in…" />
  return <>{children}</>
}
