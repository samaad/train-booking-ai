import type { User } from 'oidc-client-ts'
import { createContext, useContext } from 'react'

export interface Session {
  user: User | null
  /** Still restoring a stored session — don't redirect to login yet. */
  loading: boolean
  login: (returnTo?: string) => Promise<void>
  logout: () => Promise<void>
  displayName: string | null
  email: string | null
  roles: string[]
  isStaff: boolean
  loyaltyTier: string | null
}

export const AuthContext = createContext<Session | null>(null)

export function useAuth(): Session {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth outside AuthProvider')
  return ctx
}

/**
 * Reads display-only claims (roles, tier) from the access token. Not a security check — every service
 * validates the token itself; this only decides what the UI shows.
 */
export function accessClaims(token: string): { realm_access?: { roles?: string[] }; loyalty_tier?: string } {
  try {
    const b64 = token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/')
    const bytes = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0))
    return JSON.parse(new TextDecoder().decode(bytes))
  } catch {
    return {}
  }
}
