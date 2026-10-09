import type { User } from 'oidc-client-ts'
import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react'
import { userManager } from './oidc'
import { AuthContext, accessClaims, type Session } from './session'

const STAFF = ['AGENT', 'SUPPORT', 'ADMIN']


export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    let alive = true
    userManager.getUser().then((u) => {
      if (alive) {
        setUser(u && !u.expired ? u : null)
        setLoading(false)
      }
    })
    const loaded = (u: User) => setUser(u)
    const gone = () => setUser(null)
    userManager.events.addUserLoaded(loaded)
    userManager.events.addUserUnloaded(gone)
    userManager.events.addSilentRenewError(gone)
    return () => {
      alive = false
      userManager.events.removeUserLoaded(loaded)
      userManager.events.removeUserUnloaded(gone)
      userManager.events.removeSilentRenewError(gone)
    }
  }, [])

  const login = useCallback(async (returnTo?: string) => {
    await userManager.signinRedirect({ state: { returnTo: returnTo ?? window.location.pathname + window.location.search } })
  }, [])

  const logout = useCallback(async () => {
    await userManager.signoutRedirect()
  }, [])

  const session = useMemo<Session>(() => {
    const claims = user ? accessClaims(user.access_token) : {}
    const roles: string[] = claims.realm_access?.roles ?? []
    return {
      user,
      loading,
      login,
      logout,
      displayName: (user?.profile.name as string | undefined) ?? (user?.profile.preferred_username as string | undefined) ?? null,
      email: (user?.profile.email as string | undefined) ?? null,
      roles,
      isStaff: roles.some((r) => STAFF.includes(r)),
      loyaltyTier: claims.loyalty_tier && claims.loyalty_tier !== 'NONE' ? claims.loyalty_tier : null,
    }
  }, [user, loading, login, logout])

  return <AuthContext.Provider value={session}>{children}</AuthContext.Provider>
}
