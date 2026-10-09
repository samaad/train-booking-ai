import { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router'
import { userManager } from '../auth/oidc'
import { ErrorBox, Spinner } from '../components/ui'

/** Keycloak redirects here with the authorization code; oidc-client-ts redeems it with the PKCE verifier. */
export function CallbackPage() {
  const navigate = useNavigate()
  const [error, setError] = useState<unknown>(null)
  const once = useRef(false) // StrictMode runs effects twice; a code can be redeemed only once
  useEffect(() => {
    if (once.current) return
    once.current = true
    userManager.signinRedirectCallback()
      .then((user) => {
        const returnTo = (user.state as { returnTo?: string } | undefined)?.returnTo
        // Only ever an in-app path: never let a crafted state bounce the user off-site.
        navigate(returnTo && returnTo.startsWith('/') && !returnTo.startsWith('//') && returnTo !== '/callback' ? returnTo : '/',
          { replace: true })
      })
      .catch(setError)
  }, [navigate])
  if (error) return <ErrorBox error={error} />
  return <Spinner label="Signing you in…" />
}
