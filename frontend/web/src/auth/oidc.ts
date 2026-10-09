import { UserManager, WebStorageStateStore } from 'oidc-client-ts'
import { API_SCOPES, config } from '../config'

/**
 * Authorization Code + PKCE (S256) against the public train-web client — no client secret, no implicit
 * flow, no password grant (05-security). Tokens live in sessionStorage, so they die with the tab, and are
 * renewed with the refresh token rather than a hidden iframe (Keycloak's cookies are SameSite=None).
 */
export const userManager = new UserManager({
  authority: config.authority,
  client_id: config.clientId,
  redirect_uri: `${window.location.origin}/callback`,
  post_logout_redirect_uri: `${window.location.origin}/`,
  response_type: 'code',
  scope: ['openid', 'profile', 'email', ...API_SCOPES].join(' '),
  userStore: new WebStorageStateStore({ store: window.sessionStorage }),
  automaticSilentRenew: true,
  monitorSession: false,
})
