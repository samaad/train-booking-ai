export const config = {
  apiBase: import.meta.env.VITE_API_BASE ?? 'http://localhost:8000',
  authority: import.meta.env.VITE_OIDC_AUTHORITY ?? 'http://localhost:8180/realms/train-booking',
  clientId: import.meta.env.VITE_OIDC_CLIENT_ID ?? 'train-web',
  /** The railway's zone. Jackson normalises times to UTC; travellers must see local departure times. */
  railwayZone: 'Asia/Dubai',
} as const

/** Optional client scopes on train-web; each service checks the one its endpoint needs. */
export const API_SCOPES = ['booking:read', 'booking:write', 'payment:write', 'profile:read', 'profile:write', 'ai:chat']
