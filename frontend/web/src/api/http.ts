import { config } from '../config'
import { userManager } from '../auth/oidc'

/** An RFC 9457 problem from a service (common-web maps every DomainException to one, with a `code`). */
export class ApiError extends Error {
  readonly status: number
  readonly code: string | null
  readonly title: string | null

  constructor(status: number, code: string | null, title: string | null, detail: string) {
    super(detail)
    this.name = 'ApiError'
    this.status = status
    this.code = code
    this.title = title
  }

  /** 503 on a mutating call means "outcome unknown — retry with the same Idempotency-Key". */
  get outcomeUnknown(): boolean {
    return this.status === 503 || this.status === 0
  }
}

export async function toApiError(res: Response): Promise<ApiError> {
  let body: { code?: string; title?: string; detail?: string } = {}
  try {
    body = await res.json()
  } catch {
    // not problem+json (e.g. Kong's own 429 / 502 body)
  }
  const detail = body.detail ?? body.title ?? defaultMessage(res.status)
  return new ApiError(res.status, body.code ?? null, body.title ?? null, detail)
}

function defaultMessage(status: number): string {
  if (status === 401) return 'Your session has expired. Please sign in again.'
  if (status === 403) return 'You are not allowed to do that.'
  if (status === 404) return 'Not found.'
  if (status === 429) return 'Too many requests — slow down a little.'
  if (status >= 500) return 'The service is unavailable right now. Please try again.'
  return `Request failed (${status}).`
}

export interface RequestOptions {
  method?: string
  body?: unknown
  /** Mutating endpoints take one; callers keep it so a retry replays instead of duplicating. */
  idempotencyKey?: string
  /** Public endpoints (search, availability) work signed out too. */
  auth?: 'required' | 'optional'
  signal?: AbortSignal
  accept?: string
}

export async function authHeader(required: boolean): Promise<Record<string, string>> {
  const user = await userManager.getUser()
  if (user && !user.expired) return { Authorization: `Bearer ${user.access_token}` }
  if (required) throw new ApiError(401, 'NOT_SIGNED_IN', null, 'Please sign in first.')
  return {}
}

export async function rawRequest(path: string, opts: RequestOptions = {}): Promise<Response> {
  const headers: Record<string, string> = {
    Accept: opts.accept ?? 'application/json',
    ...(await authHeader(opts.auth !== 'optional')),
  }
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json'
  if (opts.idempotencyKey) headers['Idempotency-Key'] = opts.idempotencyKey
  let res: Response
  try {
    res = await fetch(config.apiBase + path, {
      method: opts.method ?? 'GET',
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
      signal: opts.signal,
    })
  } catch (e) {
    if ((e as Error).name === 'AbortError') throw e
    // No response at all: for a mutation, whether it committed is unknown.
    throw new ApiError(0, 'NETWORK', null, 'Could not reach the server. Check your connection and try again.')
  }
  if (!res.ok) throw await toApiError(res)
  return res
}

export async function request<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  const res = await rawRequest(path, opts)
  if (res.status === 204) return undefined as T
  return (await res.json()) as T
}
