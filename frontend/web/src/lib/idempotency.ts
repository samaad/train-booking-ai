/**
 * Idempotency keys that survive a reload. A key is minted once per *intent* (hold this seat, book this
 * hold, pay this booking) and reused for every retry of it — so a retry after a timeout replays the
 * original result instead of double-booking or double-charging (06-resilience: no blind retries).
 */
const PREFIX = 'idem:'

export function intentKey(intent: string): string {
  const storageKey = PREFIX + intent
  try {
    const existing = sessionStorage.getItem(storageKey)
    if (existing) return existing
    const key = crypto.randomUUID()
    sessionStorage.setItem(storageKey, key)
    return key
  } catch {
    return crypto.randomUUID()
  }
}

/** Forget a key once its intent has a settled, final outcome, so a deliberate new attempt gets a fresh one. */
export function forgetIntent(intent: string): void {
  try {
    sessionStorage.removeItem(PREFIX + intent)
  } catch {
    // storage unavailable: nothing to forget
  }
}
