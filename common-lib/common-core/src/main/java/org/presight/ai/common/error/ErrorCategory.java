package org.presight.ai.common.error;

/**
 * Transport-neutral failure classes. {@code common-web} maps these to HTTP statuses;
 * Kafka consumers map them to retry-vs-DLT decisions. Keep the set small.
 */
public enum ErrorCategory {
    /** Input is malformed or violates a business rule; retrying unchanged will fail again. */
    INVALID,
    /** The referenced resource does not exist (or is not visible to the caller). */
    NOT_FOUND,
    /** State conflict — e.g. seat already held, stale version. Not retryable as-is. */
    CONFLICT,
    /** Authenticated but not permitted. */
    FORBIDDEN,
    /** A dependency is down or overloaded. The only category that may be worth retrying. */
    UNAVAILABLE
}
