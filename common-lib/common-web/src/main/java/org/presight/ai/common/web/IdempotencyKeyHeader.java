package org.presight.ai.common.web;

import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.idempotency.IdempotencyKey;

/** The public-API {@code Idempotency-Key} header, required on every mutating endpoint. */
public final class IdempotencyKeyHeader {

    public static final String NAME = "Idempotency-Key";

    private IdempotencyKeyHeader() {
    }

    /** Parses a header value, turning a malformed key into a 422 rather than a 500. */
    public static IdempotencyKey parse(String raw) {
        try {
            return IdempotencyKey.of(raw);
        } catch (IllegalArgumentException e) {
            throw DomainException.invalid("INVALID_IDEMPOTENCY_KEY", e.getMessage());
        }
    }

    public static DomainException reused() {
        return DomainException.invalid("IDEMPOTENCY_KEY_REUSED",
                "This Idempotency-Key was already used for a different request");
    }
}
