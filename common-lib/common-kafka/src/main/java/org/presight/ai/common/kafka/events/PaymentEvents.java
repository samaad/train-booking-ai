package org.presight.ai.common.kafka.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Produced by payment-service, keyed by bookingId. */
public final class PaymentEvents {

    private PaymentEvents() {
    }

    /** booking-service checks {@code amount} against its own price before confirming. */
    public record PaymentAuthorized(UUID eventId, UUID paymentId, UUID bookingId, BigDecimal amount, String currency,
                                    String pspReference, Instant occurredAt) implements Event {
        public UUID key() { return bookingId; }
    }

    public record PaymentFailed(UUID eventId, UUID paymentId, UUID bookingId, String reason, Instant occurredAt)
            implements Event {
        public UUID key() { return bookingId; }
    }

    public record PaymentRefunded(UUID eventId, UUID paymentId, UUID bookingId, BigDecimal amount, String currency,
                                  Instant occurredAt) implements Event {
        public UUID key() { return bookingId; }
    }
}
