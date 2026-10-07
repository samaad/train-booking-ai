package org.presight.ai.payment.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Payloads of the events payment produces (01-services: {@code payment.authorized.v1},
 * {@code payment.failed.v1}, {@code payment.refunded.v1}). Defined now so the contract is
 * explicit; publishing through Kafka is wired in the events pass. booking-service must check
 * {@code amount} against its own price before confirming.
 */
public final class PaymentEvents {

    private PaymentEvents() {
    }

    public record PaymentAuthorized(UUID eventId, UUID paymentId, UUID bookingId, BigDecimal amount,
                                    String currency, String pspReference, Instant occurredAt) {
        public static final String TOPIC = "payment.authorized.v1";
    }

    public record PaymentFailed(UUID eventId, UUID paymentId, UUID bookingId, String reason, Instant occurredAt) {
        public static final String TOPIC = "payment.failed.v1";
    }

    public record PaymentRefunded(UUID eventId, UUID paymentId, UUID bookingId, BigDecimal amount,
                                  String currency, Instant occurredAt) {
        public static final String TOPIC = "payment.refunded.v1";
    }
}
