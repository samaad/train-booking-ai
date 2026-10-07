package org.presight.ai.payment.payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentIntent(
        UUID id,
        UUID bookingId,
        BigDecimal amount,
        String currency,
        Status status,
        String paymentToken,
        String pspReference,
        String failureReason,
        String requestHash,
        Instant createdAt) {

    /** PENDING = outcome unknown; it is re-driven on replay, never assumed failed. */
    public enum Status { PENDING, AUTHORIZED, FAILED, REFUNDED }
}
