package org.presight.ai.booking.booking;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

public record Booking(
        UUID id,
        String userId,
        String pnr,
        UUID holdId,
        Status status,
        SagaState sagaState,
        String trainNumber,
        LocalDate travelDate,
        String fareClass,
        String seatNo,
        String fromStation,
        String toStation,
        OffsetDateTime departure,
        OffsetDateTime arrival,
        String passengerName,
        String contactEmail,
        BigDecimal amount,
        String currency,
        String cancelReason,
        String requestHash,
        long version,
        Instant createdAt) {

    /** Customer-facing status. */
    public enum Status { PENDING_PAYMENT, CONFIRMED, CANCELLED }

    /** Internal choreographed-saga state (03-consistency): PAYMENT_PENDING → SEATS_COMMITTED → TICKETED, or CANCELLED. */
    public enum SagaState { PAYMENT_PENDING, SEATS_COMMITTED, TICKETED, CANCELLED }
}
