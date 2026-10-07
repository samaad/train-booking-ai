package org.presight.ai.booking.booking;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

public record Booking(
        UUID id,
        String pnr,
        UUID holdId,
        Status status,
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

    /** The saga state. CONFIRMED is reached once payment is authorised (wired with events). */
    public enum Status { PENDING_PAYMENT, CONFIRMED, CANCELLED }
}
