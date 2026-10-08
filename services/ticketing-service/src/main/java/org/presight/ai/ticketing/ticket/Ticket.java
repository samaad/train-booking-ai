package org.presight.ai.ticketing.ticket;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

public record Ticket(
        UUID id,
        String userId,
        String pnr,
        UUID bookingId,
        Status status,
        String passengerName,
        String trainNumber,
        LocalDate travelDate,
        String fareClass,
        String seatNo,
        String fromStation,
        String toStation,
        OffsetDateTime departure,
        String qrToken,
        String pdfObjectKey,
        Instant issuedAt,
        Instant voidedAt) {

    public enum Status { ISSUED, VOIDED }
}
