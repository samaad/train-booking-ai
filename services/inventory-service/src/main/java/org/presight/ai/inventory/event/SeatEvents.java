package org.presight.ai.inventory.event;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/**
 * Payloads of the events inventory produces (01-services: {@code seat.held.v1},
 * {@code seat.released.v1}, {@code seat.committed.v1}). Defined now so the contract is
 * explicit; publishing through Kafka is wired in the events pass.
 */
public final class SeatEvents {

    private SeatEvents() {
    }

    public record SeatHeld(UUID eventId, UUID holdId, String trainNumber, LocalDate travelDate,
                           String fareClass, String seatNo, Instant expiresAt, Instant occurredAt) {
        public static final String TOPIC = "seat.held.v1";
    }

    public record SeatReleased(UUID eventId, UUID holdId, String trainNumber, LocalDate travelDate,
                               String seatNo, Instant occurredAt) {
        public static final String TOPIC = "seat.released.v1";
    }

    public record SeatCommitted(UUID eventId, UUID holdId, String trainNumber, LocalDate travelDate,
                                String seatNo, Instant occurredAt) {
        public static final String TOPIC = "seat.committed.v1";
    }
}
