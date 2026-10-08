package org.presight.ai.common.kafka.events;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.presight.ai.common.kafka.Topics;

/** Produced by inventory-service, keyed by {@code trainNumber:travelDate} (the contention unit). */
public final class SeatEvents {

    private SeatEvents() {
    }

    public record SeatHeld(UUID eventId, UUID holdId, String trainNumber, LocalDate travelDate, String fareClass,
                           String seatNo, Instant expiresAt, Instant occurredAt) implements Event {
        public String key() { return Topics.seatKey(trainNumber, travelDate); }
    }

    public enum ReleaseReason { RELEASED, EXPIRED, BOOKING_CANCELLED }

    public record SeatReleased(UUID eventId, UUID holdId, String trainNumber, LocalDate travelDate, String fareClass,
                               String seatNo, ReleaseReason reason, Instant occurredAt) implements Event {
        public String key() { return Topics.seatKey(trainNumber, travelDate); }
    }

    public record SeatCommitted(UUID eventId, UUID holdId, String trainNumber, LocalDate travelDate, String fareClass,
                                String seatNo, Instant occurredAt) implements Event {
        public String key() { return Topics.seatKey(trainNumber, travelDate); }
    }
}
