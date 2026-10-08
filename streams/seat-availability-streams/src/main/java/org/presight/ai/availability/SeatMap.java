package org.presight.ai.availability;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Occupancy of one train on one date: the seats that are held or committed. A seat not listed (or listed
 * {@code FREE}) is available. Pure value logic — no Kafka here, so it is unit-testable on its own.
 */
public record SeatMap(String trainNumber, LocalDate travelDate, Map<String, Seat> seats, Instant updatedAt) {

    public enum Status { HELD, COMMITTED, FREE }

    /**
     * @param lastAt event time of the change that produced this state. The three input topics have no ordering
     *               between them, so an event older than {@code lastAt} is stale and ignored.
     */
    public record Seat(String fareClass, Status status, UUID holdId, Instant expiresAt, Instant lastAt) {
    }

    public enum Kind { HELD, COMMITTED, RELEASED }

    /** One seat.* event, normalised. */
    public record Change(Kind kind, UUID holdId, String trainNumber, LocalDate travelDate, String fareClass,
                         String seatNo, Instant expiresAt, Instant occurredAt) {
    }

    public static SeatMap empty() {
        return new SeatMap(null, null, new TreeMap<>(), Instant.EPOCH);
    }

    public SeatMap apply(Change c) {
        Map<String, Seat> next = new TreeMap<>(seats);
        Seat current = next.get(c.seatNo());
        if (current != null && c.occurredAt().isBefore(current.lastAt())) {
            return this; // stale: a newer change for this seat has already been applied
        }
        switch (c.kind()) {
            case HELD -> next.put(c.seatNo(), new Seat(c.fareClass(), Status.HELD, c.holdId(), c.expiresAt(), c.occurredAt()));
            case COMMITTED -> next.put(c.seatNo(), new Seat(c.fareClass(), Status.COMMITTED, c.holdId(), null, c.occurredAt()));
            case RELEASED -> {
                if (current != null && current.holdId() != null && !current.holdId().equals(c.holdId())) {
                    return this; // releases an earlier hold; the seat now belongs to a newer one
                }
                // Keep a FREE marker with its time, so a late, older HELD for this hold cannot resurrect it.
                next.put(c.seatNo(), new Seat(c.fareClass(), Status.FREE, c.holdId(), null, c.occurredAt()));
            }
        }
        Instant updated = c.occurredAt().isAfter(updatedAt) ? c.occurredAt() : updatedAt;
        return new SeatMap(c.trainNumber(), c.travelDate(), next, updated);
    }

    /** Effective status at {@code now}: a hold that has lapsed counts as free even before the sweeper says so. */
    public static Status effective(Seat seat, Instant now) {
        if (seat.status() == Status.HELD && seat.expiresAt() != null && !seat.expiresAt().isAfter(now)) {
            return Status.FREE;
        }
        return seat.status();
    }
}
