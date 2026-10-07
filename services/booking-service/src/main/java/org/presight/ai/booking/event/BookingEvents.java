package org.presight.ai.booking.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Payloads of the events booking produces (01-services: {@code booking.created.v1},
 * {@code booking.confirmed.v1}, {@code booking.cancelled.v1}). Defined now so the contract is
 * explicit; publishing (via the outbox) is wired in the events pass.
 */
public final class BookingEvents {

    private BookingEvents() {
    }

    /** payment-service authorises exactly this amount — the price booking computed, not a client's. */
    public record BookingCreated(UUID eventId, UUID bookingId, String pnr, UUID holdId,
                                 BigDecimal amount, String currency, Instant occurredAt) {
        public static final String TOPIC = "booking.created.v1";
    }

    public record BookingConfirmed(UUID eventId, UUID bookingId, String pnr, UUID holdId, Instant occurredAt) {
        public static final String TOPIC = "booking.confirmed.v1";
    }

    public record BookingCancelled(UUID eventId, UUID bookingId, String pnr, UUID holdId,
                                   String reason, Instant occurredAt) {
        public static final String TOPIC = "booking.cancelled.v1";
    }
}
