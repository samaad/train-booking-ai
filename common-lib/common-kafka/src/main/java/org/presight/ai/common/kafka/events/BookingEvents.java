package org.presight.ai.common.kafka.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

/** Produced by booking-service, keyed by bookingId. */
public final class BookingEvents {

    private BookingEvents() {
    }

    /**
     * Saga entry. Carries everything downstream needs so consumers never call back synchronously:
     * payment learns the authoritative amount, notification the contact address. {@code userId} is the
     * booking's owner (JWT subject): payment only accepts that user's payment, ticketing shows only them the ticket.
     */
    public record BookingCreated(UUID eventId, UUID bookingId, String userId, String pnr, UUID holdId, String trainNumber,
                                 LocalDate travelDate, String fareClass, String seatNo, String fromStation,
                                 String toStation, OffsetDateTime departure, String passengerName,
                                 String contactEmail, BigDecimal amount, String currency, Instant occurredAt)
            implements Event {
        public UUID key() { return bookingId; }
    }

    /** Payment authorised and seat committed. Ticketing issues on this. */
    public record BookingConfirmed(UUID eventId, UUID bookingId, String userId, String pnr, UUID holdId, String trainNumber,
                                   LocalDate travelDate, String fareClass, String seatNo, String fromStation,
                                   String toStation, OffsetDateTime departure, String passengerName, Instant occurredAt)
            implements Event {
        public UUID key() { return bookingId; }
    }

    public enum CancelReason { USER, PAYMENT_FAILED, HOLD_EXPIRED, AMOUNT_MISMATCH }

    /** Compensation trigger: inventory releases the seat, payment refunds, ticketing voids. */
    public record BookingCancelled(UUID eventId, UUID bookingId, String pnr, UUID holdId, CancelReason reason,
                                   String detail, Instant occurredAt) implements Event {
        public UUID key() { return bookingId; }
    }
}
