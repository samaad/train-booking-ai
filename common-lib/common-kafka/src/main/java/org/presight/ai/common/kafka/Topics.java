package org.presight.ai.common.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The topic table from 04-kafka. Saga topics key on bookingId (one booking's events stay ordered on one
 * partition); seat topics key on {@code trainNumber:travelDate}, the contention unit, with more
 * partitions because a popular train is a hot key.
 */
public final class Topics {

    public static final String BOOKING_CREATED = "booking.created.v1";
    public static final String BOOKING_CONFIRMED = "booking.confirmed.v1";
    public static final String BOOKING_CANCELLED = "booking.cancelled.v1";
    public static final String PAYMENT_AUTHORIZED = "payment.authorized.v1";
    public static final String PAYMENT_FAILED = "payment.failed.v1";
    public static final String PAYMENT_REFUNDED = "payment.refunded.v1";
    public static final String SEAT_HELD = "seat.held.v1";
    public static final String SEAT_RELEASED = "seat.released.v1";
    public static final String SEAT_COMMITTED = "seat.committed.v1";
    public static final String TICKET_ISSUED = "ticket.issued.v1";
    public static final String TICKET_VOIDED = "ticket.voided.v1";
    public static final String PROFILE_UPDATED = "profile.updated.v1";
    public static final String NOTIFICATION_SENT = "notification.sent.v1";
    public static final String CATALOG_UPDATED = "catalog.updated.v1";

    /** @param retention {@code null} means log-compacted */
    public record TopicSpec(String name, int partitions, Duration retention) {
    }

    private static final Map<String, TopicSpec> SPECS = Map.ofEntries(
            spec(BOOKING_CREATED, 12, Duration.ofDays(7)),
            spec(BOOKING_CONFIRMED, 12, Duration.ofDays(30)),
            spec(BOOKING_CANCELLED, 12, Duration.ofDays(30)),
            spec(PAYMENT_AUTHORIZED, 12, Duration.ofDays(90)), // financial audit
            spec(PAYMENT_FAILED, 12, Duration.ofDays(90)),
            spec(PAYMENT_REFUNDED, 12, Duration.ofDays(90)),
            spec(SEAT_HELD, 24, Duration.ofDays(1)),
            spec(SEAT_RELEASED, 24, Duration.ofDays(1)),
            spec(SEAT_COMMITTED, 24, Duration.ofDays(7)),
            spec(TICKET_ISSUED, 12, Duration.ofDays(90)),
            spec(TICKET_VOIDED, 12, Duration.ofDays(90)),
            spec(PROFILE_UPDATED, 6, Duration.ofDays(7)),
            spec(NOTIFICATION_SENT, 6, Duration.ofDays(1)),
            Map.entry(CATALOG_UPDATED, new TopicSpec(CATALOG_UPDATED, 6, null)));

    private Topics() {
    }

    public static TopicSpec spec(String topic) {
        return Optional.ofNullable(SPECS.get(topic))
                .orElseThrow(() -> new IllegalArgumentException("Unknown topic " + topic));
    }

    public static List<String> all() {
        return List.copyOf(SPECS.keySet());
    }

    /** Dead letters are kept per consumer group, so one group's poison message never blocks another's view. */
    public static String deadLetter(String topic, String consumerGroup) {
        return topic + "." + consumerGroup + ".DLT";
    }

    public static String seatKey(String trainNumber, Object travelDate) {
        return trainNumber + ':' + travelDate;
    }

    private static Map.Entry<String, TopicSpec> spec(String name, int partitions, Duration retention) {
        return Map.entry(name, new TopicSpec(name, partitions, retention));
    }
}
