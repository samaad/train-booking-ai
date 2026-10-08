package org.presight.ai.common.kafka.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Ticketing, profile, notification and catalog events. */
public final class OtherEvents {

    private OtherEvents() {
    }

    /** Produced by ticketing-service, keyed by bookingId. */
    public record TicketIssued(UUID eventId, UUID ticketId, UUID bookingId, String pnr, Instant occurredAt) implements Event {
        public UUID key() { return bookingId; }
    }

    public record TicketVoided(UUID eventId, UUID ticketId, UUID bookingId, String pnr, Instant occurredAt) implements Event {
        public UUID key() { return bookingId; }
    }

    /** Produced by user-profile-service. No PII beyond the subject. */
    public record ProfileUpdated(UUID eventId, String userId, long version, Instant occurredAt) implements Event {
        public String key() { return userId; }
    }

    /** Produced by notification-service. No recipient address: it is PII. */
    public record NotificationSent(UUID eventId, String notificationId, String sourceEventId, String channel,
                                   Instant occurredAt) implements Event {
        public String key() { return sourceEventId; }
    }

    /** Contract for catalog changes (RAG reindex). Catalog has no write path yet, so nothing produces it. */
    public record CatalogUpdated(UUID eventId, List<String> trainNumbers, Instant occurredAt) implements Event {
        public String key() { return String.join(",", trainNumbers); }
    }
}
