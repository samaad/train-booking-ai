package org.presight.ai.ticketing.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Payloads of the events ticketing produces (01-services: {@code ticket.issued.v1},
 * {@code ticket.voided.v1}). Defined now so the contract is explicit; publishing through Kafka
 * is wired in the events pass.
 */
public final class TicketEvents {

    private TicketEvents() {
    }

    public record TicketIssued(UUID eventId, UUID ticketId, UUID bookingId, String pnr, Instant occurredAt) {
        public static final String TOPIC = "ticket.issued.v1";
    }

    public record TicketVoided(UUID eventId, UUID ticketId, UUID bookingId, String pnr, Instant occurredAt) {
        public static final String TOPIC = "ticket.voided.v1";
    }
}
