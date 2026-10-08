package org.presight.ai.ticketing.ticket;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.id.UuidV7;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.OtherEvents.TicketIssued;
import org.presight.ai.common.kafka.events.OtherEvents.TicketVoided;
import org.presight.ai.common.kafka.outbox.Outbox;
import org.presight.ai.common.security.CallerIdentity;
import org.presight.ai.ticketing.render.QrTokens;
import org.presight.ai.ticketing.render.TicketRenderer;
import org.presight.ai.ticketing.storage.ObjectStore;
import org.presight.ai.ticketing.ticket.Ticket.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

/**
 * Issues and voids tickets. {@link #issue} and {@link #voidTicket} are the entry points for the
 * {@code booking.confirmed.v1} / {@code booking.cancelled.v1} consumers (BookingEventsListener);
 * both are idempotent because Kafka delivery is at-least-once.
 */
@Service
public class TicketService {

    private static final Logger log = LoggerFactory.getLogger(TicketService.class);

    private final TicketRepository tickets;
    private final QrTokens qrTokens;
    private final TicketRenderer renderer;
    private final ObjectStore store;
    private final Outbox outbox;
    private final TransactionalOperator tx;

    TicketService(TicketRepository tickets, QrTokens qrTokens, TicketRenderer renderer, ObjectStore store,
                  Outbox outbox, TransactionalOperator tx) {
        this.tickets = tickets;
        this.qrTokens = qrTokens;
        this.renderer = renderer;
        this.store = store;
        this.outbox = outbox;
        this.tx = tx;
    }

    public record IssueTicket(UUID bookingId, String userId, String pnr, String passengerName, String trainNumber,
                              LocalDate travelDate, String fareClass, String seatNo,
                              String fromStation, String toStation, OffsetDateTime departure) {
    }

    /**
     * Issues the booking's ticket and {@code ticket.issued.v1} in one transaction. Empty if the booking was
     * already cancelled (the cancellation overtook the confirmation); a re-issue returns the existing ticket.
     */
    public Mono<Ticket> issue(IssueTicket cmd) {
        String claims = String.join("|", cmd.pnr(), cmd.bookingId().toString(), cmd.trainNumber(),
                cmd.travelDate().toString(), cmd.seatNo());
        Ticket draft = new Ticket(UuidV7.generate(), cmd.userId(), cmd.pnr(), cmd.bookingId(), Status.ISSUED,
                cmd.passengerName(), cmd.trainNumber(), cmd.travelDate(), cmd.fareClass(), cmd.seatNo(),
                cmd.fromStation(), cmd.toStation(), cmd.departure(), qrTokens.sign(claims), null, null, null);
        return tickets.isCancelled(cmd.bookingId())
                .flatMap(cancelled -> cancelled
                        ? Mono.<Ticket>empty()
                        : tickets.insertIfAbsent(draft)
                                .flatMap(inserted -> inserted
                                        ? outbox.append(Topics.TICKET_ISSUED,
                                                new TicketIssued(UuidV7.generate(), draft.id(), cmd.bookingId(), cmd.pnr(), Instant.now()))
                                        : Mono.<Void>empty())
                                .then(tickets.findByBookingId(cmd.bookingId())))
                .as(tx::transactional);
    }

    /** Voids the booking's ticket (if any) and leaves a tombstone so a late confirmation cannot issue one. */
    public Mono<Void> voidTicket(UUID bookingId) {
        return tickets.recordCancellation(bookingId)
                .then(tickets.voidByBookingId(bookingId))
                .flatMap(rows -> rows == 0 ? Mono.<Void>empty() : tickets.findByBookingId(bookingId)
                        .flatMap(t -> outbox.append(Topics.TICKET_VOIDED,
                                new TicketVoided(UuidV7.generate(), t.id(), bookingId, t.pnr(), Instant.now()))))
                .as(tx::transactional);
    }

    /** Owner or staff; anyone else gets 404 — a PNR is short and guessable, so no existence oracle. */
    public Mono<Ticket> find(String pnr, CallerIdentity caller) {
        return tickets.findByPnr(pnr)
                .filter(t -> caller.mayAccess(t.userId()))
                .switchIfEmpty(Mono.error(() -> DomainException.notFound("TICKET_NOT_FOUND", "No ticket for PNR " + pnr)));
    }

    public Mono<byte[]> qr(String pnr, CallerIdentity caller) {
        return valid(pnr, caller).flatMap(renderer::qrPng);
    }

    /**
     * Get-or-render: served from the object store when cached, otherwise rendered and stored. An
     * object-store outage degrades to rendering on every request rather than failing.
     */
    public Mono<byte[]> pdf(String pnr, CallerIdentity caller) {
        return valid(pnr, caller).flatMap(ticket -> {
            String key = "tickets/" + ticket.pnr() + ".pdf";
            Mono<byte[]> cached = ticket.pdfObjectKey() == null ? Mono.empty()
                    : store.get(ticket.pdfObjectKey()).onErrorResume(e -> {
                        log.warn("Object store read failed for {}: {}", key, e.toString());
                        return Mono.empty();
                    });
            return cached.switchIfEmpty(Mono.defer(() -> renderer.pdf(ticket)
                    .flatMap(bytes -> store.put(key, bytes, "application/pdf")
                            .then(tickets.recordPdf(ticket.id(), key))
                            .onErrorResume(e -> {
                                log.warn("Object store write failed for {}: {}", key, e.toString());
                                return Mono.empty();
                            })
                            .thenReturn(bytes))));
        });
    }

    private Mono<Ticket> valid(String pnr, CallerIdentity caller) {
        return find(pnr, caller).flatMap(t -> t.status() == Status.VOIDED
                ? Mono.error(DomainException.conflict("TICKET_VOIDED", "Ticket " + pnr + " has been voided"))
                : Mono.just(t));
    }
}
