package org.presight.ai.booking.booking;

import java.time.Instant;
import java.util.UUID;
import org.presight.ai.booking.booking.Booking.SagaState;
import org.presight.ai.booking.booking.Booking.Status;
import org.presight.ai.booking.client.CatalogClient;
import org.presight.ai.booking.client.CatalogClient.FareRuleView;
import org.presight.ai.booking.client.CatalogClient.JourneyView;
import org.presight.ai.booking.client.InventoryClient;
import org.presight.ai.booking.client.InventoryClient.HoldView;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.id.UuidV7;
import org.presight.ai.common.idempotency.IdempotencyKey;
import org.presight.ai.common.idempotency.RequestFingerprint;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCancelled;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCreated;
import org.presight.ai.common.kafka.events.BookingEvents.CancelReason;
import org.presight.ai.common.kafka.outbox.Outbox;
import org.presight.ai.common.web.IdempotencyKeyHeader;
import org.presight.ai.common.web.RemoteCallErrors;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Service
public class BookingService {

    private static final int PNR_ATTEMPTS = 3;

    private final BookingRepository bookings;
    private final InventoryClient inventory;
    private final CatalogClient catalog;
    private final Outbox outbox;
    private final TransactionalOperator tx;

    BookingService(BookingRepository bookings, InventoryClient inventory, CatalogClient catalog, Outbox outbox,
                   TransactionalOperator tx) {
        this.bookings = bookings;
        this.inventory = inventory;
        this.catalog = catalog;
        this.outbox = outbox;
        this.tx = tx;
    }

    public record CreateBooking(UUID holdId, String fromStation, String toStation,
                                String passengerName, String contactEmail) {
    }

    /**
     * Creates a PENDING_PAYMENT booking from a live hold. The seat comes from the server-issued
     * hold, never from the request, and the price is computed here from catalog's fare rule.
     * A repeated Idempotency-Key returns the original booking instead of creating a second one.
     */
    public Mono<Booking> create(CreateBooking cmd, IdempotencyKey key) {
        String fingerprint = RequestFingerprint.of(
                cmd.holdId(), cmd.fromStation(), cmd.toStation(), cmd.passengerName(), cmd.contactEmail());
        return bookings.findByIdempotencyKey(key.value())
                .map(existing -> replay(existing, fingerprint))
                .switchIfEmpty(Mono.defer(() -> price(cmd)
                        .flatMap(draft -> insert(draft, cmd, key, fingerprint))));
    }

    public Mono<Booking> find(UUID id) {
        return bookings.findById(id).switchIfEmpty(Mono.error(() -> notFound(id)));
    }

    /**
     * Idempotent. The cancellation and its {@code booking.cancelled.v1} commit together; inventory then
     * releases the seat, payment refunds and ticketing voids — compensations driven by that one event.
     */
    public Mono<Booking> cancel(UUID id, String reason) {
        return find(id).flatMap(booking -> booking.status() == Status.CANCELLED
                ? Mono.just(booking)
                : bookings.cancel(id, booking.version(), reason)
                        .flatMap(rows -> rows == 0
                                ? Mono.error(DomainException.conflict("CONCURRENT_MODIFICATION",
                                        "Booking " + id + " changed concurrently; re-read and retry"))
                                : outbox.append(Topics.BOOKING_CANCELLED, cancelled(booking, CancelReason.USER, reason)))
                        .as(tx::transactional)
                        .then(bookings.findById(id)));
    }

    static BookingCancelled cancelled(Booking b, CancelReason reason, String detail) {
        return new BookingCancelled(UuidV7.generate(), b.id(), b.pnr(), b.holdId(), reason, detail, Instant.now());
    }

    private record Priced(HoldView hold, JourneyView journey, FareRuleView rule) {
    }

    private Mono<Priced> price(CreateBooking cmd) {
        return inventory.getHold(cmd.holdId())
                .onErrorMap(RemoteCallErrors.from("inventory"))
                .flatMap(hold -> {
                    if (!hold.isActive()) {
                        return Mono.error(DomainException.conflict("HOLD_NOT_ACTIVE",
                                "Hold " + hold.holdId() + " is " + hold.status()));
                    }
                    Mono<JourneyView> journey = catalog.search(cmd.fromStation(), cmd.toStation(), hold.travelDate())
                            .filter(j -> j.trainNumber().equals(hold.trainNumber()))
                            .next()
                            .switchIfEmpty(Mono.error(() -> DomainException.invalid("SEGMENT_NOT_SERVED",
                                    hold.trainNumber() + " does not run " + cmd.fromStation() + " → "
                                            + cmd.toStation() + " on " + hold.travelDate())));
                    Mono<FareRuleView> rule = catalog.fareRules(hold.trainNumber(), hold.fareClass())
                            .next()
                            .switchIfEmpty(Mono.error(() -> DomainException.invalid("FARE_RULE_MISSING",
                                    "No fare rule for " + hold.trainNumber() + " " + hold.fareClass())));
                    return Mono.zip(journey, rule)
                            .onErrorMap(RemoteCallErrors.from("catalog"))
                            .map(t -> new Priced(hold, t.getT1(), t.getT2()));
                });
    }

    private Mono<Booking> insert(Priced p, CreateBooking cmd, IdempotencyKey key, String fingerprint) {
        UUID id = UuidV7.generate();
        Mono<Booking> attempt = Mono.defer(() -> bookings.insert(new Booking(
                id, Pnr.next(), p.hold().holdId(), Status.PENDING_PAYMENT, SagaState.PAYMENT_PENDING,
                p.hold().trainNumber(), p.hold().travelDate(), p.hold().fareClass(), p.hold().seatNo(),
                cmd.fromStation(), cmd.toStation(), p.journey().departure(), p.journey().arrival(),
                cmd.passengerName(), cmd.contactEmail(),
                Fares.price(p.rule(), p.journey().distanceKm()), p.rule().currency(),
                null, fingerprint, 0, Instant.now()), key.value())
                // The booking and its saga-entry event commit together, or not at all.
                .flatMap(b -> outbox.append(Topics.BOOKING_CREATED, created(b)).thenReturn(b))
                .as(tx::transactional));

        return attempt
                // A PNR collision is our own uniqueness check failing — regenerating is safe. Nothing else retries.
                .retryWhen(Retry.max(PNR_ATTEMPTS - 1).filter(e -> violates(e, "uq_booking_pnr")))
                .onErrorResume(e -> violates(e, "uq_booking_idempotency_key"),
                        e -> bookings.findByIdempotencyKey(key.value()).map(existing -> replay(existing, fingerprint)))
                .onErrorMap(e -> violates(e, "uq_booking_hold"),
                        e -> DomainException.conflict("HOLD_ALREADY_BOOKED",
                                "Hold " + p.hold().holdId() + " already has a booking"));
    }

    private static BookingCreated created(Booking b) {
        return new BookingCreated(UuidV7.generate(), b.id(), b.pnr(), b.holdId(), b.trainNumber(), b.travelDate(),
                b.fareClass(), b.seatNo(), b.fromStation(), b.toStation(), b.departure(), b.passengerName(),
                b.contactEmail(), b.amount(), b.currency(), Instant.now());
    }

    private static boolean violates(Throwable e, String constraint) {
        return e instanceof DuplicateKeyException && String.valueOf(e.getMessage()).contains(constraint);
    }

    private static Booking replay(Booking existing, String fingerprint) {
        if (!existing.requestHash().equals(fingerprint)) {
            throw IdempotencyKeyHeader.reused();
        }
        return existing;
    }

    private static DomainException notFound(UUID id) {
        return DomainException.notFound("BOOKING_NOT_FOUND", "No booking " + id);
    }
}
