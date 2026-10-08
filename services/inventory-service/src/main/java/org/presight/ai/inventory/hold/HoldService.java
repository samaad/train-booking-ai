package org.presight.ai.inventory.hold;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.id.UuidV7;
import org.presight.ai.common.idempotency.IdempotencyKey;
import org.presight.ai.common.idempotency.RequestFingerprint;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.SeatEvents.ReleaseReason;
import org.presight.ai.common.kafka.events.SeatEvents.SeatCommitted;
import org.presight.ai.common.kafka.events.SeatEvents.SeatHeld;
import org.presight.ai.common.kafka.events.SeatEvents.SeatReleased;
import org.presight.ai.common.kafka.outbox.Outbox;
import org.presight.ai.common.security.CallerIdentity;
import org.presight.ai.common.web.IdempotencyKeyHeader;
import org.presight.ai.inventory.availability.SeatLayout;
import org.presight.ai.inventory.config.InventoryProperties;
import org.presight.ai.inventory.hold.Hold.HoldStatus;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@Service
public class HoldService {

    private static final String IDEMPOTENCY_CONSTRAINT = "uq_seat_hold_idempotency_key";

    private final HoldRepository holds;
    private final SeatLayout layout;
    private final SeatContentionFilter contentionFilter;
    private final TransactionalOperator tx;
    private final Outbox outbox;
    private final Duration holdTtl;
    private final MeterRegistry meters;

    HoldService(HoldRepository holds, SeatLayout layout, SeatContentionFilter contentionFilter,
                TransactionalOperator tx, Outbox outbox, InventoryProperties properties,
                MeterRegistry meters) {
        this.meters = meters;
        this.holds = holds;
        this.layout = layout;
        this.contentionFilter = contentionFilter;
        this.tx = tx;
        this.outbox = outbox;
        this.holdTtl = properties.holdTtl();
    }

    public record HoldCommand(String userId, String trainNumber, LocalDate travelDate, String fareClass, String seatNo) {
    }

    /**
     * Places a hold, or replays the original outcome for a repeated idempotency key. A replay
     * never claims a second seat — that is what makes a client or AI-tool retry safe.
     */
    public Mono<Hold> hold(HoldCommand cmd, IdempotencyKey key) {
        // The owner is part of the fingerprint: another user's request with the same key is a reuse, never a replay
        // that hands them someone else's hold.
        String fingerprint = RequestFingerprint.of(cmd.userId(), cmd.trainNumber(), cmd.travelDate(), cmd.fareClass(), cmd.seatNo());
        return layout.ensureSeats(cmd.trainNumber(), cmd.travelDate(), cmd.fareClass())
                .then(holds.findByIdempotencyKey(key.value()))
                .map(existing -> replay(existing, fingerprint))
                .switchIfEmpty(Mono.defer(() -> claim(cmd, key, fingerprint)));
    }

    /** Owner, staff or a service; anyone else gets 404 — a hold id is not an existence oracle. */
    public Mono<Hold> find(UUID holdId, CallerIdentity caller) {
        return holds.findById(holdId)
                .filter(h -> mayAccess(h, caller))
                .switchIfEmpty(Mono.error(() -> holdNotFound(holdId)));
    }

    /** Owner or staff only. */
    public Mono<Void> release(UUID holdId, CallerIdentity caller) {
        return find(holdId, caller).then(release(holdId));
    }

    private static boolean mayAccess(Hold hold, CallerIdentity caller) {
        return caller.hasRole("SERVICE") || caller.mayAccess(hold.userId());
    }

    /** Idempotent: committing an already-committed hold returns it unchanged. */
    public Mono<Hold> commit(UUID holdId) {
        return holds.lockById(holdId)
                .switchIfEmpty(Mono.error(() -> holdNotFound(holdId)))
                .flatMap(hold -> switch (hold.status()) {
                    case COMMITTED -> Mono.just(hold);
                    case RELEASED -> Mono.error(DomainException.conflict("HOLD_RELEASED", "Hold " + holdId + " was released"));
                    case EXPIRED -> Mono.error(expired(holdId));
                    case ACTIVE -> holds.commitSeat(holdId)
                            .flatMap(rows -> rows == 0
                                    ? Mono.error(expired(holdId)) // lapsed between the lock and the update
                                    : holds.updateStatus(holdId, HoldStatus.COMMITTED))
                            .then(outbox.append(Topics.SEAT_COMMITTED, new SeatCommitted(UuidV7.generate(), hold.id(),
                                    hold.trainNumber(), hold.travelDate(), hold.fareClass(), hold.seatNo(), Instant.now())))
                            .then(holds.findById(holdId));
                })
                .as(tx::transactional);
    }

    /** Idempotent: releasing a released or lapsed hold succeeds. A committed seat is never released here. */
    public Mono<Void> release(UUID holdId) {
        return holds.lockById(holdId)
                .switchIfEmpty(Mono.error(() -> holdNotFound(holdId)))
                .flatMap(hold -> switch (hold.status()) {
                    case RELEASED -> Mono.<Void>empty();
                    case COMMITTED -> Mono.<Void>error(DomainException.conflict("HOLD_ALREADY_COMMITTED",
                            "Hold " + holdId + " is committed to a booking; cancel the booking instead"));
                    case ACTIVE, EXPIRED -> holds.releaseSeat(holdId)
                            .flatMap(rows -> released(hold, rows, ReleaseReason.RELEASED))
                            .then(holds.updateStatus(holdId, HoldStatus.RELEASED))
                            .then();
                })
                .as(tx::transactional);
    }

    /**
     * Compensation for {@code booking.cancelled.v1}: frees the seat whether it was still held or already
     * committed to the now-cancelled booking. Idempotent; an unknown hold is a no-op (nothing to free).
     */
    public Mono<Void> releaseForCancelledBooking(UUID holdId) {
        return holds.lockById(holdId)
                .flatMap(hold -> hold.status() == HoldStatus.RELEASED
                        ? Mono.<Void>empty()
                        : holds.releaseSeatHeldOrCommitted(holdId)
                                .flatMap(rows -> released(hold, rows, ReleaseReason.BOOKING_CANCELLED))
                                .doOnSuccess(v -> meters.counter("saga.compensation", "type", "seat_release").increment())
                                .then(holds.updateStatus(holdId, HoldStatus.RELEASED))
                                .then())
                .as(tx::transactional);
    }

    /** Called by the sweeper inside its batch transaction. */
    Mono<Void> expire(Hold hold) {
        return holds.releaseSeat(hold.id())
                .flatMap(rows -> released(hold, rows, ReleaseReason.EXPIRED))
                .doOnSuccess(v -> meters.counter("seat.hold.expired").increment())
                .then(holds.updateStatus(hold.id(), HoldStatus.EXPIRED))
                .then();
    }

    /** Emits seat.released only when a seat row actually changed (a lapsed hold's seat may belong to someone else now). */
    private Mono<Void> released(Hold hold, long rows, ReleaseReason reason) {
        return rows == 0 ? Mono.empty() : outbox.append(Topics.SEAT_RELEASED, new SeatReleased(UuidV7.generate(),
                hold.id(), hold.trainNumber(), hold.travelDate(), hold.fareClass(), hold.seatNo(), reason, Instant.now()));
    }

    private Mono<Hold> claim(HoldCommand cmd, IdempotencyKey key, String fingerprint) {
        UUID holdId = UuidV7.generate();
        Mono<HoldRepository.ClaimedSeat> seat = cmd.seatNo() == null
                ? holds.claimAnySeat(holdId, cmd.trainNumber(), cmd.travelDate(), cmd.fareClass(), holdTtl)
                        .switchIfEmpty(Mono.error(() -> DomainException.conflict("SOLD_OUT",
                                "No " + cmd.fareClass() + " seats left on " + cmd.trainNumber() + " for " + cmd.travelDate())))
                : holds.claimSeat(holdId, cmd.trainNumber(), cmd.travelDate(), cmd.fareClass(), cmd.seatNo(), holdTtl)
                        .switchIfEmpty(Mono.defer(() -> seatNotClaimable(cmd)));

        Mono<Hold> attempt = seat
                .flatMap(s -> holds.insertHold(holdId, cmd.trainNumber(), cmd.travelDate(), s, key.value(), fingerprint, cmd.userId()))
                .flatMap(h -> outbox.append(Topics.SEAT_HELD, new SeatHeld(UuidV7.generate(), h.id(), h.trainNumber(),
                        h.travelDate(), h.fareClass(), h.seatNo(), h.expiresAt(), Instant.now())).thenReturn(h))
                .as(tx::transactional)
                .doOnNext(h -> {
                    meters.counter("booking.funnel", "stage", "hold").increment();
                    meters.counter("seat.hold.placed").increment(); // not "created": Prometheus reserves the _created suffix
                })
                .doOnError(e -> e instanceof DomainException d && "SEAT_UNAVAILABLE".equals(d.code()),
                        // A lost race for a seat: per-train contention is the capacity signal (07-observability).
                        e -> meters.counter("seat.hold.contention", "train", cmd.trainNumber()).increment());

        if (cmd.seatNo() != null) {
            String seatKey = cmd.trainNumber() + ':' + cmd.travelDate() + ':' + cmd.seatNo();
            attempt = contentionFilter.guard(seatKey, holdId.toString(), attempt)
                    .doOnError(e -> e instanceof DomainException d && "SEAT_UNAVAILABLE".equals(d.code())
                                    && e.getMessage() != null && e.getMessage().contains("another request"),
                            e -> meters.counter("seat.hold.contention", "train", cmd.trainNumber()).increment());
        }

        // Same key raced in concurrently: our transaction rolled back (seat included); answer with the winner's hold.
        return attempt.onErrorResume(
                e -> e instanceof DuplicateKeyException && String.valueOf(e.getMessage()).contains(IDEMPOTENCY_CONSTRAINT),
                e -> holds.findByIdempotencyKey(key.value()).map(existing -> replay(existing, fingerprint)));
    }

    private Mono<HoldRepository.ClaimedSeat> seatNotClaimable(HoldCommand cmd) {
        return holds.seatExists(cmd.trainNumber(), cmd.travelDate(), cmd.fareClass(), cmd.seatNo())
                .flatMap(exists -> Mono.error(exists
                        ? DomainException.conflict("SEAT_UNAVAILABLE", "Seat " + cmd.seatNo() + " is not available")
                        : DomainException.notFound("SEAT_NOT_FOUND",
                                "No " + cmd.fareClass() + " seat " + cmd.seatNo() + " on " + cmd.trainNumber())));
    }

    private static Hold replay(Hold existing, String fingerprint) {
        if (!existing.requestHash().equals(fingerprint)) {
            throw IdempotencyKeyHeader.reused();
        }
        return existing;
    }

    private static DomainException holdNotFound(UUID holdId) {
        return DomainException.notFound("HOLD_NOT_FOUND", "No hold " + holdId);
    }

    private static DomainException expired(UUID holdId) {
        return DomainException.conflict("HOLD_EXPIRED", "Hold " + holdId + " has expired");
    }
}
