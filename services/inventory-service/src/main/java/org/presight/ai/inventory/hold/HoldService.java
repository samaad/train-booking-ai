package org.presight.ai.inventory.hold;

import java.time.Duration;
import java.time.LocalDate;
import java.util.UUID;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.id.UuidV7;
import org.presight.ai.common.idempotency.IdempotencyKey;
import org.presight.ai.common.idempotency.RequestFingerprint;
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
    private final Duration holdTtl;

    HoldService(HoldRepository holds, SeatLayout layout, SeatContentionFilter contentionFilter,
                TransactionalOperator tx, InventoryProperties properties) {
        this.holds = holds;
        this.layout = layout;
        this.contentionFilter = contentionFilter;
        this.tx = tx;
        this.holdTtl = properties.holdTtl();
    }

    public record HoldCommand(String trainNumber, LocalDate travelDate, String fareClass, String seatNo) {
    }

    /**
     * Places a hold, or replays the original outcome for a repeated idempotency key. A replay
     * never claims a second seat — that is what makes a client or AI-tool retry safe.
     */
    public Mono<Hold> hold(HoldCommand cmd, IdempotencyKey key) {
        String fingerprint = RequestFingerprint.of(cmd.trainNumber(), cmd.travelDate(), cmd.fareClass(), cmd.seatNo());
        return layout.ensureSeats(cmd.trainNumber(), cmd.travelDate(), cmd.fareClass())
                .then(holds.findByIdempotencyKey(key.value()))
                .map(existing -> replay(existing, fingerprint))
                .switchIfEmpty(Mono.defer(() -> claim(cmd, key, fingerprint)));
    }

    public Mono<Hold> find(UUID holdId) {
        return holds.findById(holdId).switchIfEmpty(Mono.error(() -> holdNotFound(holdId)));
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
                            .then(holds.updateStatus(holdId, HoldStatus.RELEASED))
                            .then();
                })
                .as(tx::transactional);
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
                .flatMap(s -> holds.insertHold(holdId, cmd.trainNumber(), cmd.travelDate(), s, key.value(), fingerprint))
                .as(tx::transactional);

        if (cmd.seatNo() != null) {
            String seatKey = cmd.trainNumber() + ':' + cmd.travelDate() + ':' + cmd.seatNo();
            attempt = contentionFilter.guard(seatKey, holdId.toString(), attempt);
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
