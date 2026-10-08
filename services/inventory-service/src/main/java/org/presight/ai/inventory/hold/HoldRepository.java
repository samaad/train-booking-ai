package org.presight.ai.inventory.hold;

import io.r2dbc.spi.Readable;
import java.time.Duration;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.presight.ai.inventory.hold.Hold.HoldStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * Every state transition is a single conditional statement whose WHERE clause is the
 * invariant. Under READ COMMITTED, Postgres re-evaluates that WHERE clause against the
 * latest row version after waiting on a concurrent writer, so of N racing claims on one
 * seat exactly one matches. No application-level lock — and no Redis — is involved.
 */
@Repository
class HoldRepository {

    /** A seat is claimable when free, or when its hold has lapsed by the database clock. */
    private static final String CLAIMABLE = "(status = 'AVAILABLE' OR (status = 'HELD' AND hold_expires_at < now()))";

    private static final String HOLD_COLUMNS = """
            id, train_number, travel_date, fare_class, seat_no, expires_at, request_hash,
            CASE WHEN status = 'ACTIVE' AND expires_at <= now() THEN 'EXPIRED' ELSE status END AS effective_status
            """;

    private final DatabaseClient db;

    HoldRepository(DatabaseClient db) {
        this.db = db;
    }

    record ClaimedSeat(UUID seatId, String seatNo, String fareClass, OffsetDateTime expiresAt) {
    }

    Mono<ClaimedSeat> claimSeat(UUID holdId, String train, LocalDate date, String fareClass, String seatNo, Duration ttl) {
        return db.sql("""
                        UPDATE seat_inventory
                        SET status = 'HELD', hold_id = :holdId,
                            hold_expires_at = now() + (:ttlSeconds * interval '1 second'), version = version + 1
                        WHERE train_number = :train AND travel_date = :date AND fare_class = :fareClass
                          AND seat_no = :seatNo AND %s
                        RETURNING id, seat_no, fare_class, hold_expires_at
                        """.formatted(CLAIMABLE))
                .bind("holdId", holdId)
                .bind("ttlSeconds", ttl.toSeconds())
                .bind("train", train)
                .bind("date", date)
                .bind("fareClass", fareClass)
                .bind("seatNo", seatNo)
                .map(HoldRepository::claimedSeat)
                .one();
    }

    /** Takes the lowest free seat; SKIP LOCKED lets concurrent callers pick different seats instead of queueing. */
    Mono<ClaimedSeat> claimAnySeat(UUID holdId, String train, LocalDate date, String fareClass, Duration ttl) {
        return db.sql("""
                        UPDATE seat_inventory
                        SET status = 'HELD', hold_id = :holdId,
                            hold_expires_at = now() + (:ttlSeconds * interval '1 second'), version = version + 1
                        WHERE id = (
                            SELECT id FROM seat_inventory
                            WHERE train_number = :train AND travel_date = :date AND fare_class = :fareClass AND %s
                            ORDER BY length(seat_no), seat_no
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED)
                        RETURNING id, seat_no, fare_class, hold_expires_at
                        """.formatted(CLAIMABLE))
                .bind("holdId", holdId)
                .bind("ttlSeconds", ttl.toSeconds())
                .bind("train", train)
                .bind("date", date)
                .bind("fareClass", fareClass)
                .map(HoldRepository::claimedSeat)
                .one();
    }

    Mono<Boolean> seatExists(String train, LocalDate date, String fareClass, String seatNo) {
        return db.sql("""
                        SELECT EXISTS (SELECT 1 FROM seat_inventory
                                       WHERE train_number = :train AND travel_date = :date
                                         AND fare_class = :fareClass AND seat_no = :seatNo) AS present
                        """)
                .bind("train", train)
                .bind("date", date)
                .bind("fareClass", fareClass)
                .bind("seatNo", seatNo)
                .map(row -> Boolean.TRUE.equals(row.get("present", Boolean.class)))
                .one();
    }

    Mono<Hold> insertHold(UUID holdId, String train, LocalDate date, ClaimedSeat seat,
                          String idempotencyKey, String requestHash) {
        return db.sql("""
                        INSERT INTO seat_hold (id, seat_id, train_number, travel_date, fare_class, seat_no,
                                               status, expires_at, idempotency_key, request_hash)
                        VALUES (:id, :seatId, :train, :date, :fareClass, :seatNo,
                                'ACTIVE', :expiresAt, :key, :hash)
                        """)
                .bind("id", holdId)
                .bind("seatId", seat.seatId())
                .bind("train", train)
                .bind("date", date)
                .bind("fareClass", seat.fareClass())
                .bind("seatNo", seat.seatNo())
                .bind("expiresAt", seat.expiresAt())
                .bind("key", idempotencyKey)
                .bind("hash", requestHash)
                .then()
                .then(findById(holdId));
    }

    Mono<Hold> findById(UUID holdId) {
        return db.sql("SELECT " + HOLD_COLUMNS + " FROM seat_hold WHERE id = :id")
                .bind("id", holdId)
                .map(HoldRepository::hold)
                .one();
    }

    Mono<Hold> findByIdempotencyKey(String idempotencyKey) {
        return db.sql("SELECT " + HOLD_COLUMNS + " FROM seat_hold WHERE idempotency_key = :key")
                .bind("key", idempotencyKey)
                .map(HoldRepository::hold)
                .one();
    }

    /** Row-locks the hold so commit and release of the same hold serialise. Call inside a transaction. */
    Mono<Hold> lockById(UUID holdId) {
        return db.sql("SELECT " + HOLD_COLUMNS + " FROM seat_hold WHERE id = :id FOR UPDATE")
                .bind("id", holdId)
                .map(HoldRepository::hold)
                .one();
    }

    /** Commits only a live hold — an expired one may already belong to someone else. */
    Mono<Long> commitSeat(UUID holdId) {
        return db.sql("""
                        UPDATE seat_inventory
                        SET status = 'COMMITTED', hold_expires_at = NULL, version = version + 1
                        WHERE hold_id = :holdId AND status = 'HELD' AND hold_expires_at > now()
                        """)
                .bind("holdId", holdId)
                .fetch()
                .rowsUpdated();
    }

    /** No-op if the seat was already reclaimed by a newer hold after this one lapsed. */
    Mono<Long> releaseSeat(UUID holdId) {
        return db.sql("""
                        UPDATE seat_inventory
                        SET status = 'AVAILABLE', hold_id = NULL, hold_expires_at = NULL, version = version + 1
                        WHERE hold_id = :holdId AND status = 'HELD'
                        """)
                .bind("holdId", holdId)
                .fetch()
                .rowsUpdated();
    }

    /** Cancellation compensation: frees the seat this hold owns, held or committed. */
    Mono<Long> releaseSeatHeldOrCommitted(UUID holdId) {
        return db.sql("""
                        UPDATE seat_inventory
                        SET status = 'AVAILABLE', hold_id = NULL, hold_expires_at = NULL, version = version + 1
                        WHERE hold_id = :holdId AND status IN ('HELD', 'COMMITTED')
                        """)
                .bind("holdId", holdId)
                .fetch()
                .rowsUpdated();
    }

    /** Lapsed ACTIVE holds, locked so concurrent sweepers take disjoint batches. Call inside a transaction. */
    reactor.core.publisher.Flux<Hold> lockExpired(int limit) {
        return db.sql("SELECT " + HOLD_COLUMNS + " FROM seat_hold WHERE status = 'ACTIVE' AND expires_at < now() "
                        + "ORDER BY expires_at LIMIT :limit FOR UPDATE SKIP LOCKED")
                .bind("limit", limit)
                .map(HoldRepository::hold)
                .all();
    }

    Mono<Long> updateStatus(UUID holdId, HoldStatus status) {
        return db.sql("UPDATE seat_hold SET status = :status, updated_at = now() WHERE id = :id")
                .bind("status", status.name())
                .bind("id", holdId)
                .fetch()
                .rowsUpdated();
    }

    private static ClaimedSeat claimedSeat(Readable row) {
        return new ClaimedSeat(
                row.get("id", UUID.class),
                row.get("seat_no", String.class),
                row.get("fare_class", String.class),
                row.get("hold_expires_at", OffsetDateTime.class));
    }

    private static Hold hold(Readable row) {
        return new Hold(
                row.get("id", UUID.class),
                row.get("train_number", String.class),
                row.get("travel_date", LocalDate.class),
                row.get("fare_class", String.class),
                row.get("seat_no", String.class),
                HoldStatus.valueOf(row.get("effective_status", String.class)),
                row.get("expires_at", OffsetDateTime.class).toInstant(),
                row.get("request_hash", String.class));
    }
}
