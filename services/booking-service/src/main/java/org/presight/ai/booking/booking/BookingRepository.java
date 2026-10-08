package org.presight.ai.booking.booking;

import io.r2dbc.spi.Readable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.presight.ai.booking.booking.Booking.SagaState;
import org.presight.ai.booking.booking.Booking.Status;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
class BookingRepository {

    private final DatabaseClient db;

    BookingRepository(DatabaseClient db) {
        this.db = db;
    }

    Mono<Booking> insert(Booking b, String idempotencyKey) {
        return db.sql("""
                        INSERT INTO booking (id, pnr, hold_id, status, train_number, travel_date, fare_class, seat_no,
                                             from_station, to_station, departure, arrival, passenger_name, contact_email,
                                             amount, currency, idempotency_key, request_hash)
                        VALUES (:id, :pnr, :holdId, :status, :train, :date, :fareClass, :seatNo,
                                :from, :to, :departure, :arrival, :passenger, :email,
                                :amount, :currency, :key, :hash)
                        """)
                .bind("id", b.id())
                .bind("pnr", b.pnr())
                .bind("holdId", b.holdId())
                .bind("status", b.status().name())
                .bind("train", b.trainNumber())
                .bind("date", b.travelDate())
                .bind("fareClass", b.fareClass())
                .bind("seatNo", b.seatNo())
                .bind("from", b.fromStation())
                .bind("to", b.toStation())
                .bind("departure", b.departure())
                .bind("arrival", b.arrival())
                .bind("passenger", b.passengerName())
                .bind("email", b.contactEmail())
                .bind("amount", b.amount())
                .bind("currency", b.currency())
                .bind("key", idempotencyKey)
                .bind("hash", b.requestHash())
                .then()
                .then(findById(b.id()));
    }

    Mono<Booking> findById(UUID id) {
        return db.sql("SELECT * FROM booking WHERE id = :id").bind("id", id).map(BookingRepository::booking).one();
    }

    Mono<Booking> findByIdempotencyKey(String key) {
        return db.sql("SELECT * FROM booking WHERE idempotency_key = :key").bind("key", key)
                .map(BookingRepository::booking).one();
    }

    /** Optimistic transition: succeeds only if nobody changed the booking since {@code expectedVersion} was read. */
    Mono<Long> cancel(UUID id, long expectedVersion, String reason) {
        return db.sql("""
                        UPDATE booking
                        SET status = 'CANCELLED', saga_state = 'CANCELLED', cancel_reason = :reason,
                            version = version + 1, updated_at = now()
                        WHERE id = :id AND version = :version AND status <> 'CANCELLED'
                        """)
                .bind("id", id)
                .bind("version", expectedVersion)
                .bind("reason", io.r2dbc.spi.Parameters.in(io.r2dbc.spi.R2dbcType.VARCHAR, reason))
                .fetch()
                .rowsUpdated();
    }

    /**
     * A saga step: moves {@code from → to} only if the booking is still in {@code from}. Zero rows means the
     * event is stale or out of order (e.g. the user cancelled first) and the step must not apply.
     */
    Mono<Long> advance(UUID id, SagaState from, SagaState to, Status status, String cancelReason) {
        return db.sql("""
                        UPDATE booking
                        SET saga_state = :to, status = :status, cancel_reason = COALESCE(:reason, cancel_reason),
                            version = version + 1, updated_at = now()
                        WHERE id = :id AND saga_state = :from
                        """)
                .bind("id", id)
                .bind("from", from.name())
                .bind("to", to.name())
                .bind("status", status.name())
                .bind("reason", io.r2dbc.spi.Parameters.in(io.r2dbc.spi.R2dbcType.VARCHAR, cancelReason))
                .fetch()
                .rowsUpdated();
    }

    private static Booking booking(Readable row) {
        return new Booking(
                row.get("id", UUID.class),
                row.get("pnr", String.class),
                row.get("hold_id", UUID.class),
                Status.valueOf(row.get("status", String.class)),
                SagaState.valueOf(row.get("saga_state", String.class)),
                row.get("train_number", String.class),
                row.get("travel_date", LocalDate.class),
                row.get("fare_class", String.class),
                row.get("seat_no", String.class),
                row.get("from_station", String.class),
                row.get("to_station", String.class),
                row.get("departure", OffsetDateTime.class),
                row.get("arrival", OffsetDateTime.class),
                row.get("passenger_name", String.class),
                row.get("contact_email", String.class),
                row.get("amount", BigDecimal.class),
                row.get("currency", String.class),
                row.get("cancel_reason", String.class),
                row.get("request_hash", String.class),
                row.get("version", Long.class),
                row.get("created_at", OffsetDateTime.class).toInstant());
    }
}
