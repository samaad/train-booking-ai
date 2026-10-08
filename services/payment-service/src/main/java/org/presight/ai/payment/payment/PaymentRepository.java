package org.presight.ai.payment.payment;

import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Readable;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.presight.ai.payment.payment.PaymentIntent.Status;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
class PaymentRepository {

    private final DatabaseClient db;

    PaymentRepository(DatabaseClient db) {
        this.db = db;
    }

    Mono<PaymentIntent> insertPending(UUID id, UUID bookingId, BigDecimal amount, String currency,
                                      String paymentToken, String idempotencyKey, String requestHash) {
        return db.sql("""
                        INSERT INTO payment_intent (id, booking_id, amount, currency, status, payment_token,
                                                    idempotency_key, request_hash)
                        VALUES (:id, :bookingId, :amount, :currency, 'PENDING', :token, :key, :hash)
                        """)
                .bind("id", id)
                .bind("bookingId", bookingId)
                .bind("amount", amount)
                .bind("currency", currency)
                .bind("token", paymentToken)
                .bind("key", idempotencyKey)
                .bind("hash", requestHash)
                .then()
                .then(findById(id));
    }

    Mono<PaymentIntent> findById(UUID id) {
        return db.sql("SELECT * FROM payment_intent WHERE id = :id").bind("id", id)
                .map(PaymentRepository::intent).one();
    }

    Mono<PaymentIntent> findByIdempotencyKey(String key) {
        return db.sql("SELECT * FROM payment_intent WHERE idempotency_key = :key").bind("key", key)
                .map(PaymentRepository::intent).one();
    }

    /**
     * Moves an intent out of {@code from}. Conditional, so when two replays race to record the same
     * PSP outcome only one write lands; the loser just re-reads.
     */
    Mono<Long> transition(UUID id, Status from, Status to, String pspReference, String failureReason) {
        return db.sql("""
                        UPDATE payment_intent
                        SET status = :to,
                            psp_reference = COALESCE(:pspReference, psp_reference),
                            failure_reason = :failureReason,
                            updated_at = now()
                        WHERE id = :id AND status = :from
                        """)
                .bind("id", id)
                .bind("from", from.name())
                .bind("to", to.name())
                .bind("pspReference", Parameters.in(R2dbcType.VARCHAR, pspReference))
                .bind("failureReason", Parameters.in(R2dbcType.VARCHAR, failureReason))
                .fetch()
                .rowsUpdated();
    }

    record Charge(UUID bookingId, String userId, BigDecimal amount, String currency, boolean cancelled) {
    }

    Mono<Void> upsertCharge(UUID bookingId, String userId, BigDecimal amount, String currency) {
        return db.sql("""
                        INSERT INTO booking_charge (booking_id, user_id, amount, currency) VALUES (:id, :userId, :amount, :currency)
                        ON CONFLICT (booking_id) DO NOTHING
                        """)
                .bind("id", bookingId)
                .bind("userId", Parameters.in(R2dbcType.VARCHAR, userId))
                .bind("amount", amount)
                .bind("currency", currency)
                .then();
    }

    Mono<Charge> findCharge(UUID bookingId) {
        return db.sql("SELECT * FROM booking_charge WHERE booking_id = :id")
                .bind("id", bookingId)
                .map(r -> new Charge(r.get("booking_id", UUID.class), r.get("user_id", String.class), r.get("amount", BigDecimal.class),
                        r.get("currency", String.class), "CANCELLED".equals(r.get("status", String.class))))
                .one();
    }

    /** A tombstone if booking.created has not arrived yet: the late event then finds the booking cancelled. */
    Mono<Void> cancelCharge(UUID bookingId) {
        return db.sql("""
                        INSERT INTO booking_charge (booking_id, status) VALUES (:id, 'CANCELLED')
                        ON CONFLICT (booking_id) DO UPDATE SET status = 'CANCELLED', updated_at = now()
                        """)
                .bind("id", bookingId)
                .then();
    }

    /** The booking's non-failed payment, if any (at most one, by ux_payment_live_booking). */
    Mono<PaymentIntent> findLiveByBooking(UUID bookingId) {
        return db.sql("SELECT * FROM payment_intent WHERE booking_id = :id AND status <> 'FAILED'")
                .bind("id", bookingId)
                .map(PaymentRepository::intent)
                .one();
    }

    private static PaymentIntent intent(Readable row) {
        return new PaymentIntent(
                row.get("id", UUID.class),
                row.get("booking_id", UUID.class),
                row.get("amount", BigDecimal.class),
                row.get("currency", String.class),
                Status.valueOf(row.get("status", String.class)),
                row.get("payment_token", String.class),
                row.get("psp_reference", String.class),
                row.get("failure_reason", String.class),
                row.get("request_hash", String.class),
                row.get("created_at", OffsetDateTime.class).toInstant());
    }
}
