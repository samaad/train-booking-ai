package org.presight.ai.payment.payment;

import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.error.ErrorCategory;
import org.presight.ai.common.id.UuidV7;
import org.presight.ai.common.idempotency.IdempotencyKey;
import org.presight.ai.common.idempotency.RequestFingerprint;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.PaymentEvents.PaymentAuthorized;
import org.presight.ai.common.kafka.events.PaymentEvents.PaymentFailed;
import org.presight.ai.common.kafka.events.PaymentEvents.PaymentRefunded;
import org.presight.ai.common.kafka.outbox.Outbox;
import org.presight.ai.common.security.CallerIdentity;
import org.presight.ai.common.web.IdempotencyKeyHeader;
import org.presight.ai.payment.payment.PaymentIntent.Status;
import org.presight.ai.payment.payment.PaymentRepository.Charge;
import org.presight.ai.payment.psp.PaymentGateway.Approved;
import org.presight.ai.payment.psp.PaymentGateway.Declined;
import org.presight.ai.payment.psp.PaymentGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

/**
 * Authorises and refunds payments without ever charging twice:
 * <ol>
 *   <li>a booking may only be charged the amount booking-service priced it at (learned from
 *       {@code booking.created.v1}), and nothing once it is cancelled;</li>
 *   <li>the intent is persisted PENDING before the PSP is called, with the intent id as PSP idempotency key;</li>
 *   <li>a PSP error leaves the intent PENDING (outcome unknown) and answers 503; a replay with the original
 *       Idempotency-Key re-drives that same PSP call — never a blind retry with a fresh key;</li>
 *   <li>each settled outcome and its event commit in one transaction (transactional outbox).</li>
 * </ol>
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository payments;
    private final PaymentGateway psp;
    private final Outbox outbox;
    private final TransactionalOperator tx;
    private final Duration pspTimeout;
    private final MeterRegistry meters;

    PaymentService(PaymentRepository payments, PaymentGateway psp, Outbox outbox, TransactionalOperator tx,
                   @Value("${payment.psp-timeout}") Duration pspTimeout, MeterRegistry meters) {
        this.meters = meters;
        this.payments = payments;
        this.psp = psp;
        this.outbox = outbox;
        this.tx = tx;
        this.pspTimeout = pspTimeout;
    }

    public record Authorize(UUID bookingId, BigDecimal amount, String currency, String paymentToken) {
    }

    public Mono<PaymentIntent> authorize(Authorize cmd, IdempotencyKey key, CallerIdentity caller) {
        BigDecimal amount = cmd.amount().setScale(2, RoundingMode.UNNECESSARY);
        String fingerprint = RequestFingerprint.of(caller.userId(), cmd.bookingId(), amount.toPlainString(), cmd.currency(), cmd.paymentToken());
        return payments.findByIdempotencyKey(key.value())
                .flatMap(existing -> drive(replay(existing, fingerprint)))
                .switchIfEmpty(Mono.defer(() -> payable(cmd.bookingId(), amount, cmd.currency(), caller)
                        .then(payments.insertPending(UuidV7.generate(), cmd.bookingId(), amount, cmd.currency(),
                                cmd.paymentToken(), key.value(), fingerprint))
                        .onErrorResume(e -> violates(e, "uq_payment_idempotency_key"),
                                e -> payments.findByIdempotencyKey(key.value()).map(existing -> replay(existing, fingerprint)))
                        .onErrorMap(e -> violates(e, "ux_payment_live_booking"),
                                e -> DomainException.conflict("PAYMENT_ALREADY_EXISTS",
                                        "Booking " + cmd.bookingId() + " already has a live payment"))
                        .flatMap(this::drive)));
    }

    /** Only the booking's owner (or staff), only the amount booking-service computed, only while it is open. */
    private Mono<Void> payable(UUID bookingId, BigDecimal amount, String currency, CallerIdentity caller) {
        return payments.findCharge(bookingId)
                .switchIfEmpty(Mono.error(() -> new DomainException(ErrorCategory.UNAVAILABLE, "PAYMENT_NOT_READY",
                        "Booking " + bookingId + " is not payable yet; retry with the same Idempotency-Key")))
                .flatMap(charge -> {
                    if (charge.userId() != null && !caller.mayAccess(charge.userId())) {
                        return Mono.error(DomainException.notFound("BOOKING_NOT_FOUND", "No booking " + bookingId));
                    }
                    if (charge.cancelled()) {
                        return Mono.error(DomainException.conflict("BOOKING_CANCELLED", "Booking " + bookingId + " is cancelled"));
                    }
                    if (charge.amount().compareTo(amount) != 0 || !charge.currency().equals(currency)) {
                        return Mono.error(DomainException.invalid("AMOUNT_MISMATCH",
                                "Booking " + bookingId + " costs " + charge.amount() + " " + charge.currency()));
                    }
                    return Mono.empty();
                });
    }

    /** Idempotent: refunding a refunded intent returns it unchanged. */
    public Mono<PaymentIntent> refund(UUID id) {
        return payments.findById(id)
                .switchIfEmpty(Mono.error(() -> DomainException.notFound("PAYMENT_NOT_FOUND", "No payment " + id)))
                .flatMap(intent -> switch (intent.status()) {
                    case REFUNDED -> Mono.just(intent);
                    case AUTHORIZED -> psp.refund(intent.id(), intent.pspReference(), intent.amount(), intent.currency())
                            .timeout(pspTimeout)
                            .onErrorMap(PaymentService::pspUnavailable)
                            .flatMap(outcome -> outcome instanceof Approved
                                    ? payments.transition(id, Status.AUTHORIZED, Status.REFUNDED, null, null)
                                            .flatMap(rows -> rows == 0 ? Mono.<Void>empty() : outbox.append(Topics.PAYMENT_REFUNDED,
                                                    new PaymentRefunded(UuidV7.generate(), intent.id(), intent.bookingId(),
                                                            intent.amount(), intent.currency(), Instant.now())))
                                            .as(tx::transactional)
                                    : Mono.error(DomainException.conflict("REFUND_DECLINED", "The PSP declined the refund")))
                            .then(payments.findById(id));
                    case PENDING, FAILED -> Mono.error(DomainException.conflict("NOT_REFUNDABLE",
                            "Payment " + id + " is " + intent.status() + " and cannot be refunded"));
                });
    }

    // ------------------------------------------------------------------ saga reactions (from Kafka)

    Mono<Void> bookingCreated(UUID bookingId, String userId, BigDecimal amount, String currency) {
        return payments.upsertCharge(bookingId, userId, amount, currency);
    }

    /** Compensation: no further charges, and an authorised payment is refunded. */
    Mono<Void> bookingCancelled(UUID bookingId) {
        return payments.cancelCharge(bookingId)
                .then(payments.findLiveByBooking(bookingId))
                .filter(intent -> intent.status() == Status.AUTHORIZED)
                .flatMap(intent -> refund(intent.id())
                        .doOnSuccess(r -> meters.counter("saga.compensation", "type", "refund").increment()))
                .then();
    }

    // ------------------------------------------------------------------ internals

    /** Settles a PENDING intent with the PSP; any settled intent is returned as-is. */
    private Mono<PaymentIntent> drive(PaymentIntent intent) {
        if (intent.status() != Status.PENDING) {
            return Mono.just(intent);
        }
        return psp.authorize(intent.id(), intent.amount(), intent.currency(), intent.paymentToken())
                .timeout(pspTimeout)
                .onErrorMap(PaymentService::pspUnavailable)
                .flatMap(outcome -> settle(intent, outcome))
                .then(refundIfCancelledMeanwhile(intent.bookingId()))
                .then(payments.findById(intent.id()));
    }

    private Mono<Void> settle(PaymentIntent intent, PaymentGateway.Outcome outcome) {
        return switch (outcome) {
            case Approved a -> payments.transition(intent.id(), Status.PENDING, Status.AUTHORIZED, a.pspReference(), null)
                    .flatMap(rows -> rows == 0 ? Mono.<Void>empty() : outbox.append(Topics.PAYMENT_AUTHORIZED,
                            new PaymentAuthorized(UuidV7.generate(), intent.id(), intent.bookingId(), intent.amount(),
                                    intent.currency(), a.pspReference(), Instant.now()))
                            .doOnSuccess(v -> meters.counter("booking.funnel", "stage", "pay").increment()))
                    .as(tx::transactional);
            case Declined d -> payments.transition(intent.id(), Status.PENDING, Status.FAILED, null, d.reason())
                    .flatMap(rows -> rows == 0 ? Mono.<Void>empty() : outbox.append(Topics.PAYMENT_FAILED,
                            new PaymentFailed(UuidV7.generate(), intent.id(), intent.bookingId(), d.reason(), Instant.now())))
                    .as(tx::transactional);
        };
    }

    /**
     * The booking may have been cancelled while the PSP call was in flight — its booking.cancelled handler
     * then saw a PENDING intent and could not refund. Close that race here.
     */
    private Mono<Void> refundIfCancelledMeanwhile(UUID bookingId) {
        return payments.findCharge(bookingId)
                .filter(Charge::cancelled)
                .flatMap(c -> payments.findLiveByBooking(bookingId))
                .filter(i -> i.status() == Status.AUTHORIZED)
                .doOnNext(i -> log.info("Booking {} was cancelled during authorisation; refunding {}", bookingId, i.id()))
                .flatMap(i -> refund(i.id()))
                .then();
    }

    private static Throwable pspUnavailable(Throwable e) {
        return e instanceof DomainException ? e : new DomainException(ErrorCategory.UNAVAILABLE, "PSP_UNAVAILABLE",
                "The payment provider did not answer; retry with the same Idempotency-Key", e);
    }

    private static boolean violates(Throwable e, String constraint) {
        return e instanceof DuplicateKeyException && String.valueOf(e.getMessage()).contains(constraint);
    }

    private static PaymentIntent replay(PaymentIntent existing, String fingerprint) {
        if (!existing.requestHash().equals(fingerprint)) {
            throw IdempotencyKeyHeader.reused();
        }
        return existing;
    }
}
