package org.presight.ai.payment.payment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.UUID;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.error.ErrorCategory;
import org.presight.ai.common.id.UuidV7;
import org.presight.ai.common.idempotency.IdempotencyKey;
import org.presight.ai.common.idempotency.RequestFingerprint;
import org.presight.ai.common.web.IdempotencyKeyHeader;
import org.presight.ai.payment.payment.PaymentIntent.Status;
import org.presight.ai.payment.psp.PaymentGateway;
import org.presight.ai.payment.psp.PaymentGateway.Approved;
import org.presight.ai.payment.psp.PaymentGateway.Declined;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Authorises and refunds payments without ever charging twice:
 * <ol>
 *   <li>the intent is persisted PENDING before the PSP is called;</li>
 *   <li>the PSP is called with the intent id as its idempotency key;</li>
 *   <li>a PSP error leaves the intent PENDING (outcome unknown) and answers 503;</li>
 *   <li>a replay with the original Idempotency-Key re-drives that same PSP call — never a blind
 *       retry with a fresh key.</li>
 * </ol>
 */
@Service
public class PaymentService {

    private final PaymentRepository payments;
    private final PaymentGateway psp;
    private final Duration pspTimeout;

    PaymentService(PaymentRepository payments, PaymentGateway psp, @Value("${payment.psp-timeout}") Duration pspTimeout) {
        this.payments = payments;
        this.psp = psp;
        this.pspTimeout = pspTimeout;
    }

    public record Authorize(UUID bookingId, BigDecimal amount, String currency, String paymentToken) {
    }

    public Mono<PaymentIntent> authorize(Authorize cmd, IdempotencyKey key) {
        BigDecimal amount = cmd.amount().setScale(2, RoundingMode.UNNECESSARY);
        String fingerprint = RequestFingerprint.of(cmd.bookingId(), amount.toPlainString(), cmd.currency(), cmd.paymentToken());
        return payments.findByIdempotencyKey(key.value())
                .flatMap(existing -> drive(replay(existing, fingerprint)))
                .switchIfEmpty(Mono.defer(() -> payments
                        .insertPending(UuidV7.generate(), cmd.bookingId(), amount, cmd.currency(),
                                cmd.paymentToken(), key.value(), fingerprint)
                        .onErrorResume(e -> violates(e, "uq_payment_idempotency_key"),
                                e -> payments.findByIdempotencyKey(key.value()).map(existing -> replay(existing, fingerprint)))
                        .onErrorMap(e -> violates(e, "ux_payment_live_booking"),
                                e -> DomainException.conflict("PAYMENT_ALREADY_EXISTS",
                                        "Booking " + cmd.bookingId() + " already has a live payment"))
                        .flatMap(this::drive)));
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
                                    : Mono.error(DomainException.conflict("REFUND_DECLINED", "The PSP declined the refund")))
                            .then(payments.findById(id));
                    case PENDING, FAILED -> Mono.error(DomainException.conflict("NOT_REFUNDABLE",
                            "Payment " + id + " is " + intent.status() + " and cannot be refunded"));
                });
    }

    /** Settles a PENDING intent with the PSP; any settled intent is returned as-is. */
    private Mono<PaymentIntent> drive(PaymentIntent intent) {
        if (intent.status() != Status.PENDING) {
            return Mono.just(intent);
        }
        return psp.authorize(intent.id(), intent.amount(), intent.currency(), intent.paymentToken())
                .timeout(pspTimeout)
                .onErrorMap(PaymentService::pspUnavailable)
                .flatMap(outcome -> switch (outcome) {
                    case Approved a -> payments.transition(intent.id(), Status.PENDING, Status.AUTHORIZED, a.pspReference(), null);
                    case Declined d -> payments.transition(intent.id(), Status.PENDING, Status.FAILED, null, d.reason());
                })
                .then(payments.findById(intent.id()));
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
