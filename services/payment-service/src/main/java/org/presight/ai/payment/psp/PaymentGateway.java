package org.presight.ai.payment.psp;

import java.math.BigDecimal;
import java.util.UUID;
import reactor.core.publisher.Mono;

/**
 * The payment service provider. Both operations take our intent id as the PSP-side idempotency
 * key, so re-driving an operation whose outcome is unknown can never charge or refund twice.
 *
 * <p>An error signal means the outcome is <b>unknown</b> (timeout, PSP down) — not a decline.
 */
public interface PaymentGateway {

    sealed interface Outcome permits Approved, Declined {
    }

    record Approved(String pspReference) implements Outcome {
    }

    record Declined(String reason) implements Outcome {
    }

    Mono<Outcome> authorize(UUID idempotencyKey, BigDecimal amount, String currency, String paymentToken);

    Mono<Outcome> refund(UUID idempotencyKey, String pspReference, BigDecimal amount, String currency);
}
