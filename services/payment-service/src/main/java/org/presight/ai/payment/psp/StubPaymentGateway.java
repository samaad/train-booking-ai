package org.presight.ai.payment.psp;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * In-memory stand-in for a real PSP, honouring the idempotency contract: the same key always
 * yields the same outcome. Test tokens:
 * <ul>
 *   <li>{@code tok_decline…} — declined ({@code card_declined})</li>
 *   <li>{@code tok_unavailable…} — PSP error: outcome unknown</li>
 *   <li>anything else — approved</li>
 * </ul>
 */
@Component
class StubPaymentGateway implements PaymentGateway {

    private final Map<UUID, Outcome> authorizations = new ConcurrentHashMap<>();
    private final Map<UUID, Outcome> refunds = new ConcurrentHashMap<>();

    @Override
    public Mono<Outcome> authorize(UUID idempotencyKey, BigDecimal amount, String currency, String paymentToken) {
        if (paymentToken.startsWith("tok_unavailable")) {
            return Mono.error(new IllegalStateException("stub PSP unavailable"));
        }
        return Mono.fromSupplier(() -> authorizations.computeIfAbsent(idempotencyKey, k ->
                paymentToken.startsWith("tok_decline")
                        ? new Declined("card_declined")
                        : new Approved("psp_auth_" + UUID.randomUUID())));
    }

    @Override
    public Mono<Outcome> refund(UUID idempotencyKey, String pspReference, BigDecimal amount, String currency) {
        return Mono.fromSupplier(() -> refunds.computeIfAbsent(idempotencyKey, k -> new Approved("psp_refund_" + UUID.randomUUID())));
    }
}
