package org.presight.ai.payment.payment;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import org.presight.ai.common.web.IdempotencyKeyHeader;
import org.presight.ai.payment.payment.PaymentIntent.Status;
import org.presight.ai.payment.payment.PaymentService.Authorize;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentService service;

    public PaymentController(PaymentService service) {
        this.service = service;
    }

    /** Takes a PSP token, never card data — PANs must not reach this service. */
    public record PaymentRequest(
            @NotNull UUID bookingId,
            @NotNull @DecimalMin(value = "0.01") @Digits(integer = 10, fraction = 2) BigDecimal amount,
            @NotNull @Pattern(regexp = "[A-Z]{3}") String currency,
            @NotNull @Pattern(regexp = "tok_[A-Za-z0-9_]{1,64}") String paymentToken) {
    }

    public record PaymentResponse(UUID paymentId, UUID bookingId, BigDecimal amount, String currency,
                                  Status status, String pspReference, String failureReason, Instant createdAt) {

        static PaymentResponse of(PaymentIntent p) {
            return new PaymentResponse(p.id(), p.bookingId(), p.amount(), p.currency(), p.status(),
                    p.pspReference(), p.failureReason(), p.createdAt());
        }
    }

    /**
     * 201 for every settled outcome — a decline is a valid result ({@code status: FAILED}), and a
     * replayed Idempotency-Key returns the same body. 503 means "unknown, retry with the same key".
     */
    @PostMapping
    public Mono<ResponseEntity<PaymentResponse>> authorize(
            @RequestHeader(IdempotencyKeyHeader.NAME) String idempotencyKey,
            @Valid @RequestBody PaymentRequest request) {
        return Mono.fromCallable(() -> IdempotencyKeyHeader.parse(idempotencyKey))
                .flatMap(key -> service.authorize(new Authorize(
                        request.bookingId(), request.amount(), request.currency(), request.paymentToken()), key))
                .map(p -> ResponseEntity.created(URI.create("/payments/" + p.id())).body(PaymentResponse.of(p)));
    }

    @PostMapping("/{paymentId}/refund")
    public Mono<PaymentResponse> refund(@PathVariable UUID paymentId) {
        return service.refund(paymentId).map(PaymentResponse::of);
    }
}
