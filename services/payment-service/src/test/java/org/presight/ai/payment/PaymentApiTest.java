package org.presight.ai.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.presight.ai.payment.psp.PaymentGateway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.r2dbc.core.DatabaseClient;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Mono;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Import(PaymentApiTest.Config.class)
public class PaymentApiTest {

    static final org.presight.ai.common.security.testing.TestIssuer issuer = new org.presight.ai.common.security.testing.TestIssuer();

    @org.springframework.test.context.DynamicPropertySource
    static void oidc(org.springframework.test.context.DynamicPropertyRegistry registry) {
        issuer.register(registry);
    }

    WebTestClient raw;
    WebTestClient admin;

    @Autowired
    void tokens(WebTestClient raw) {
        this.raw = raw;
        this.client = raw.mutate().defaultHeader("Authorization", "Bearer " + issuer.passenger("user-1")).build();
        this.admin = raw.mutate().defaultHeader("Authorization", "Bearer " + issuer.withRoles("ops-1", "ADMIN")).build();
    }


    /** A PSP that honours idempotency keys, can be taken down, and counts real charges. */
    public static class FakePsp implements PaymentGateway {
        volatile boolean down;
        final Set<UUID> charged = ConcurrentHashMap.newKeySet();
        final AtomicInteger authorizeCalls = new AtomicInteger();

        @Override
        public Mono<Outcome> authorize(UUID key, BigDecimal amount, String currency, String token) {
            authorizeCalls.incrementAndGet();
            if (down) {
                return Mono.error(new IllegalStateException("PSP timeout"));
            }
            if (token.startsWith("tok_decline")) {
                return Mono.just(new Declined("card_declined"));
            }
            charged.add(key); // a set: re-driving the same key never charges twice
            return Mono.just(new Approved("psp_" + key));
        }

        @Override
        public Mono<Outcome> refund(UUID key, String pspReference, BigDecimal amount, String currency) {
            return down ? Mono.error(new IllegalStateException("PSP timeout")) : Mono.just(new Approved("refund_" + key));
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    public static class Config {
        @Bean
        @ServiceConnection
        PostgreSQLContainer postgres() {
            return new PostgreSQLContainer("postgres:18.6-alpine");
        }

        @Bean
        @Primary
        FakePsp fakePsp() {
            return new FakePsp();
        }

        @Bean
        @ServiceConnection
        KafkaContainer kafka() {
            return new KafkaContainer("apache/kafka:4.3.1");
        }
    }

    WebTestClient client;

    @Autowired
    FakePsp psp;

    @Autowired
    DatabaseClient db;

    @BeforeEach
    void pspUp() {
        psp.down = false;
    }

    @Test
    void authorizesAndReplaysTheOriginalOutcome() {
        UUID booking = UUID.randomUUID();
        String key = key();

        Map<?, ?> first = pay(booking, "142.00", "tok_visa", key).expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody();
        assertThat(first.get("status")).isEqualTo("AUTHORIZED");
        assertThat(first.get("pspReference")).asString().startsWith("psp_");

        Map<?, ?> replay = pay(booking, "142.00", "tok_visa", key).expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody();
        assertThat(replay.get("paymentId")).isEqualTo(first.get("paymentId"));

        client.post().uri("/payments").header("Idempotency-Key", key)
                .bodyValue(Map.of("bookingId", booking.toString(), "amount", new BigDecimal("999.00"),
                        "currency", "AED", "paymentToken", "tok_visa"))
                .exchange().expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.code").isEqualTo("IDEMPOTENCY_KEY_REUSED");
    }

    @Test
    void aBookingCannotHoldTwoLivePayments() {
        UUID booking = UUID.randomUUID();
        pay(booking, "50.00", "tok_visa", key()).expectStatus().isCreated();

        pay(booking, "50.00", "tok_visa", key()).expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.code").isEqualTo("PAYMENT_ALREADY_EXISTS");
    }

    @Test
    void aDeclineIsAResultAndMayBeRetriedWithANewIntent() {
        UUID booking = UUID.randomUUID();
        pay(booking, "50.00", "tok_decline", key()).expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.status").isEqualTo("FAILED")
                .jsonPath("$.failureReason").isEqualTo("card_declined");

        pay(booking, "50.00", "tok_visa", key()).expectStatus().isCreated()
                .expectBody().jsonPath("$.status").isEqualTo("AUTHORIZED");
    }

    @Test
    void unknownPspOutcomeStaysPendingAndReplayChargesExactlyOnce() {
        UUID booking = UUID.randomUUID();
        String key = key();
        psp.down = true;

        pay(booking, "75.00", "tok_visa", key).expectStatus().isEqualTo(503)
                .expectBody().jsonPath("$.code").isEqualTo("PSP_UNAVAILABLE");
        // Not marked FAILED: a second payment for the booking is still blocked.
        pay(booking, "75.00", "tok_visa", key()).expectStatus().isEqualTo(409);

        psp.down = false;
        int chargesBefore = psp.charged.size();
        String id = (String) pay(booking, "75.00", "tok_visa", key).expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody().get("paymentId");
        pay(booking, "75.00", "tok_visa", key).expectStatus().isCreated();

        assertThat(psp.charged).contains(UUID.fromString(id));
        assertThat(psp.charged.size() - chargesBefore).isEqualTo(1);
    }

    @Test
    void refundIsIdempotentAndOnlyForAuthorizedPayments() {
        String id = (String) pay(UUID.randomUUID(), "20.00", "tok_visa", key()).expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody().get("paymentId");

        client.post().uri("/payments/{id}/refund", id).exchange().expectStatus().isForbidden(); // travellers cancel instead
        admin.post().uri("/payments/{id}/refund", id).exchange()
                .expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("REFUNDED");
        admin.post().uri("/payments/{id}/refund", id).exchange()
                .expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("REFUNDED");

        String declined = (String) pay(UUID.randomUUID(), "20.00", "tok_decline", key()).expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody().get("paymentId");
        admin.post().uri("/payments/{id}/refund", declined).exchange()
                .expectStatus().isEqualTo(409).expectBody().jsonPath("$.code").isEqualTo("NOT_REFUNDABLE");
        admin.post().uri("/payments/{id}/refund", UUID.randomUUID()).exchange().expectStatus().isNotFound();
    }

    @Test
    void onlyTheBookingsOwnerMayPayForIt() {
        UUID booking = UUID.randomUUID();
        db.sql("INSERT INTO booking_charge (booking_id, user_id, amount, currency) VALUES (:id, 'user-9', 30.00, 'AED')")
                .bind("id", booking).then().block();

        client.post().uri("/payments").header("Idempotency-Key", key())
                .bodyValue(Map.of("bookingId", booking.toString(), "amount", new BigDecimal("30.00"), "currency", "AED", "paymentToken", "tok_visa"))
                .exchange().expectStatus().isNotFound().expectBody().jsonPath("$.code").isEqualTo("BOOKING_NOT_FOUND");
        raw.post().uri("/payments").header("Idempotency-Key", key())
                .bodyValue(Map.of("bookingId", booking.toString(), "amount", new BigDecimal("30.00"), "currency", "AED", "paymentToken", "tok_visa"))
                .exchange().expectStatus().isUnauthorized();
        raw.mutate().defaultHeader("Authorization", "Bearer " + issuer.passenger("user-9")).build()
                .post().uri("/payments").header("Idempotency-Key", key())
                .bodyValue(Map.of("bookingId", booking.toString(), "amount", new BigDecimal("30.00"), "currency", "AED", "paymentToken", "tok_visa"))
                .exchange().expectStatus().isCreated();
    }

    @Test
    void rejectsCardDataAndBadAmounts() {
        pay(UUID.randomUUID(), "10.00", "4111111111111111", key()).expectStatus().isBadRequest();
        pay(UUID.randomUUID(), "10.001", "tok_visa", key()).expectStatus().isBadRequest();
        pay(UUID.randomUUID(), "0.00", "tok_visa", key()).expectStatus().isBadRequest();
    }

    /** booking.created normally creates the charge; seeded directly here, the event path is tested separately. */
    private WebTestClient.ResponseSpec pay(UUID booking, String amount, String token, String key) {
        db.sql("INSERT INTO booking_charge (booking_id, amount, currency) VALUES (:id, :amount, 'AED') ON CONFLICT DO NOTHING")
                .bind("id", booking).bind("amount", new BigDecimal(amount).max(new BigDecimal("0.01"))).then().block();
        return client.post().uri("/payments").header("Idempotency-Key", key)
                .bodyValue(Map.of("bookingId", booking.toString(), "amount", new BigDecimal(amount),
                        "currency", "AED", "paymentToken", token))
                .exchange();
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }
}
