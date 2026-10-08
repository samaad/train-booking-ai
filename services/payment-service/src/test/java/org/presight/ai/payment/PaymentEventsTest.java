package org.presight.ai.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCancelled;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCreated;
import org.presight.ai.common.kafka.events.BookingEvents.CancelReason;
import org.presight.ai.common.kafka.events.PaymentEvents.PaymentAuthorized;
import org.presight.ai.common.kafka.events.PaymentEvents.PaymentFailed;
import org.presight.ai.common.kafka.events.PaymentEvents.PaymentRefunded;
import org.presight.ai.common.kafka.testing.KafkaTestSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.kafka.KafkaContainer;

/** payment-service's saga role against a real broker. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Import(PaymentApiTest.Config.class)
class PaymentEventsTest {

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


    private static final Duration WAIT = Duration.ofSeconds(20);

    WebTestClient client;
    @Autowired KafkaContainer kafka;
    @Autowired PaymentApiTest.FakePsp psp;

    static KafkaTestSupport events;

    @BeforeEach
    void setUp() {
        psp.down = false;
        if (events == null) {
            events = new KafkaTestSupport(kafka.getBootstrapServers(),
                    List.of(Topics.PAYMENT_AUTHORIZED, Topics.PAYMENT_FAILED, Topics.PAYMENT_REFUNDED));
        }
    }

    @AfterAll
    static void close() {
        if (events != null) {
            events.close();
        }
    }

    @Test
    void onlyTheAmountBookingComputedCanBeCharged() {
        UUID booking = UUID.randomUUID();
        pay(booking, "142.00", "tok_visa").expectStatus().isEqualTo(503)
                .expectBody().jsonPath("$.code").isEqualTo("PAYMENT_NOT_READY"); // booking.created not seen yet

        events.publish(Topics.BOOKING_CREATED, created(booking, "142.00"));
        awaitPayable(booking, "142.00");

        pay(booking, "1.00", "tok_visa").expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.code").isEqualTo("AMOUNT_MISMATCH");
        String paymentId = (String) pay(booking, "142.00", "tok_visa").expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody().get("paymentId");

        PaymentAuthorized authorized = events.await(Topics.PAYMENT_AUTHORIZED, PaymentAuthorized.class,
                e -> e.bookingId().equals(booking), WAIT);
        assertThat(authorized.paymentId()).isEqualTo(UUID.fromString(paymentId));
        assertThat(authorized.amount()).isEqualByComparingTo("142.00");
    }

    @Test
    void aDeclineEmitsPaymentFailed() {
        UUID booking = UUID.randomUUID();
        events.publish(Topics.BOOKING_CREATED, created(booking, "50.00"));
        awaitPayable(booking, "50.00");

        pay(booking, "50.00", "tok_decline").expectStatus().isCreated();

        assertThat(events.await(Topics.PAYMENT_FAILED, PaymentFailed.class, e -> e.bookingId().equals(booking), WAIT).reason())
                .isEqualTo("card_declined");
    }

    @Test
    void aCancelledBookingIsRefundedExactlyOnce() {
        UUID booking = UUID.randomUUID();
        events.publish(Topics.BOOKING_CREATED, created(booking, "75.00"));
        awaitPayable(booking, "75.00");
        pay(booking, "75.00", "tok_visa").expectStatus().isCreated();

        BookingCancelled cancelled = new BookingCancelled(UUID.randomUUID(), booking, "ABC234", UUID.randomUUID(),
                CancelReason.HOLD_EXPIRED, "hold lapsed", Instant.now());
        events.publish(Topics.BOOKING_CANCELLED, cancelled);
        events.publish(Topics.BOOKING_CANCELLED, cancelled);

        events.await(Topics.PAYMENT_REFUNDED, PaymentRefunded.class, e -> e.bookingId().equals(booking), WAIT);
        assertThat(events.settle(Topics.PAYMENT_REFUNDED, PaymentRefunded.class, Duration.ofSeconds(2)))
                .filteredOn(e -> e.bookingId().equals(booking)).hasSize(1);
        pay(booking, "75.00", "tok_visa").expectStatus().isEqualTo(409); // no new charge for a cancelled booking
    }

    @Test
    void aCancellationThatOvertakesTheCreationKeepsTheBookingUnpayable() {
        UUID booking = UUID.randomUUID();
        events.publish(Topics.BOOKING_CANCELLED, new BookingCancelled(UUID.randomUUID(), booking, "ABC234",
                UUID.randomUUID(), CancelReason.USER, null, Instant.now()));
        events.publish(Topics.BOOKING_CREATED, created(booking, "20.00"));

        long deadline = System.currentTimeMillis() + WAIT.toMillis();
        int status = 0;
        while (System.currentTimeMillis() < deadline) {
            status = pay(booking, "20.00", "tok_visa").returnResult(Void.class).getStatus().value();
            if (status == 409) {
                break;
            }
            sleep();
        }
        assertThat(status).isEqualTo(409);
    }

    private void awaitPayable(UUID booking, String amount) {
        // Probe with a deliberately wrong amount: 422 means the charge exists (and nothing is created).
        long deadline = System.currentTimeMillis() + WAIT.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (pay(booking, "0.01", "tok_visa").returnResult(Void.class).getStatus().value() == 422) {
                return;
            }
            sleep();
        }
        throw new AssertionError("booking.created for " + booking + " was not consumed");
    }

    private WebTestClient.ResponseSpec pay(UUID booking, String amount, String token) {
        return client.post().uri("/payments").header("Idempotency-Key", UUID.randomUUID().toString())
                .bodyValue(Map.of("bookingId", booking.toString(), "amount", new BigDecimal(amount),
                        "currency", "AED", "paymentToken", token))
                .exchange();
    }

    private static BookingCreated created(UUID booking, String amount) {
        return new BookingCreated(UUID.randomUUID(), booking, "user-1", "ABC234", UUID.randomUUID(), "ER101", LocalDate.now().plusDays(5),
                "BUSINESS", "B4", "DXB", "FUJ", OffsetDateTime.now().plusDays(5), "Aisha Khan", "aisha@example.com",
                new BigDecimal(amount), "AED", Instant.now());
    }

    private static void sleep() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
