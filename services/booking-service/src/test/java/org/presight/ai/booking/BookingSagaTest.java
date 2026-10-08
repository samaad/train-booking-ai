package org.presight.ai.booking;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCancelled;
import org.presight.ai.common.kafka.events.BookingEvents.BookingConfirmed;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCreated;
import org.presight.ai.common.kafka.events.BookingEvents.CancelReason;
import org.presight.ai.common.kafka.events.OtherEvents.TicketIssued;
import org.presight.ai.common.kafka.events.PaymentEvents.PaymentAuthorized;
import org.presight.ai.common.kafka.events.PaymentEvents.PaymentFailed;
import org.presight.ai.common.kafka.testing.KafkaTestSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.kafka.KafkaContainer;

/** booking-service's saga steps against a real broker: what it emits, and how it reacts. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Import(BookingApiTest.Containers.class)
class BookingSagaTest {

    static final org.presight.ai.common.security.testing.TestIssuer issuer = new org.presight.ai.common.security.testing.TestIssuer();
    static final String USER_TOKEN = issuer.passenger("user-1");

    @DynamicPropertySource
    static void oidc(DynamicPropertyRegistry registry) {
        issuer.register(registry);
    }

    WebTestClient raw;

    @Autowired
    void tokens(WebTestClient raw) {
        this.raw = raw;
        this.client = raw.mutate().defaultHeader("Authorization", "Bearer " + USER_TOKEN).build();
    }


    static final DownstreamStub stub = new DownstreamStub();
    static final LocalDate DATE = LocalDate.now().plusDays(10);
    static final Duration WAIT = Duration.ofSeconds(20);

    @DynamicPropertySource
    static void downstream(DynamicPropertyRegistry registry) {
        registry.add("spring.http.serviceclient.inventory.base-url", stub::baseUrl);
        registry.add("spring.http.serviceclient.catalog.base-url", stub::baseUrl);
    }

    @AfterAll
    static void stopStub() {
        stub.stop();
        if (events != null) {
            events.close();
        }
    }

    WebTestClient client;

    @Autowired
    KafkaContainer kafka;

    static KafkaTestSupport events;

    @BeforeEach
    void setUp() {
        if (events == null) {
            events = new KafkaTestSupport(kafka.getBootstrapServers(),
                    List.of(Topics.BOOKING_CREATED, Topics.BOOKING_CONFIRMED, Topics.BOOKING_CANCELLED));
        }
        stub.journeysJson = """
                [{"trainNumber":"ER101","from":"DXB","to":"FUJ","departure":"%sT07:55:00+04:00",
                  "arrival":"%sT09:05:00+04:00","distanceKm":130,"fareClasses":["BUSINESS","ECONOMY"]}]""".formatted(DATE, DATE);
        stub.fareRulesJson = """
                [{"trainNumber":"ER101","fareClass":"BUSINESS","baseFare":25.00,"perKm":0.90,"currency":"AED"}]""";
    }

    @Test
    void creatingABookingStartsTheSaga() {
        Map<?, ?> booking = book();
        UUID id = UUID.fromString((String) booking.get("bookingId"));

        BookingCreated created = events.await(Topics.BOOKING_CREATED, BookingCreated.class, e -> e.bookingId().equals(id), WAIT);
        assertThat(created.amount()).isEqualByComparingTo("142.00");
        assertThat(created.userId()).isEqualTo("user-1"); // payment and ticketing enforce ownership from this
        assertThat(created.contactEmail()).isEqualTo("aisha@example.com");
        assertThat(booking.get("sagaState")).isEqualTo("PAYMENT_PENDING");
    }

    @Test
    void anAuthorisedPaymentCommitsTheSeatAndConfirmsExactlyOnce() {
        Map<?, ?> booking = book();
        UUID id = UUID.fromString((String) booking.get("bookingId"));
        UUID holdId = UUID.fromString((String) booking.get("holdId"));
        PaymentAuthorized authorized = new PaymentAuthorized(UUID.randomUUID(), UUID.randomUUID(), id,
                new BigDecimal("142.00"), "AED", "psp_1", Instant.now());

        events.publish(Topics.PAYMENT_AUTHORIZED, authorized);
        events.publish(Topics.PAYMENT_AUTHORIZED, authorized); // at-least-once redelivery

        BookingConfirmed confirmed = events.await(Topics.BOOKING_CONFIRMED, BookingConfirmed.class, e -> e.bookingId().equals(id), WAIT);
        assertThat(confirmed.userId()).isEqualTo("user-1");
        assertThat(stub.committed).contains(holdId);
        // No user is behind a saga step: the commit carried booking-service's own SERVICE token, not the user's.
        assertThat(stub.commitAuth).isNotEmpty().allSatisfy(auth -> {
            assertThat(auth).startsWith("Bearer ").isNotEqualTo("Bearer " + USER_TOKEN);
            String payload = new String(java.util.Base64.getUrlDecoder().decode(auth.substring(7).split("\\.")[1]));
            assertThat(payload).contains("\"SERVICE\"").contains("service-account-train-services");
        });
        assertThat(events.settle(Topics.BOOKING_CONFIRMED, BookingConfirmed.class, Duration.ofSeconds(2)))
                .filteredOn(e -> e.bookingId().equals(id)).hasSize(1);
        assertState(id, "CONFIRMED", "SEATS_COMMITTED");

        events.publish(Topics.TICKET_ISSUED, new TicketIssued(UUID.randomUUID(), UUID.randomUUID(), id, "ABC234", Instant.now()));
        awaitState(id, "TICKETED");
    }

    @Test
    void aRevokedServiceTokenIsRenewedOnceAndTheStepStillCompletes() {
        Map<?, ?> booking = book();
        UUID id = UUID.fromString((String) booking.get("bookingId"));
        stub.commitAuth.clear();
        stub.rejectNextCommit.set(true);

        events.publish(Topics.PAYMENT_AUTHORIZED, new PaymentAuthorized(UUID.randomUUID(), UUID.randomUUID(), id,
                new BigDecimal("142.00"), "AED", "psp_5", Instant.now()));

        events.await(Topics.BOOKING_CONFIRMED, BookingConfirmed.class, e -> e.bookingId().equals(id), WAIT);
        assertThat(stub.commitAuth).hasSize(2);
        assertThat(stub.commitAuth.get(1)).isNotEqualTo(stub.commitAuth.get(0)); // a freshly issued token
    }

    @Test
    void aWrongAmountNeverConfirmsASeat() {
        UUID id = UUID.fromString((String) book().get("bookingId"));

        events.publish(Topics.PAYMENT_AUTHORIZED, new PaymentAuthorized(UUID.randomUUID(), UUID.randomUUID(), id,
                new BigDecimal("1.00"), "AED", "psp_2", Instant.now()));

        BookingCancelled cancelled = events.await(Topics.BOOKING_CANCELLED, BookingCancelled.class, e -> e.bookingId().equals(id), WAIT);
        assertThat(cancelled.reason()).isEqualTo(CancelReason.AMOUNT_MISMATCH);
        assertState(id, "CANCELLED", "CANCELLED");
    }

    @Test
    void aHoldThatLapsedBeforePaymentCompensates() {
        Map<?, ?> booking = book();
        UUID id = UUID.fromString((String) booking.get("bookingId"));
        stub.expiredOnCommit.add(UUID.fromString((String) booking.get("holdId")));

        events.publish(Topics.PAYMENT_AUTHORIZED, new PaymentAuthorized(UUID.randomUUID(), UUID.randomUUID(), id,
                new BigDecimal("142.00"), "AED", "psp_3", Instant.now()));

        BookingCancelled cancelled = events.await(Topics.BOOKING_CANCELLED, BookingCancelled.class, e -> e.bookingId().equals(id), WAIT);
        assertThat(cancelled.reason()).isEqualTo(CancelReason.HOLD_EXPIRED); // payment refunds on this
        assertThat(cancelled.holdId()).isEqualTo(UUID.fromString((String) booking.get("holdId")));
    }

    @Test
    void aFailedPaymentCancelsTheBooking() {
        UUID id = UUID.fromString((String) book().get("bookingId"));

        events.publish(Topics.PAYMENT_FAILED, new PaymentFailed(UUID.randomUUID(), UUID.randomUUID(), id, "card_declined", Instant.now()));

        BookingCancelled cancelled = events.await(Topics.BOOKING_CANCELLED, BookingCancelled.class, e -> e.bookingId().equals(id), WAIT);
        assertThat(cancelled.reason()).isEqualTo(CancelReason.PAYMENT_FAILED);
    }

    @Test
    void aUserCancellationEmitsTheCompensationEventAndLatePaymentChangesNothing() {
        UUID id = UUID.fromString((String) book().get("bookingId"));
        client.post().uri("/bookings/{id}/cancel", id).exchange().expectStatus().isOk();

        assertThat(events.await(Topics.BOOKING_CANCELLED, BookingCancelled.class, e -> e.bookingId().equals(id), WAIT).reason())
                .isEqualTo(CancelReason.USER);

        // Payment authorised after the user cancelled: booking stays cancelled (payment refunds on booking.cancelled).
        events.publish(Topics.PAYMENT_AUTHORIZED, new PaymentAuthorized(UUID.randomUUID(), UUID.randomUUID(), id,
                new BigDecimal("142.00"), "AED", "psp_4", Instant.now()));
        assertThat(events.settle(Topics.BOOKING_CONFIRMED, BookingConfirmed.class, Duration.ofSeconds(3)))
                .noneMatch(e -> e.bookingId().equals(id));
        assertState(id, "CANCELLED", "CANCELLED");
    }

    private Map<?, ?> book() {
        UUID holdId = UUID.randomUUID();
        stub.holds.put(holdId, """
                {"holdId":"%s","trainNumber":"ER101","travelDate":"%s","fareClass":"BUSINESS",
                 "seatNo":"B4","status":"ACTIVE","expiresAt":"%s"}""".formatted(holdId, DATE, Instant.now().plusSeconds(600)));
        return client.post().uri("/bookings").header("Idempotency-Key", UUID.randomUUID().toString())
                .bodyValue(Map.of("holdId", holdId.toString(), "fromStation", "DXB", "toStation", "FUJ",
                        "passengerName", "Aisha Khan", "contactEmail", "aisha@example.com"))
                .exchange().expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody();
    }

    private void assertState(UUID id, String status, String sagaState) {
        client.get().uri("/bookings/{id}", id).exchange().expectBody()
                .jsonPath("$.status").isEqualTo(status)
                .jsonPath("$.sagaState").isEqualTo(sagaState);
    }

    private void awaitState(UUID id, String sagaState) {
        long deadline = System.currentTimeMillis() + WAIT.toMillis();
        String last = null;
        while (System.currentTimeMillis() < deadline) {
            last = (String) client.get().uri("/bookings/{id}", id).exchange()
                    .expectBody(Map.class).returnResult().getResponseBody().get("sagaState");
            if (sagaState.equals(last)) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new AssertionError("sagaState stayed " + last + ", expected " + sagaState);
    }
}
