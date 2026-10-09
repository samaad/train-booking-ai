package org.presight.ai.booking;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Import(BookingApiTest.Containers.class)
public class BookingApiTest {

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


    @TestConfiguration(proxyBeanMethods = false)
    public static class Containers {
        @Bean
        @ServiceConnection
        PostgreSQLContainer postgres() {
            return new PostgreSQLContainer("postgres:18.6-alpine");
        }

        @Bean
        @ServiceConnection
        KafkaContainer kafka() {
            return new KafkaContainer("apache/kafka:4.3.1");
        }
    }

    static final DownstreamStub stub = new DownstreamStub();
    static final LocalDate DATE = LocalDate.now().plusDays(10);

    @DynamicPropertySource
    static void downstream(DynamicPropertyRegistry registry) {
        registry.add("spring.http.serviceclient.inventory.base-url", stub::baseUrl);
        registry.add("spring.http.serviceclient.catalog.base-url", stub::baseUrl);
    }

    @AfterAll
    static void stopStub() {
        stub.stop();
    }

    WebTestClient client;

    @BeforeEach
    void catalog() {
        stub.inventoryDown = false;
        stub.journeysJson = """
                [{"trainNumber":"ER101","trainName":"Gulf Express","from":"DXB","to":"FUJ",
                  "departure":"%sT07:55:00+04:00","arrival":"%sT09:05:00+04:00",
                  "durationMinutes":70,"distanceKm":130,"fareClasses":["BUSINESS","ECONOMY"]}]
                """.formatted(DATE, DATE);
        stub.fareRulesJson = """
                [{"id":"ER101:BUSINESS","trainNumber":"ER101","fareClass":"BUSINESS",
                  "baseFare":25.00,"perKm":0.90,"currency":"AED"}]
                """;
    }

    @Test
    void createsAPricedPendingBookingFromTheHold() {
        UUID holdId = activeHold("B4");

        create(holdId, key()).expectStatus().isCreated()
                .expectHeader().valueMatches("Location", "/bookings/.+")
                .expectBody()
                .jsonPath("$.status").isEqualTo("PENDING_PAYMENT")
                .jsonPath("$.pnr").value(pnr -> assertThat((String) pnr).matches("[A-HJ-NP-Z2-9]{6}"))
                .jsonPath("$.seatNo").isEqualTo("B4")                 // from the hold, not the request
                .jsonPath("$.amount").isEqualTo(142.00)               // 25.00 + 0.90 × 130 km
                .jsonPath("$.currency").isEqualTo("AED")
                .jsonPath("$.departure").value(d -> assertThat(java.time.OffsetDateTime.parse((String) d).toInstant())
                        .isEqualTo(java.time.OffsetDateTime.parse(DATE + "T07:55:00+04:00").toInstant()));
    }

    @Test
    void replayedKeyReturnsTheOriginalAndReuseIsRejected() {
        UUID holdId = activeHold("B5");
        String key = key();
        String first = idOf(create(holdId, key).expectStatus().isCreated());
        String replay = idOf(create(holdId, key).expectStatus().isCreated());
        assertThat(replay).isEqualTo(first);

        client.post().uri("/bookings").header("Idempotency-Key", key)
                .bodyValue(body(holdId, "Someone Else")).exchange()
                .expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.code").isEqualTo("IDEMPOTENCY_KEY_REUSED");
    }

    @Test
    void aHoldCanBeBookedOnlyOnce() {
        UUID holdId = activeHold("B6");
        create(holdId, key()).expectStatus().isCreated();

        create(holdId, key()).expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.code").isEqualTo("HOLD_ALREADY_BOOKED");
    }

    @Test
    void rejectsHoldsThatAreMissingOrNotActive() {
        create(UUID.randomUUID(), key()).expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.code").isEqualTo("HOLD_NOT_FOUND"); // downstream code preserved

        UUID expired = UUID.randomUUID();
        stub.holds.put(expired, holdJson(expired, "B7", "EXPIRED"));
        create(expired, key()).expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.code").isEqualTo("HOLD_NOT_ACTIVE");
    }

    @Test
    void rejectsASegmentTheHeldTrainDoesNotServe() {
        stub.journeysJson = "[]";
        create(activeHold("B8"), key()).expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.code").isEqualTo("SEGMENT_NOT_SERVED");
    }

    @Test
    void inventoryOutageIsA503NotA500() {
        stub.inventoryDown = true;
        create(UUID.randomUUID(), key()).expectStatus().isEqualTo(503)
                .expectBody().jsonPath("$.code").isEqualTo("INVENTORY_UNAVAILABLE");
    }

    @Test
    void cancelIsIdempotentAndKeepsTheReason() {
        String id = idOf(create(activeHold("B9"), key()).expectStatus().isCreated());

        client.post().uri("/bookings/{id}/cancel", id).bodyValue(Map.of("reason", "plans changed")).exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("CANCELLED")
                .jsonPath("$.cancelReason").isEqualTo("plans changed");
        client.post().uri("/bookings/{id}/cancel", id).exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.cancelReason").isEqualTo("plans changed");

        client.get().uri("/bookings/{id}", id).exchange()
                .expectBody().jsonPath("$.status").isEqualTo("CANCELLED");
        client.get().uri("/bookings/{id}", UUID.randomUUID()).exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("BOOKING_NOT_FOUND");
    }

    @Test
    void theUsersOwnTokenIsUsedToReadTheirHold() {
        create(activeHold("B10"), key()).expectStatus().isCreated();
        assertThat(stub.holdReadAuth).last().isEqualTo("Bearer " + USER_TOKEN); // inventory checks it is their hold
    }

    @Test
    void bookingsArePrivateToTheirOwner() {
        String id = idOf(create(activeHold("B11"), key()).expectStatus().isCreated());
        WebTestClient other = as(issuer.passenger("user-2"));

        other.get().uri("/bookings/{id}", id).exchange().expectStatus().isNotFound(); // not 403: no oracle
        other.post().uri("/bookings/{id}/cancel", id).exchange().expectStatus().isNotFound();
        as(issuer.withRoles("desk-1", "AGENT")).get().uri("/bookings/{id}", id).exchange().expectStatus().isOk();
        client.get().uri("/bookings/{id}", id).exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("PENDING_PAYMENT");
    }

    @Test
    void listsOnlyTheCallersOwnBookings() {
        String id = idOf(as(issuer.passenger("lister-1")).post().uri("/bookings").header("Idempotency-Key", key())
                .bodyValue(body(activeHold("B12"), "Lister")).exchange().expectStatus().isCreated());

        as(issuer.passenger("lister-1")).get().uri("/bookings").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(1).jsonPath("$[0].bookingId").isEqualTo(id);
        as(issuer.passenger("lister-2")).get().uri("/bookings").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(0);
    }

    @Test
    void tokensAndScopesAreRequired() {
        raw.post().uri("/bookings").header("Idempotency-Key", key()).bodyValue(body(UUID.randomUUID(), "A")).exchange()
                .expectStatus().isUnauthorized();
        as(issuer.withScopes("user-3", "booking:read")).post().uri("/bookings").header("Idempotency-Key", key())
                .bodyValue(body(UUID.randomUUID(), "A")).exchange().expectStatus().isForbidden();
        as(issuer.wrongAudience("user-1")).get().uri("/bookings/{id}", UUID.randomUUID()).exchange().expectStatus().isUnauthorized();
    }

    private WebTestClient as(String token) {
        return raw.mutate().defaultHeader("Authorization", "Bearer " + token).build();
    }

    @Test
    void validatesTheRequest() {
        client.post().uri("/bookings").header("Idempotency-Key", key())
                .bodyValue(Map.of("holdId", UUID.randomUUID().toString(), "fromStation", "DXB", "toStation", "FUJ",
                        "passengerName", "A", "contactEmail", "not-an-email"))
                .exchange().expectStatus().isBadRequest();
        client.post().uri("/bookings").bodyValue(body(UUID.randomUUID(), "A")).exchange()
                .expectStatus().isBadRequest(); // missing Idempotency-Key (with a valid token)
    }

    private UUID activeHold(String seat) {
        UUID id = UUID.randomUUID();
        stub.holds.put(id, holdJson(id, seat, "ACTIVE"));
        return id;
    }

    private static String holdJson(UUID id, String seat, String status) {
        return """
                {"holdId":"%s","trainNumber":"ER101","travelDate":"%s","fareClass":"BUSINESS",
                 "seatNo":"%s","status":"%s","expiresAt":"%s"}
                """.formatted(id, DATE, seat, status, Instant.now().plusSeconds(600));
    }

    private WebTestClient.ResponseSpec create(UUID holdId, String key) {
        return client.post().uri("/bookings").header("Idempotency-Key", key).bodyValue(body(holdId, "Aisha Khan")).exchange();
    }

    private static Map<String, Object> body(UUID holdId, String passenger) {
        return Map.of("holdId", holdId.toString(), "fromStation", "DXB", "toStation", "FUJ",
                "passengerName", passenger, "contactEmail", "aisha@example.com");
    }

    private static String idOf(WebTestClient.ResponseSpec spec) {
        return spec.expectBody(Map.class).returnResult().getResponseBody().get("bookingId").toString();
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }
}
