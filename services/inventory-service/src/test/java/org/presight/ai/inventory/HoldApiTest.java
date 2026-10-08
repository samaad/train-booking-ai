package org.presight.ai.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Import(TestcontainersConfiguration.class)
class HoldApiTest {

    @org.springframework.test.context.DynamicPropertySource
    static void oidc(org.springframework.test.context.DynamicPropertyRegistry registry) {
        Tokens.ISSUER.register(registry);
    }


    private static final LocalDate DATE = LocalDate.now(ZoneId.of("Asia/Dubai")).plusDays(7);

    WebTestClient client;

    @Autowired
    void client(WebTestClient raw) {
        this.raw = raw;
        this.client = raw.mutate().defaultHeader("Authorization", "Bearer " + Tokens.PASSENGER).build();
    }

    WebTestClient raw;

    @Autowired
    DatabaseClient db;

    @Test
    void availabilityListsEveryFareClassOfTheLayout() {
        client.get().uri("/availability?trainNumber=ER101&travelDate={d}", DATE).exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.classes[0].fareClass").isEqualTo("BUSINESS")
                .jsonPath("$.classes[0].total").isEqualTo(12)
                .jsonPath("$.classes[0].availableSeats[0]").isEqualTo("B1")
                .jsonPath("$.classes[1].fareClass").isEqualTo("ECONOMY")
                .jsonPath("$.classes[1].total").isEqualTo(48);
    }

    @Test
    void holdCommitLifecycle() {
        String holdId = hold("ER102", "BUSINESS", "B3", key())
                .expectStatus().isCreated()
                .expectHeader().valueMatches("Location", "/holds/.+")
                .expectBody(Map.class).returnResult().getResponseBody().get("holdId").toString();

        hold("ER102", "BUSINESS", "B3", key()).expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.code").isEqualTo("SEAT_UNAVAILABLE");

        client.get().uri("/availability?trainNumber=ER102&travelDate={d}", DATE).exchange()
                .expectBody().jsonPath("$.classes[0].available").isEqualTo(11);

        asService().post().uri("/holds/{id}/commit", holdId).exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("COMMITTED");

        // Idempotent re-commit; and a committed seat cannot be released through the hold.
        asService().post().uri("/holds/{id}/commit", holdId).exchange()
                .expectStatus().isOk().expectBody().jsonPath("$.status").isEqualTo("COMMITTED");
        client.delete().uri("/holds/{id}", holdId).exchange()
                .expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.code").isEqualTo("HOLD_ALREADY_COMMITTED");
    }

    @Test
    void releaseFreesTheSeatAndIsIdempotent() {
        String holdId = holdIdOf(hold("ER102", "ECONOMY", "E7", key()).expectStatus().isCreated());

        client.delete().uri("/holds/{id}", holdId).exchange().expectStatus().isNoContent();
        client.delete().uri("/holds/{id}", holdId).exchange().expectStatus().isNoContent();
        client.get().uri("/holds/{id}", holdId).exchange()
                .expectBody().jsonPath("$.status").isEqualTo("RELEASED");
        asService().post().uri("/holds/{id}/commit", holdId).exchange()
                .expectStatus().isEqualTo(409).expectBody().jsonPath("$.code").isEqualTo("HOLD_RELEASED");

        hold("ER102", "ECONOMY", "E7", key()).expectStatus().isCreated();
    }

    @Test
    void replayedIdempotencyKeyReturnsTheOriginalHold() {
        String key = key();
        String first = holdIdOf(hold("ER201", "ECONOMY", "E1", key).expectStatus().isCreated());
        String replay = holdIdOf(hold("ER201", "ECONOMY", "E1", key).expectStatus().isCreated());

        assertThat(replay).isEqualTo(first);
        assertThat(count("SELECT count(*) AS n FROM seat_hold WHERE idempotency_key = '" + key + "'")).isEqualTo(1);

        hold("ER201", "ECONOMY", "E2", key).expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.code").isEqualTo("IDEMPOTENCY_KEY_REUSED");
    }

    @Test
    void lapsedHoldIsReclaimableAndCannotBeCommitted() {
        String stale = holdIdOf(hold("ER201", "BUSINESS", "B1", key()).expectStatus().isCreated());
        expire(stale);

        client.get().uri("/holds/{id}", stale).exchange().expectBody().jsonPath("$.status").isEqualTo("EXPIRED");
        String fresh = holdIdOf(hold("ER201", "BUSINESS", "B1", key()).expectStatus().isCreated());

        asService().post().uri("/holds/{id}/commit", stale).exchange()
                .expectStatus().isEqualTo(409).expectBody().jsonPath("$.code").isEqualTo("HOLD_EXPIRED");
        // Releasing the stale hold must not free the seat the fresh hold now owns.
        client.delete().uri("/holds/{id}", stale).exchange().expectStatus().isNoContent();
        asService().post().uri("/holds/{id}/commit", fresh).exchange().expectStatus().isOk();
    }

    @Test
    void anySeatHoldPicksTheLowestFreeSeat() {
        hold("ER301", "ECONOMY", null, key()).expectStatus().isCreated()
                .expectBody().jsonPath("$.seatNo").isEqualTo("E1");
        hold("ER301", "ECONOMY", null, key()).expectStatus().isCreated()
                .expectBody().jsonPath("$.seatNo").isEqualTo("E2");
    }

    @Test
    void rejectsBadInputAsProblems() {
        hold("NOPE1", "ECONOMY", "E1", key()).expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("TRAIN_NOT_FOUND");
        hold("ER301", "BUSINESS", "B1", key()).expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.code").isEqualTo("UNKNOWN_FARE_CLASS");
        hold("ER301", "ECONOMY", "E99", key()).expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("SEAT_NOT_FOUND");
        hold("ER301", "ECONOMY", "E1", "has space").expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.code").isEqualTo("INVALID_IDEMPOTENCY_KEY");

        client.post().uri("/holds")
                .bodyValue(Map.of("trainNumber", "ER301", "travelDate", DATE.toString(), "fareClass", "ECONOMY"))
                .exchange().expectStatus().isBadRequest(); // missing Idempotency-Key
        client.post().uri("/holds").header("Idempotency-Key", key())
                .bodyValue(Map.of("trainNumber", "ER301", "travelDate", DATE.minusDays(30).toString(), "fareClass", "ECONOMY"))
                .exchange().expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.code").isEqualTo("TRAVEL_DATE_OUT_OF_RANGE");
        client.get().uri("/holds/{id}", UUID.randomUUID()).exchange().expectStatus().isNotFound();
    }

    @Test
    void holdsArePrivateToTheirOwner() {
        String holdId = holdIdOf(hold("ER201", "ECONOMY", "E30", key()).expectStatus().isCreated());
        WebTestClient other = as(Tokens.ISSUER.passenger("user-2"));

        other.get().uri("/holds/{id}", holdId).exchange().expectStatus().isNotFound(); // not 403: no oracle
        other.delete().uri("/holds/{id}", holdId).exchange().expectStatus().isNotFound();
        as(Tokens.ISSUER.withRoles("desk-1", "AGENT")).get().uri("/holds/{id}", holdId).exchange().expectStatus().isOk();
        client.get().uri("/holds/{id}", holdId).exchange().expectStatus().isOk();
    }

    @Test
    void onlyTheBookingSagaMayCommitASeat() {
        String holdId = holdIdOf(hold("ER201", "ECONOMY", "E31", key()).expectStatus().isCreated());

        client.post().uri("/holds/{id}/commit", holdId).exchange().expectStatus().isForbidden(); // keep a seat unpaid? no
        asService().post().uri("/holds/{id}/commit", holdId).exchange().expectStatus().isOk();
    }

    @Test
    void tokensAndScopesAreRequired() {
        raw.get().uri("/availability?trainNumber=ER201&travelDate={d}", DATE).exchange().expectStatus().isOk(); // public
        raw.post().uri("/holds").header("Idempotency-Key", key())
                .bodyValue(Map.of("trainNumber", "ER201", "travelDate", DATE.toString(), "fareClass", "ECONOMY"))
                .exchange().expectStatus().isUnauthorized();
        as(Tokens.ISSUER.withScopes("user-3", "booking:read")).post().uri("/holds").header("Idempotency-Key", key())
                .bodyValue(Map.of("trainNumber", "ER201", "travelDate", DATE.toString(), "fareClass", "ECONOMY"))
                .exchange().expectStatus().isForbidden();
        as(Tokens.ISSUER.expired("user-1")).get().uri("/holds/{id}", UUID.randomUUID()).exchange().expectStatus().isUnauthorized();
    }

    @Test
    void anotherUsersIdempotencyKeyNeverReplaysTheirHold() {
        String key = key();
        hold("ER201", "ECONOMY", "E32", key).expectStatus().isCreated();

        as(Tokens.ISSUER.passenger("user-2")).post().uri("/holds").header("Idempotency-Key", key)
                .bodyValue(Map.of("trainNumber", "ER201", "travelDate", DATE.toString(), "fareClass", "ECONOMY", "seatNo", "E32"))
                .exchange().expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.code").isEqualTo("IDEMPOTENCY_KEY_REUSED");
    }

    private WebTestClient as(String token) {
        return raw.mutate().defaultHeader("Authorization", "Bearer " + token).build();
    }

    private WebTestClient asService() {
        return as(Tokens.SERVICE);
    }

    private WebTestClient.ResponseSpec hold(String train, String fareClass, String seatNo, String key) {
        var body = new java.util.HashMap<String, Object>(Map.of(
                "trainNumber", train, "travelDate", DATE.toString(), "fareClass", fareClass));
        if (seatNo != null) {
            body.put("seatNo", seatNo);
        }
        return client.post().uri("/holds").header("Idempotency-Key", key).bodyValue(body).exchange();
    }

    private static String holdIdOf(WebTestClient.ResponseSpec spec) {
        return spec.expectBody(Map.class).returnResult().getResponseBody().get("holdId").toString();
    }

    private void expire(String holdId) {
        UUID id = UUID.fromString(holdId);
        db.sql("UPDATE seat_hold SET expires_at = now() - interval '1 minute' WHERE id = :id").bind("id", id)
                .then().block();
        db.sql("UPDATE seat_inventory SET hold_expires_at = now() - interval '1 minute' WHERE hold_id = :id")
                .bind("id", id).then().block();
    }

    private long count(String sql) {
        return db.sql(sql).map(row -> row.get("n", Long.class)).one().block();
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }
}
