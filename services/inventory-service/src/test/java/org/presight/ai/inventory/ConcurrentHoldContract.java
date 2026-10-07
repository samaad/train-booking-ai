package org.presight.ai.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The roadmap's Phase 1 exit criterion: N parallel holds on one seat → exactly 1 success.
 * Run once with the Redis filter and once without, proving Postgres alone is correct.
 */
abstract class ConcurrentHoldContract {

    private static final int RACERS = 24;
    private static final LocalDate DATE = LocalDate.now(ZoneId.of("Asia/Dubai")).plusDays(14);

    @LocalServerPort
    int port;

    @Autowired
    DatabaseClient db;

    WebClient http;

    @BeforeEach
    void setUp() {
        http = WebClient.create("http://localhost:" + port);
        // Reset rather than delete: seat rows are never deleted in production, and SeatLayout
        // remembers which train-dates it has already materialised.
        db.sql("DELETE FROM seat_hold").then()
                .then(db.sql("UPDATE seat_inventory SET status = 'AVAILABLE', hold_id = NULL, hold_expires_at = NULL").then())
                .block();
    }

    record Outcome(int status, Map<String, Object> body) {
    }

    @Test
    void parallelHoldsOnOneSeatYieldExactlyOneWinner() {
        List<Outcome> outcomes = race(i -> request("ER101", "BUSINESS", "B5", UUID.randomUUID().toString()));

        assertThat(outcomes).filteredOn(o -> o.status() == 201).hasSize(1);
        assertThat(outcomes).filteredOn(o -> o.status() == 409)
                .hasSize(RACERS - 1)
                .allSatisfy(o -> assertThat(o.body()).containsEntry("code", "SEAT_UNAVAILABLE"));
        assertThat(count("SELECT count(*) AS n FROM seat_hold WHERE seat_no = 'B5'")).isEqualTo(1);
        assertThat(count("SELECT count(*) AS n FROM seat_inventory WHERE seat_no = 'B5' AND status = 'HELD'")).isEqualTo(1);
    }

    @Test
    void parallelAnySeatHoldsNeverShareASeatAndStopAtCapacity() {
        List<Outcome> outcomes = race(i -> request("ER101", "BUSINESS", null, UUID.randomUUID().toString()));

        List<Outcome> won = outcomes.stream().filter(o -> o.status() == 201).toList();
        assertThat(won).hasSize(12); // BUSINESS capacity
        assertThat(won.stream().map(o -> o.body().get("seatNo")).collect(Collectors.toSet())).hasSize(12);
        assertThat(outcomes).filteredOn(o -> o.status() == 409)
                .hasSize(RACERS - 12)
                .allSatisfy(o -> assertThat(o.body()).containsEntry("code", "SOLD_OUT"));
    }

    @Test
    void parallelRetriesWithOneIdempotencyKeyClaimOneSeat() {
        String key = UUID.randomUUID().toString();
        List<Outcome> outcomes = race(i -> request("ER101", "ECONOMY", null, key));

        assertThat(outcomes).allSatisfy(o -> assertThat(o.status()).isEqualTo(201));
        assertThat(outcomes.stream().map(o -> o.body().get("holdId")).distinct()).hasSize(1);
        assertThat(count("SELECT count(*) AS n FROM seat_inventory WHERE status = 'HELD'")).isEqualTo(1);
    }

    private List<Outcome> race(Function<Integer, Mono<Outcome>> attempt) {
        // Materialise the seat map first so every racer contends on the hold itself.
        http.get().uri("/availability?trainNumber=ER101&travelDate={d}", DATE).retrieve().toBodilessEntity().block();
        return Flux.range(0, RACERS).flatMap(attempt::apply, RACERS).collectList().block();
    }

    private Mono<Outcome> request(String train, String fareClass, String seatNo, String key) {
        Map<String, Object> body = new HashMap<>(Map.of("trainNumber", train, "travelDate", DATE.toString(), "fareClass", fareClass));
        if (seatNo != null) {
            body.put("seatNo", seatNo);
        }
        return http.post().uri("/holds").header("Idempotency-Key", key).bodyValue(body)
                .exchangeToMono(resp -> resp.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                        .map(b -> new Outcome(resp.statusCode().value(), b)));
    }

    private long count(String sql) {
        return db.sql(sql).map(row -> row.get("n", Long.class)).one().block();
    }
}
