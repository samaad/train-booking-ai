package org.presight.ai.assistant;

import io.netty.handler.codec.http.QueryStringDecoder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

/**
 * Stands in for catalog-, inventory- and booking-service over real HTTP, recording every request's
 * method, path, query and {@code Authorization} header so tests can prove the caller's own JWT was relayed.
 */
final class DownstreamStub {

    record Seen(String method, String path, String query, String authorization, String idempotencyKey) {
    }

    final List<Seen> seen = Collections.synchronizedList(new ArrayList<>());
    final Map<UUID, String> holds = new ConcurrentHashMap<>();
    volatile UUID nextHoldId = UUID.randomUUID();
    volatile String bookingJson;

    private final DisposableServer server = HttpServer.create().port(0)
            .route(r -> r
                    .get("/stations", (req, res) -> json(record(req), res, """
                            [{"code":"DXB","name":"Dubai","city":"Dubai"},{"code":"FUJ","name":"Fujairah","city":"Fujairah"},
                             {"code":"AUH","name":"Abu Dhabi","city":"Abu Dhabi"}]"""))
                    .get("/routes/search", (req, res) -> {
                        record(req);
                        String date = new QueryStringDecoder(req.uri()).parameters().get("date").getFirst();
                        return json(null, res, """
                                [{"trainNumber":"ER101","trainName":"Gulf Express","from":"DXB","to":"FUJ",
                                  "departure":"%sT07:55:00+04:00","arrival":"%sT09:05:00+04:00","durationMinutes":70,
                                  "distanceKm":130,"fareClasses":["BUSINESS","ECONOMY"]}]""".formatted(date, date));
                    })
                    .get("/availability", (req, res) -> json(record(req), res, """
                            {"trainNumber":"ER101","travelDate":"2026-10-20","classes":[
                              {"fareClass":"BUSINESS","total":12,"available":11,"availableSeats":["B1","B2","B3"]},
                              {"fareClass":"ECONOMY","total":48,"available":48,"availableSeats":["E1"]}]}"""))
                    .post("/holds", (req, res) -> {
                        record(req);
                        UUID id = nextHoldId;
                        String hold = holdJson(id);
                        holds.put(id, hold);
                        return res.status(201).header("Content-Type", "application/json").sendString(Mono.just(hold)).then();
                    })
                    .get("/holds/{id}", (req, res) -> {
                        record(req);
                        String hold = holds.get(UUID.fromString(req.param("id")));
                        return hold == null
                                ? res.status(404).header("Content-Type", "application/problem+json")
                                        .sendString(Mono.just("{\"status\":404,\"code\":\"HOLD_NOT_FOUND\",\"detail\":\"No hold\"}")).then()
                                : json(null, res, hold);
                    })
                    .get("/bookings/{id}", (req, res) -> json(record(req), res, bookingJson.formatted(req.param("id")))))
            .bindNow();

    String baseUrl() {
        return "http://localhost:" + server.port();
    }

    void stop() {
        server.disposeNow();
    }

    void reset() {
        seen.clear();
        holds.clear();
        nextHoldId = UUID.randomUUID();
        bookingJson = """
                {"bookingId":"%s","pnr":"ABC234","status":"PENDING_PAYMENT","trainNumber":"ER101","travelDate":"2026-10-20",
                 "fareClass":"BUSINESS","seatNo":"B4","fromStation":"DXB","toStation":"FUJ",
                 "departure":"2026-10-20T07:55:00+04:00","amount":142.00,"currency":"AED"}""";
    }

    List<Seen> seen(String method, String pathPrefix) {
        synchronized (seen) {
            return seen.stream().filter(s -> s.method().equals(method) && s.path().startsWith(pathPrefix)).toList();
        }
    }

    private static String holdJson(UUID id) {
        return """
                {"holdId":"%s","trainNumber":"ER101","travelDate":"2026-10-20","fareClass":"BUSINESS","seatNo":"B4",
                 "status":"ACTIVE","expiresAt":"%s"}""".formatted(id, Instant.now().plusSeconds(600));
    }

    private HttpServerRequest record(HttpServerRequest req) {
        String uri = req.uri();
        int q = uri.indexOf('?');
        seen.add(new Seen(req.method().name(), q < 0 ? uri : uri.substring(0, q), q < 0 ? "" : uri.substring(q + 1),
                req.requestHeaders().get("Authorization"), req.requestHeaders().get("Idempotency-Key")));
        return req;
    }

    private static Mono<Void> json(Object ignored, HttpServerResponse res, String body) {
        return res.header("Content-Type", "application/json").sendString(Mono.just(body)).then();
    }
}
