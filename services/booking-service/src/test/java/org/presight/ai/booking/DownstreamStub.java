package org.presight.ai.booking;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerResponse;

/**
 * A real HTTP server standing in for inventory- and catalog-service, so tests exercise the actual
 * {@code @HttpExchange} wiring, JSON decoding and downstream error translation — not mocks of them.
 */
final class DownstreamStub {

    final Map<UUID, String> holds = new ConcurrentHashMap<>();
    volatile String journeysJson = "[]";
    volatile String fareRulesJson = "[]";
    volatile boolean inventoryDown;
    /** Holds whose commit should fail with 409 HOLD_EXPIRED (lapsed before payment arrived). */
    final java.util.Set<UUID> expiredOnCommit = ConcurrentHashMap.newKeySet();
    final java.util.Set<UUID> committed = ConcurrentHashMap.newKeySet();

    private final DisposableServer server = HttpServer.create()
            .port(0)
            .route(routes -> routes
                    .get("/holds/{id}", (req, res) -> {
                        if (inventoryDown) {
                            return res.status(HttpResponseStatus.SERVICE_UNAVAILABLE).send();
                        }
                        String hold = holds.get(UUID.fromString(req.param("id")));
                        return hold != null
                                ? json(res, HttpResponseStatus.OK, hold)
                                : problem(res, 404, "HOLD_NOT_FOUND", "No hold " + req.param("id"));
                    })
                    .post("/holds/{id}/commit", (req, res) -> {
                        UUID id = UUID.fromString(req.param("id"));
                        if (expiredOnCommit.contains(id)) {
                            return problem(res, 409, "HOLD_EXPIRED", "Hold " + id + " has expired");
                        }
                        committed.add(id);
                        String hold = holds.get(id);
                        return hold != null ? json(res, HttpResponseStatus.OK, hold.replace("ACTIVE", "COMMITTED"))
                                : problem(res, 404, "HOLD_NOT_FOUND", "No hold " + id);
                    })
                    .get("/routes/search", (req, res) -> {
                        // Strict like the real catalog: required params, ISO date — or 400.
                        var query = new QueryStringDecoder(req.uri()).parameters();
                        try {
                            LocalDate.parse(query.get("date").getFirst());
                            query.get("from").getFirst();
                            query.get("to").getFirst();
                        } catch (RuntimeException e) {
                            return problem(res, 400, null, "Type mismatch: " + req.uri());
                        }
                        return json(res, HttpResponseStatus.OK, journeysJson);
                    })
                    .get("/fares/rules", (req, res) -> json(res, HttpResponseStatus.OK, fareRulesJson)))
            .bindNow();

    String baseUrl() {
        return "http://localhost:" + server.port();
    }

    void stop() {
        server.disposeNow();
    }

    private static Mono<Void> json(HttpServerResponse res, HttpResponseStatus status, String body) {
        return res.status(status).header("Content-Type", "application/json").sendString(Mono.just(body)).then();
    }

    private static Mono<Void> problem(HttpServerResponse res, int status, String code, String detail) {
        return res.status(status).header("Content-Type", "application/problem+json")
                .sendString(Mono.just("""
                        {"status":%d,"title":"Error","detail":"%s"%s}""".formatted(status, detail,
                        code == null ? "" : ",\"code\":\"" + code + "\"")))
                .then();
    }
}
