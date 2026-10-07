package org.presight.ai.assistant;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * A real HTTP server speaking Ollama's API — {@code /api/chat} as streamed NDJSON (tokens and
 * {@code tool_calls}) and {@code /api/embed} — so tests exercise Spring AI's actual client, parsing and
 * our tool loop. A per-test {@link Brain} decides each reply from the request it receives.
 */
final class FakeOllama {

    sealed interface Reply {
    }

    record Tokens(List<String> tokens) implements Reply {
    }

    record Call(String name, Map<String, Object> arguments) {
    }

    record ToolCalls(List<Call> calls) implements Reply {
    }

    record Fail(int status) implements Reply {
    }

    /** Streams some tokens, then drops the connection mid-response. */
    record Abort(List<String> tokensFirst) implements Reply {
    }

    @FunctionalInterface
    interface Brain extends Function<JsonNode, Reply> {
    }

    static final Brain ECHO = request -> new Tokens(List.of("OK"));

    private static final JsonMapper JSON = JsonMapper.builder().build();

    final List<JsonNode> chatRequests = Collections.synchronizedList(new ArrayList<>());
    final List<String> embedInputs = Collections.synchronizedList(new ArrayList<>());
    volatile Brain brain = ECHO;

    private final DisposableServer server = HttpServer.create().port(0)
            .route(r -> r
                    .post("/api/chat", (req, res) -> req.receive().aggregate().asString().flatMap(body -> {
                        JsonNode request = JSON.readTree(body);
                        chatRequests.add(request);
                        Reply reply = brain.apply(request);
                        if (reply instanceof Fail f) {
                            return res.status(f.status()).send().then();
                        }
                        res.header("Content-Type", "application/x-ndjson");
                        return res.sendString(lines(reply)).then();
                    }))
                    .post("/api/embed", (req, res) -> req.receive().aggregate().asString().flatMap(body -> {
                        JsonNode request = JSON.readTree(body);
                        List<List<Float>> embeddings = new ArrayList<>();
                        for (JsonNode input : request.get("input")) {
                            embedInputs.add(input.asString());
                            embeddings.add(vector(input.asString()));
                        }
                        String json = JSON.writeValueAsString(Map.of("model", "mxbai-embed-large", "embeddings", embeddings,
                                "prompt_eval_count", 10));
                        return res.header("Content-Type", "application/json").sendString(Mono.just(json)).then();
                    })))
            .bindNow();

    String baseUrl() {
        return "http://localhost:" + server.port();
    }

    void stop() {
        server.disposeNow();
    }

    void reset(Brain brain) {
        this.brain = brain;
        chatRequests.clear();
        embedInputs.clear();
    }

    private static Flux<String> lines(Reply reply) {
        return switch (reply) {
            case Tokens t -> Flux.fromIterable(t.tokens()).map(FakeOllama::tokenLine).concatWith(Mono.just(doneLine()));
            case ToolCalls c -> Flux.just(toolCallLine(c.calls()), doneLine());
            case Abort a -> Flux.fromIterable(a.tokensFirst()).map(FakeOllama::tokenLine)
                    .concatWith(Flux.error(new IllegalStateException("simulated Ollama crash")));
            case Fail f -> Flux.empty();
        };
    }

    private static String tokenLine(String text) {
        return JSON.writeValueAsString(Map.of("model", "gemma4:latest", "created_at", "2026-10-07T00:00:00Z",
                "message", Map.of("role", "assistant", "content", text), "done", false)) + "\n";
    }

    private static String toolCallLine(List<Call> calls) {
        List<Map<String, Object>> toolCalls = calls.stream()
                .map(c -> Map.<String, Object>of("function", Map.of("name", c.name(), "arguments", c.arguments())))
                .toList();
        return JSON.writeValueAsString(Map.of("model", "gemma4:latest", "created_at", "2026-10-07T00:00:00Z",
                "message", Map.of("role", "assistant", "content", "", "tool_calls", toolCalls), "done", false)) + "\n";
    }

    private static String doneLine() {
        return JSON.writeValueAsString(Map.of("model", "gemma4:latest", "created_at", "2026-10-07T00:00:00Z",
                "message", Map.of("role", "assistant", "content", ""), "done", true, "done_reason", "stop",
                "total_duration", 1000, "prompt_eval_count", 42, "eval_count", 7)) + "\n";
    }

    /** Deterministic unit vector of 1024 dims derived from the text — enough for pgvector to rank. */
    private static List<Float> vector(String text) {
        try {
            byte[] seed = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            List<Float> v = new ArrayList<>(1024);
            double norm = 0;
            float[] raw = new float[1024];
            for (int i = 0; i < 1024; i++) {
                raw[i] = (seed[i % seed.length] * (i + 1) % 97) / 97f;
                norm += raw[i] * raw[i];
            }
            for (float f : raw) {
                v.add((float) (f / Math.sqrt(norm)));
            }
            return v;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------------------------------------------------------------- helpers for brains

    static String lastRole(JsonNode request) {
        JsonNode messages = request.get("messages");
        return messages.get(messages.size() - 1).get("role").asString();
    }

    static String lastContent(JsonNode request) {
        JsonNode messages = request.get("messages");
        return messages.get(messages.size() - 1).get("content").asString();
    }
}
