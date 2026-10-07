package org.presight.ai.assistant;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.presight.ai.assistant.FakeOllama.Call;
import org.presight.ai.assistant.FakeOllama.ToolCalls;
import org.presight.ai.assistant.FakeOllama.Tokens;
import org.presight.ai.assistant.rag.PgVectorRag;
import org.presight.ai.common.idempotency.IdempotencyKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.mongodb.MongoDBContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import tools.jackson.databind.JsonNode;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "assistant.rag.ingest-on-startup=false",
        "assistant.rag.similarity-threshold=0.0", // fake embeddings: rank everything, assert plumbing not relevance
        "logging.level.org.presight.ai.assistant.tools.TrainTools=DEBUG"
})
@ExtendWith(OutputCaptureExtension.class)
@AutoConfigureWebTestClient(timeout = "30s")
@Import(ChatFlowTest.Containers.class)
class ChatFlowTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean
        @ServiceConnection
        PostgreSQLContainer postgres() {
            return new PostgreSQLContainer(DockerImageName.parse("pgvector/pgvector:pg18").asCompatibleSubstituteFor("postgres"));
        }

        @Bean
        @ServiceConnection
        MongoDBContainer mongo() {
            return new MongoDBContainer("mongo:8.0.32");
        }
    }

    static final FakeOllama ollama = new FakeOllama();
    static final DownstreamStub services = new DownstreamStub();
    static final TestIssuer issuer = new TestIssuer();
    static final String DATE = "2026-10-20";

    @DynamicPropertySource
    static void wiring(DynamicPropertyRegistry registry) {
        registry.add("spring.ai.ollama.base-url", ollama::baseUrl);
        registry.add("spring.http.serviceclient.catalog.base-url", services::baseUrl);
        registry.add("spring.http.serviceclient.inventory.base-url", services::baseUrl);
        registry.add("spring.http.serviceclient.booking.base-url", services::baseUrl);
        registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", issuer::issuer);
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri", issuer::jwksUri);
    }

    @AfterAll
    static void stop() {
        ollama.stop();
        services.stop();
        issuer.stop();
    }

    @Autowired
    WebTestClient client;

    @Autowired
    PgVectorRag rag;

    private static boolean ingested;

    @BeforeEach
    void reset() {
        if (!ingested) {
            assertThat(rag.ingest().block(Duration.ofSeconds(30))).isPositive();
            ingested = true;
        }
        ollama.reset(FakeOllama.ECHO);
        services.reset();
    }

    // ------------------------------------------------------------------------------------------ tool flow

    @Test
    void toolCallRunsWithTheCallersIdentityAndStreamsTheAnswer(CapturedOutput output) {
        ollama.brain = req -> "tool".equals(FakeOllama.lastRole(req))
                ? new Tokens(List.of("ER101 ", "departs ", "07:55."))
                : new ToolCalls(List.of(new Call("searchTrains", Map.of("from", "Dubai", "to", "FUJ", "date", DATE))));
        String token = issuer.token("user-a", "Aisha", "aisha@example.com");

        List<ServerSentEvent<Map<String, Object>>> events = chat(token, null, "Trains from Dubai to Fujairah on " + DATE + "?");

        assertThat(names(events)).startsWith("conversation").endsWith("done").contains("tool", "token");
        assertThat(text(events)).isEqualTo("ER101 departs 07:55.");
        Map<String, Object> tool = first(events, "tool");
        assertThat(tool).containsEntry("tool", "searchTrains").containsEntry("outcome", "success");
        // Local railway time, not UTC: the model narrates these strings verbatim.
        assertThat(tool.get("result").toString()).contains("departure=" + DATE + "T07:55+04:00");
        // The one blocking seam ran on the bounded aiScheduler — not Netty, not Schedulers.boundedElastic().
        assertThat(output.getOut()).containsPattern("tool searchTrains -> success on thread ai-\\d+");

        // The caller's own JWT went downstream — station resolution and the search itself.
        assertThat(services.seen("GET", "/routes/search")).singleElement().satisfies(s -> {
            assertThat(s.authorization()).isEqualTo("Bearer " + token);
            assertThat(s.query()).contains("from=DXB", "to=FUJ", "date=" + DATE); // ISO date, code resolved from "Dubai"
        });

        JsonNode firstRequest = ollama.chatRequests.getFirst();
        assertThat(firstRequest.at("/options/temperature").asDouble()).isEqualTo(0.2);
        assertThat(firstRequest.get("think").asBoolean()).isFalse();
        assertThat(toolNames(firstRequest)).containsExactlyInAnyOrder(
                "searchTrains", "checkAvailability", "getBookingStatus", "holdSeat", "confirmBooking", "cancelBooking");
        // Identity is not part of the schema the model sees: it cannot even express "for another user".
        assertThat(firstRequest.get("tools").toString().toLowerCase()).doesNotContain("userid", "\"user\"", "token");
        // RAG: retrieved reference material is in the system prompt, queried with mxbai's asymmetric prefix.
        String system = firstRequest.at("/messages/0/content").asString();
        assertThat(system.substring(system.indexOf("Reference material"))).contains("## ").contains("\n---\n"); // top-k corpus chunks
        assertThat(ollama.embedInputs).anySatisfy(input ->
                assertThat(input).startsWith("Represent this sentence for searching relevant passages: Trains from Dubai"));
        // Second round carried the tool result back to the model.
        assertThat(ollama.chatRequests.get(1).get("messages").toString()).contains("ER101").contains("\"role\":\"tool\"");
    }

    @Test
    void concurrentUsersNeverSeeEachOthersIdentity() {
        ollama.brain = req -> "tool".equals(FakeOllama.lastRole(req))
                ? new Tokens(List.of("done"))
                : new ToolCalls(List.of(new Call("searchTrains", Map.of("from", "DXB", "to", "FUJ",
                        "date", FakeOllama.lastContent(req).substring(FakeOllama.lastContent(req).length() - 10)))));
        Map<String, String> tokenByDate = new HashMap<>();
        List<Flux<ServerSentEvent<Map<String, Object>>>> chats = new ArrayList<>();
        for (int i = 1; i <= 8; i++) {
            String user = i % 2 == 0 ? "user-b" : "user-a";
            String token = issuer.token(user + "-" + i, user, user + "@example.com");
            String date = "2026-11-%02d".formatted(i);
            tokenByDate.put(date, "Bearer " + token);
            chats.add(chatFlux(token, null, "search " + date));
        }
        Flux.merge(chats).collectList().block(Duration.ofSeconds(30));

        assertThat(services.seen("GET", "/routes/search")).hasSize(8).allSatisfy(s -> {
            String date = s.query().replaceAll(".*date=([0-9-]+).*", "$1");
            assertThat(s.authorization()).isEqualTo(tokenByDate.get(date));
        });
    }

    // ------------------------------------------------------------------------------------------ guardrails

    @Test
    void holdThenConfirmShowsACardWithAServerDerivedKeyAndNeverBooks() {
        UUID holdId = services.nextHoldId;
        ollama.brain = req -> {
            String messages = req.get("messages").toString();
            if (!messages.contains("holdId")) {
                return new ToolCalls(List.of(new Call("holdSeat", Map.of("trainNumber", "ER101", "date", DATE, "fareClass", "BUSINESS", "seatNo", "B4"))));
            }
            if (!messages.contains("AWAITING_USER_CONFIRMATION")) {
                return new ToolCalls(List.of(new Call("confirmBooking", Map.of("holdId", holdId.toString(), "fromStation", "DXB", "toStation", "FUJ"))));
            }
            return new Tokens(List.of("Please confirm the booking card."));
        };

        List<ServerSentEvent<Map<String, Object>>> events = chat(issuer.token("user-c", "C", "c@example.com"), null, "Book B4 on ER101");

        Map<String, Object> card = first(events, "confirmation");
        assertThat(card).containsEntry("action", "CONFIRM_BOOKING").containsEntry("path", "/api/booking/bookings")
                .containsEntry("idempotencyKey", IdempotencyKey.derive("user-c", holdId.toString(), "confirmBooking").value());
        assertThat(services.seen("POST", "/holds")).singleElement()
                .satisfies(s -> assertThat(s.idempotencyKey()).startsWith("d:")); // server-derived, never model-supplied
        assertThat(services.seen("POST", "/bookings")).isEmpty();              // the model never books
    }

    @Test
    void aFabricatedHoldIdNeverReachesConfirmation() {
        String invented = UUID.randomUUID().toString(); // not inside the brain: it runs on a Netty thread
        ollama.brain = req -> "tool".equals(FakeOllama.lastRole(req))
                ? new Tokens(List.of("I could not confirm that."))
                : new ToolCalls(List.of(new Call("confirmBooking",
                        Map.of("holdId", invented, "fromStation", "DXB", "toStation", "FUJ"))));

        List<ServerSentEvent<Map<String, Object>>> events = chat(issuer.token("user-d", "D", "d@example.com"), null, "confirm it");

        assertThat(names(events)).doesNotContain("confirmation");
        assertThat(first(events, "tool")).containsEntry("outcome", "error")
                .extractingByKey("result").asString().contains("UNKNOWN_HOLD");
        assertThat(services.seen("GET", "/holds")).isEmpty(); // rejected before any service call
    }

    @Test
    void theToolBudgetIsEnforcedByTheServerNotThePrompt() {
        List<Call> calls = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            calls.add(new Call("holdSeat", Map.of("trainNumber", "ER101", "date", DATE, "fareClass", "BUSINESS", "seatNo", "B" + (i + 1))));
        }
        for (int i = 0; i < 4; i++) {
            calls.add(new Call("checkAvailability", Map.of("trainNumber", "ER101", "date", DATE, "fareClass", "BUSINESS")));
        }
        ollama.brain = req -> "tool".equals(FakeOllama.lastRole(req)) ? new Tokens(List.of("ok")) : new ToolCalls(calls);

        List<ServerSentEvent<Map<String, Object>>> events = chat(issuer.token("user-e", "E", "e@example.com"), null, "go");

        List<String> outcomes = events.stream().filter(e -> "tool".equals(e.event())).map(e -> (String) e.data().get("outcome")).toList();
        assertThat(outcomes).hasSize(7);
        assertThat(outcomes.stream().filter("success"::equals).count()).isEqualTo(5);       // max 5 calls per turn
        assertThat(services.seen("POST", "/holds")).hasSize(2);                              // max 2 mutating
    }

    @Test
    void aPriceTheToolsNeverReturnedIsFlagged() {
        String bookingId = UUID.randomUUID().toString();
        ollama.brain = req -> "tool".equals(FakeOllama.lastRole(req))
                ? new Tokens(List.of("Your fare was AED 99.00."))
                : new ToolCalls(List.of(new Call("getBookingStatus", Map.of("bookingId", bookingId))));

        List<ServerSentEvent<Map<String, Object>>> events = chat(issuer.token("user-f", "F", "f@example.com"), null, "status?");
        assertThat(first(events, "warning")).containsEntry("code", "UNVERIFIED_PRICE");

        ollama.brain = req -> "tool".equals(FakeOllama.lastRole(req))
                ? new Tokens(List.of("Your fare was AED 142.00."))
                : new ToolCalls(List.of(new Call("getBookingStatus", Map.of("bookingId", bookingId))));
        assertThat(names(chat(issuer.token("user-f", "F", "f@example.com"), null, "status?"))).doesNotContain("warning");
    }

    // ------------------------------------------------------------------------------------------ memory

    @Test
    void conversationsAreRememberedAndPrivateToTheirOwner() {
        String alice = issuer.token("user-g", "G", "g@example.com");
        ollama.brain = req -> new Tokens(List.of("first answer"));
        String conversationId = (String) first(chat(alice, null, "first question"), "conversation").get("conversationId");

        ollama.brain = req -> new Tokens(List.of("second answer"));
        chat(alice, conversationId, "second question");
        String history = ollama.chatRequests.getLast().get("messages").toString();
        assertThat(history).contains("first question", "first answer", "second question");

        String mallory = issuer.token("user-h", "H", "h@example.com");
        client.post().uri("/chat").header("Authorization", "Bearer " + mallory).accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(Map.of("conversationId", conversationId, "message", "show me")).exchange()
                .expectStatus().isNotFound()
                .expectBody(String.class).value(body -> assertThat(body).contains("CONVERSATION_NOT_FOUND"));
        client.get().uri("/conversations/{id}/messages", conversationId).header("Authorization", "Bearer " + mallory)
                .exchange().expectStatus().isNotFound();
        client.get().uri("/conversations/{id}/messages", conversationId).header("Authorization", "Bearer " + alice)
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(4);
    }

    // ------------------------------------------------------------------------------------------ failure modes

    @Test
    void ollamaDownFallsBackToDeterministicSearch() {
        ollama.brain = req -> new FakeOllama.Fail(503);
        String token = issuer.token("user-i", "I", "i@example.com");

        Map<String, Object> tier3 = first(chat(token, null, "Dubai to Fujairah tomorrow"), "fallback");
        assertThat(tier3).containsEntry("tier", 3);
        assertThat(tier3.get("results").toString()).contains("ER101");

        Map<String, Object> tier4 = first(chat(token, null, "what's the meaning of life"), "fallback");
        assertThat(tier4).containsEntry("tier", 4);
    }

    @Test
    void aStreamThatDiesMidAnswerEndsWithAnExplicitError() {
        ollama.brain = req -> new FakeOllama.Abort(List.of("The next ", "train "));

        List<ServerSentEvent<Map<String, Object>>> events = chat(issuer.token("user-j", "J", "j@example.com"), null, "hi");

        assertThat(text(events)).isEqualTo("The next train ");
        assertThat(first(events, "error")).containsEntry("code", "RESPONSE_INTERRUPTED");
        assertThat(names(events).getLast()).isEqualTo("done"); // never a silent truncation
    }

    @Test
    void chatRequiresAValidToken() {
        client.post().uri("/chat").bodyValue(Map.of("message", "hi")).exchange().expectStatus().isUnauthorized();
        client.post().uri("/chat").header("Authorization", "Bearer " + issuer.forgedToken("x"))
                .bodyValue(Map.of("message", "hi")).exchange().expectStatus().isUnauthorized();
    }

    // ------------------------------------------------------------------------------------------ helpers

    private List<ServerSentEvent<Map<String, Object>>> chat(String token, String conversationId, String message) {
        return chatFlux(token, conversationId, message).collectList().block(Duration.ofSeconds(30));
    }

    private Flux<ServerSentEvent<Map<String, Object>>> chatFlux(String token, String conversationId, String message) {
        Map<String, Object> body = new HashMap<>();
        body.put("message", message);
        if (conversationId != null) {
            body.put("conversationId", conversationId);
        }
        return client.post().uri("/chat").header("Authorization", "Bearer " + token)
                .accept(MediaType.TEXT_EVENT_STREAM).bodyValue(body).exchange()
                .expectStatus().isOk()
                .returnResult(new ParameterizedTypeReference<ServerSentEvent<Map<String, Object>>>() {})
                .getResponseBody();
    }

    private static List<String> names(List<ServerSentEvent<Map<String, Object>>> events) {
        return events.stream().map(ServerSentEvent::event).toList();
    }

    private static String text(List<ServerSentEvent<Map<String, Object>>> events) {
        StringBuilder sb = new StringBuilder();
        events.stream().filter(e -> "token".equals(e.event())).forEach(e -> sb.append(e.data().get("text")));
        return sb.toString();
    }

    private static Map<String, Object> first(List<ServerSentEvent<Map<String, Object>>> events, String name) {
        return events.stream().filter(e -> name.equals(e.event())).findFirst()
                .orElseThrow(() -> new AssertionError("No '" + name + "' event in " + names(events))).data();
    }

    private static List<String> toolNames(JsonNode request) {
        List<String> names = new ArrayList<>();
        request.get("tools").forEach(t -> names.add(t.at("/function/name").asString()));
        return names;
    }
}
