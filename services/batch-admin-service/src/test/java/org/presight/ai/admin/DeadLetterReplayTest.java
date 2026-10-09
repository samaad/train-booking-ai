package org.presight.ai.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.presight.ai.admin.dlt.DeadLetters;
import org.presight.ai.common.kafka.EventHeaders;
import org.presight.ai.common.security.testing.TestIssuer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Real broker, real tokens: dead letters are browsed and replayed only by an ADMIN, and only as designed. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(DeadLetterReplayTest.Containers.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DeadLetterReplayTest {

    static final String ORIGINAL = "booking.created.v1";
    static final String DLT = ORIGINAL + ".payment-service.DLT";
    static final TestIssuer issuer = new TestIssuer();
    static final JsonMapper JSON = JsonMapper.builder().build();
    static final List<String> EVENT_IDS = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
            UUID.randomUUID().toString());

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean
        @ServiceConnection
        KafkaContainer kafka() {
            return new KafkaContainer("apache/kafka:4.3.1");
        }
    }

    @DynamicPropertySource
    static void security(DynamicPropertyRegistry registry) {
        issuer.register(registry);
    }

    @Autowired
    KafkaContainer kafka;

    @LocalServerPort
    int port;

    final HttpClient http = HttpClient.newHttpClient();

    @BeforeAll
    void deadLetterThreeMessages() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(ORIGINAL, 2, (short) 1), new NewTopic(DLT, 1, (short) 1))).all().get();
        }
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 3; i++) {
                // What DeadLetterPublishingRecoverer writes: the original record plus kafka_dlt-* headers.
                var record = new ProducerRecord<>(DLT, "booking-" + i, "{\"bookingId\":\"booking-" + i + "\"}");
                header(record, EventHeaders.EVENT_ID, EVENT_IDS.get(i));
                header(record, EventHeaders.EVENT_TYPE, ORIGINAL);
                header(record, "traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC, ORIGINAL);
                header(record, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP, "payment-service");
                header(record, KafkaHeaders.DLT_EXCEPTION_FQCN, "org.presight.ai.common.error.DomainException");
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE, "AMOUNT_MISMATCH");
                if (i == 2) {
                    header(record, DeadLetters.REPLAY_COUNT, "3"); // came back three times already: poison
                }
                producer.send(record).get();
            }
        }
    }

    @Test
    @Order(1)
    void onlyAdminsMayTouchDeadLetters() throws Exception {
        assertThat(get("/dlt", null).statusCode()).isEqualTo(401);
        assertThat(get("/dlt", issuer.passenger("user-1")).statusCode()).isEqualTo(403);
        assertThat(get("/dlt", issuer.withRoles("support-1", "SUPPORT")).statusCode()).isEqualTo(403);
        assertThat(post("/dlt/" + DLT + "/replay", issuer.withRoles("agent-1", "AGENT")).statusCode()).isEqualTo(403);
        assertThat(get("/actuator/health/readiness", null).statusCode()).isEqualTo(200);
    }

    @Test
    @Order(2)
    void listsDeadLetterTopicsWithPendingCounts() throws Exception {
        JsonNode summaries = json(get("/dlt", admin()));
        assertThat(summaries.findValues("topic").stream().map(JsonNode::asString)).containsExactly(DLT);
        assertThat(summaries.get(0).get("total").asLong()).isEqualTo(3);
        assertThat(summaries.get(0).get("pending").asLong()).isEqualTo(3);
    }

    @Test
    @Order(3)
    void browsingShowsWhyEachMessageFailed() throws Exception {
        JsonNode letters = json(get("/dlt/" + DLT + "/messages?limit=10", admin()));
        assertThat(letters.size()).isEqualTo(3);
        JsonNode first = letters.get(0);
        assertThat(first.get("eventId").asString()).isEqualTo(EVENT_IDS.get(0));
        assertThat(first.get("originalTopic").asString()).isEqualTo(ORIGINAL);
        assertThat(first.get("consumerGroup").asString()).isEqualTo("payment-service");
        assertThat(first.get("exceptionMessage").asString()).isEqualTo("AMOUNT_MISMATCH");
        assertThat(letters.get(2).get("replayCount").asInt()).isEqualTo(3);
        // Browsing moved nothing.
        assertThat(json(get("/dlt", admin())).get(0).get("pending").asLong()).isEqualTo(3);
    }

    @Test
    @Order(4)
    void replayingOneMessageRepublishesItAsTheOriginalEvent() throws Exception {
        var response = post("/dlt/" + DLT + "/messages/0/0/replay", admin());
        assertThat(response.statusCode()).isEqualTo(200);

        List<ConsumerRecord<String, String>> replayed = readOriginal(1);
        ConsumerRecord<String, String> record = replayed.getFirst();
        assertThat(record.key()).isEqualTo("booking-0"); // same key → same partition, same ordering unit
        assertThat(record.value()).isEqualTo("{\"bookingId\":\"booking-0\"}");
        assertThat(header(record, EventHeaders.EVENT_ID)).isEqualTo(EVENT_IDS.get(0)); // consumers dedupe on it
        // The replay continues the admin's request trace; the failed attempt's trace is kept for correlation.
        assertThat(record.headers().headers("traceparent")).hasSize(1);
        assertThat(header(record, "traceparent")).doesNotContain("0af7651916cd43dd8448eb211c80319c");
        assertThat(header(record, DeadLetters.ORIGINAL_TRACEPARENT)).startsWith("00-0af7651916cd43dd8448eb211c80319c");
        assertThat(header(record, DeadLetters.REPLAY_COUNT)).isEqualTo("1");
        assertThat(header(record, DeadLetters.REPLAYED_FROM)).isEqualTo(DLT + "-0@0");
        assertThat(header(record, DeadLetters.REPLAYED_BY)).isEqualTo("ops-1");
        assertThat(record.headers().toArray()).extracting(Header::key).noneMatch(k -> k.startsWith("kafka_dlt-"));

        // One-off replays don't move bulk replay's position.
        assertThat(json(get("/dlt", admin())).get(0).get("pending").asLong()).isEqualTo(3);
    }

    @Test
    @Order(5)
    void aPoisonMessageIsRefusedIndividually() throws Exception {
        var response = post("/dlt/" + DLT + "/messages/0/2/replay", admin());
        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(json(response).get("code").asString()).isEqualTo("REPLAY_LIMIT_REACHED");
    }

    @Test
    @Order(6)
    void bulkReplayRepublishesPendingMessagesSkipsPoisonAndRemembersItsPosition() throws Exception {
        JsonNode result = json(post("/dlt/" + DLT + "/replay", admin()));
        assertThat(result.get("replayed").asInt()).isEqualTo(2);
        assertThat(result.get("skipped").size()).isEqualTo(1);
        assertThat(result.get("skipped").get(0).get("offset").asLong()).isEqualTo(2);

        // 1 one-off + 2 bulk (offset 0 again — harmless, consumers deduplicate on eventId).
        assertThat(readOriginal(3)).extracting(r -> header(r, EventHeaders.EVENT_ID))
                .containsExactlyInAnyOrder(EVENT_IDS.get(0), EVENT_IDS.get(0), EVENT_IDS.get(1));
        assertThat(json(get("/dlt", admin())).get(0).get("pending").asLong()).isZero();

        JsonNode again = json(post("/dlt/" + DLT + "/replay", admin()));
        assertThat(again.get("replayed").asInt()).isZero();
    }

    @Test
    @Order(7)
    void arbitraryTopicsCannotBeReadOrReplayed() throws Exception {
        assertThat(get("/dlt/" + ORIGINAL + "/messages", admin()).statusCode()).isEqualTo(422);
        assertThat(post("/dlt/" + ORIGINAL + "/replay", admin()).statusCode()).isEqualTo(422);
    }

    private String admin() {
        return issuer.withRoles("ops-1", "ADMIN");
    }

    private HttpResponse<String> get(String path, String token) throws Exception {
        return send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET(), token);
    }

    private HttpResponse<String> post(String path, String token) throws Exception {
        return send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).POST(HttpRequest.BodyPublishers.noBody()), token);
    }

    private HttpResponse<String> send(HttpRequest.Builder request, String token) throws Exception {
        if (token != null) {
            request.header("Authorization", "Bearer " + token);
        }
        return http.send(request.timeout(Duration.ofSeconds(60)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode json(HttpResponse<String> response) {
        assertThat(response.statusCode()).as(response.body()).isLessThan(500);
        return JSON.readTree(response.body());
    }

    private List<ConsumerRecord<String, String>> readOriginal(int expected) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(ORIGINAL));
            long deadline = System.currentTimeMillis() + 20_000;
            while (records.size() < expected && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
        }
        assertThat(records).hasSize(expected);
        return records;
    }

    private static void header(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
