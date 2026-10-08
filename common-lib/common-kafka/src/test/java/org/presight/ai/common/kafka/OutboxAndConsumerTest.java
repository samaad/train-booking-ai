package org.presight.ai.common.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.presight.ai.common.kafka.consumer.IdempotentHandler;
import org.presight.ai.common.kafka.events.Event;
import org.presight.ai.common.kafka.outbox.Outbox;
import org.presight.ai.common.kafka.outbox.OutboxRelay;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(properties = {
        "outbox.relay.enabled=false", // driven by hand for determinism
        "spring.kafka.consumer.group-id=test-group",
        "spring.kafka.producer.properties.max.block.ms=2000"
})
class OutboxAndConsumerTest {

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class App {
        @Bean
        @ServiceConnection
        PostgreSQLContainer postgres() {
            return new PostgreSQLContainer("postgres:18.6-alpine");
        }

        @Bean
        @ServiceConnection
        KafkaContainer kafka() {
            // Like infra/docker-compose.yml: topics must be declared, a typo fails.
            return new KafkaContainer("apache/kafka:4.3.1").withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        }
    }

    record Ping(UUID eventId, String key, int n, Instant occurredAt) implements Event {
    }

    static final String TOPIC = "test.ping.v1";

    @Autowired DatabaseClient db;
    @Autowired ReactiveTransactionManager txManager;
    @Autowired Outbox outbox;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired IdempotentHandler handler;
    @Autowired JsonMapper json;
    @Autowired KafkaContainer broker;

    OutboxRelay relay;
    TransactionalOperator tx;

    private static boolean schema;

    @BeforeEach
    void setUp() throws Exception {
        if (!schema) {
            String ddl = new ClassPathResource("db/outbox/V1000__outbox_and_processed_events.sql").getContentAsString(StandardCharsets.UTF_8);
            for (String statement : ddl.replaceAll("(?m)^--.*$", "").split(";")) {
                if (!statement.isBlank()) {
                    db.sql(statement).then().block();
                }
            }
            db.sql("CREATE TABLE effects (n int)").then().block();
            try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", broker.getBootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
            }
            schema = true;
        }
        db.sql("DELETE FROM outbox").then().block();
        tx = TransactionalOperator.create(txManager);
        relay = new OutboxRelay(db, tx, kafka, Duration.ofSeconds(1), 100);
    }

    @Test
    void committedEventsArePublishedInOrderWithTheirIdHeader() {
        List<Ping> pings = List.of(ping(1), ping(2), ping(3));
        Mono.when(pings.stream().map(p -> outbox.append(TOPIC, p)).toList()).as(tx::transactional).block();
        // Appended in one transaction but serialised by seq; publish order must follow it.
        assertThat(relay.relayOnce().block()).isEqualTo(3);
        assertThat(relay.relayOnce().block()).isZero(); // marked published

        List<ConsumerRecord<String, String>> received = consume(3);
        assertThat(received).extracting(r -> json.readValue(r.value(), Ping.class).n()).containsExactly(1, 2, 3);
        assertThat(received).extracting(EventHeaders::eventId).containsExactlyElementsOf(pings.stream().map(Ping::eventId).toList());
    }

    @Test
    void aRolledBackTransactionLeavesNoEvent() {
        outbox.append(TOPIC, ping(9)).then(Mono.error(new IllegalStateException("business write failed")))
                .as(tx::transactional).onErrorResume(e -> Mono.empty()).block();

        assertThat(relay.relayOnce().block()).isZero();
    }

    @Test
    void aFailedSendKeepsTheEventAndBlocksLaterOnes() {
        outbox.append("undeclared.topic.v1", ping(1)).then(outbox.append(TOPIC, ping(2))).as(tx::transactional).block();

        assertThat(relay.relayOnce().block()).isZero();
        Map<String, Object> stuck = db.sql("SELECT count(*) FILTER (WHERE published_at IS NULL) AS unpublished, max(attempts) AS attempts, "
                        + "max(last_error) AS err FROM outbox").fetch().one().block();
        assertThat(((Number) stuck.get("unpublished")).intValue()).isEqualTo(2); // the later event did not overtake
        assertThat(((Number) stuck.get("attempts")).intValue()).isEqualTo(1);
        assertThat(stuck.get("err")).isNotNull();
    }

    @Test
    void aRedeliveredEventTakesEffectOnce() {
        Ping p = ping(7);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 0, 0, p.key(), json.writeValueAsString(p));
        record.headers().add(EventHeaders.EVENT_ID, p.eventId().toString().getBytes(StandardCharsets.UTF_8));
        AtomicInteger calls = new AtomicInteger();

        for (int i = 0; i < 3; i++) {
            handler.handle(record, Ping.class, ping -> {
                calls.incrementAndGet();
                return db.sql("INSERT INTO effects (n) VALUES (:n)").bind("n", ping.n()).then();
            });
        }

        assertThat(calls).hasValue(1);
        assertThat(db.sql("SELECT count(*) AS c FROM effects WHERE n = 7").map(r -> r.get("c", Long.class)).one().block()).isEqualTo(1);
    }

    @Test
    void aFailingEffectIsRolledBackTogetherWithItsDedupRow() {
        Ping p = ping(8);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 0, 0, p.key(), json.writeValueAsString(p));
        record.headers().add(EventHeaders.EVENT_ID, p.eventId().toString().getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> handler.handle(record, Ping.class, ping -> Mono.error(new IllegalStateException("boom"))))
                .hasMessageContaining("boom");
        AtomicInteger calls = new AtomicInteger();
        handler.handle(record, Ping.class, ping -> Mono.fromRunnable(calls::incrementAndGet)); // the retry is not a "duplicate"
        assertThat(calls).hasValue(1);
    }

    @Test
    void businessRejectionsAreNotRetriedButOutagesAre() {
        assertThat(KafkaEventsAutoConfiguration.isPermanent(
                org.presight.ai.common.error.DomainException.conflict("HOLD_EXPIRED", "x"))).isTrue();
        assertThat(KafkaEventsAutoConfiguration.isPermanent(new RuntimeException(
                new org.presight.ai.common.error.DomainException(org.presight.ai.common.error.ErrorCategory.UNAVAILABLE, "DOWN", "x")))).isFalse();
        assertThat(KafkaEventsAutoConfiguration.isPermanent(new IllegalStateException("unexpected"))).isFalse();
    }

    private static Ping ping(int n) {
        return new Ping(UUID.randomUUID(), "k", n, Instant.now());
    }

    private List<ConsumerRecord<String, String>> consume(int expected) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(TOPIC));
            List<ConsumerRecord<String, String>> out = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 15_000;
            while (out.size() < expected && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(out::add);
            }
            return out.subList(Math.max(0, out.size() - expected), out.size());
        }
    }

    @BeforeAll
    static void noop() {
    }
}
