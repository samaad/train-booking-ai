package org.presight.ai.common.kafka.consumer;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.presight.ai.common.kafka.EventHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

/**
 * Exactly-once <em>effect</em> on top of at-least-once delivery: the {@code processed_events} insert and the
 * handler's writes commit in one transaction, so a redelivered event finds its row and does nothing.
 *
 * <p>Called from {@code @KafkaListener} threads — a separate blocking domain from the event loop
 * (04-kafka) — so bridging into the reactive pipeline with {@code block()} is deliberate and safe here.
 */
public class IdempotentHandler {

    private static final Logger log = LoggerFactory.getLogger(IdempotentHandler.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final DatabaseClient db;
    private final TransactionalOperator tx;
    private final JsonMapper json;
    private final String consumerGroup;

    public IdempotentHandler(DatabaseClient db, TransactionalOperator tx, JsonMapper json, String consumerGroup) {
        this.db = db;
        this.tx = tx;
        this.json = json;
        this.consumerGroup = consumerGroup;
    }

    /** Throws on failure so the container's error handler retries, then dead-letters. */
    public <T> void handle(ConsumerRecord<String, String> record, Class<T> type, Function<T, Mono<?>> effect) {
        UUID eventId = EventHeaders.eventId(record);
        T event = json.readValue(record.value(), type);
        Boolean applied = firstDelivery(eventId, record.topic())
                .flatMap(first -> first
                        ? effect.apply(event).then(Mono.just(true))
                        : Mono.just(false))
                .as(tx::transactional)
                .block(TIMEOUT);
        if (Boolean.FALSE.equals(applied)) {
            log.debug("Duplicate {} {} ignored by {}", record.topic(), eventId, consumerGroup);
        }
    }

    private Mono<Boolean> firstDelivery(UUID eventId, String topic) {
        return db.sql("""
                        INSERT INTO processed_events (consumer_group, event_id, topic) VALUES (:group, :id, :topic)
                        ON CONFLICT DO NOTHING
                        """)
                .bind("group", consumerGroup)
                .bind("id", eventId)
                .bind("topic", topic)
                .fetch()
                .rowsUpdated()
                .map(rows -> rows == 1);
    }
}
