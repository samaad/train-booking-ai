package org.presight.ai.common.kafka.outbox;

import io.r2dbc.spi.Readable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.presight.ai.common.kafka.EventHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Polling outbox relay (03-consistency): every {@code interval}, lock up to {@code batchSize} unpublished
 * rows with {@code FOR UPDATE SKIP LOCKED}, publish them in order, mark them published, commit.
 *
 * <ul>
 *   <li>SKIP LOCKED lets several instances relay concurrently without publishing a row twice <em>in the
 *       normal case</em>. A crash after the Kafka ack but before the commit re-publishes the row — delivery
 *       is at-least-once, which is why every consumer deduplicates on {@code eventId}.</li>
 *   <li>A failed send stops the batch (later rows must not overtake it) and is retried next tick.</li>
 *   <li>Runs on its own single thread: {@code KafkaTemplate.send} can block on metadata when the broker is
 *       away, and that must never happen on an event-loop thread.</li>
 * </ul>
 */
public class OutboxRelay implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final DatabaseClient db;
    private final TransactionalOperator tx;
    private final KafkaTemplate<String, String> kafka;
    private final Duration interval;
    private final int batchSize;
    private final TraceContext traces;
    private final tools.jackson.databind.json.JsonMapper json;
    private final AtomicLong lagSeconds = new AtomicLong();
    private Scheduler scheduler;
    private Disposable loop;

    public OutboxRelay(DatabaseClient db, TransactionalOperator tx, KafkaTemplate<String, String> kafka,
                       Duration interval, int batchSize, TraceContext traces, tools.jackson.databind.json.JsonMapper json) {
        this.db = db;
        this.tx = tx;
        this.kafka = kafka;
        this.interval = interval;
        this.batchSize = batchSize;
        this.traces = traces;
        this.json = json;
    }

    private record Row(UUID id, String topic, String key, String payload, String headers) {
    }

    /**
     * Publishes one batch; emits how many rows were published. The batch is collected before sending
     * (r2dbc-postgresql runs one statement at a time per connection, so updating while a cursor is still
     * streaming would deadlock). On the first failed send it stops and commits the published prefix, so
     * rows Kafka already acknowledged are not re-sent.
     */
    public Mono<Integer> relayOnce() {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        return db.sql("""
                        SELECT id, topic, message_key, payload::text AS payload, headers::text AS headers FROM outbox
                        WHERE published_at IS NULL
                        ORDER BY seq
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """)
                .bind("limit", batchSize)
                .map(OutboxRelay::row)
                .all()
                .collectList()
                .flatMapMany(Flux::fromIterable)
                .concatMap(row -> send(row) // strictly in order
                        .then(db.sql("UPDATE outbox SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = :id")
                                .bind("id", row.id()).then())
                        .thenReturn(row))
                .onErrorResume(e -> {
                    failure.set(e);
                    return Flux.empty(); // end the batch here; later rows must not overtake the failed one
                })
                .count()
                .map(Long::intValue)
                .as(tx::transactional)
                .flatMap(published -> failure.get() == null ? Mono.just(published)
                        : recordFailure(failure.get()).thenReturn(published));
    }

    private Mono<Void> send(Row row) {
        ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.key(), row.payload());
        record.headers().add(EventHeaders.EVENT_ID, row.id().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(EventHeaders.EVENT_TYPE, row.topic().getBytes(StandardCharsets.UTF_8));
        // Publish inside the trace that wrote the event, so the send (and the consumer) continue that trace.
        java.util.Map<String, String> traceHeaders = row.headers() == null ? java.util.Map.of()
                : json.readValue(row.headers(), new tools.jackson.core.type.TypeReference<java.util.Map<String, String>>() {});
        return Mono.fromFuture(() -> traces.within(traceHeaders, "outbox publish " + row.topic(), () -> kafka.send(record)))
                .timeout(SEND_TIMEOUT).then();
    }

    private Mono<Void> recordFailure(Throwable e) {
        log.warn("Outbox send failed, will retry next tick: {}", e.toString());
        // After the batch transaction: note the error on the oldest unpublished row for operators.
        return db.sql("""
                        UPDATE outbox SET attempts = attempts + 1, last_error = :error
                        WHERE id = (SELECT id FROM outbox WHERE published_at IS NULL ORDER BY seq LIMIT 1)
                        """)
                .bind("error", String.valueOf(e.getMessage()))
                .then()
                .onErrorResume(ignored -> Mono.empty());
    }

    private Mono<Void> measureLag() {
        return db.sql("SELECT min(created_at) AS oldest FROM outbox WHERE published_at IS NULL")
                .map(r -> java.util.Optional.ofNullable(r.get("oldest", OffsetDateTime.class)))
                .one()
                .doOnNext(oldest -> lagSeconds.set(oldest
                        .map(t -> Duration.between(t, OffsetDateTime.now()).toSeconds()).orElse(0L)))
                .then()
                .onErrorResume(ignored -> Mono.empty());
    }

    /** Age of the oldest unpublished event in seconds — the plan's "page on this" metric. */
    public long lagSeconds() {
        return lagSeconds.get();
    }

    @Override
    public void start() {
        scheduler = Schedulers.newSingle("outbox-relay");
        loop = Flux.interval(interval, scheduler)
                .onBackpressureDrop()
                .concatMap(tick -> relayOnce()
                        .onErrorResume(e -> {
                            log.warn("Outbox relay batch failed, will retry: {}", e.toString());
                            return Mono.just(0);
                        })
                        .then(measureLag()), 1)
                .subscribe();
    }

    @Override
    public void stop() {
        if (loop != null) {
            loop.dispose();
        }
        if (scheduler != null) {
            scheduler.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return loop != null && !loop.isDisposed();
    }

    private static Row row(Readable r) {
        return new Row(r.get("id", UUID.class), r.get("topic", String.class), r.get("message_key", String.class),
                r.get("payload", String.class), r.get("headers", String.class));
    }
}
