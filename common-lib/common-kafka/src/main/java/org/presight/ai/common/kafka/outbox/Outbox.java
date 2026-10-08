package org.presight.ai.common.kafka.outbox;

import org.presight.ai.common.kafka.events.Event;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

/**
 * Writes an event to the outbox table. <b>Call it inside the same R2DBC transaction as the state change
 * it describes</b> — that single commit is the whole point: the event exists if and only if the change does.
 * {@code DatabaseClient} joins the caller's reactive transaction through the subscriber context.
 */
public class Outbox {

    private final DatabaseClient db;
    private final JsonMapper json;

    public Outbox(DatabaseClient db, JsonMapper json) {
        this.db = db;
        this.json = json;
    }

    public Mono<Void> append(String topic, Event event) {
        return Mono.fromCallable(() -> json.writeValueAsString(event))
                .flatMap(payload -> db.sql("""
                                INSERT INTO outbox (id, topic, message_key, payload)
                                VALUES (:id, :topic, :key, CAST(:payload AS jsonb))
                                """)
                        .bind("id", event.eventId())
                        .bind("topic", topic)
                        .bind("key", String.valueOf(event.key()))
                        .bind("payload", payload)
                        .then());
    }
}
