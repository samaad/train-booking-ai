package org.presight.ai.common.kafka.outbox;

import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import java.util.Map;
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
    private final TraceContext traces;

    public Outbox(DatabaseClient db, JsonMapper json, TraceContext traces) {
        this.db = db;
        this.json = json;
        this.traces = traces;
    }

    public Mono<Void> append(String topic, Event event) {
        // fromCallable runs with the caller's trace context restored (automatic context propagation).
        return Mono.fromCallable(() -> {
                    Map<String, String> trace = traces.capture();
                    return new String[] {json.writeValueAsString(event), json.writeValueAsString(trace), trace.get("traceparent")};
                })
                .flatMap(values -> db.sql("""
                                INSERT INTO outbox (id, topic, message_key, payload, headers, traceparent)
                                VALUES (:id, :topic, :key, CAST(:payload AS jsonb), CAST(:headers AS jsonb), :traceparent)
                                """)
                        .bind("id", event.eventId())
                        .bind("topic", topic)
                        .bind("key", String.valueOf(event.key()))
                        .bind("payload", values[0])
                        .bind("headers", values[1])
                        // Debezium (outbox.mode=cdc) lifts this column into the Kafka traceparent header.
                        .bind("traceparent", Parameters.in(R2dbcType.VARCHAR, values[2]))
                        .then());
    }
}
