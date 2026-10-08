package org.presight.ai.common.kafka;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/** Kafka headers every event carries. The value is the bare JSON payload. */
public final class EventHeaders {

    public static final String EVENT_ID = "eventId";
    public static final String EVENT_TYPE = "eventType";

    private EventHeaders() {
    }

    public static UUID eventId(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(EVENT_ID);
        if (header == null) {
            throw new IllegalArgumentException("Event on " + record.topic() + " has no " + EVENT_ID + " header");
        }
        return UUID.fromString(new String(header.value(), StandardCharsets.UTF_8));
    }
}
