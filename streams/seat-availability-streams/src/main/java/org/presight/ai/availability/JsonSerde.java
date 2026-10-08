package org.presight.ai.availability;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import tools.jackson.databind.json.JsonMapper;

/** JSON serde over our Jackson 3 mapper (Kafka's own JSON support is Jackson 2). */
final class JsonSerde {

    private JsonSerde() {
    }

    static <T> Serde<T> of(JsonMapper json, Class<T> type) {
        return Serdes.serdeFrom(
                (topic, value) -> value == null ? null : json.writeValueAsString(value).getBytes(StandardCharsets.UTF_8),
                (topic, bytes) -> bytes == null ? null : json.readValue(bytes, type));
    }
}
