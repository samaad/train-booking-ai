package org.presight.ai.common.kafka.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Every event payload. {@code eventId} is what consumers deduplicate on; it is also the outbox row id and
 * the {@code eventId} Kafka header. Records are the contract: consumers ignore unknown fields, so adding a
 * field is backwards compatible and removing or renaming one needs a new {@code .v2} topic.
 */
public interface Event {

    UUID eventId();

    Instant occurredAt();

    /** The Kafka message key (partitioning and ordering unit). */
    Object key();
}
