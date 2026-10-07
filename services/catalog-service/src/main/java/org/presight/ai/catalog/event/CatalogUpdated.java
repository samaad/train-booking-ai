package org.presight.ai.catalog.event;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Payload of {@code catalog.updated.v1} (01-services). ai-assistant consumes it to reindex RAG.
 * Defined now so the contract is explicit; publishing through Kafka is wired in the events pass.
 */
public record CatalogUpdated(UUID eventId, List<String> trainNumbers, Instant occurredAt) {
    public static final String TOPIC = "catalog.updated.v1";
}
