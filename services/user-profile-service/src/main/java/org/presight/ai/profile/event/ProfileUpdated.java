package org.presight.ai.profile.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Payload of {@code profile.updated.v1} (01-services). Defined now so the contract is explicit;
 * publishing through Kafka is wired in the events pass. Carries no PII beyond the subject.
 */
public record ProfileUpdated(UUID eventId, String userId, long version, Instant occurredAt) {
    public static final String TOPIC = "profile.updated.v1";
}
