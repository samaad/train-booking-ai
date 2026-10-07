package org.presight.ai.notification.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Payload of {@code notification.sent.v1} (01-services). Defined now so the contract is explicit;
 * publishing through Kafka is wired in the events pass. No recipient address: it is PII.
 */
public record NotificationSent(UUID eventId, String notificationId, String sourceEventId, String channel,
                               Instant occurredAt) {
    public static final String TOPIC = "notification.sent.v1";
}
