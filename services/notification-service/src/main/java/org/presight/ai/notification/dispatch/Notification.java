package org.presight.ai.notification.dispatch;

import java.time.Instant;
import java.util.Map;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * One delivery attempt record per (source event, channel) — append-mostly, semi-structured,
 * which is why notifications live in MongoDB. The (eventId, channel) unique index is the
 * idempotency guard for at-least-once event delivery.
 */
@Document("notifications")
public record Notification(
        @Id String id,
        String eventId,
        String eventType,
        Channel channel,
        String recipient,
        String subject,
        String body,
        Map<String, String> params,
        Status status,
        String failureReason,
        Instant createdAt,
        Instant settledAt) {

    public enum Channel { EMAIL, SMS, PUSH }

    public enum Status { PENDING, SENT, FAILED }
}
