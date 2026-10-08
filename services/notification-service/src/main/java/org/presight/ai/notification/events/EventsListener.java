package org.presight.ai.notification.events;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.error.ErrorCategory;
import org.presight.ai.common.id.UuidV7;
import org.presight.ai.common.kafka.EventHeaders;
import org.presight.ai.common.kafka.TopicDeclarations;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCreated;
import org.presight.ai.common.kafka.events.OtherEvents.NotificationSent;
import org.presight.ai.notification.dispatch.Notification;
import org.presight.ai.notification.dispatch.NotificationDispatcher;
import org.presight.ai.notification.dispatch.NotificationDispatcher.Dispatch;
import org.presight.ai.notification.dispatch.NotificationDispatcher.Recipient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Turns saga events into passenger notifications. Runs on Kafka listener threads (a blocking domain), so
 * bridging into the reactive dispatcher with {@code block()} is deliberate.
 *
 * <p>Recipients come from a contact projection built from {@code booking.created.v1}, so no synchronous call to
 * booking-service is needed. An event that overtakes its booking.created finds no contact yet and is retried,
 * then dead-lettered. Dispatch is idempotent per (eventId, channel), so redelivery never re-sends.
 */
@Component
class EventsListener {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final List<String> NOTIFY_ON = List.of(Topics.BOOKING_CONFIRMED, Topics.BOOKING_CANCELLED,
            Topics.PAYMENT_FAILED, Topics.PAYMENT_REFUNDED, Topics.TICKET_ISSUED);

    @Document("booking_contacts")
    record Contact(@Id UUID bookingId, String pnr, String email, String passengerName, String trainNumber,
                   String fromStation, String toStation, String travelDate, String seatNo) {
    }

    private final ReactiveMongoTemplate mongo;
    private final NotificationDispatcher dispatcher;
    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;

    EventsListener(ReactiveMongoTemplate mongo, NotificationDispatcher dispatcher, KafkaTemplate<String, String> kafka,
                   JsonMapper json) {
        this.mongo = mongo;
        this.dispatcher = dispatcher;
        this.kafka = kafka;
        this.json = json;
    }

    @KafkaListener(topics = Topics.BOOKING_CREATED)
    void onBookingCreated(ConsumerRecord<String, String> record) {
        BookingCreated e = json.readValue(record.value(), BookingCreated.class);
        mongo.save(new Contact(e.bookingId(), e.pnr(), e.contactEmail(), e.passengerName(), e.trainNumber(),
                e.fromStation(), e.toStation(), e.travelDate().toString(), e.seatNo())).block(TIMEOUT);
    }

    @KafkaListener(topics = {Topics.BOOKING_CONFIRMED, Topics.BOOKING_CANCELLED, Topics.PAYMENT_FAILED,
            Topics.PAYMENT_REFUNDED, Topics.TICKET_ISSUED})
    void onSagaEvent(ConsumerRecord<String, String> record) {
        UUID eventId = EventHeaders.eventId(record);
        JsonNode event = json.readTree(record.value());
        UUID bookingId = UUID.fromString(event.get("bookingId").asString());
        Contact contact = mongo.findById(bookingId, Contact.class).blockOptional(TIMEOUT)
                .orElseThrow(() -> new DomainException(ErrorCategory.UNAVAILABLE, "CONTACT_NOT_YET_KNOWN",
                        "No contact for booking " + bookingId + " yet; booking.created not processed"));

        Map<String, String> params = new HashMap<>();
        params.put("pnr", contact.pnr());
        params.put("trainNumber", contact.trainNumber());
        params.put("from", contact.fromStation());
        params.put("to", contact.toStation());
        params.put("travelDate", contact.travelDate());
        params.put("seatNo", contact.seatNo());
        if (event.has("amount")) {
            params.put("amount", event.get("amount").asString());
            params.put("currency", event.get("currency").asString());
        }
        List<Notification> sent = dispatcher.dispatch(new Dispatch(eventId.toString(), record.topic(),
                new Recipient(contact.email(), null, null), params)).collectList().block(TIMEOUT);

        // notification.sent is informational, so it is published directly rather than through an outbox
        // (this service has no relational store); a lost one costs an audit line, never a user-visible effect.
        for (Notification n : sent) {
            if (n.status() == Notification.Status.SENT) {
                NotificationSent out = new NotificationSent(UuidV7.generate(), n.id(), eventId.toString(),
                        n.channel().name(), Instant.now());
                kafka.send(Topics.NOTIFICATION_SENT, out.key(), json.writeValueAsString(out));
            }
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TopicsConfig {
        @Bean
        KafkaAdmin.NewTopics notificationTopics(@Value("${spring.kafka.consumer.group-id}") String group,
                                                @Value("${kafka.replicas:1}") int replicas) {
            List<String> consumed = new java.util.ArrayList<>(NOTIFY_ON);
            consumed.add(Topics.BOOKING_CREATED);
            return TopicDeclarations.of(List.of(Topics.NOTIFICATION_SENT), group, consumed, replicas);
        }
    }
}
