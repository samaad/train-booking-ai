package org.presight.ai.notification;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.BookingEvents.BookingConfirmed;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCreated;
import org.presight.ai.common.kafka.events.OtherEvents.NotificationSent;
import org.presight.ai.common.kafka.testing.KafkaTestSupport;
import org.presight.ai.notification.dispatch.Notification;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest
@Import(NotificationDispatcherTest.Containers.class)
class NotificationEventsTest {

    private static final Duration WAIT = Duration.ofSeconds(20);

    @Autowired KafkaContainer kafka;
    @Autowired ReactiveMongoTemplate mongo;

    static KafkaTestSupport events;

    @BeforeEach
    void setUp() {
        if (events == null) {
            events = new KafkaTestSupport(kafka.getBootstrapServers(), List.of(Topics.NOTIFICATION_SENT));
        }
    }

    @AfterAll
    static void close() {
        if (events != null) {
            events.close();
        }
    }

    @Test
    void aConfirmedBookingNotifiesThePassengerOnce() {
        UUID booking = UUID.randomUUID();
        events.publish(Topics.BOOKING_CREATED, created(booking));
        BookingConfirmed confirmed = confirmed(booking);
        events.publish(Topics.BOOKING_CONFIRMED, confirmed);
        events.publish(Topics.BOOKING_CONFIRMED, confirmed); // redelivery: no second email

        NotificationSent sent = events.await(Topics.NOTIFICATION_SENT, NotificationSent.class,
                e -> e.sourceEventId().equals(confirmed.eventId().toString()), WAIT);
        assertThat(sent.channel()).isEqualTo("EMAIL");

        Notification n = mongo.findOne(Query.query(Criteria.where("eventId").is(confirmed.eventId().toString())), Notification.class).block();
        assertThat(n.recipient()).isEqualTo("aisha@example.com");
        assertThat(n.body()).contains("ABC234").contains("ER101 (DXB to FUJ)").contains("Seat B4");
        assertThat(events.settle(Topics.NOTIFICATION_SENT, NotificationSent.class, Duration.ofSeconds(2)))
                .filteredOn(e -> e.sourceEventId().equals(confirmed.eventId().toString())).hasSize(1);
    }

    @Test
    void anEventThatOvertakesItsBookingIsRetriedUntilTheContactIsKnown() {
        UUID booking = UUID.randomUUID();
        BookingConfirmed confirmed = confirmed(booking);
        events.publish(Topics.BOOKING_CONFIRMED, confirmed); // contact unknown: retried by the error handler
        events.publish(Topics.BOOKING_CREATED, created(booking));

        events.await(Topics.NOTIFICATION_SENT, NotificationSent.class,
                e -> e.sourceEventId().equals(confirmed.eventId().toString()), WAIT);
    }

    private static BookingCreated created(UUID booking) {
        return new BookingCreated(UUID.randomUUID(), booking, "user-1", "ABC234", UUID.randomUUID(), "ER101", LocalDate.of(2026, 10, 20),
                "BUSINESS", "B4", "DXB", "FUJ", OffsetDateTime.now(), "Aisha Khan", "aisha@example.com",
                new BigDecimal("142.00"), "AED", Instant.now());
    }

    private static BookingConfirmed confirmed(UUID booking) {
        return new BookingConfirmed(UUID.randomUUID(), booking, "user-1", "ABC234", UUID.randomUUID(), "ER101", LocalDate.of(2026, 10, 20),
                "BUSINESS", "B4", "DXB", "FUJ", OffsetDateTime.now(), "Aisha Khan", Instant.now());
    }
}
