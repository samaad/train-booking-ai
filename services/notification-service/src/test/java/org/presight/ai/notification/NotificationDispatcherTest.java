package org.presight.ai.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.presight.ai.notification.dispatch.Notification;
import org.presight.ai.notification.dispatch.Notification.Channel;
import org.presight.ai.notification.dispatch.Notification.Status;
import org.presight.ai.notification.dispatch.NotificationDispatcher;
import org.presight.ai.notification.dispatch.NotificationDispatcher.Dispatch;
import org.presight.ai.notification.dispatch.NotificationDispatcher.Recipient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;

@SpringBootTest
@Import(NotificationDispatcherTest.Containers.class)
public class NotificationDispatcherTest {

    @TestConfiguration(proxyBeanMethods = false)
    public static class Containers {
        @Bean
        @ServiceConnection
        KafkaContainer kafka() {
            return new KafkaContainer("apache/kafka:4.3.1");
        }

        @Bean
        @ServiceConnection
        MongoDBContainer mongo() {
            return new MongoDBContainer("mongo:8.0.32");
        }
    }

    private static final Map<String, String> CONFIRMED = Map.of("pnr", "ABC234", "trainNumber", "ER101",
            "from", "DXB", "to", "FUJ", "travelDate", "2026-10-20", "seatNo", "B4");

    @Autowired
    NotificationDispatcher dispatcher;

    @Autowired
    ReactiveMongoTemplate mongo;

    @Test
    void fansOutToEveryChannelWithAnAddress() {
        String eventId = UUID.randomUUID().toString();
        List<Notification> sent = dispatcher.dispatch(new Dispatch(eventId, "booking.confirmed.v1",
                new Recipient("aisha@example.com", "+971501234567", null), CONFIRMED)).collectList().block();

        assertThat(sent).extracting(Notification::channel).containsExactlyInAnyOrder(Channel.EMAIL, Channel.SMS);
        assertThat(sent).allSatisfy(n -> {
            assertThat(n.status()).isEqualTo(Status.SENT);
            assertThat(n.subject()).isEqualTo("Booking ABC234 confirmed");
            assertThat(n.body()).contains("ER101 (DXB to FUJ)").contains("Seat B4");
        });
    }

    @Test
    void aRedeliveredEventSendsNothingTwice() {
        String eventId = UUID.randomUUID().toString();
        Dispatch d = new Dispatch(eventId, "booking.confirmed.v1", new Recipient("aisha@example.com", null, null), CONFIRMED);

        assertThat(dispatcher.dispatch(d).collectList().block()).hasSize(1);
        assertThat(dispatcher.dispatch(d).collectList().block()).isEmpty();
        assertThat(mongo.count(Query.query(Criteria.where("eventId").is(eventId)), Notification.class).block()).isEqualTo(1);
    }

    @Test
    void oneFailingChannelDoesNotBlockTheOthers() {
        List<Notification> settled = dispatcher.dispatch(new Dispatch(UUID.randomUUID().toString(), "ticket.issued.v1",
                new Recipient("aisha@example.com", "not-a-phone", null), Map.of("pnr", "ABC234"))).collectList().block();

        assertThat(settled).filteredOn(n -> n.channel() == Channel.EMAIL)
                .singleElement().extracting(Notification::status).isEqualTo(Status.SENT);
        assertThat(settled).filteredOn(n -> n.channel() == Channel.SMS).singleElement().satisfies(n -> {
            assertThat(n.status()).isEqualTo(Status.FAILED);
            assertThat(n.failureReason()).isEqualTo("Invalid SMS address");
        });
    }

    @Test
    void unknownEventTypesAreIgnoredAndMissingParametersRejected() {
        assertThat(dispatcher.dispatch(new Dispatch(UUID.randomUUID().toString(), "seat.held.v1",
                new Recipient("aisha@example.com", null, null), Map.of())).collectList().block()).isEmpty();

        assertThatThrownBy(() -> dispatcher.dispatch(new Dispatch(UUID.randomUUID().toString(), "booking.cancelled.v1",
                new Recipient("aisha@example.com", null, null), Map.of())).collectList().block())
                .hasMessageContaining("Missing template parameter: pnr");
    }
}
