package org.presight.ai.ticketing;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.presight.ai.common.kafka.events.BookingEvents.BookingCancelled;
import org.presight.ai.common.kafka.events.BookingEvents.BookingConfirmed;
import org.presight.ai.common.kafka.events.BookingEvents.CancelReason;
import org.presight.ai.common.kafka.events.OtherEvents.TicketIssued;
import org.presight.ai.common.kafka.events.OtherEvents.TicketVoided;
import org.presight.ai.common.kafka.testing.KafkaTestSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Import(TicketApiTest.Containers.class)
class TicketEventsTest {

    private static final Duration WAIT = Duration.ofSeconds(20);

    @DynamicPropertySource
    static void objectStore(DynamicPropertyRegistry registry) {
        registry.add("ticketing.object-store.endpoint",
                () -> "http://" + TicketApiTest.s3.getHost() + ":" + TicketApiTest.s3.getMappedPort(9090));
    }

    @Autowired WebTestClient client;
    @Autowired KafkaContainer kafka;

    static KafkaTestSupport events;

    @BeforeEach
    void setUp() {
        if (events == null) {
            events = new KafkaTestSupport(kafka.getBootstrapServers(), List.of(Topics.TICKET_ISSUED, Topics.TICKET_VOIDED));
        }
    }

    @AfterAll
    static void close() {
        if (events != null) {
            events.close();
        }
    }

    @Test
    void aConfirmedBookingIsTicketedOnceAndACancellationVoidsIt() {
        UUID booking = UUID.randomUUID();
        BookingConfirmed confirmed = confirmed(booking, "TKA234");
        events.publish(Topics.BOOKING_CONFIRMED, confirmed);
        events.publish(Topics.BOOKING_CONFIRMED, confirmed); // redelivery

        events.await(Topics.TICKET_ISSUED, TicketIssued.class, e -> e.bookingId().equals(booking), WAIT);
        assertThat(events.settle(Topics.TICKET_ISSUED, TicketIssued.class, Duration.ofSeconds(2)))
                .filteredOn(e -> e.bookingId().equals(booking)).hasSize(1);
        client.get().uri("/tickets/TKA234").exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("ISSUED").jsonPath("$.seatNo").isEqualTo("B4");

        events.publish(Topics.BOOKING_CANCELLED, new BookingCancelled(UUID.randomUUID(), booking, "TKA234",
                UUID.randomUUID(), CancelReason.USER, null, Instant.now()));
        events.await(Topics.TICKET_VOIDED, TicketVoided.class, e -> e.bookingId().equals(booking), WAIT);
        client.get().uri("/tickets/TKA234").exchange().expectBody().jsonPath("$.status").isEqualTo("VOIDED");
    }

    @Test
    void aCancellationThatOvertakesTheConfirmationPreventsTheTicket() {
        UUID booking = UUID.randomUUID();
        events.publish(Topics.BOOKING_CANCELLED, new BookingCancelled(UUID.randomUUID(), booking, "TKB234",
                UUID.randomUUID(), CancelReason.USER, null, Instant.now()));
        events.publish(Topics.BOOKING_CONFIRMED, confirmed(booking, "TKB234"));

        assertThat(events.settle(Topics.TICKET_ISSUED, TicketIssued.class, Duration.ofSeconds(4)))
                .noneMatch(e -> e.bookingId().equals(booking));
        client.get().uri("/tickets/TKB234").exchange().expectStatus().isNotFound();
    }

    private static BookingConfirmed confirmed(UUID booking, String pnr) {
        return new BookingConfirmed(UUID.randomUUID(), booking, pnr, UUID.randomUUID(), "ER101", LocalDate.now().plusDays(5),
                "BUSINESS", "B4", "DXB", "FUJ", OffsetDateTime.now().plusDays(5), "Aisha Khan", Instant.now());
    }
}
