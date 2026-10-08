package org.presight.ai.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCancelled;
import org.presight.ai.common.kafka.events.BookingEvents.CancelReason;
import org.presight.ai.common.kafka.events.SeatEvents.ReleaseReason;
import org.presight.ai.common.kafka.events.SeatEvents.SeatCommitted;
import org.presight.ai.common.kafka.events.SeatEvents.SeatHeld;
import org.presight.ai.common.kafka.events.SeatEvents.SeatReleased;
import org.presight.ai.common.kafka.testing.KafkaTestSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "inventory.sweeper.interval=1s")
@AutoConfigureWebTestClient
@Import(TestcontainersConfiguration.class)
class InventoryEventsTest {

    private static final LocalDate DATE = LocalDate.now(ZoneId.of("Asia/Dubai")).plusDays(21);
    private static final Duration WAIT = Duration.ofSeconds(20);

    @Autowired WebTestClient client;
    @Autowired DatabaseClient db;
    @Autowired KafkaContainer kafka;

    static KafkaTestSupport events;

    @BeforeEach
    void setUp() {
        if (events == null) {
            events = new KafkaTestSupport(kafka.getBootstrapServers(),
                    List.of(Topics.SEAT_HELD, Topics.SEAT_RELEASED, Topics.SEAT_COMMITTED));
        }
    }

    @AfterAll
    static void close() {
        if (events != null) {
            events.close();
        }
    }

    @Test
    void holdAndCommitEmitSeatEventsKeyedByTrainAndDate() {
        UUID holdId = hold("B2");
        SeatHeld held = events.await(Topics.SEAT_HELD, SeatHeld.class, e -> e.holdId().equals(holdId), WAIT);
        assertThat(held.key()).isEqualTo("ER202:" + DATE);
        assertThat(held.seatNo()).isEqualTo("B2");

        client.post().uri("/holds/{id}/commit", holdId).exchange().expectStatus().isOk();
        events.await(Topics.SEAT_COMMITTED, SeatCommitted.class, e -> e.holdId().equals(holdId), WAIT);
    }

    @Test
    void aCancelledBookingReleasesItsCommittedSeatOnce() {
        UUID holdId = hold("B3");
        client.post().uri("/holds/{id}/commit", holdId).exchange().expectStatus().isOk();
        BookingCancelled cancelled = new BookingCancelled(UUID.randomUUID(), UUID.randomUUID(), "ABC234", holdId,
                CancelReason.USER, "plans changed", Instant.now());

        events.publish(Topics.BOOKING_CANCELLED, cancelled);
        events.publish(Topics.BOOKING_CANCELLED, cancelled); // redelivery

        SeatReleased released = events.await(Topics.SEAT_RELEASED, SeatReleased.class, e -> e.holdId().equals(holdId), WAIT);
        assertThat(released.reason()).isEqualTo(ReleaseReason.BOOKING_CANCELLED);
        assertThat(events.settle(Topics.SEAT_RELEASED, SeatReleased.class, Duration.ofSeconds(2)))
                .filteredOn(e -> e.holdId().equals(holdId)).hasSize(1);
        assertThat(seatStatus("B3")).isEqualTo("AVAILABLE");
        client.get().uri("/holds/{id}", holdId).exchange().expectBody().jsonPath("$.status").isEqualTo("RELEASED");
    }

    @Test
    void theSweeperReleasesLapsedHolds() {
        UUID holdId = hold("B4");
        db.sql("UPDATE seat_hold SET expires_at = now() - interval '1 minute' WHERE id = :id").bind("id", holdId).then().block();
        db.sql("UPDATE seat_inventory SET hold_expires_at = now() - interval '1 minute' WHERE hold_id = :id").bind("id", holdId).then().block();

        SeatReleased released = events.await(Topics.SEAT_RELEASED, SeatReleased.class, e -> e.holdId().equals(holdId), WAIT);
        assertThat(released.reason()).isEqualTo(ReleaseReason.EXPIRED);
        assertThat(seatStatus("B4")).isEqualTo("AVAILABLE");
        client.get().uri("/holds/{id}", holdId).exchange().expectBody().jsonPath("$.status").isEqualTo("EXPIRED");
    }

    private UUID hold(String seat) {
        Map<?, ?> body = client.post().uri("/holds").header("Idempotency-Key", UUID.randomUUID().toString())
                .bodyValue(Map.of("trainNumber", "ER202", "travelDate", DATE.toString(), "fareClass", "BUSINESS", "seatNo", seat))
                .exchange().expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody();
        return UUID.fromString((String) body.get("holdId"));
    }

    private String seatStatus(String seat) {
        return db.sql("SELECT status FROM seat_inventory WHERE train_number = 'ER202' AND travel_date = :d AND seat_no = :s")
                .bind("d", DATE).bind("s", seat).map(r -> r.get("status", String.class)).one().block();
    }
}
