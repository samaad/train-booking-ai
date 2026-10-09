package org.presight.ai.availability;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.SeatEvents.SeatCommitted;
import org.presight.ai.common.kafka.events.SeatEvents.SeatHeld;
import org.presight.ai.common.kafka.testing.KafkaTestSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestClient;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.testcontainers.kafka.KafkaContainer;

/** The running app against a real broker: events in → queryable store and live WebSocket push out. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.kafka.streams.state-dir=target/streams-state-${random.uuid}")
@Import(SeatAvailabilityIntegrationTest.Containers.class)
class SeatAvailabilityIntegrationTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean
        @ServiceConnection
        KafkaContainer kafka() {
            return new KafkaContainer("apache/kafka:4.3.1");
        }
    }

    private static final LocalDate DATE = LocalDate.of(2026, 10, 21);

    @LocalServerPort
    int port;

    @Autowired
    KafkaContainer kafka;

    @Test
    void seatEventsAreQueryableAndPushedLive() throws Exception {
        BlockingQueue<String> pushes = new LinkedBlockingQueue<>();
        WebSocketHttpHeaders headers = new WebSocketHttpHeaders();
        headers.setOrigin("http://localhost:5173");
        WebSocketSession ws = new StandardWebSocketClient().execute(new TextWebSocketHandler() {
            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                pushes.add(message.getPayload());
            }
        }, headers, URI.create("ws://localhost:" + port + "/ws/seat-map?train=ER202&date=" + DATE)).get(10, TimeUnit.SECONDS);

        UUID hold = UUID.randomUUID();
        try (KafkaTestSupport events = new KafkaTestSupport(kafka.getBootstrapServers(), List.of())) {
            events.publish(Topics.SEAT_HELD, new SeatHeld(UUID.randomUUID(), hold, "ER202", DATE, "BUSINESS", "B3",
                    Instant.now().plusSeconds(600), Instant.now(), "user-1"));
            events.publish(Topics.SEAT_COMMITTED, new SeatCommitted(UUID.randomUUID(), hold, "ER202", DATE, "BUSINESS", "B3",
                    Instant.now().plusSeconds(1)));
        }

        RestClient http = RestClient.create("http://localhost:" + port);
        Map<?, ?> occupancy = null;
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                occupancy = http.get().uri("/occupancy/ER202/{d}", DATE).retrieve().body(Map.class);
                if (occupancy != null && "COMMITTED".equals(((Map<?, ?>) occupancy.get("seats")).get("B3"))) {
                    break;
                }
            } catch (RuntimeException notRunningYet) {
                // 503 until the stream reaches RUNNING
            }
            Thread.sleep(300);
        }
        assertThat(occupancy).isNotNull();
        assertThat(((Map<?, ?>) occupancy.get("seats")).get("B3")).isEqualTo("COMMITTED");

        String push = pushes.poll(30, TimeUnit.SECONDS);
        assertThat(push).isNotNull().contains("\"B3\"");
        ws.close();
    }
}
