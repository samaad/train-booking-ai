package org.presight.ai.availability;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.presight.ai.common.kafka.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.databind.json.JsonMapper;

/**
 * The live seat map (02-ai-layer reserves WebSocket for this — it is genuinely push). A client connects to
 * {@code /ws/seat-map?train=ER101&date=2026-10-20} and receives the occupancy every time it changes.
 *
 * <p>{@link #publish} runs on a Kafka Streams thread, which must never block on a slow browser: each session
 * is wrapped with a bounded send buffer and time limit, and a client that falls behind is dropped.
 */
@Component
public class LiveSeatMap extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(LiveSeatMap.class);
    private static final int SEND_TIME_LIMIT_MS = 2_000;
    private static final int BUFFER_LIMIT_BYTES = 256 * 1024;

    private final Map<String, Set<WebSocketSession>> subscribers = new ConcurrentHashMap<>();
    private final JsonMapper json;

    public LiveSeatMap(JsonMapper json) {
        this.json = json;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws IOException {
        String key = keyOf(session.getUri());
        if (key == null) {
            session.close(CloseStatus.BAD_DATA.withReason("train and date query parameters are required"));
            return;
        }
        session.getAttributes().put("key", key);
        subscribers.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet())
                .add(new ConcurrentWebSocketSessionDecorator(session, SEND_TIME_LIMIT_MS, BUFFER_LIMIT_BYTES,
                        ConcurrentWebSocketSessionDecorator.OverflowStrategy.TERMINATE));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        Object key = session.getAttributes().get("key");
        if (key != null) {
            Set<WebSocketSession> sessions = subscribers.get(key);
            if (sessions != null) {
                sessions.removeIf(s -> s.getId().equals(session.getId()));
            }
        }
    }

    /** Called from the topology for every updated seat map. */
    public void publish(String key, SeatMap map) {
        Set<WebSocketSession> sessions = subscribers.get(key);
        if (sessions == null || sessions.isEmpty()) {
            return;
        }
        TextMessage message = new TextMessage(json.writeValueAsString(
                SeatAvailabilityApplication.AvailabilityQuery.summarise(map, Instant.now())));
        for (WebSocketSession session : sessions) {
            try {
                session.sendMessage(message);
            } catch (IOException | IllegalStateException e) {
                log.debug("Dropping live seat-map subscriber {}: {}", session.getId(), e.toString());
                sessions.remove(session);
            }
        }
    }

    private static String keyOf(URI uri) {
        if (uri == null) {
            return null;
        }
        var params = UriComponentsBuilder.fromUri(uri).build().getQueryParams();
        String train = params.getFirst("train");
        String date = params.getFirst("date");
        return train == null || date == null ? null : Topics.seatKey(train, date);
    }
}
