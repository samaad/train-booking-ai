package org.presight.ai.availability;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/** Occupancy is public (like inventory's /availability), so the live map needs no token. */
@Configuration(proxyBeanMethods = false)
@EnableWebSocket
class WebSocketConfiguration implements WebSocketConfigurer {

    private final LiveSeatMap liveSeatMap;

    WebSocketConfiguration(LiveSeatMap liveSeatMap) {
        this.liveSeatMap = liveSeatMap;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(liveSeatMap, "/ws/seat-map").setAllowedOrigins("http://localhost:5173");
    }
}
