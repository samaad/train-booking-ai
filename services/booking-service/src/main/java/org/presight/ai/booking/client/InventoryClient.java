package org.presight.ai.booking.client;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.annotation.PostExchange;
import reactor.core.publisher.Mono;

/** inventory-service, as booking needs to see it. A tolerant reader: unknown fields are ignored. */
@HttpExchange
public interface InventoryClient {

    record HoldView(UUID holdId, String trainNumber, LocalDate travelDate, String fareClass,
                    String seatNo, String status, Instant expiresAt) {

        public boolean isActive() {
            return "ACTIVE".equals(status);
        }
    }

    @GetExchange("/holds/{holdId}")
    Mono<HoldView> getHold(@PathVariable UUID holdId);

    /** Idempotent in inventory (re-committing a committed hold returns it), so a redelivered saga event may call it again. */
    @PostExchange("/holds/{holdId}/commit")
    Mono<HoldView> commit(@PathVariable UUID holdId);
}
