package org.presight.ai.inventory.hold;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/**
 * @param status       effective status — {@code EXPIRED} is derived by the database clock
 * @param requestHash  fingerprint of the request that created the hold, for idempotent replay
 * @param userId       owner (JWT subject); null only for holds created before Phase 4
 */
public record Hold(
        UUID id,
        String trainNumber,
        LocalDate travelDate,
        String fareClass,
        String seatNo,
        HoldStatus status,
        Instant expiresAt,
        String requestHash,
        String userId) {

    public enum HoldStatus { ACTIVE, EXPIRED, RELEASED, COMMITTED }
}
