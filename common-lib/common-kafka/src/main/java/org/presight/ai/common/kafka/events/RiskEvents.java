package org.presight.ai.common.kafka.events;

import java.time.Instant;
import java.util.UUID;

/** Produced by risk-signals-streams, keyed by userId. Signals are advisory: nothing blocks a user on its own. */
public final class RiskEvents {

    private RiskEvents() {
    }

    public enum RiskKind {
        /** Many holds in one burst of activity, few of them turned into bookings: seats kept from other passengers. */
        SEAT_SQUATTING
    }

    /**
     * @param holds          seat holds in the user's current activity session
     * @param bookings       bookings created in the same session
     * @param sessionStart   first event of the session
     * @param sessionEnd     latest event of the session
     */
    public record RiskSignal(UUID eventId, String userId, RiskKind kind, long holds, long bookings,
                             Instant sessionStart, Instant sessionEnd, Instant occurredAt) implements Event {
        public String key() { return userId; }
    }
}
