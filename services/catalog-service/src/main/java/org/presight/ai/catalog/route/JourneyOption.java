package org.presight.ai.catalog.route;

import java.time.OffsetDateTime;
import java.util.List;

/** One train that serves {@code from → to} on the requested date. Carries no price — see FareRule. */
public record JourneyOption(
        String trainNumber,
        String trainName,
        String from,
        String to,
        OffsetDateTime departure,
        OffsetDateTime arrival,
        long durationMinutes,
        int distanceKm,
        List<String> fareClasses) {
}
