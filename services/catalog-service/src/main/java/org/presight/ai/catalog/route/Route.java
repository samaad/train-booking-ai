package org.presight.ai.catalog.route;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.util.List;
import java.util.Set;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * A scheduled train, denormalised with its stops — the shape the search reads in one fetch.
 *
 * @param runsOn days on which the train departs its <em>origin</em>; a stop with
 *               {@code dayOffset > 0} is reached on a later calendar day
 */
@Document("routes")
public record Route(
        @Id String trainNumber,
        String trainName,
        Set<DayOfWeek> runsOn,
        List<Stop> stops,
        List<String> fareClasses) {

    /**
     * @param arrival    {@code null} at the origin
     * @param departure  {@code null} at the terminus
     * @param dayOffset  calendar days after the origin departure
     * @param distanceKm cumulative distance from the origin
     */
    public record Stop(String stationCode, LocalTime arrival, LocalTime departure, int dayOffset, int distanceKm) {
    }
}
