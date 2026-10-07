package org.presight.ai.catalog.config;

import java.time.Duration;
import java.time.ZoneId;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param zone           zone the timetable's local times are expressed in
 * @param searchCacheTtl bounds staleness until catalog.updated invalidation is wired
 * @param seed           load the sample timetable into an empty database at startup
 */
@ConfigurationProperties("catalog")
public record CatalogProperties(ZoneId zone, Duration searchCacheTtl, boolean seed) {
}
