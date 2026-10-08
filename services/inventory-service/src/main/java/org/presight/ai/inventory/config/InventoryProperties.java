package org.presight.ai.inventory.config;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param holdTtl         how long a hold reserves a seat before it becomes reclaimable
 * @param bookingHorizon  how far ahead seats may be held; bounds lazy seat materialisation
 * @param zone            the railway's local zone, which defines "today" for travel dates
 * @param trains          train number → fare class → seat block
 */
@ConfigurationProperties("inventory")
public record InventoryProperties(
        Duration holdTtl,
        Duration bookingHorizon,
        ZoneId zone,
        ContentionFilter contentionFilter,
        Sweeper sweeper,
        Map<String, Map<String, SeatBlock>> trains) {

    public record Sweeper(Duration interval, int batchSize) {
    }

    public record ContentionFilter(boolean enabled, Duration lockTtl) {
    }

    /** {@code seats} seats numbered {@code prefix + 1 .. prefix + seats}. */
    public record SeatBlock(String prefix, int seats) {
    }
}
