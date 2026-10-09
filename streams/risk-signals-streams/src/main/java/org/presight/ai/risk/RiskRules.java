package org.presight.ai.risk;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Seat-squatting rule: within one activity session (events no more than {@code sessionGap} apart), a user
 * who places {@code holdThreshold} or more holds while converting at most {@code maxConversion} of them into
 * bookings is flagged — again at every further multiple of the threshold.
 */
@ConfigurationProperties("risk.squatting")
public record RiskRules(int holdThreshold, double maxConversion, Duration sessionGap, Duration grace) {

    public RiskRules {
        if (holdThreshold < 1) {
            throw new IllegalArgumentException("risk.squatting.hold-threshold must be at least 1");
        }
    }

    /** How many thresholds the activity has crossed while converting badly; 0 means "not suspicious". */
    int level(long holds, long bookings) {
        boolean lowConversion = holds > 0 && (double) bookings / holds <= maxConversion;
        return lowConversion ? (int) (holds / holdThreshold) : 0;
    }
}
