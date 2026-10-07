package org.presight.ai.booking.booking;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.presight.ai.booking.client.CatalogClient.FareRuleView;

/**
 * The one place a fare is computed (01-services: booking owns the authoritative price; catalog
 * only stores the rule). {@code baseFare + perKm × distanceKm}, rounded half-up to the cent.
 */
final class Fares {

    private Fares() {
    }

    static BigDecimal price(FareRuleView rule, int distanceKm) {
        if (distanceKm <= 0) {
            throw new IllegalArgumentException("distanceKm must be positive: " + distanceKm);
        }
        return rule.baseFare()
                .add(rule.perKm().multiply(BigDecimal.valueOf(distanceKm)))
                .setScale(2, RoundingMode.HALF_UP);
    }
}
