package org.presight.ai.catalog.fare;

import java.math.BigDecimal;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Pricing <em>inputs</em> only. Catalog deliberately does not compute fares: booking-service
 * owns the authoritative price at quote time, so the UI can never quote one number and charge
 * another. A fare is {@code baseFare + perKm × distanceKm} in {@code currency}.
 *
 * @param id {@code trainNumber:fareClass}
 */
@Document("fare_rules")
public record FareRule(@Id String id, String trainNumber, String fareClass,
                       BigDecimal baseFare, BigDecimal perKm, String currency) {

    public static FareRule of(String trainNumber, String fareClass, String baseFare, String perKm, String currency) {
        return new FareRule(trainNumber + ':' + fareClass, trainNumber, fareClass,
                new BigDecimal(baseFare), new BigDecimal(perKm), currency);
    }
}
