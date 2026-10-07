package org.presight.ai.booking.client;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;
import reactor.core.publisher.Flux;

/** catalog-service, as booking needs to see it. A tolerant reader: unknown fields are ignored. */
@HttpExchange
public interface CatalogClient {

    record JourneyView(String trainNumber, String from, String to, OffsetDateTime departure,
                       OffsetDateTime arrival, int distanceKm, List<String> fareClasses) {
    }

    record FareRuleView(String trainNumber, String fareClass, BigDecimal baseFare, BigDecimal perKm, String currency) {
    }

    /** The explicit ISO format matters: without it the date is sent in a locale format (e.g. 17/10/2026). */
    @GetExchange("/routes/search")
    Flux<JourneyView> search(@RequestParam String from, @RequestParam String to,
                             @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date);

    @GetExchange("/fares/rules")
    Flux<FareRuleView> fareRules(@RequestParam String trainNumber, @RequestParam String fareClass);
}
