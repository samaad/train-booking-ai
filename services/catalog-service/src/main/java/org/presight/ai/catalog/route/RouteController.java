package org.presight.ai.catalog.route;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.LocalDate;
import java.util.List;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class RouteController {

    private final RouteSearch search;
    private final SearchCache cache;
    private final Counter searches;

    RouteController(RouteSearch search, SearchCache cache, MeterRegistry meters) {
        this.search = search;
        this.cache = cache;
        this.searches = meters.counter("booking.funnel", "stage", "search");
    }

    @GetMapping("/routes/search")
    public Mono<List<JourneyOption>> search(
            @RequestParam String from,
            @RequestParam String to,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        searches.increment();
        return cache.getOrLoad(SearchCache.key(from, to, date), () -> search.search(from, to, date).collectList());
    }
}
