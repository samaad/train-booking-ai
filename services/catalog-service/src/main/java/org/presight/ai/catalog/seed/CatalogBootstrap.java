package org.presight.ai.catalog.seed;

import static java.time.DayOfWeek.FRIDAY;
import static java.time.DayOfWeek.SATURDAY;
import static java.time.DayOfWeek.SUNDAY;
import static java.time.DayOfWeek.THURSDAY;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.util.EnumSet;
import java.util.List;
import org.presight.ai.catalog.config.CatalogProperties;
import org.presight.ai.catalog.fare.FareRule;
import org.presight.ai.catalog.route.Route;
import org.presight.ai.catalog.route.Route.Stop;
import org.presight.ai.catalog.station.Station;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Ensures indexes and, on an empty database, loads a <b>sample</b> UAE timetable. The
 * schedules are invented for development; train numbers and fare classes must stay in step
 * with inventory-service's seat layout config.
 *
 * <p>Blocking here is fine: it runs once on the main thread before the server accepts traffic.
 */
@Component
class CatalogBootstrap implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CatalogBootstrap.class);
    private static final EnumSet<DayOfWeek> DAILY = EnumSet.allOf(DayOfWeek.class);
    private static final List<String> TWO_CLASS = List.of("BUSINESS", "ECONOMY");

    private final ReactiveMongoTemplate mongo;
    private final boolean seed;

    CatalogBootstrap(ReactiveMongoTemplate mongo, CatalogProperties properties) {
        this.mongo = mongo;
        this.seed = properties.seed();
    }

    @Override
    public void run(ApplicationArguments args) {
        mongo.indexOps(Route.class).createIndex(new Index("stops.stationCode", Sort.Direction.ASC)).block();
        if (seed) {
            mongo.count(new Query(), Route.class)
                    .filter(n -> n == 0)
                    .flatMap(empty -> load())
                    .block();
        }
    }

    private Mono<Void> load() {
        log.info("Empty catalog — loading sample timetable");
        return Flux.concat(
                mongo.insertAll(stations()),
                mongo.insertAll(routes()),
                mongo.insertAll(fareRules())).then();
    }

    private static List<Station> stations() {
        return List.of(
                new Station("RUW", "Al Ruwais", "Al Ruwais"),
                new Station("MRF", "Al Mirfa", "Al Mirfa"),
                new Station("AUH", "Abu Dhabi", "Abu Dhabi"),
                new Station("DXB", "Dubai", "Dubai"),
                new Station("SHJ", "Sharjah", "Sharjah"),
                new Station("FUJ", "Fujairah", "Fujairah"));
    }

    private static List<Route> routes() {
        return List.of(
                new Route("ER101", "Gulf Express", DAILY, List.of(
                        stop("AUH", null, "07:00", 0, 0),
                        stop("DXB", "07:50", "07:55", 0, 140),
                        stop("SHJ", "08:15", "08:18", 0, 170),
                        stop("FUJ", "09:05", null, 0, 270)), TWO_CLASS),
                new Route("ER102", "Gulf Express", DAILY, List.of(
                        stop("FUJ", null, "17:00", 0, 0),
                        stop("SHJ", "17:47", "17:50", 0, 100),
                        stop("DXB", "18:10", "18:15", 0, 130),
                        stop("AUH", "19:05", null, 0, 270)), TWO_CLASS),
                new Route("ER201", "Western Line", DAILY, List.of(
                        stop("RUW", null, "06:00", 0, 0),
                        stop("MRF", "06:45", "06:48", 0, 130),
                        stop("AUH", "07:40", null, 0, 240)), TWO_CLASS),
                new Route("ER202", "Western Line", DAILY, List.of(
                        stop("AUH", null, "20:00", 0, 0),
                        stop("MRF", "20:52", "20:55", 0, 110),
                        stop("RUW", "21:40", null, 0, 240)), TWO_CLASS),
                // Overnight weekend services: later stops fall on the next calendar day.
                new Route("ER301", "Night Coastal", EnumSet.of(FRIDAY, SATURDAY), List.of(
                        stop("RUW", null, "22:30", 0, 0),
                        stop("AUH", "00:10", "00:20", 1, 240),
                        stop("DXB", "01:10", null, 1, 380)), List.of("ECONOMY")),
                new Route("ER302", "Night Coastal", EnumSet.of(THURSDAY, SUNDAY), List.of(
                        stop("DXB", null, "23:30", 0, 0),
                        stop("AUH", "00:20", "00:30", 1, 140),
                        stop("RUW", "02:10", null, 1, 380)), List.of("ECONOMY")));
    }

    private static List<FareRule> fareRules() {
        return routes().stream()
                .flatMap(r -> r.fareClasses().stream().map(c -> c.equals("BUSINESS")
                        ? FareRule.of(r.trainNumber(), c, "25.00", "0.90", "AED")
                        : FareRule.of(r.trainNumber(), c, "10.00", "0.35", "AED")))
                .toList();
    }

    private static Stop stop(String station, String arrival, String departure, int dayOffset, int km) {
        return new Stop(station, arrival == null ? null : LocalTime.parse(arrival),
                departure == null ? null : LocalTime.parse(departure), dayOffset, km);
    }
}
