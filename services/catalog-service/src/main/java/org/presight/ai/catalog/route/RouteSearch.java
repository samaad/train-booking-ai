package org.presight.ai.catalog.route;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.Optional;
import org.presight.ai.catalog.config.CatalogProperties;
import org.presight.ai.catalog.route.Route.Stop;
import org.presight.ai.catalog.station.StationRepository;
import org.presight.ai.common.error.DomainException;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class RouteSearch {

    private final ReactiveMongoTemplate mongo;
    private final StationRepository stations;
    private final ZoneId zone;

    public RouteSearch(ReactiveMongoTemplate mongo, StationRepository stations, CatalogProperties properties) {
        this.mongo = mongo;
        this.stations = stations;
        this.zone = properties.zone();
    }

    /** Trains calling at {@code from} then {@code to}, departing {@code from} on {@code date}, earliest first. */
    public Flux<JourneyOption> search(String from, String to, LocalDate date) {
        if (from.equals(to)) {
            return Flux.error(DomainException.invalid("SAME_ORIGIN_AND_DESTINATION", "from and to must differ"));
        }
        return requireStation(from)
                .then(requireStation(to))
                .thenMany(mongo.find(Query.query(Criteria.where("stops.stationCode").all(from, to)), Route.class))
                .flatMapIterable(route -> journey(route, from, to, date).stream().toList())
                .sort(Comparator.comparing(JourneyOption::departure));
    }

    private Mono<Void> requireStation(String code) {
        return stations.existsById(code)
                .flatMap(exists -> exists ? Mono.<Void>empty()
                        : Mono.error(DomainException.notFound("STATION_NOT_FOUND", "Unknown station " + code)));
    }

    Optional<JourneyOption> journey(Route route, String from, String to, LocalDate date) {
        int fromIdx = indexOf(route, from);
        int toIdx = indexOf(route, to);
        if (fromIdx < 0 || toIdx <= fromIdx) {
            return Optional.empty(); // wrong direction
        }
        Stop board = route.stops().get(fromIdx);
        Stop alight = route.stops().get(toIdx);
        LocalDate originDate = date.minusDays(board.dayOffset());
        if (board.departure() == null || !route.runsOn().contains(originDate.getDayOfWeek())) {
            return Optional.empty();
        }
        ZonedDateTime departure = originDate.plusDays(board.dayOffset()).atTime(board.departure()).atZone(zone);
        ZonedDateTime arrival = originDate.plusDays(alight.dayOffset()).atTime(alight.arrival()).atZone(zone);
        return Optional.of(new JourneyOption(
                route.trainNumber(), route.trainName(), from, to,
                departure.toOffsetDateTime(), arrival.toOffsetDateTime(),
                Duration.between(departure, arrival).toMinutes(),
                alight.distanceKm() - board.distanceKm(),
                route.fareClasses()));
    }

    private static int indexOf(Route route, String stationCode) {
        for (int i = 0; i < route.stops().size(); i++) {
            if (route.stops().get(i).stationCode().equals(stationCode)) {
                return i;
            }
        }
        return -1;
    }
}
