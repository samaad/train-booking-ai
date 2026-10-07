package org.presight.ai.inventory.availability;

import java.time.LocalDate;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.inventory.config.InventoryProperties;
import org.presight.ai.inventory.config.InventoryProperties.SeatBlock;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Validates train / fare class / travel date and materialises a train-date's seat rows on
 * first use. Materialisation is {@code INSERT … ON CONFLICT DO NOTHING}, so concurrent first
 * requests for the same train-date are safe and the in-memory set is only an optimisation.
 */
@Component
public class SeatLayout {

    private final InventoryProperties properties;
    private final DatabaseClient db;
    private final Set<String> materialised = ConcurrentHashMap.newKeySet();

    public SeatLayout(InventoryProperties properties, DatabaseClient db) {
        this.properties = properties;
        this.db = db;
    }

    /** Validates the train and date, then ensures every fare class's seats exist. */
    public Mono<Void> ensureSeats(String trainNumber, LocalDate travelDate) {
        return Mono.defer(() -> {
            Map<String, SeatBlock> layout = layoutOf(trainNumber);
            checkTravelDate(travelDate);
            String key = trainNumber + '|' + travelDate;
            if (materialised.contains(key)) {
                return Mono.empty();
            }
            return Flux.fromIterable(layout.entrySet())
                    .concatMap(e -> insertSeats(trainNumber, travelDate, e.getKey(), e.getValue()))
                    .then(Mono.fromRunnable(() -> materialised.add(key)));
        });
    }

    /** As {@link #ensureSeats}, additionally rejecting a fare class the train does not have. */
    public Mono<Void> ensureSeats(String trainNumber, LocalDate travelDate, String fareClass) {
        return Mono.defer(() -> {
            if (!layoutOf(trainNumber).containsKey(fareClass)) {
                return Mono.error(DomainException.invalid("UNKNOWN_FARE_CLASS",
                        "Train " + trainNumber + " has no fare class " + fareClass));
            }
            return ensureSeats(trainNumber, travelDate);
        });
    }

    private Map<String, SeatBlock> layoutOf(String trainNumber) {
        Map<String, SeatBlock> layout = properties.trains().get(trainNumber);
        if (layout == null) {
            throw DomainException.notFound("TRAIN_NOT_FOUND", "Unknown train " + trainNumber);
        }
        return layout;
    }

    private void checkTravelDate(LocalDate travelDate) {
        LocalDate today = LocalDate.now(properties.zone());
        LocalDate last = today.plusDays(properties.bookingHorizon().toDays());
        if (travelDate.isBefore(today) || travelDate.isAfter(last)) {
            throw DomainException.invalid("TRAVEL_DATE_OUT_OF_RANGE",
                    "Travel date must be between " + today + " and " + last);
        }
    }

    private Mono<Long> insertSeats(String trainNumber, LocalDate travelDate, String fareClass, SeatBlock block) {
        return db.sql("""
                        INSERT INTO seat_inventory (train_number, travel_date, fare_class, seat_no)
                        SELECT :train, :date, :fareClass, :prefix || g FROM generate_series(1, :seats) g
                        ON CONFLICT ON CONSTRAINT uq_seat DO NOTHING
                        """)
                .bind("train", trainNumber)
                .bind("date", travelDate)
                .bind("fareClass", fareClass)
                .bind("prefix", block.prefix())
                .bind("seats", block.seats())
                .fetch()
                .rowsUpdated();
    }
}
