package org.presight.ai.inventory.availability;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class AvailabilityController {

    private final SeatLayout layout;
    private final DatabaseClient db;

    public AvailabilityController(SeatLayout layout, DatabaseClient db) {
        this.layout = layout;
        this.db = db;
    }

    public record ClassAvailability(String fareClass, int total, int available, List<String> availableSeats) {
    }

    public record AvailabilityResponse(String trainNumber, LocalDate travelDate, List<ClassAvailability> classes) {
    }

    /** A seat whose hold has expired counts as available — the same rule the hold UPDATE applies. */
    @GetMapping("/availability")
    public Mono<AvailabilityResponse> availability(
            @RequestParam String trainNumber,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate travelDate) {
        return layout.ensureSeats(trainNumber, travelDate)
                .thenMany(db.sql("""
                                SELECT fare_class, seat_no,
                                       (status = 'AVAILABLE' OR (status = 'HELD' AND hold_expires_at < now())) AS free
                                FROM seat_inventory
                                WHERE train_number = :train AND travel_date = :date
                                ORDER BY fare_class, length(seat_no), seat_no
                                """)
                        .bind("train", trainNumber)
                        .bind("date", travelDate)
                        .map((row, meta) -> new SeatRow(
                                row.get("fare_class", String.class),
                                row.get("seat_no", String.class),
                                Boolean.TRUE.equals(row.get("free", Boolean.class))))
                        .all())
                .collectList()
                .map(rows -> new AvailabilityResponse(trainNumber, travelDate, summarise(rows)));
    }

    private record SeatRow(String fareClass, String seatNo, boolean free) {
    }

    private static List<ClassAvailability> summarise(List<SeatRow> rows) {
        Map<String, List<SeatRow>> byClass = new LinkedHashMap<>();
        rows.forEach(r -> byClass.computeIfAbsent(r.fareClass(), k -> new ArrayList<>()).add(r));
        return byClass.entrySet().stream()
                .map(e -> {
                    List<String> free = e.getValue().stream().filter(SeatRow::free).map(SeatRow::seatNo).toList();
                    return new ClassAvailability(e.getKey(), e.getValue().size(), free.size(), free);
                })
                .toList();
    }
}
