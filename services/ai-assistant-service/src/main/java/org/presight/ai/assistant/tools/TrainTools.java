package org.presight.ai.assistant.tools;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.presight.ai.assistant.client.ServiceClients.Availability;
import org.presight.ai.assistant.client.ServiceClients.Booking;
import org.presight.ai.assistant.client.ServiceClients.BookingClient;
import org.presight.ai.assistant.client.ServiceClients.CatalogClient;
import org.presight.ai.assistant.client.ServiceClients.Hold;
import org.presight.ai.assistant.client.ServiceClients.HoldRequest;
import org.presight.ai.assistant.client.ServiceClients.InventoryClient;
import org.presight.ai.assistant.client.ServiceClients.Journey;
import org.presight.ai.assistant.client.ServiceClients.Station;
import org.presight.ai.assistant.config.AssistantProperties;
import org.presight.ai.assistant.event.ChatEvent;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.idempotency.IdempotencyKey;
import org.presight.ai.common.web.RemoteCallErrors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.model.ToolContext;
import org.springframework.ai.support.ToolCallbacks;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.stereotype.Component;
import reactor.core.scheduler.Schedulers;
import tools.jackson.databind.json.JsonMapper;

/**
 * Thin façades over real service calls. They return structured data, never prose — the model narrates,
 * it never authors facts.
 *
 * <p>Invariants enforced here, not in the prompt:
 * <ul>
 *   <li>Identity comes from the {@link Turn} in ToolContext. No tool has a user parameter, so the model
 *       cannot even express "for another user".</li>
 *   <li>Idempotency keys are derived server-side from (user, subject, operation) — never model-supplied.</li>
 *   <li>{@code confirmBooking}/{@code cancelBooking} never act: they emit a confirmation card and the user
 *       confirms in the UI. {@code confirmBooking} only accepts a hold issued earlier in this conversation.</li>
 *   <li>Tool calls run on {@code aiScheduler} threads; one landing on an event-loop thread is refused.</li>
 * </ul>
 * Each call blocks on its service — legitimate on the bounded aiScheduler, which exists for exactly this.
 */
@Component
public class TrainTools implements TrainToolRegistry {

    private static final Logger log = LoggerFactory.getLogger(TrainTools.class);

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration HOLD_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration STATIONS_TTL = Duration.ofMinutes(10);

    private final CatalogClient catalog;
    private final InventoryClient inventory;
    private final BookingClient bookings;
    private final JsonMapper json;
    private final MeterRegistry meters;
    private final AssistantProperties properties;
    private final List<ToolCallback> callbacks;

    private volatile List<Station> stations = List.of();
    private volatile Instant stationsLoadedAt = Instant.EPOCH;

    TrainTools(CatalogClient catalog, InventoryClient inventory, BookingClient bookings, JsonMapper json,
               MeterRegistry meters, AssistantProperties properties) {
        this.catalog = catalog;
        this.inventory = inventory;
        this.bookings = bookings;
        this.json = json;
        this.meters = meters;
        this.properties = properties;
        this.callbacks = List.of(ToolCallbacks.from(this));
    }

    @Override
    public List<ToolCallback> callbacks() {
        return callbacks;
    }

    // ------------------------------------------------------------------ result shapes (what the model sees)

    public record TrainOption(String trainNumber, String trainName, String from, String to, String departure,
                              String arrival, long durationMinutes, List<String> fareClasses) {
    }

    public record SeatAvailability(String trainNumber, LocalDate travelDate, String fareClass, int available,
                                   int total, List<String> someFreeSeats) {
    }

    public record HeldSeat(UUID holdId, String trainNumber, LocalDate travelDate, String fareClass, String seatNo,
                           String expiresAt) {
    }

    public record BookingStatus(UUID bookingId, String pnr, String status, String trainNumber, LocalDate travelDate,
                                String seatNo, String fromStation, String toStation, String amount, String currency) {
    }

    public record AwaitingUser(String status, String message) {
    }

    public record ToolError(String error, String message) {
    }

    // ------------------------------------------------------------------ read-only tools

    @Tool(name = "searchTrains", description = "Find trains between two stations on a date. Returns real schedules.")
    public Object searchTrains(
            @ToolParam(description = "origin station code or name, e.g. DXB or Dubai") String from,
            @ToolParam(description = "destination station code or name") String to,
            @ToolParam(description = "travel date as yyyy-MM-dd, or 'today' / 'tomorrow'") String date,
            ToolContext context) {
        Turn turn = turn(context);
        return run(turn, "searchTrains", false, () -> {
            String fromCode = stationCode(turn, from);
            String toCode = stationCode(turn, to);
            List<Journey> journeys = catalog.search(turn.caller().authorization(), fromCode, toCode, parseDate(date))
                    .collectList()
                    .onErrorMap(RemoteCallErrors.from("catalog"))
                    .block(READ_TIMEOUT);
            return journeys.stream().map(j -> new TrainOption(j.trainNumber(), j.trainName(), j.from(), j.to(),
                    local(j.departure()), local(j.arrival()), j.durationMinutes(), j.fareClasses())).toList();
        });
    }

    @Tool(name = "checkAvailability", description = "Count free seats on a train for a date and fare class.")
    public Object checkAvailability(
            @ToolParam(description = "train number, e.g. ER101") String trainNumber,
            @ToolParam(description = "travel date as yyyy-MM-dd") String date,
            @ToolParam(description = "fare class: BUSINESS or ECONOMY") String fareClass,
            ToolContext context) {
        Turn turn = turn(context);
        return run(turn, "checkAvailability", false, () -> {
            LocalDate travelDate = parseDate(date);
            Availability availability = inventory.availability(turn.caller().authorization(), trainNumber, travelDate)
                    .onErrorMap(RemoteCallErrors.from("inventory"))
                    .block(READ_TIMEOUT);
            String wanted = fareClass.toUpperCase(Locale.ROOT);
            return availability.classes().stream()
                    .filter(c -> c.fareClass().equals(wanted))
                    .findFirst()
                    .<Object>map(c -> new SeatAvailability(trainNumber, travelDate, c.fareClass(), c.available(),
                            c.total(), c.availableSeats().stream().limit(10).toList()))
                    .orElseGet(() -> new ToolError("UNKNOWN_FARE_CLASS", trainNumber + " has no " + wanted + " class"));
        });
    }

    @Tool(name = "getBookingStatus", description = "Look up a booking by its booking id.")
    public Object getBookingStatus(@ToolParam(description = "booking id (UUID)") String bookingId, ToolContext context) {
        Turn turn = turn(context);
        return run(turn, "getBookingStatus", false, () -> {
            Booking b = bookings.get(turn.caller().authorization(), parseUuid(bookingId, "booking id"))
                    .onErrorMap(RemoteCallErrors.from("booking"))
                    .block(READ_TIMEOUT);
            return new BookingStatus(b.bookingId(), b.pnr(), b.status(), b.trainNumber(), b.travelDate(), b.seatNo(),
                    b.fromStation(), b.toStation(), b.amount().toPlainString(), b.currency());
        });
    }

    // ------------------------------------------------------------------ mutating tools

    /** Reversible: a hold lapses on its own. The server-issued holdId is what makes confirmBooking reachable. */
    @Tool(name = "holdSeat", description = "Temporarily reserve one seat. Required before confirmBooking. Holds expire after a few minutes.")
    public Object holdSeat(
            @ToolParam(description = "train number, e.g. ER101") String trainNumber,
            @ToolParam(description = "travel date as yyyy-MM-dd") String date,
            @ToolParam(description = "fare class: BUSINESS or ECONOMY") String fareClass,
            @ToolParam(description = "specific seat like B4; omit for any seat", required = false) String seatNo,
            ToolContext context) {
        Turn turn = turn(context);
        return run(turn, "holdSeat", true, () -> {
            LocalDate travelDate = parseDate(date);
            String cls = fareClass.toUpperCase(Locale.ROOT);
            String seat = seatNo == null || seatNo.isBlank() ? null : seatNo.toUpperCase(Locale.ROOT);
            // Same user, same turn, same request → same key: a re-emitted tool call yields one hold.
            String subject = turn.conversationId() + ':' + turn.turnSeq() + ':' + trainNumber + ':' + travelDate + ':' + cls + ':' + seat;
            IdempotencyKey key = IdempotencyKey.derive(turn.caller().userId(), subject, "holdSeat");
            Hold hold = inventory.hold(turn.caller().authorization(), key.value(), new HoldRequest(trainNumber, travelDate, cls, seat))
                    .onErrorMap(RemoteCallErrors.from("inventory"))
                    .block(HOLD_TIMEOUT);
            turn.issueHold(hold.holdId());
            return new HeldSeat(hold.holdId(), hold.trainNumber(), hold.travelDate(), hold.fareClass(), hold.seatNo(),
                    local(hold.expiresAt().atOffset(java.time.ZoneOffset.UTC)));
        });
    }

    @Tool(name = "confirmBooking", description = "Ask the user to confirm booking a held seat. Shows them a confirmation card; it does NOT book by itself.")
    public Object confirmBooking(
            @ToolParam(description = "holdId returned by holdSeat in this conversation") String holdId,
            @ToolParam(description = "boarding station code") String fromStation,
            @ToolParam(description = "destination station code") String toStation,
            ToolContext context) {
        Turn turn = turn(context);
        return run(turn, "confirmBooking", true, () -> {
            UUID id = parseUuid(holdId, "holdId");
            if (!turn.wasIssued(id)) {
                // The model cannot fabricate its way to a booking: only holds this conversation created count.
                throw DomainException.invalid("UNKNOWN_HOLD", "That holdId was not issued in this conversation. Call holdSeat first.");
            }
            Hold hold = inventory.getHold(turn.caller().authorization(), id)
                    .onErrorMap(RemoteCallErrors.from("inventory"))
                    .block(READ_TIMEOUT);
            if (!"ACTIVE".equals(hold.status())) {
                throw DomainException.conflict("HOLD_NOT_ACTIVE", "The hold is " + hold.status() + "; hold a seat again.");
            }
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("holdId", hold.holdId());
            details.put("trainNumber", hold.trainNumber());
            details.put("travelDate", hold.travelDate());
            details.put("fareClass", hold.fareClass());
            details.put("seatNo", hold.seatNo());
            details.put("fromStation", stationCode(turn, fromStation));
            details.put("toStation", stationCode(turn, toStation));
            details.put("holdExpiresAt", hold.expiresAt());
            turn.emit(new ChatEvent.ConfirmationRequired("CONFIRM_BOOKING", "POST", "/api/booking/bookings",
                    IdempotencyKey.derive(turn.caller().userId(), hold.holdId().toString(), "confirmBooking").value(), details));
            return new AwaitingUser("AWAITING_USER_CONFIRMATION",
                    "A confirmation card is now shown to the user. Nothing is booked until they confirm it. Do not say it is booked.");
        });
    }

    @Tool(name = "cancelBooking", description = "Ask the user to confirm cancelling a booking. Shows a confirmation card; it does NOT cancel by itself.")
    public Object cancelBooking(
            @ToolParam(description = "booking id (UUID)") String bookingId,
            @ToolParam(description = "short reason given by the user", required = false) String reason,
            ToolContext context) {
        Turn turn = turn(context);
        return run(turn, "cancelBooking", true, () -> {
            Booking b = bookings.get(turn.caller().authorization(), parseUuid(bookingId, "booking id"))
                    .onErrorMap(RemoteCallErrors.from("booking"))
                    .block(READ_TIMEOUT);
            if ("CANCELLED".equals(b.status())) {
                throw DomainException.conflict("ALREADY_CANCELLED", "Booking " + b.pnr() + " is already cancelled.");
            }
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("bookingId", b.bookingId());
            details.put("pnr", b.pnr());
            details.put("trainNumber", b.trainNumber());
            details.put("travelDate", b.travelDate());
            details.put("reason", reason);
            turn.emit(new ChatEvent.ConfirmationRequired("CANCEL_BOOKING", "POST",
                    "/api/booking/bookings/" + b.bookingId() + "/cancel",
                    IdempotencyKey.derive(turn.caller().userId(), b.bookingId().toString(), "cancelBooking").value(), details));
            return new AwaitingUser("AWAITING_USER_CONFIRMATION",
                    "A cancellation card is now shown to the user. Nothing is cancelled until they confirm it.");
        });
    }

    // ------------------------------------------------------------------ plumbing

    private Object run(Turn turn, String tool, boolean mutating, Supplier<Object> body) {
        if (Schedulers.isInNonBlockingThread()) {
            throw new IllegalStateException(tool + " invoked on a non-blocking thread; tool callbacks must run on aiScheduler");
        }
        Object result;
        String outcome;
        try {
            turn.acquire(tool, mutating);
            result = body.get();
            outcome = "success";
        } catch (Turn.ToolBudgetExceeded e) {
            result = new ToolError("TOOL_BUDGET_EXCEEDED", e.getMessage());
            outcome = "budget_exceeded";
        } catch (DomainException e) {
            result = new ToolError(e.code(), e.getMessage());
            outcome = "error";
        } catch (IllegalArgumentException e) {
            result = new ToolError("INVALID_ARGUMENT", e.getMessage());
            outcome = "invalid_argument";
        } catch (RuntimeException e) {
            // Say what happened plainly rather than let the model guess — and only blame time when it was time.
            boolean timedOut = e.getCause() instanceof java.util.concurrent.TimeoutException
                    || String.valueOf(e.getMessage()).startsWith("Timeout on blocking read");
            log.warn("Tool {} failed", tool, e);
            result = new ToolError(timedOut ? "TOOL_TIMEOUT" : "TOOL_FAILED",
                    tool + (timedOut ? " failed: the service did not answer in time" : " failed unexpectedly"));
            outcome = timedOut ? "timeout" : "error";
        }
        meters.counter("tool.call", "tool", tool, "outcome", outcome).increment();
        log.debug("tool {} -> {} on thread {}", tool, outcome, Thread.currentThread().getName());
        turn.recordResult(json.writeValueAsString(result));
        turn.emit(new ChatEvent.ToolActivity(tool, outcome, result));
        return result;
    }

    private static Turn turn(ToolContext context) {
        Object turn = context == null ? null : context.getContext().get(Turn.KEY);
        if (!(turn instanceof Turn t)) {
            // Fail closed: no identity bound means no call, never "no restriction".
            throw new IllegalStateException("Tool invoked without a bound Turn — refusing");
        }
        return t;
    }

    private LocalDate parseDate(String date) {
        LocalDate today = LocalDate.now(properties.zone());
        String d = date == null ? "" : date.trim().toLowerCase(Locale.ROOT);
        return switch (d) {
            case "today" -> today;
            case "tomorrow" -> today.plusDays(1);
            default -> {
                try {
                    yield LocalDate.parse(d);
                } catch (DateTimeParseException e) {
                    throw new IllegalArgumentException("Date must be yyyy-MM-dd, today or tomorrow: " + date);
                }
            }
        };
    }

    /**
     * Times shown to the model are in the railway's own zone. JSON decoding normalises offsets to UTC, and
     * a model reading "03:55Z" reports it as a 03:55 departure — the train actually leaves at 07:55 local.
     */
    private String local(java.time.OffsetDateTime time) {
        return time.atZoneSameInstant(properties.zone()).toOffsetDateTime().toString();
    }

    private static UUID parseUuid(String value, String what) {
        try {
            return UUID.fromString(value.trim());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Not a valid " + what + ": " + value);
        }
    }

    /** Accepts a code (DXB) or a name/city (Dubai), resolved against catalog's real station list. */
    private String stationCode(Turn turn, String input) {
        String wanted = input == null ? "" : input.trim();
        return stations(turn).stream()
                .filter(s -> s.code().equalsIgnoreCase(wanted) || s.name().equalsIgnoreCase(wanted) || s.city().equalsIgnoreCase(wanted))
                .map(Station::code)
                .findFirst()
                .orElseThrow(() -> DomainException.invalid("UNKNOWN_STATION", "No station called " + input));
    }

    private List<Station> stations(Turn turn) {
        if (stations.isEmpty() || stationsLoadedAt.plus(STATIONS_TTL).isBefore(Instant.now())) {
            stations = catalog.stations(turn.caller().authorization())
                    .collectList()
                    .onErrorMap(RemoteCallErrors.from("catalog"))
                    .blockOptional(READ_TIMEOUT)
                    .orElse(List.of());
            stationsLoadedAt = Instant.now();
        }
        return stations;
    }
}
