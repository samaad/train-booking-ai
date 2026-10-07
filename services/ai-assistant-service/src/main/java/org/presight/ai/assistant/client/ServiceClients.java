package org.presight.ai.assistant.client;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.annotation.PostExchange;
import org.springframework.web.service.registry.HttpServiceGroup.ClientType;
import org.springframework.web.service.registry.ImportHttpServices;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The deterministic services the tools call. Every method takes the caller's {@code Authorization}
 * header explicitly — the JWT relay is visible in the signature, not hidden in a filter that might
 * read an empty context on another thread.
 */
@Configuration(proxyBeanMethods = false)
@ImportHttpServices(group = "catalog", types = ServiceClients.CatalogClient.class, clientType = ClientType.WEB_CLIENT)
@ImportHttpServices(group = "inventory", types = ServiceClients.InventoryClient.class, clientType = ClientType.WEB_CLIENT)
@ImportHttpServices(group = "booking", types = ServiceClients.BookingClient.class, clientType = ClientType.WEB_CLIENT)
public class ServiceClients {

    public record Station(String code, String name, String city) {
    }

    public record Journey(String trainNumber, String trainName, String from, String to, OffsetDateTime departure,
                          OffsetDateTime arrival, long durationMinutes, int distanceKm, List<String> fareClasses) {
    }

    public record ClassAvailability(String fareClass, int total, int available, List<String> availableSeats) {
    }

    public record Availability(String trainNumber, LocalDate travelDate, List<ClassAvailability> classes) {
    }

    public record HoldRequest(String trainNumber, LocalDate travelDate, String fareClass, String seatNo) {
    }

    public record Hold(UUID holdId, String trainNumber, LocalDate travelDate, String fareClass, String seatNo,
                       String status, Instant expiresAt) {
    }

    public record Booking(UUID bookingId, String pnr, String status, String trainNumber, LocalDate travelDate,
                          String fareClass, String seatNo, String fromStation, String toStation,
                          OffsetDateTime departure, BigDecimal amount, String currency) {
    }

    @HttpExchange
    public interface CatalogClient {
        @GetExchange("/stations")
        Flux<Station> stations(@RequestHeader("Authorization") String authorization);

        @GetExchange("/routes/search")
        Flux<Journey> search(@RequestHeader("Authorization") String authorization,
                             @RequestParam String from, @RequestParam String to,
                             @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date);
    }

    @HttpExchange
    public interface InventoryClient {
        @GetExchange("/availability")
        Mono<Availability> availability(@RequestHeader("Authorization") String authorization,
                                        @RequestParam String trainNumber,
                                        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate travelDate);

        /** Mutating: always called with a server-derived Idempotency-Key, never retried blindly. */
        @PostExchange("/holds")
        Mono<Hold> hold(@RequestHeader("Authorization") String authorization,
                        @RequestHeader("Idempotency-Key") String idempotencyKey,
                        @RequestBody HoldRequest request);

        @GetExchange("/holds/{holdId}")
        Mono<Hold> getHold(@RequestHeader("Authorization") String authorization, @PathVariable UUID holdId);
    }

    @HttpExchange
    public interface BookingClient {
        @GetExchange("/bookings/{bookingId}")
        Mono<Booking> get(@RequestHeader("Authorization") String authorization, @PathVariable UUID bookingId);
    }
}
