package org.presight.ai.booking.booking;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.presight.ai.booking.booking.Booking.SagaState;
import org.presight.ai.booking.booking.Booking.Status;
import org.presight.ai.booking.booking.BookingService.CreateBooking;
import org.presight.ai.common.web.IdempotencyKeyHeader;
import org.presight.ai.common.security.CallerIdentity;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/bookings")
public class BookingController {

    private final BookingService service;

    public BookingController(BookingService service) {
        this.service = service;
    }

    /** No seat, train or price fields: those come from the server-issued hold and catalog fare rules. */
    public record BookingRequest(
            @NotNull UUID holdId,
            @NotBlank String fromStation,
            @NotBlank String toStation,
            @NotBlank @Size(max = 200) String passengerName,
            @NotBlank @Email String contactEmail) {
    }

    public record CancelRequest(@Size(max = 500) String reason) {
    }

    public record BookingResponse(
            UUID bookingId, String pnr, Status status, SagaState sagaState, UUID holdId,
            String trainNumber, LocalDate travelDate, String fareClass, String seatNo,
            String fromStation, String toStation, OffsetDateTime departure, OffsetDateTime arrival,
            String passengerName, BigDecimal amount, String currency, String cancelReason, Instant createdAt) {

        static BookingResponse of(Booking b) {
            return new BookingResponse(b.id(), b.pnr(), b.status(), b.sagaState(), b.holdId(),
                    b.trainNumber(), b.travelDate(), b.fareClass(), b.seatNo(),
                    b.fromStation(), b.toStation(), b.departure(), b.arrival(),
                    b.passengerName(), b.amount(), b.currency(), b.cancelReason(), b.createdAt());
        }
    }

    /** A replayed Idempotency-Key returns the original booking with 201 again. */
    @PostMapping
    @PreAuthorize("hasAuthority('SCOPE_booking:write')")
    public Mono<ResponseEntity<BookingResponse>> create(
            @AuthenticationPrincipal Jwt jwt,
            @RequestHeader(IdempotencyKeyHeader.NAME) String idempotencyKey,
            @Valid @RequestBody BookingRequest request) {
        CallerIdentity caller = CallerIdentity.of(jwt);
        return Mono.fromCallable(() -> IdempotencyKeyHeader.parse(idempotencyKey))
                .flatMap(key -> service.create(new CreateBooking(request.holdId(), request.fromStation(),
                        request.toStation(), request.passengerName(), request.contactEmail()), key, caller))
                .map(b -> ResponseEntity.created(URI.create("/bookings/" + b.id())).body(BookingResponse.of(b)));
    }

    @GetMapping
    @PreAuthorize("hasAuthority('SCOPE_booking:read')")
    public Flux<BookingResponse> mine(@AuthenticationPrincipal Jwt jwt) {
        return service.mine(CallerIdentity.of(jwt), 50).map(BookingResponse::of);
    }

    @GetMapping("/{bookingId}")
    @PreAuthorize("hasAuthority('SCOPE_booking:read')")
    public Mono<BookingResponse> get(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID bookingId) {
        return service.find(bookingId, CallerIdentity.of(jwt)).map(BookingResponse::of);
    }

    @PostMapping("/{bookingId}/cancel")
    @PreAuthorize("hasAuthority('SCOPE_booking:write')")
    public Mono<BookingResponse> cancel(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID bookingId,
                                        @Valid @RequestBody(required = false) CancelRequest request) {
        return service.cancel(bookingId, request == null ? null : request.reason(), CallerIdentity.of(jwt))
                .map(BookingResponse::of);
    }
}
