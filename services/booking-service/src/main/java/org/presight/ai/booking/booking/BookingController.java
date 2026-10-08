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
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
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
    public Mono<ResponseEntity<BookingResponse>> create(
            @RequestHeader(IdempotencyKeyHeader.NAME) String idempotencyKey,
            @Valid @RequestBody BookingRequest request) {
        return Mono.fromCallable(() -> IdempotencyKeyHeader.parse(idempotencyKey))
                .flatMap(key -> service.create(new CreateBooking(request.holdId(), request.fromStation(),
                        request.toStation(), request.passengerName(), request.contactEmail()), key))
                .map(b -> ResponseEntity.created(URI.create("/bookings/" + b.id())).body(BookingResponse.of(b)));
    }

    @GetMapping("/{bookingId}")
    public Mono<BookingResponse> get(@PathVariable UUID bookingId) {
        return service.find(bookingId).map(BookingResponse::of);
    }

    @PostMapping("/{bookingId}/cancel")
    public Mono<BookingResponse> cancel(@PathVariable UUID bookingId,
                                        @Valid @RequestBody(required = false) CancelRequest request) {
        return service.cancel(bookingId, request == null ? null : request.reason()).map(BookingResponse::of);
    }
}
