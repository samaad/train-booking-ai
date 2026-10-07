package org.presight.ai.inventory.hold;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.presight.ai.common.web.IdempotencyKeyHeader;
import org.presight.ai.inventory.hold.Hold.HoldStatus;
import org.presight.ai.inventory.hold.HoldService.HoldCommand;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/holds")
public class HoldController {

    private final HoldService service;

    public HoldController(HoldService service) {
        this.service = service;
    }

    /** {@code seatNo} omitted means "any seat in this fare class". */
    public record HoldRequest(
            @NotBlank String trainNumber,
            @NotNull LocalDate travelDate,
            @NotBlank String fareClass,
            @Pattern(regexp = "[A-Z]{1,2}[0-9]{1,3}") String seatNo) {
    }

    public record HoldResponse(
            UUID holdId, String trainNumber, LocalDate travelDate, String fareClass,
            String seatNo, HoldStatus status, Instant expiresAt) {

        static HoldResponse of(Hold h) {
            return new HoldResponse(h.id(), h.trainNumber(), h.travelDate(), h.fareClass(),
                    h.seatNo(), h.status(), h.expiresAt());
        }
    }

    /** A replayed Idempotency-Key returns the original hold with 201 again, not a confusing 409. */
    @PostMapping
    public Mono<ResponseEntity<HoldResponse>> create(
            @RequestHeader(IdempotencyKeyHeader.NAME) String idempotencyKey,
            @Valid @RequestBody HoldRequest request) {
        return Mono.fromCallable(() -> IdempotencyKeyHeader.parse(idempotencyKey))
                .flatMap(key -> service.hold(new HoldCommand(
                        request.trainNumber(), request.travelDate(), request.fareClass(), request.seatNo()), key))
                .map(hold -> ResponseEntity.created(URI.create("/holds/" + hold.id())).body(HoldResponse.of(hold)));
    }

    @GetMapping("/{holdId}")
    public Mono<HoldResponse> get(@PathVariable UUID holdId) {
        return service.find(holdId).map(HoldResponse::of);
    }

    @DeleteMapping("/{holdId}")
    public Mono<ResponseEntity<Void>> release(@PathVariable UUID holdId) {
        return service.release(holdId).thenReturn(ResponseEntity.noContent().build());
    }

    @PostMapping("/{holdId}/commit")
    public Mono<HoldResponse> commit(@PathVariable UUID holdId) {
        return service.commit(holdId).map(HoldResponse::of);
    }
}
