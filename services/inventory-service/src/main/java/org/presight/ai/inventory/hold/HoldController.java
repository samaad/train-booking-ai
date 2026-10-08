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
import org.presight.ai.common.security.CallerIdentity;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
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
    @PreAuthorize("hasAuthority('SCOPE_booking:write')")
    public Mono<ResponseEntity<HoldResponse>> create(
            @AuthenticationPrincipal Jwt jwt,
            @RequestHeader(IdempotencyKeyHeader.NAME) String idempotencyKey,
            @Valid @RequestBody HoldRequest request) {
        return Mono.fromCallable(() -> IdempotencyKeyHeader.parse(idempotencyKey))
                .flatMap(key -> service.hold(new HoldCommand(jwt.getSubject(),
                        request.trainNumber(), request.travelDate(), request.fareClass(), request.seatNo()), key))
                .map(hold -> ResponseEntity.created(URI.create("/holds/" + hold.id())).body(HoldResponse.of(hold)));
    }

    @GetMapping("/{holdId}")
    @PreAuthorize("hasAuthority('SCOPE_booking:read') or hasRole('SERVICE')")
    public Mono<HoldResponse> get(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID holdId) {
        return service.find(holdId, CallerIdentity.of(jwt)).map(HoldResponse::of);
    }

    @DeleteMapping("/{holdId}")
    @PreAuthorize("hasAuthority('SCOPE_booking:write')")
    public Mono<ResponseEntity<Void>> release(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID holdId) {
        return service.release(holdId, CallerIdentity.of(jwt)).thenReturn(ResponseEntity.noContent().build());
    }

    /**
     * Seats are committed only by the booking saga after payment is authorised — never by a user, or a
     * client could keep a seat without paying.
     */
    @PostMapping("/{holdId}/commit")
    @PreAuthorize("hasRole('SERVICE')")
    public Mono<HoldResponse> commit(@PathVariable UUID holdId) {
        return service.commit(holdId).map(HoldResponse::of);
    }
}
