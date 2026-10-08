package org.presight.ai.ticketing.ticket;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import org.presight.ai.ticketing.ticket.Ticket.Status;
import org.presight.ai.common.security.CallerIdentity;
import org.springframework.http.CacheControl;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/tickets/{pnr:[A-HJ-NP-Z2-9]{6}}")
public class TicketController {

    private final TicketService service;

    public TicketController(TicketService service) {
        this.service = service;
    }

    public record TicketResponse(String pnr, Status status, String passengerName, String trainNumber,
                                 LocalDate travelDate, String fareClass, String seatNo, String fromStation,
                                 String toStation, OffsetDateTime departure, String qrToken,
                                 Instant issuedAt, Instant voidedAt) {

        static TicketResponse of(Ticket t) {
            return new TicketResponse(t.pnr(), t.status(), t.passengerName(), t.trainNumber(), t.travelDate(),
                    t.fareClass(), t.seatNo(), t.fromStation(), t.toStation(), t.departure(),
                    t.status() == Status.ISSUED ? t.qrToken() : null, t.issuedAt(), t.voidedAt());
        }
    }

    @GetMapping
    @PreAuthorize("hasAuthority('SCOPE_booking:read')")
    public Mono<TicketResponse> ticket(@AuthenticationPrincipal Jwt jwt, @PathVariable String pnr) {
        return service.find(pnr, CallerIdentity.of(jwt)).map(TicketResponse::of);
    }

    @GetMapping(value = "/qr", produces = MediaType.IMAGE_PNG_VALUE)
    @PreAuthorize("hasAuthority('SCOPE_booking:read')")
    public Mono<ResponseEntity<byte[]>> qr(@AuthenticationPrincipal Jwt jwt, @PathVariable String pnr) {
        return service.qr(pnr, CallerIdentity.of(jwt)).map(png -> ResponseEntity.ok()
                .cacheControl(CacheControl.noStore()) // a voided ticket's QR must not linger in caches
                .contentType(MediaType.IMAGE_PNG)
                .body(png));
    }

    @GetMapping(value = "/pdf", produces = MediaType.APPLICATION_PDF_VALUE)
    @PreAuthorize("hasAuthority('SCOPE_booking:read')")
    public Mono<ResponseEntity<byte[]>> pdf(@AuthenticationPrincipal Jwt jwt, @PathVariable String pnr) {
        return service.pdf(pnr, CallerIdentity.of(jwt)).map(pdf -> ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        ContentDisposition.inline().filename("ticket-" + pnr + ".pdf").build().toString())
                .contentType(MediaType.APPLICATION_PDF)
                .body(pdf));
    }
}
