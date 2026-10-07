package org.presight.ai.profile.traveller;

import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Readable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Past;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.time.LocalDate;
import java.util.UUID;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.id.UuidV7;
import org.springframework.http.ResponseEntity;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Saved co-travellers. Every statement is scoped by the JWT subject, so another user's traveller id
 * simply does not exist from the caller's point of view (404, not 403 — no existence oracle).
 */
@RestController
@RequestMapping("/me/travellers")
public class TravellerController {

    private final DatabaseClient db;

    public TravellerController(DatabaseClient db) {
        this.db = db;
    }

    public record TravellerRequest(
            @NotBlank @Size(max = 200) String fullName,
            @Past LocalDate dateOfBirth,
            @Pattern(regexp = "PASSPORT|EMIRATES_ID|NATIONAL_ID") String documentType,
            @Pattern(regexp = "[A-Za-z0-9-]{4,30}") String documentNumber) {
    }

    /** Document numbers are returned masked; the full value never leaves the service after capture. */
    public record Traveller(UUID id, String fullName, LocalDate dateOfBirth, String documentType, String documentNumberMasked) {
    }

    @GetMapping
    public Flux<Traveller> list(@AuthenticationPrincipal Jwt jwt) {
        return db.sql("SELECT * FROM traveller WHERE user_id = :userId ORDER BY created_at")
                .bind("userId", jwt.getSubject())
                .map(TravellerController::traveller)
                .all();
    }

    @PostMapping
    public Mono<ResponseEntity<Traveller>> add(@AuthenticationPrincipal Jwt jwt, @Valid @RequestBody TravellerRequest req) {
        UUID id = UuidV7.generate();
        return requireProfile(jwt.getSubject())
                .then(db.sql("""
                                INSERT INTO traveller (id, user_id, full_name, date_of_birth, document_type, document_number)
                                VALUES (:id, :userId, :name, :dob, :docType, :docNumber)
                                """)
                        .bind("id", id)
                        .bind("userId", jwt.getSubject())
                        .bind("name", req.fullName())
                        .bind("dob", Parameters.in(R2dbcType.DATE, req.dateOfBirth()))
                        .bind("docType", Parameters.in(R2dbcType.VARCHAR, req.documentType()))
                        .bind("docNumber", Parameters.in(R2dbcType.VARCHAR, req.documentNumber()))
                        .then())
                .then(db.sql("SELECT * FROM traveller WHERE id = :id").bind("id", id).map(TravellerController::traveller).one())
                .map(t -> ResponseEntity.created(URI.create("/me/travellers/" + t.id())).body(t));
    }

    @DeleteMapping("/{travellerId}")
    public Mono<ResponseEntity<Void>> remove(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID travellerId) {
        return db.sql("DELETE FROM traveller WHERE id = :id AND user_id = :userId")
                .bind("id", travellerId)
                .bind("userId", jwt.getSubject())
                .fetch()
                .rowsUpdated()
                .flatMap(rows -> rows == 0
                        ? Mono.error(DomainException.notFound("TRAVELLER_NOT_FOUND", "No traveller " + travellerId))
                        : Mono.just(ResponseEntity.noContent().<Void>build()));
    }

    private Mono<Void> requireProfile(String userId) {
        return db.sql("INSERT INTO profile (user_id) VALUES (:userId) ON CONFLICT (user_id) DO NOTHING")
                .bind("userId", userId)
                .then();
    }

    private static Traveller traveller(Readable row) {
        String doc = row.get("document_number", String.class);
        String masked = doc == null ? null : "•".repeat(Math.max(0, doc.length() - 4)) + doc.substring(Math.max(0, doc.length() - 4));
        return new Traveller(
                row.get("id", UUID.class),
                row.get("full_name", String.class),
                row.get("date_of_birth", LocalDate.class),
                row.get("document_type", String.class),
                masked);
    }
}
