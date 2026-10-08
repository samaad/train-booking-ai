package org.presight.ai.profile.profile;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import java.util.List;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.id.UuidV7;
import org.presight.ai.common.kafka.TopicDeclarations;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.OtherEvents.ProfileUpdated;
import org.presight.ai.common.kafka.outbox.Outbox;
import org.presight.ai.profile.profile.Profile.SeatPreference;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

/**
 * The caller's own profile. The user is identified solely by the validated JWT's subject — there is
 * no user id in any path or body, so one user cannot address another's profile.
 */
@RestController
@RequestMapping("/me")
public class ProfileController {

    private final ProfileRepository profiles;
    private final Outbox outbox;
    private final TransactionalOperator tx;

    public ProfileController(ProfileRepository profiles, Outbox outbox, TransactionalOperator tx) {
        this.profiles = profiles;
        this.outbox = outbox;
        this.tx = tx;
    }

    /** {@code version} must echo the value last read: a stale write is rejected, not silently lost. */
    public record ProfileUpdate(
            @NotNull Long version,
            @Size(max = 200) String displayName,
            @Pattern(regexp = "\\+?[0-9 ]{6,20}") String phone,
            @Pattern(regexp = "BUSINESS|ECONOMY") String preferredFareClass,
            @NotNull SeatPreference seatPreference,
            boolean emailNotifications,
            boolean smsNotifications) {
    }

    @GetMapping
    public Mono<Profile> me(@AuthenticationPrincipal Jwt jwt) {
        return current(jwt);
    }

    @PutMapping
    public Mono<Profile> update(@AuthenticationPrincipal Jwt jwt, @Valid @RequestBody ProfileUpdate update) {
        String userId = jwt.getSubject();
        return current(jwt)
                .then(profiles.update(userId, update.version(), update.displayName(), update.phone(),
                        update.preferredFareClass(), update.seatPreference(),
                        update.emailNotifications(), update.smsNotifications()))
                .flatMap(rows -> rows == 0
                        ? Mono.<Profile>error(DomainException.conflict("STALE_PROFILE",
                                "Profile changed since version " + update.version() + "; re-read and retry"))
                        : profiles.find(userId).flatMap(p -> outbox.append(Topics.PROFILE_UPDATED,
                                new ProfileUpdated(UuidV7.generate(), userId, p.version(), Instant.now())).thenReturn(p)))
                .as(tx::transactional);
    }

    @Configuration(proxyBeanMethods = false)
    static class TopicsConfig {
        @Bean
        KafkaAdmin.NewTopics profileTopics(@Value("${kafka.replicas:1}") int replicas) {
            return TopicDeclarations.of(List.of(Topics.PROFILE_UPDATED), "user-profile-service", List.of(), replicas);
        }
    }

    private Mono<Profile> current(Jwt jwt) {
        String name = jwt.getClaimAsString("name") != null ? jwt.getClaimAsString("name") : jwt.getClaimAsString("preferred_username");
        return profiles.findOrCreate(jwt.getSubject(), name, jwt.getClaimAsString("email"));
    }
}
