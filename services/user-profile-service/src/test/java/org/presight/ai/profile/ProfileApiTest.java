package org.presight.ai.profile;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.postgresql.PostgreSQLContainer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Import(ProfileApiTest.Containers.class)
class ProfileApiTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean
        @ServiceConnection
        PostgreSQLContainer postgres() {
            return new PostgreSQLContainer("postgres:18.6-alpine");
        }
    }

    static final TestIssuer issuer = new TestIssuer();

    @DynamicPropertySource
    static void oidc(DynamicPropertyRegistry registry) {
        registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", issuer::issuer);
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri", issuer::jwksUri);
    }

    @AfterAll
    static void stopIssuer() {
        issuer.stop();
    }

    @Autowired
    WebTestClient client;

    @Test
    void everythingButHealthRequiresAToken() {
        client.get().uri("/me").exchange().expectStatus().isUnauthorized();
        client.get().uri("/me/travellers").exchange().expectStatus().isUnauthorized();
        client.get().uri("/actuator/health").exchange().expectStatus().isOk();
    }

    @Test
    void rejectsExpiredForeignAndForgedTokens() {
        bearer(issuer.expiredToken("mallory")).get().uri("/me").exchange().expectStatus().isUnauthorized();
        bearer(issuer.tokenFromOtherIssuer("mallory")).get().uri("/me").exchange().expectStatus().isUnauthorized();
        bearer(issuer.forgedToken("mallory")).get().uri("/me").exchange().expectStatus().isUnauthorized();
    }

    @Test
    void firstReadCreatesTheProfileFromTokenClaims() {
        WebTestClient alice = as("alice-" + UUID.randomUUID(), "Alice Example", "alice@example.com");

        String loyalty = (String) alice.get().uri("/me").exchange()
                .expectStatus().isOk()
                .expectBody(Map.class).returnResult().getResponseBody().get("loyaltyNumber");
        assertThat(loyalty).matches("TB\\d{8}");

        alice.get().uri("/me").exchange()
                .expectBody()
                .jsonPath("$.loyaltyNumber").isEqualTo(loyalty) // stable across reads
                .jsonPath("$.displayName").isEqualTo("Alice Example")
                .jsonPath("$.email").isEqualTo("alice@example.com")
                .jsonPath("$.seatPreference").isEqualTo("NO_PREFERENCE");
    }

    @Test
    void updatesAreOptimisticallyVersioned() {
        WebTestClient bob = as("bob-" + UUID.randomUUID(), "Bob", "bob@example.com");
        int version = ((Number) bob.get().uri("/me").exchange()
                .expectBody(Map.class).returnResult().getResponseBody().get("version")).intValue();

        bob.put().uri("/me").bodyValue(update(version, "WINDOW")).exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.seatPreference").isEqualTo("WINDOW")
                .jsonPath("$.version").isEqualTo(version + 1)
                .jsonPath("$.email").isEqualTo("bob@example.com"); // identity-owned, untouched

        bob.put().uri("/me").bodyValue(update(version, "AISLE")).exchange()
                .expectStatus().isEqualTo(409)
                .expectBody().jsonPath("$.code").isEqualTo("STALE_PROFILE");
    }

    @Test
    void travellersAreScopedToTheirOwner() {
        WebTestClient carol = as("carol-" + UUID.randomUUID(), "Carol", "carol@example.com");
        WebTestClient dave = as("dave-" + UUID.randomUUID(), "Dave", "dave@example.com");

        String id = (String) carol.post().uri("/me/travellers")
                .bodyValue(Map.of("fullName", "Omar Carol", "dateOfBirth", "2015-03-01",
                        "documentType", "PASSPORT", "documentNumber", "N1234567"))
                .exchange()
                .expectStatus().isCreated()
                .expectBody(Map.class).returnResult().getResponseBody().get("id");

        carol.get().uri("/me/travellers").exchange()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(1)
                .jsonPath("$[0].documentNumberMasked").isEqualTo("••••4567");

        dave.get().uri("/me/travellers").exchange().expectBody().jsonPath("$.length()").isEqualTo(0);
        dave.delete().uri("/me/travellers/{id}", id).exchange()
                .expectStatus().isNotFound(); // not 403: no existence oracle for other users' data

        carol.delete().uri("/me/travellers/{id}", id).exchange().expectStatus().isNoContent();
        carol.get().uri("/me/travellers").exchange().expectBody().jsonPath("$.length()").isEqualTo(0);
    }

    @Test
    void validatesInput() {
        WebTestClient erin = as("erin-" + UUID.randomUUID(), "Erin", "erin@example.com");
        erin.post().uri("/me/travellers").bodyValue(Map.of("fullName", " ")).exchange().expectStatus().isBadRequest();
        erin.put().uri("/me").bodyValue(Map.of("seatPreference", "WINDOW")).exchange().expectStatus().isBadRequest();
    }

    private WebTestClient as(String subject, String name, String email) {
        return bearer(issuer.token(subject, name, email));
    }

    private WebTestClient bearer(String token) {
        return client.mutate().defaultHeader("Authorization", "Bearer " + token).build();
    }

    private static Map<String, Object> update(int version, String seat) {
        Map<String, Object> body = new HashMap<>();
        body.put("version", version);
        body.put("displayName", "Bob B.");
        body.put("seatPreference", seat);
        body.put("emailNotifications", true);
        body.put("smsNotifications", false);
        return body;
    }
}
