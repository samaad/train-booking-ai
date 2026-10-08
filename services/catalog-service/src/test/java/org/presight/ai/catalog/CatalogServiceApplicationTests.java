package org.presight.ai.catalog;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.presight.ai.common.security.testing.TestIssuer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Import(TestcontainersConfiguration.class)
class CatalogServiceApplicationTests {

    @Autowired
    WebTestClient client;

    @Test
    void healthAndProbesAreUp() {
        client.get().uri("/actuator/health").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.status").isEqualTo("UP");
        client.get().uri("/actuator/health/readiness").exchange().expectStatus().isOk();
        client.get().uri("/actuator/health/liveness").exchange().expectStatus().isOk();
    }

    static final TestIssuer issuer = new TestIssuer();

    @DynamicPropertySource
    static void oidc(DynamicPropertyRegistry registry) {
        issuer.register(registry);
    }

    @AfterAll
    static void stopIssuer() {
        issuer.stop();
    }

    @Test
    void unknownRoutesNeedATokenAndThenAreProblemJson() {
        client.get().uri("/no-such-route").exchange().expectStatus().isUnauthorized();
        client.get().uri("/no-such-route").header("Authorization", "Bearer " + issuer.passenger("u1")).exchange()
                .expectStatus().isNotFound()
                .expectHeader().contentTypeCompatibleWith("application/problem+json");
    }

    @Test
    void theTimetableIsPublic() {
        client.get().uri("/stations").exchange().expectStatus().isOk();
        client.post().uri("/stations").exchange().expectStatus().isUnauthorized(); // only GETs are public
    }
}
