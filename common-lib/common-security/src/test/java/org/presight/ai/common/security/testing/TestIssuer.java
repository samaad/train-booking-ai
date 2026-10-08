package org.presight.ai.common.security.testing;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.test.context.DynamicPropertyRegistry;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

/**
 * A stand-in Keycloak realm for tests: a real JWK set and client-credentials token endpoint over HTTP, and
 * RS256 tokens shaped like Keycloak's ({@code realm_access.roles}, space-separated {@code scope}, {@code aud}).
 * Services validate them with their real decoder, so signature, issuer, audience and expiry are all exercised.
 */
public final class TestIssuer {

    public static final String AUDIENCE = "train-api";
    public static final Set<String> PASSENGER_SCOPES = Set.of("booking:read", "booking:write", "payment:write",
            "profile:read", "profile:write", "ai:chat");

    private final RSAKey key = generate();
    private final DisposableServer server = HttpServer.create().port(0)
            .route(r -> r
                    .get("/certs", (req, res) -> res.header("Content-Type", "application/json")
                            .sendString(Mono.just(new JWKSet(key.toPublicJWK()).toString())).then())
                    .post("/token", (req, res) -> res.header("Content-Type", "application/json")
                            .sendString(Mono.just("""
                                    {"access_token":"%s","token_type":"Bearer","expires_in":300}"""
                                    .formatted(service("service-account-train-services")))).then()))
            .bindNow();

    public String issuer() {
        return "http://localhost:" + server.port() + "/realms/train-booking";
    }

    public String jwksUri() {
        return "http://localhost:" + server.port() + "/certs";
    }

    public String tokenUri() {
        return "http://localhost:" + server.port() + "/token";
    }

    /** Wires a service under test to this issuer (resource server, and the service-token client if it has one). */
    public void register(DynamicPropertyRegistry registry) {
        registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", this::issuer);
        registry.add("spring.security.oauth2.resourceserver.jwt.jwk-set-uri", this::jwksUri);
        registry.add("spring.security.oauth2.client.provider.keycloak.token-uri", this::tokenUri);
    }

    /** An ordinary signed-in traveller: PASSENGER with the web client's scopes. */
    public String passenger(String subject) {
        return token(subject, Set.of("PASSENGER"), PASSENGER_SCOPES);
    }

    /** A passenger with extra claims, e.g. {@code name}, {@code given_name}, {@code email}. */
    public String passenger(String subject, Map<String, Object> claims) {
        return sign(key, issuer(), subject, Set.of("PASSENGER"), PASSENGER_SCOPES, AUDIENCE, Instant.now().plusSeconds(300), claims);
    }

    public String withRoles(String subject, String... roles) {
        return token(subject, Set.of(roles), PASSENGER_SCOPES);
    }

    public String withScopes(String subject, String... scopes) {
        return token(subject, Set.of("PASSENGER"), Set.of(scopes));
    }

    public String service(String subject) {
        return token(subject, Set.of("SERVICE"), Set.of());
    }

    public String token(String subject, Collection<String> roles, Collection<String> scopes) {
        return sign(key, issuer(), subject, roles, scopes, AUDIENCE, Instant.now().plusSeconds(300));
    }

    public String expired(String subject) {
        return sign(key, issuer(), subject, Set.of("PASSENGER"), PASSENGER_SCOPES, AUDIENCE, Instant.now().minusSeconds(60));
    }

    public String wrongAudience(String subject) {
        return sign(key, issuer(), subject, Set.of("PASSENGER"), PASSENGER_SCOPES, "some-other-api", Instant.now().plusSeconds(300));
    }

    public String otherIssuer(String subject) {
        return sign(key, "http://evil.example/realms/train-booking", subject, Set.of("PASSENGER"), PASSENGER_SCOPES,
                AUDIENCE, Instant.now().plusSeconds(300));
    }

    /** Right issuer, audience and kid — signed with a key the JWK set does not contain. */
    public String forged(String subject) {
        return sign(generate(), issuer(), subject, Set.of("ADMIN"), PASSENGER_SCOPES, AUDIENCE, Instant.now().plusSeconds(300));
    }

    public void stop() {
        server.disposeNow();
    }

    private static String sign(RSAKey key, String issuer, String subject, Collection<String> roles, Collection<String> scopes,
                               String audience, Instant expiry) {
        return sign(key, issuer, subject, roles, scopes, audience, expiry, Map.of());
    }

    private static String sign(RSAKey key, String issuer, String subject, Collection<String> roles, Collection<String> scopes,
                               String audience, Instant expiry, Map<String, Object> extra) {
        try {
            JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder()
                    .issuer(issuer).subject(subject).audience(audience)
                    .jwtID(UUID.randomUUID().toString())
                    .issueTime(new Date()).expirationTime(Date.from(expiry))
                    .claim("realm_access", Map.of("roles", List.copyOf(roles)))
                    .claim("scope", String.join(" ", scopes))
                    .claim("preferred_username", subject)
                    .claim("given_name", subject)
                    .claim("email", subject + "@example.com");
            extra.forEach(builder::claim);
            JWTClaimsSet claims = builder.build();
            SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("test-key").build(), claims);
            jwt.sign(new RSASSASigner(key));
            return jwt.serialize();
        } catch (JOSEException e) {
            throw new IllegalStateException(e);
        }
    }

    private static RSAKey generate() {
        try {
            return new RSAKeyGenerator(2048).keyID("test-key").generate();
        } catch (JOSEException e) {
            throw new IllegalStateException(e);
        }
    }
}
