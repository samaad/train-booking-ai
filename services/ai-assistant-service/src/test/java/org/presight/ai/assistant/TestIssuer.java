package org.presight.ai.assistant;

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
import java.util.Date;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

/**
 * A minimal OIDC issuer: serves a JWK set over real HTTP and signs RS256 tokens, so tests exercise
 * the service's actual JWT validation (signature, issuer, expiry) instead of bypassing it.
 */
final class TestIssuer {

    private final RSAKey key = generate("test-key");
    private final DisposableServer server = HttpServer.create().port(0)
            .route(r -> r.get("/certs", (req, res) -> res.header("Content-Type", "application/json")
                    .sendString(Mono.just(new JWKSet(key.toPublicJWK()).toString())).then()))
            .bindNow();

    String issuer() {
        return "http://localhost:" + server.port();
    }

    String jwksUri() {
        return issuer() + "/certs";
    }

    String token(String subject, String name, String email) {
        return sign(key, issuer(), subject, name, email, Instant.now().plusSeconds(300));
    }

    String expiredToken(String subject) {
        return sign(key, issuer(), subject, null, null, Instant.now().minusSeconds(300));
    }

    String tokenFromOtherIssuer(String subject) {
        return sign(key, "http://evil.example", subject, null, null, Instant.now().plusSeconds(300));
    }

    /** Correct issuer and kid, signed with a key the JWK set does not contain. */
    String forgedToken(String subject) {
        return sign(generate("test-key"), issuer(), subject, null, null, Instant.now().plusSeconds(300));
    }

    void stop() {
        server.disposeNow();
    }

    private static String sign(RSAKey key, String issuer, String subject, String name, String email, Instant expiry) {
        try {
            JWTClaimsSet.Builder claims = new JWTClaimsSet.Builder()
                    .issuer(issuer).subject(subject)
                    .issueTime(new Date()).expirationTime(Date.from(expiry));
            if (name != null) {
                claims.claim("name", name);
            }
            if (email != null) {
                claims.claim("email", email);
            }
            SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(key.getKeyID()).build(), claims.build());
            jwt.sign(new RSASSASigner(key));
            return jwt.serialize();
        } catch (JOSEException e) {
            throw new IllegalStateException(e);
        }
    }

    private static RSAKey generate(String kid) {
        try {
            return new RSAKeyGenerator(2048).keyID(kid).generate();
        } catch (JOSEException e) {
            throw new IllegalStateException(e);
        }
    }
}
