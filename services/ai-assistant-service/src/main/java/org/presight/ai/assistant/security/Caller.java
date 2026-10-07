package org.presight.ai.assistant.security;

import java.util.Objects;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Who is chatting — captured <b>eagerly</b> from the validated JWT in the controller, then passed
 * explicitly (and into Spring AI's ToolContext). Nothing downstream reads the SecurityContext:
 * Reactor Context does not cross the scheduler boundary, and an empty context read lazily inside a
 * tool must never be mistaken for "no restriction".
 *
 * @param bearerToken relayed on every downstream call so each service re-authorises the user itself
 */
public record Caller(String userId, String displayName, String bearerToken) {

    public Caller {
        Objects.requireNonNull(userId, "userId");
        Objects.requireNonNull(bearerToken, "bearerToken");
    }

    public static Caller of(Jwt jwt) {
        String name = jwt.getClaimAsString("given_name") != null ? jwt.getClaimAsString("given_name")
                : jwt.getClaimAsString("name") != null ? jwt.getClaimAsString("name")
                : jwt.getClaimAsString("preferred_username");
        return new Caller(jwt.getSubject(), name, jwt.getTokenValue());
    }

    public String authorization() {
        return "Bearer " + bearerToken;
    }

    @Override
    public String toString() {
        return "Caller[userId=" + userId + "]"; // never print the token
    }
}
