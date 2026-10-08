package org.presight.ai.common.security;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Who is calling, read once from the validated JWT at the edge of a request and passed down explicitly.
 * Never re-read lazily from the SecurityContext on another thread: Reactor Context does not cross a
 * scheduler hop, and an empty context must never be mistaken for "no restriction".
 */
public record CallerIdentity(String userId, Set<String> roles, String bearerToken) {

    /** Roles that may act on any user's records (support desks, agents, operators). */
    public static final Set<String> STAFF = Set.of("AGENT", "SUPPORT", "ADMIN");

    public CallerIdentity {
        Objects.requireNonNull(userId, "userId");
        roles = Set.copyOf(roles);
    }

    public static CallerIdentity of(Jwt jwt) {
        Set<String> roles = Set.of();
        if (jwt.getClaim("realm_access") instanceof Map<?, ?> access && access.get("roles") instanceof Collection<?> r) {
            roles = r.stream().map(String::valueOf).collect(Collectors.toUnmodifiableSet());
        }
        return new CallerIdentity(jwt.getSubject(), roles, jwt.getTokenValue());
    }

    public boolean hasRole(String role) {
        return roles.contains(role);
    }

    public boolean isStaff() {
        return roles.stream().anyMatch(STAFF::contains);
    }

    /** Owner or staff. Callers turn a {@code false} into 404, not 403, so ids are not an existence oracle. */
    public boolean mayAccess(String ownerId) {
        return userId.equals(ownerId) || isStaff();
    }

    public String authorization() {
        return "Bearer " + bearerToken;
    }

    @Override
    public String toString() {
        return "CallerIdentity[userId=" + userId + ", roles=" + roles + "]"; // never the token
    }
}
