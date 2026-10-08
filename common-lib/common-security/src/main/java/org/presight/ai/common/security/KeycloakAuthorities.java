package org.presight.ai.common.security;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Keycloak puts realm roles under {@code realm_access.roles}, not where Spring looks — skip this mapping and a
 * token silently yields zero authorities. Maps roles to {@code ROLE_*} and OAuth scopes to {@code SCOPE_*}.
 */
public final class KeycloakAuthorities implements Converter<Jwt, Collection<GrantedAuthority>> {

    @Override
    public Collection<GrantedAuthority> convert(Jwt jwt) {
        Set<GrantedAuthority> authorities = new LinkedHashSet<>();
        if (jwt.getClaim("realm_access") instanceof Map<?, ?> realmAccess
                && realmAccess.get("roles") instanceof Collection<?> roles) {
            for (Object role : roles) {
                String name = String.valueOf(role);
                authorities.add(new SimpleGrantedAuthority(name.startsWith("ROLE_") ? name : "ROLE_" + name));
            }
        }
        Object scope = jwt.getClaim("scope");
        List<String> scopes = scope instanceof String s ? List.of(s.split(" ")) : scope instanceof Collection<?> c
                ? c.stream().map(String::valueOf).toList() : List.of();
        for (String s : scopes) {
            if (!s.isBlank()) {
                authorities.add(new SimpleGrantedAuthority("SCOPE_" + s));
            }
        }
        return authorities;
    }
}
