package org.presight.ai.common.security;

import org.springframework.security.config.Customizer;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity.AuthorizeExchangeSpec;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.ReactiveJwtAuthenticationConverterAdapter;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.web.server.context.NoOpServerSecurityContextRepository;

/**
 * The one way every service builds its {@link SecurityWebFilterChain} (Spring Security 7, lambda DSL):
 * a stateless JWT resource server. Kong is not trusted for authentication — each service validates the
 * token itself (signature via auto-rotating JWKS, issuer, audience, expiry).
 *
 * <p>Probes and metrics are public; a service adds its own rules; anything not matched requires a valid token.
 */
public final class ResourceServerSecurity {

    private ResourceServerSecurity() {
    }

    public static SecurityWebFilterChain build(ServerHttpSecurity http, Customizer<AuthorizeExchangeSpec> rules) {
        JwtAuthenticationConverter jwtConverter = new JwtAuthenticationConverter();
        jwtConverter.setJwtGrantedAuthoritiesConverter(new KeycloakAuthorities());
        return http
                .authorizeExchange(exchange -> {
                    exchange.pathMatchers("/actuator/health/**", "/actuator/info", "/actuator/prometheus").permitAll();
                    rules.customize(exchange);
                    exchange.anyExchange().authenticated();
                })
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt ->
                        jwt.jwtAuthenticationConverter(new ReactiveJwtAuthenticationConverterAdapter(jwtConverter))))
                .securityContextRepository(NoOpServerSecurityContextRepository.getInstance()) // stateless
                .csrf(ServerHttpSecurity.CsrfSpec::disable) // bearer tokens only, no cookies
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .build();
    }
}
