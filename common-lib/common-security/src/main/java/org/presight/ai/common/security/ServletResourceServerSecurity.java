package org.presight.ai.common.security;

import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.AuthorizeHttpRequestsConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;

/**
 * {@link ResourceServerSecurity} for the one servlet application (batch-admin-service): same token rules,
 * same Keycloak authority mapping, same open actuator endpoints — only the web stack differs.
 */
public final class ServletResourceServerSecurity {

    private ServletResourceServerSecurity() {
    }

    public static SecurityFilterChain build(HttpSecurity http,
                                            Customizer<AuthorizeHttpRequestsConfigurer<HttpSecurity>.AuthorizationManagerRequestMatcherRegistry> rules)
            throws Exception {
        JwtAuthenticationConverter jwtConverter = new JwtAuthenticationConverter();
        jwtConverter.setJwtGrantedAuthoritiesConverter(new KeycloakAuthorities());
        return http
                .authorizeHttpRequests(requests -> {
                    requests.requestMatchers("/actuator/health/**", "/actuator/info", "/actuator/prometheus").permitAll();
                    rules.customize(requests);
                    requests.anyRequest().authenticated();
                })
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt.jwtAuthenticationConverter(jwtConverter)))
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .csrf(AbstractHttpConfigurer::disable) // bearer tokens only, no cookies
                .httpBasic(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)
                .build();
    }
}
