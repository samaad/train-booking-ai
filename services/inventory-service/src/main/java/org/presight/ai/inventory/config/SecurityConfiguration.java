package org.presight.ai.inventory.config;

import org.presight.ai.common.security.ResourceServerSecurity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

/** Availability is public; holds need a token (scopes/roles enforced per handler with @PreAuthorize). */
@Configuration(proxyBeanMethods = false)
@EnableWebFluxSecurity
class SecurityConfiguration {

    @Bean
    SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return ResourceServerSecurity.build(http, rules -> rules
                .pathMatchers(HttpMethod.GET, "/availability").permitAll());
    }
}
