package org.presight.ai.profile.security;

import org.presight.ai.common.security.ResourceServerSecurity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

/** JWT resource server via the shared baseline; scopes are enforced per handler with @PreAuthorize. */
@Configuration(proxyBeanMethods = false)
@EnableWebFluxSecurity
class SecurityConfiguration {

    @Bean
    SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return ResourceServerSecurity.build(http, rules -> {
        });
    }
}
