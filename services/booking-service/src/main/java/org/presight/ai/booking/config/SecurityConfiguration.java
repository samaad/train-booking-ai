package org.presight.ai.booking.config;

import org.presight.ai.common.security.ResourceServerSecurity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

/** Every booking endpoint needs a token; scopes are enforced per handler, ownership in BookingService. */
@Configuration(proxyBeanMethods = false)
@EnableWebFluxSecurity
class SecurityConfiguration {

    @Bean
    SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return ResourceServerSecurity.build(http, rules -> {
        });
    }
}
