package org.presight.ai.catalog.config;

import org.presight.ai.common.security.ResourceServerSecurity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

/** The timetable is public: travellers search before they sign in. Anything else needs a token. */
@Configuration(proxyBeanMethods = false)
@EnableWebFluxSecurity
class SecurityConfiguration {

    @Bean
    SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return ResourceServerSecurity.build(http, rules -> rules
                .pathMatchers(HttpMethod.GET, "/stations", "/routes/search", "/fares/rules").permitAll());
    }
}
