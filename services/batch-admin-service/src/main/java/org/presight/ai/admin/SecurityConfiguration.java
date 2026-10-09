package org.presight.ai.admin;

import org.presight.ai.common.security.ServletResourceServerSecurity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration(proxyBeanMethods = false)
class SecurityConfiguration {

    /** Replaying events re-drives sagas and payments: ADMIN only, never SUPPORT or AGENT. */
    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        return ServletResourceServerSecurity.build(http, requests -> requests.requestMatchers("/dlt/**").hasRole("ADMIN"));
    }
}
