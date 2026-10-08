package org.presight.ai.common.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableReactiveMethodSecurity;
import org.springframework.security.oauth2.client.AuthorizedClientServiceReactiveOAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.InMemoryReactiveOAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.ReactiveOAuth2AuthorizedClientProviderBuilder;
import org.springframework.security.oauth2.client.registration.ReactiveClientRegistrationRepository;

/** {@code @PreAuthorize} on reactive handlers, and service tokens when a client registration is configured. */
@AutoConfiguration(afterName = "org.springframework.boot.security.oauth2.client.autoconfigure.reactive.ReactiveOAuth2ClientAutoConfiguration")
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
@EnableReactiveMethodSecurity
public class CommonSecurityAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(ReactiveClientRegistrationRepository.class)
    @ConditionalOnBean(ReactiveClientRegistrationRepository.class)
    static class ServiceTokenConfiguration {

        @Bean
        ServiceTokens serviceTokens(ReactiveClientRegistrationRepository registrations,
                                    @Value("${spring.application.name}") String application) {
            var cache = new InMemoryReactiveOAuth2AuthorizedClientService(registrations);
            var manager = new AuthorizedClientServiceReactiveOAuth2AuthorizedClientManager(registrations, cache);
            manager.setAuthorizedClientProvider(ReactiveOAuth2AuthorizedClientProviderBuilder.builder().clientCredentials().build());
            return new ServiceTokens(manager, cache, application);
        }
    }
}
