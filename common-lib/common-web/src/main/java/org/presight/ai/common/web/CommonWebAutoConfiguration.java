package org.presight.ai.common.web;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.webflux.autoconfigure.error.ErrorWebFluxAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.result.method.annotation.ResponseEntityExceptionHandler;

/**
 * Shared reactive web defaults, picked up by every service that depends on {@code common-web}.
 *
 * <p>Inter-service {@code @HttpExchange} clients need no code here: Boot 4.1 builds them from
 * {@code @ImportHttpServices(group = ...)} plus {@code spring.http.serviceclient.<group>.*}
 * properties, with timeouts under {@code spring.http.clients.*}.
 */
@AutoConfiguration(before = ErrorWebFluxAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
public class CommonWebAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ResponseEntityExceptionHandler.class)
    ProblemDetailsExceptionHandler problemDetailsExceptionHandler() {
        return new ProblemDetailsExceptionHandler();
    }
}
