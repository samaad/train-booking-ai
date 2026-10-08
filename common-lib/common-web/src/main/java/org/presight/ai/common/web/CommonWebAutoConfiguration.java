package org.presight.ai.common.web;

import io.micrometer.tracing.exporter.SpanExportingPredicate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.webflux.autoconfigure.error.ErrorWebFluxAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(SpanExportingPredicate.class)
    static class Tracing {

        /**
         * Drops root-level R2DBC query spans: background polling (outbox relay every 200 ms, hold sweeper) would
         * otherwise export a trace per query — dozens of junk traces a second burying the real ones. Queries
         * inside a request or a Kafka listener keep their spans. This must filter at export: an
         * {@code ObservationPredicate} cannot, because r2dbc-proxy sets the observation's name and parent only
         * after predicates have run.
         */
        @Bean
        SpanExportingPredicate noOrphanDatabaseQueries() {
            return span -> !("query".equals(span.getName()) && isRoot(span.getParentId()));
        }

        private static boolean isRoot(String parentId) {
            return parentId == null || parentId.isEmpty() || parentId.chars().allMatch(c -> c == '0');
        }
    }
}
