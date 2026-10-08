package org.presight.ai.common.kafka;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import org.presight.ai.common.kafka.consumer.IdempotentHandler;
import org.presight.ai.common.kafka.outbox.Outbox;
import org.presight.ai.common.kafka.outbox.OutboxRelay;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcAutoConfiguration;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcTransactionManagerAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import tools.jackson.databind.json.JsonMapper;

/**
 * Transactional outbox and idempotent consumption for services that own a Postgres database (R2DBC).
 * Their schema comes from Flyway location {@code classpath:db/outbox}.
 */
@AutoConfiguration(after = {R2dbcAutoConfiguration.class, R2dbcTransactionManagerAutoConfiguration.class,
        KafkaAutoConfiguration.class})
@ConditionalOnClass(DatabaseClient.class)
@ConditionalOnBean({DatabaseClient.class, ReactiveTransactionManager.class})
public class OutboxAutoConfiguration {

    @Bean
    Outbox outbox(DatabaseClient db, JsonMapper json) {
        return new Outbox(db, json);
    }

    @Bean
    @ConditionalOnProperty(name = "outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
    OutboxRelay outboxRelay(DatabaseClient db, ReactiveTransactionManager txManager, KafkaTemplate<String, String> kafka,
                            @Value("${outbox.relay.interval:200ms}") Duration interval,
                            @Value("${outbox.relay.batch-size:100}") int batchSize,
                            ObjectProvider<MeterRegistry> meters) {
        OutboxRelay relay = new OutboxRelay(db, TransactionalOperator.create(txManager), kafka, interval, batchSize);
        meters.ifAvailable(registry -> Gauge.builder("outbox.lag", relay, OutboxRelay::lagSeconds)
                .description("Age of the oldest unpublished event — page on this")
                .baseUnit("seconds")
                .register(registry));
        return relay;
    }

    @Bean
    IdempotentHandler idempotentHandler(DatabaseClient db, ReactiveTransactionManager txManager, JsonMapper json,
                                        @Value("${spring.kafka.consumer.group-id:${spring.application.name}}") String group) {
        return new IdempotentHandler(db, TransactionalOperator.create(txManager), json, group);
    }
}
