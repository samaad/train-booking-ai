package org.presight.ai.common.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.error.ErrorCategory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

/**
 * Kafka conventions for every service: JSON payloads as strings with event metadata in headers, an
 * idempotent {@code acks=all} producer, and a consumer error handler that retries transient failures with
 * exponential backoff (0.5 s → 8 s, five attempts, ~15 s — long enough for a dependency to recover or for an
 * event on another topic to catch up), then dead-letters to {@code <topic>.<group>.DLT}.
 */
@AutoConfiguration(before = KafkaAutoConfiguration.class)
public class KafkaEventsAutoConfiguration {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(KafkaEventsAutoConfiguration.class);

    @Bean
    DefaultKafkaProducerFactoryCustomizer eventProducerDefaults() {
        return factory -> factory.updateConfigs(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true));
    }

    @Bean
    DefaultKafkaConsumerFactoryCustomizer eventConsumerDefaults() {
        return factory -> factory.updateConfigs(Map.of(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false));
    }

    /**
     * Only {@link ErrorCategory#UNAVAILABLE} failures (and unexpected exceptions) are retried; a business
     * rejection or an unreadable payload will fail the same way every time, so it goes straight to the DLT.
     */
    @Bean
    CommonErrorHandler eventErrorHandler(KafkaTemplate<Object, Object> template,
                                         @Value("${spring.kafka.consumer.group-id:}") String group,
                                         org.springframework.beans.factory.ObjectProvider<MeterRegistry> meters) {
        DeadLetterPublishingRecoverer publisher = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(Topics.deadLetter(record.topic(), group), -1));
        // A dead-lettered event is an incident: say so, with the cause, before parking it.
        org.springframework.kafka.listener.ConsumerRecordRecoverer recoverer = (record, ex) -> {
            log.error("Dead-lettering {}-{}@{} to {}: {}", record.topic(), record.partition(), record.offset(),
                    Topics.deadLetter(record.topic(), group), rootCause(ex).toString());
            meters.ifAvailable(m -> m.counter("kafka.dead_letter", "topic", record.topic(), "group", group).increment());
            publisher.accept(record, ex);
        };
        org.springframework.kafka.support.ExponentialBackOffWithMaxRetries backOff =
                new org.springframework.kafka.support.ExponentialBackOffWithMaxRetries(5);
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(8_000);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.setBackOffFunction((record, ex) -> isPermanent(ex) ? new FixedBackOff(0, 0) : null);
        return handler;
    }

    private static Throwable rootCause(Throwable ex) {
        Throwable t = ex;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t;
    }

    static boolean isPermanent(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof DomainException d) {
                return d.category() != ErrorCategory.UNAVAILABLE;
            }
            if (t instanceof JacksonException || t instanceof IllegalArgumentException) {
                return true;
            }
        }
        return false;
    }
}
