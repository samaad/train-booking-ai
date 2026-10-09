package org.presight.ai.risk;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.presight.ai.common.kafka.TopicDeclarations;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.RiskEvents.RiskSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.core.KafkaAdmin;
import tools.jackson.databind.json.JsonMapper;

@SpringBootApplication
@EnableKafkaStreams
@EnableConfigurationProperties(RiskRules.class)
public class RiskSignalsApplication {

    private static final Logger log = LoggerFactory.getLogger(RiskSignalsApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(RiskSignalsApplication.class, args);
    }

    @Bean
    KStream<String, RiskSignal> riskSignals(StreamsBuilder builder, JsonMapper json, RiskRules rules, MeterRegistry meters) {
        return RiskSignalsTopology.build(builder, json, rules, signal -> {
            // userId is the JWT subject (an opaque id), not personal data.
            log.warn("Risk signal {} for user {}: {} holds, {} bookings since {}", signal.kind(), signal.userId(),
                    signal.holds(), signal.bookings(), signal.sessionStart());
            meters.counter("risk.signal", "kind", signal.kind().name()).increment();
        });
    }

    /** Output plus the source topics (idempotent, same spec as their producers): a missing source is fatal to Streams. */
    @Bean
    KafkaAdmin.NewTopics riskTopics(@Value("${kafka.replicas:1}") int replicas) {
        return TopicDeclarations.of(List.of(Topics.RISK_SIGNAL, Topics.SEAT_HELD, Topics.BOOKING_CREATED),
                "risk-signals-streams", List.of(), replicas);
    }
}
