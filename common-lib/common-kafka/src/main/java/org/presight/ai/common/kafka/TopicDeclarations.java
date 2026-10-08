package org.presight.ai.common.kafka;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Topics are declared, never auto-created (the broker has auto-create off, so a typo fails loudly).
 * Each service declares the topics it produces plus the dead-letter topics of the topics it consumes.
 */
public final class TopicDeclarations {

    private TopicDeclarations() {
    }

    public static KafkaAdmin.NewTopics of(List<String> produced, String consumerGroup, List<String> consumed, int replicas) {
        List<NewTopic> topics = new ArrayList<>();
        for (String name : produced) {
            Topics.TopicSpec spec = Topics.spec(name);
            TopicBuilder builder = TopicBuilder.name(name).partitions(spec.partitions()).replicas(replicas);
            builder = spec.retention() == null
                    ? builder.compact()
                    : builder.config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(spec.retention().toMillis()));
            topics.add(builder.build());
        }
        for (String name : consumed) {
            topics.add(TopicBuilder.name(Topics.deadLetter(name, consumerGroup)).partitions(3).replicas(replicas)
                    .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(java.time.Duration.ofDays(30).toMillis()))
                    .build());
        }
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }
}
