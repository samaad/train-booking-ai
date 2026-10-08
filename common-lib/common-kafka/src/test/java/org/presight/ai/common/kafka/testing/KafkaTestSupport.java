package org.presight.ai.common.kafka.testing;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.presight.ai.common.kafka.EventHeaders;
import org.presight.ai.common.kafka.events.Event;
import tools.jackson.databind.json.JsonMapper;

/**
 * Test helper for services: publish an event exactly as the outbox relay does (JSON value, eventId header),
 * and watch topics for what a service emits.
 */
public final class KafkaTestSupport implements AutoCloseable {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> seen = new ArrayList<>();

    public KafkaTestSupport(String bootstrapServers, Collection<String> watchedTopics) {
        this.producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        this.consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-probe-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        if (!watchedTopics.isEmpty()) {
            consumer.subscribe(watchedTopics);
        }
    }

    /** Publishes {@code event}; publishing the same instance twice simulates at-least-once redelivery. */
    public void publish(String topic, Event event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(event.key()), JSON.writeValueAsString(event));
        record.headers().add(EventHeaders.EVENT_ID, event.eventId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(EventHeaders.EVENT_TYPE, topic.getBytes(StandardCharsets.UTF_8));
        try {
            producer.send(record).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Waits until an event on {@code topic} matches, then returns it parsed as {@code type}. */
    public <T> T await(String topic, Class<T> type, Predicate<T> match, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            for (T e : all(topic, type)) {
                if (match.test(e)) {
                    return e;
                }
            }
            consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
        }
        throw new AssertionError("No matching event on " + topic + " within " + timeout + "; saw " + all(topic, type));
    }

    /** Everything seen so far on {@code topic} (after polling briefly for stragglers). */
    public <T> List<T> all(String topic, Class<T> type) {
        consumer.poll(Duration.ofMillis(100)).forEach(seen::add);
        return seen.stream().filter(r -> r.topic().equals(topic)).map(r -> JSON.readValue(r.value(), type)).toList();
    }

    /** Polls for {@code quietPeriod} and returns all events on {@code topic} — for asserting "exactly one". */
    public <T> List<T> settle(String topic, Class<T> type, Duration quietPeriod) {
        long deadline = System.nanoTime() + quietPeriod.toNanos();
        while (System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
        }
        return all(topic, type);
    }

    @Override
    public void close() {
        producer.close();
        consumer.close();
    }
}
