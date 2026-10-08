package org.presight.ai.availability;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.presight.ai.availability.SeatMap.Status;
import org.presight.ai.common.kafka.TopicDeclarations;
import org.presight.ai.common.kafka.Topics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.json.JsonMapper;

@SpringBootApplication
@EnableKafkaStreams
public class SeatAvailabilityApplication {

    public static void main(String[] args) {
        SpringApplication.run(SeatAvailabilityApplication.class, args);
    }

    @Bean
    org.apache.kafka.streams.kstream.KStream<String, SeatMap> seatAvailability(StreamsBuilder builder, JsonMapper json,
                                                                                LiveSeatMap live) {
        return SeatAvailabilityTopology.build(builder, json, live::publish);
    }

    /**
     * Declares the output and — idempotently, same spec as inventory — the source topics: Kafka Streams treats a
     * missing source topic as fatal, and this app must not depend on inventory having started first.
     */
    @Bean
    KafkaAdmin.NewTopics availabilityTopics(@Value("${kafka.replicas:1}") int replicas) {
        return TopicDeclarations.of(List.of(Topics.SEAT_AVAILABILITY, Topics.SEAT_HELD, Topics.SEAT_RELEASED,
                Topics.SEAT_COMMITTED), "seat-availability-streams", List.of(), replicas);
    }

    /**
     * Interactive query over the local state store. Single instance locally; with several instances a key may
     * live on another host (route via {@code application.server} + streams metadata — not needed yet).
     */
    @RestController
    static class AvailabilityQuery {

        private final StreamsBuilderFactoryBean streams;

        AvailabilityQuery(StreamsBuilderFactoryBean streams) {
            this.streams = streams;
        }

        public record ClassOccupancy(String fareClass, int held, int committed) {
        }

        public record Occupancy(String trainNumber, String travelDate, List<ClassOccupancy> classes,
                                Map<String, Status> seats, Instant updatedAt) {
        }

        @GetMapping("/occupancy/{trainNumber}/{travelDate}")
        public Occupancy occupancy(@PathVariable String trainNumber, @PathVariable String travelDate) {
            KafkaStreams kafkaStreams = streams.getKafkaStreams();
            if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Stream not running yet");
            }
            ReadOnlyKeyValueStore<String, SeatMap> store = kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                    SeatAvailabilityTopology.STORE, QueryableStoreTypes.keyValueStore()));
            SeatMap map = store.get(Topics.seatKey(trainNumber, travelDate));
            return map == null ? new Occupancy(trainNumber, travelDate, List.of(), Map.of(), null) : summarise(map, Instant.now());
        }

        static Occupancy summarise(SeatMap map, Instant now) {
            Map<String, Status> seats = new LinkedHashMap<>();
            Map<String, int[]> byClass = new LinkedHashMap<>();
            map.seats().forEach((seatNo, seat) -> {
                Status status = SeatMap.effective(seat, now);
                if (status != Status.FREE) {
                    seats.put(seatNo, status);
                    int[] counts = byClass.computeIfAbsent(seat.fareClass(), k -> new int[2]);
                    counts[status == Status.HELD ? 0 : 1]++;
                }
            });
            List<ClassOccupancy> classes = byClass.entrySet().stream()
                    .map(e -> new ClassOccupancy(e.getKey(), e.getValue()[0], e.getValue()[1])).toList();
            return new Occupancy(map.trainNumber(), String.valueOf(map.travelDate()), classes, seats, map.updatedAt());
        }
    }
}
