package org.presight.ai.availability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.presight.ai.availability.SeatMap.Status;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.SeatEvents.ReleaseReason;
import org.presight.ai.common.kafka.events.SeatEvents.SeatCommitted;
import org.presight.ai.common.kafka.events.SeatEvents.SeatHeld;
import org.presight.ai.common.kafka.events.SeatEvents.SeatReleased;
import tools.jackson.databind.json.JsonMapper;

/** The topology without a broker: Kafka's TopologyTestDriver runs it deterministically, one record at a time. */
class SeatAvailabilityTopologyTest {

    private static final JsonMapper JSON = JsonMapper.builder().findAndAddModules().build();
    private static final LocalDate DATE = LocalDate.of(2026, 10, 20);
    private static final String KEY = Topics.seatKey("ER101", DATE);
    private static final Instant T0 = Instant.parse("2026-10-07T10:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> held;
    private TestInputTopic<String, String> released;
    private TestInputTopic<String, String> committed;
    private TestOutputTopic<String, String> out;
    private final List<String> pushed = new ArrayList<>();

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        SeatAvailabilityTopology.build(builder, JSON, (key, map) -> pushed.add(key));
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), props);
        held = driver.createInputTopic(Topics.SEAT_HELD, new StringSerializer(), new StringSerializer());
        released = driver.createInputTopic(Topics.SEAT_RELEASED, new StringSerializer(), new StringSerializer());
        committed = driver.createInputTopic(Topics.SEAT_COMMITTED, new StringSerializer(), new StringSerializer());
        out = driver.createOutputTopic(Topics.SEAT_AVAILABILITY, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void holdThenCommitIsCommitted() {
        UUID hold = UUID.randomUUID();
        held.pipeInput(KEY, json(new SeatHeld(UUID.randomUUID(), hold, "ER101", DATE, "BUSINESS", "B4", T0.plusSeconds(600), T0)));
        committed.pipeInput(KEY, json(new SeatCommitted(UUID.randomUUID(), hold, "ER101", DATE, "BUSINESS", "B4", T0.plusSeconds(5))));

        assertThat(seat("B4").status()).isEqualTo(Status.COMMITTED);
        assertThat(out.readKeyValuesToList()).hasSize(2).allSatisfy(kv -> assertThat(kv.key).isEqualTo(KEY));
        assertThat(pushed).containsExactly(KEY, KEY); // every change reaches the live seat map
    }

    @Test
    void aReleaseFreesTheSeat() {
        UUID hold = UUID.randomUUID();
        held.pipeInput(KEY, json(new SeatHeld(UUID.randomUUID(), hold, "ER101", DATE, "BUSINESS", "B5", T0.plusSeconds(600), T0)));
        released.pipeInput(KEY, json(new SeatReleased(UUID.randomUUID(), hold, "ER101", DATE, "BUSINESS", "B5", ReleaseReason.RELEASED, T0.plusSeconds(3))));

        assertThat(seat("B5").status()).isEqualTo(Status.FREE);
    }

    @Test
    void aReleaseThatOvertakesItsHoldDoesNotLeaveTheSeatHeld() {
        UUID hold = UUID.randomUUID();
        // Different topics: the release (t+3) is processed before the hold (t+0).
        released.pipeInput(KEY, json(new SeatReleased(UUID.randomUUID(), hold, "ER101", DATE, "BUSINESS", "B6", ReleaseReason.EXPIRED, T0.plusSeconds(3))));
        held.pipeInput(KEY, json(new SeatHeld(UUID.randomUUID(), hold, "ER101", DATE, "BUSINESS", "B6", T0.plusSeconds(600), T0)));

        assertThat(seat("B6").status()).isEqualTo(Status.FREE);
    }

    @Test
    void releasingAnOlderHoldDoesNotFreeASeatANewerHoldOwns() {
        UUID oldHold = UUID.randomUUID();
        UUID newHold = UUID.randomUUID();
        held.pipeInput(KEY, json(new SeatHeld(UUID.randomUUID(), oldHold, "ER101", DATE, "BUSINESS", "B7", T0.plusSeconds(60), T0)));
        held.pipeInput(KEY, json(new SeatHeld(UUID.randomUUID(), newHold, "ER101", DATE, "BUSINESS", "B7", T0.plusSeconds(900), T0.plusSeconds(120))));
        released.pipeInput(KEY, json(new SeatReleased(UUID.randomUUID(), oldHold, "ER101", DATE, "BUSINESS", "B7", ReleaseReason.RELEASED, T0.plusSeconds(130))));

        assertThat(seat("B7").status()).isEqualTo(Status.HELD);
        assertThat(seat("B7").holdId()).isEqualTo(newHold);
    }

    @Test
    void aLapsedHoldCountsAsFreeBeforeTheSweeperSaysSo() {
        held.pipeInput(KEY, json(new SeatHeld(UUID.randomUUID(), UUID.randomUUID(), "ER101", DATE, "ECONOMY", "E1", T0.plusSeconds(60), T0)));
        held.pipeInput(KEY, json(new SeatHeld(UUID.randomUUID(), UUID.randomUUID(), "ER101", DATE, "ECONOMY", "E2", T0.plusSeconds(6000), T0)));

        var occupancy = SeatAvailabilityApplication.AvailabilityQuery.summarise(store().get(KEY), T0.plusSeconds(120));
        assertThat(occupancy.seats()).containsOnlyKeys("E2");
        assertThat(occupancy.classes()).singleElement().satisfies(c -> {
            assertThat(c.fareClass()).isEqualTo("ECONOMY");
            assertThat(c.held()).isEqualTo(1);
        });
    }

    private SeatMap.Seat seat(String seatNo) {
        return store().get(KEY).seats().get(seatNo);
    }

    private KeyValueStore<String, SeatMap> store() {
        return driver.getKeyValueStore(SeatAvailabilityTopology.STORE);
    }

    private static String json(Object event) {
        return JSON.writeValueAsString(event);
    }
}
