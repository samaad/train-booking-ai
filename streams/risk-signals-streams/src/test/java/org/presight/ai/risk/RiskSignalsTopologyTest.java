package org.presight.ai.risk;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.presight.ai.common.kafka.EventHeaders;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCreated;
import org.presight.ai.common.kafka.events.RiskEvents.RiskKind;
import org.presight.ai.common.kafka.events.RiskEvents.RiskSignal;
import org.presight.ai.common.kafka.events.SeatEvents.SeatHeld;
import tools.jackson.databind.json.JsonMapper;

/** Session-window rules without a broker: TopologyTestDriver advances stream time by record timestamps. */
class RiskSignalsTopologyTest {

    private static final JsonMapper JSON = JsonMapper.builder().findAndAddModules().build();
    private static final RiskRules RULES = new RiskRules(8, 0.25, Duration.ofMinutes(15), Duration.ofMinutes(1));
    private static final LocalDate DATE = LocalDate.of(2026, 11, 1);
    private static final Instant T0 = Instant.parse("2026-10-08T10:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> held;
    private TestInputTopic<String, String> created;
    private TestOutputTopic<String, String> signals;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        RiskSignalsTopology.build(builder, JSON, RULES, signal -> { });
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), props);
        held = driver.createInputTopic(Topics.SEAT_HELD, new StringSerializer(), new StringSerializer());
        created = driver.createInputTopic(Topics.BOOKING_CREATED, new StringSerializer(), new StringSerializer());
        signals = driver.createOutputTopic(Topics.RISK_SIGNAL, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void manyHoldsAndNoBookingsInOneBurstRaiseOneSignalPerThreshold() {
        for (int i = 0; i < 17; i++) {
            hold("squatter", T0.plusSeconds(30L * i)); // 30 s apart: one session
        }
        List<RiskSignal> raised = drain();
        assertThat(raised).extracting(RiskSignal::holds).containsExactly(8L, 16L);
        assertThat(raised).allSatisfy(s -> {
            assertThat(s.userId()).isEqualTo("squatter");
            assertThat(s.kind()).isEqualTo(RiskKind.SEAT_SQUATTING);
            assertThat(s.sessionStart()).isEqualTo(T0);
        });
    }

    @Test
    void holdsThatTurnIntoBookingsAreNotSquatting() {
        for (int i = 0; i < 8; i++) {
            hold("traveller", T0.plusSeconds(60L * i));
            if (i % 2 == 0) {
                book("traveller", T0.plusSeconds(60L * i + 10)); // half the holds become bookings
            }
        }
        assertThat(drain()).isEmpty();
    }

    @Test
    void holdsSpreadOverTheDayAreSeparateSessions() {
        for (int i = 0; i < 12; i++) {
            hold("occasional", T0.plus(Duration.ofMinutes(20L * i))); // gap 20 min > 15 min session gap
        }
        assertThat(drain()).isEmpty();
    }

    @Test
    void aHoldBridgingTwoSessionsMergesThemAndCanRaiseTheSignal() {
        for (int i = 0; i < 4; i++) {
            hold("bridger", T0.plusSeconds(10L * i));                          // session A: 10:00:00–10:00:30
        }
        for (int i = 0; i < 4; i++) {
            hold("bridger", T0.plus(Duration.ofMinutes(28)).plusSeconds(10L * i)); // session B, 28 min later
        }
        assertThat(drain()).isEmpty();

        hold("bridger", T0.plus(Duration.ofMinutes(14))); // within 15 min of both: A + B + this = 9 holds
        List<RiskSignal> raised = drain();
        assertThat(raised).singleElement().satisfies(s -> {
            assertThat(s.holds()).isEqualTo(9);
            assertThat(s.sessionStart()).isEqualTo(T0);
            assertThat(s.sessionEnd()).isEqualTo(T0.plus(Duration.ofMinutes(28)).plusSeconds(30));
        });
    }

    @Test
    void usersAreJudgedSeparately() {
        for (int i = 0; i < 8; i++) {
            hold(i % 2 == 0 ? "alice" : "bob", T0.plusSeconds(20L * i)); // 4 each
        }
        assertThat(drain()).isEmpty();
    }

    @Test
    void signalsCarryEventHeadersAndAStableId() {
        for (int i = 0; i < 8; i++) {
            hold("squatter", T0.plusSeconds(30L * i));
        }
        TestRecord<String, String> record = signals.readRecord();
        RiskSignal signal = JSON.readValue(record.value(), RiskSignal.class);
        assertThat(record.key()).isEqualTo("squatter");
        assertThat(new String(record.headers().lastHeader(EventHeaders.EVENT_ID).value(), StandardCharsets.UTF_8))
                .isEqualTo(signal.eventId().toString());
        assertThat(new String(record.headers().lastHeader(EventHeaders.EVENT_TYPE).value(), StandardCharsets.UTF_8))
                .isEqualTo(Topics.RISK_SIGNAL);
        // Only the signal's own headers plus the trace link — never the triggering hold's event id.
        assertThat(record.headers().toArray()).extracting(h -> h.key())
                .containsExactlyInAnyOrder("traceparent", EventHeaders.EVENT_ID, EventHeaders.EVENT_TYPE);
        assertThat(record.headers().headers(EventHeaders.EVENT_ID)).hasSize(1);
        // Recomputed after a crash, the same signal gets the same id — consumers deduplicate it.
        assertThat(signal.eventId())
                .isEqualTo(UUID.nameUUIDFromBytes(("squatter|" + T0 + "|1").getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void holdsWithoutAnOwnerAreIgnored() {
        for (int i = 0; i < 10; i++) {
            hold(null, T0.plusSeconds(10L * i));
        }
        assertThat(drain()).isEmpty();
    }

    private void hold(String userId, Instant at) {
        SeatHeld event = new SeatHeld(UUID.randomUUID(), UUID.randomUUID(), "ER101", DATE, "ECONOMY", "E1",
                at.plusSeconds(600), at, userId);
        var headers = new RecordHeaders()
                .add(EventHeaders.EVENT_ID, event.eventId().toString().getBytes(StandardCharsets.UTF_8))
                .add("id", event.eventId().toString().getBytes(StandardCharsets.UTF_8)) // as Debezium sets it
                .add("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".getBytes(StandardCharsets.UTF_8));
        held.pipeInput(new TestRecord<>(Topics.seatKey("ER101", DATE), JSON.writeValueAsString(event), headers, at));
    }

    private void book(String userId, Instant at) {
        UUID bookingId = UUID.randomUUID();
        BookingCreated event = new BookingCreated(UUID.randomUUID(), bookingId, userId, "PNR123", UUID.randomUUID(),
                "ER101", DATE, "ECONOMY", "E1", "DXB", "FUJ", OffsetDateTime.parse("2026-11-01T07:55:00+04:00"),
                "Passenger", "p@example.com", new BigDecimal("42.00"), "AED", at);
        created.pipeInput(bookingId.toString(), JSON.writeValueAsString(event), at);
    }

    private List<RiskSignal> drain() {
        return signals.readValuesToList().stream().map(v -> JSON.readValue(v, RiskSignal.class)).toList();
    }
}
