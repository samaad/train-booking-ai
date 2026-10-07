package org.presight.ai.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@Import(TestcontainersConfiguration.class)
class CatalogApiTest {

    private static final LocalDate NEXT_MONDAY = LocalDate.now().with(TemporalAdjusters.next(DayOfWeek.MONDAY));

    @Autowired
    WebTestClient client;

    @Autowired
    ReactiveStringRedisTemplate redis;

    @Autowired
    ReactiveMongoTemplate mongo;

    @Test
    void stationsAreSeededAndSortedByName() {
        client.get().uri("/stations").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(6)
                .jsonPath("$[0].code").isEqualTo("AUH")
                .jsonPath("$[0].name").isEqualTo("Abu Dhabi");
    }

    @Test
    void searchReturnsSegmentTimesAndDistance() {
        client.get().uri("/routes/search?from=DXB&to=FUJ&date={d}", NEXT_MONDAY).exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(1)
                .jsonPath("$[0].trainNumber").isEqualTo("ER101")
                .jsonPath("$[0].departure").isEqualTo(NEXT_MONDAY + "T07:55:00+04:00")
                .jsonPath("$[0].arrival").isEqualTo(NEXT_MONDAY + "T09:05:00+04:00")
                .jsonPath("$[0].durationMinutes").isEqualTo(70)
                .jsonPath("$[0].distanceKm").isEqualTo(130)
                .jsonPath("$[0].fareClasses[0]").isEqualTo("BUSINESS");
    }

    @Test
    void searchRespectsDirection() {
        client.get().uri("/routes/search?from=FUJ&to=AUH&date={d}", NEXT_MONDAY).exchange()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(1)
                .jsonPath("$[0].trainNumber").isEqualTo("ER102");
    }

    @Test
    void overnightTrainIsMatchedByItsOriginDay() {
        // ER301 leaves RUW Fri/Sat 22:30 and reaches AUH at 00:10 the next calendar day.
        LocalDate saturday = NEXT_MONDAY.with(TemporalAdjusters.next(DayOfWeek.SATURDAY));
        client.get().uri("/routes/search?from=AUH&to=DXB&date={d}", saturday).exchange()
                .expectBody()
                .jsonPath("$[?(@.trainNumber == 'ER301')].departure").isEqualTo(saturday + "T00:20:00+04:00");

        LocalDate friday = saturday.minusDays(1); // origin day would be Thursday — not a running day
        client.get().uri("/routes/search?from=AUH&to=DXB&date={d}", friday).exchange()
                .expectBody()
                .jsonPath("$[?(@.trainNumber == 'ER301')]").isEmpty();
    }

    @Test
    void searchResultIsCachedInRedis() {
        LocalDate date = NEXT_MONDAY.plusWeeks(1);
        client.get().uri("/routes/search?from=AUH&to=DXB&date={d}", date).exchange().expectStatus().isOk();

        assertThat(redis.hasKey("catalog:search:v1:AUH:DXB:" + date).block()).isTrue();
    }

    @Test
    void rejectsUnknownStationsAndDegenerateSearches() {
        client.get().uri("/routes/search?from=XXX&to=DXB&date={d}", NEXT_MONDAY).exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.code").isEqualTo("STATION_NOT_FOUND");
        client.get().uri("/routes/search?from=DXB&to=DXB&date={d}", NEXT_MONDAY).exchange()
                .expectStatus().isEqualTo(422)
                .expectBody().jsonPath("$.code").isEqualTo("SAME_ORIGIN_AND_DESTINATION");
        client.get().uri("/routes/search?from=DXB&to=AUH&date=not-a-date").exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void fareRulesAreExactDecimals() {
        client.get().uri("/fares/rules?trainNumber=ER101&fareClass=BUSINESS").exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(1)
                .jsonPath("$[0].baseFare").isEqualTo(25.00)
                .jsonPath("$[0].perKm").isEqualTo(0.90)
                .jsonPath("$[0].currency").isEqualTo("AED");
        client.get().uri("/fares/rules?trainNumber=ER301").exchange()
                .expectBody().jsonPath("$.length()").isEqualTo(1);

        Document stored = mongo.getCollection("fare_rules")
                .flatMap(c -> reactor.core.publisher.Mono.from(c.find(new Document("_id", "ER101:BUSINESS")).first()))
                .block();
        assertThat(stored.get("perKm")).isInstanceOf(org.bson.types.Decimal128.class);
    }
}
