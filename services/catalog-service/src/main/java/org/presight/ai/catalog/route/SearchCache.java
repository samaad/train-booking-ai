package org.presight.ai.catalog.route;

import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Supplier;
import org.presight.ai.catalog.config.CatalogProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * Cache-aside for route search. Redis is an optimisation only: any Redis error or timeout
 * falls through to MongoDB, so a Redis outage costs latency, never availability.
 */
@Component
class SearchCache {

    private static final Logger log = LoggerFactory.getLogger(SearchCache.class);
    private static final Duration REDIS_BUDGET = Duration.ofMillis(200);
    private static final TypeReference<List<JourneyOption>> JOURNEYS = new TypeReference<>() {};

    private final ReactiveStringRedisTemplate redis;
    private final JsonMapper json;
    private final Duration ttl;

    SearchCache(ReactiveStringRedisTemplate redis, JsonMapper json, CatalogProperties properties) {
        this.redis = redis;
        this.json = json;
        this.ttl = properties.searchCacheTtl();
    }

    static String key(String from, String to, LocalDate date) {
        return "catalog:search:v1:" + from + ':' + to + ':' + date;
    }

    Mono<List<JourneyOption>> getOrLoad(String key, Supplier<Mono<List<JourneyOption>>> loader) {
        return read(key).switchIfEmpty(Mono.defer(() -> loader.get().flatMap(result -> write(key, result))));
    }

    private Mono<List<JourneyOption>> read(String key) {
        return redis.opsForValue().get(key)
                .timeout(REDIS_BUDGET)
                .map(cached -> json.readValue(cached, JOURNEYS))
                .onErrorResume(e -> {
                    log.warn("Search cache read failed, falling back to MongoDB: {}", e.toString());
                    return Mono.empty();
                });
    }

    private Mono<List<JourneyOption>> write(String key, List<JourneyOption> result) {
        return Mono.fromCallable(() -> json.writeValueAsString(result))
                .flatMap(payload -> redis.opsForValue().set(key, payload, ttl))
                .timeout(REDIS_BUDGET)
                .onErrorResume(e -> {
                    log.warn("Search cache write failed: {}", e.toString());
                    return Mono.just(false);
                })
                .thenReturn(result);
    }
}
