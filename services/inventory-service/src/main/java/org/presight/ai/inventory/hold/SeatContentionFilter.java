package org.presight.ai.inventory.hold;

import java.time.Duration;
import java.util.List;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.inventory.config.InventoryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Sheds a thundering herd on one hot seat before it reaches Postgres: the first request
 * takes a short-lived in-flight marker, concurrent ones fail fast with 409.
 *
 * <p><b>This is a filter, not a lock.</b> Postgres alone decides who holds a seat. If Redis is
 * slow or down the filter steps aside and every request goes to the database, which is still
 * correct — just less efficient under contention.
 */
@Component
class SeatContentionFilter {

    private static final Logger log = LoggerFactory.getLogger(SeatContentionFilter.class);
    private static final Duration REDIS_BUDGET = Duration.ofMillis(300);
    private static final RedisScript<Long> COMPARE_AND_DELETE = RedisScript.of("""
            if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end
            """, Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final boolean enabled;
    private final Duration lockTtl;

    SeatContentionFilter(ReactiveStringRedisTemplate redis, InventoryProperties properties) {
        this.redis = redis;
        this.enabled = properties.contentionFilter().enabled();
        this.lockTtl = properties.contentionFilter().lockTtl();
    }

    <T> Mono<T> guard(String seatKey, String token, Mono<T> attempt) {
        if (!enabled) {
            return attempt;
        }
        String key = "inv:inflight:" + seatKey;
        return Mono.usingWhen(
                tryMark(key, token),
                marked -> marked ? attempt : Mono.error(contended(seatKey)),
                marked -> marked ? unmark(key, token) : Mono.empty());
    }

    private Mono<Boolean> tryMark(String key, String token) {
        return redis.opsForValue().setIfAbsent(key, token, lockTtl)
                .timeout(REDIS_BUDGET)
                .onErrorResume(e -> {
                    log.warn("Contention filter bypassed, Redis unavailable: {}", e.toString());
                    return Mono.just(true);
                });
    }

    private Mono<Void> unmark(String key, String token) {
        return redis.execute(COMPARE_AND_DELETE, List.of(key), List.of(token))
                .timeout(REDIS_BUDGET)
                .onErrorResume(e -> Mono.empty()) // the marker expires on its own within lockTtl
                .then();
    }

    private static DomainException contended(String seatKey) {
        return DomainException.conflict("SEAT_UNAVAILABLE", "Seat " + seatKey + " is being held by another request");
    }
}
