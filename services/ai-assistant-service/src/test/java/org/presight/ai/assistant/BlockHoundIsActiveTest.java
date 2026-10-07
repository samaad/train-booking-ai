package org.presight.ai.assistant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Proves the safety net is actually installed: a blocking call on a non-blocking thread must fail.
 * If this test ever passes vacuously, every "no blocking on the event loop" guarantee in this module is void.
 */
class BlockHoundIsActiveTest {

    @Test
    void blockingOnANonBlockingThreadIsDetected() {
        assertThatThrownBy(() -> Mono.fromCallable(() -> {
                    Thread.sleep(1);
                    return "slept";
                })
                .subscribeOn(Schedulers.parallel())
                .block(Duration.ofSeconds(5)))
                .hasCauseInstanceOf(BlockingOperationError.class);
    }
}
