package org.presight.ai.inventory.hold;

import org.presight.ai.inventory.config.InventoryProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * 03-consistency's "slow but total" expiry guarantee: every interval, release lapsed holds in batches
 * ({@code FOR UPDATE SKIP LOCKED}, so instances never collide) and emit {@code seat.released.v1}.
 * The UPDATE's self-healing clause already makes a lapsed seat reclaimable; this frees it for availability
 * views even when nobody contends for it. Redis keyspace notifications are deliberately not used — lossy.
 */
@Component
class HoldExpirySweeper implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(HoldExpirySweeper.class);

    private final HoldRepository holds;
    private final HoldService service;
    private final TransactionalOperator tx;
    private final InventoryProperties.Sweeper config;
    private Scheduler scheduler;
    private Disposable loop;

    HoldExpirySweeper(HoldRepository holds, HoldService service, TransactionalOperator tx, InventoryProperties properties) {
        this.holds = holds;
        this.service = service;
        this.tx = tx;
        this.config = properties.sweeper();
    }

    /** One batch; emits how many holds expired. */
    Mono<Long> sweepOnce() {
        return holds.lockExpired(config.batchSize())
                .collectList() // finish the cursor before issuing updates on the same connection
                .flatMapMany(Flux::fromIterable)
                .concatMap(hold -> service.expire(hold).thenReturn(hold))
                .count()
                .as(tx::transactional);
    }

    @Override
    public void start() {
        scheduler = Schedulers.newSingle("hold-sweeper");
        loop = Flux.interval(config.interval(), scheduler)
                .onBackpressureDrop()
                .concatMap(tick -> sweepOnce()
                        .doOnNext(n -> {
                            if (n > 0) {
                                log.info("Released {} expired holds", n);
                            }
                        })
                        .onErrorResume(e -> {
                            log.warn("Hold sweep failed, will retry: {}", e.toString());
                            return Mono.empty();
                        }), 1)
                .subscribe();
    }

    @Override
    public void stop() {
        if (loop != null) {
            loop.dispose();
        }
        if (scheduler != null) {
            scheduler.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return loop != null && !loop.isDisposed();
    }
}
