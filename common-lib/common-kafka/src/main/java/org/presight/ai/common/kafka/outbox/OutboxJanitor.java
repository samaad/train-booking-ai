package org.presight.ai.common.kafka.outbox;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * CDC mode only: Debezium publishes outbox rows from the WAL and never marks them, so they are deleted once
 * older than the retention. Debezium's connector skips deletes ({@code skipped.operations}), so cleaning up
 * publishes nothing.
 */
public class OutboxJanitor implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxJanitor.class);

    private final DatabaseClient db;
    private final Duration retention;
    private final Duration interval;
    private Scheduler scheduler;
    private Disposable loop;

    public OutboxJanitor(DatabaseClient db, Duration retention, Duration interval) {
        this.db = db;
        this.retention = retention;
        this.interval = interval;
    }

    /** Deletes rows older than the retention; emits how many. */
    public Mono<Long> cleanOnce() {
        return db.sql("DELETE FROM outbox WHERE created_at < now() - make_interval(secs => :seconds)")
                .bind("seconds", (double) retention.toSeconds())
                .fetch()
                .rowsUpdated();
    }

    @Override
    public void start() {
        scheduler = Schedulers.newSingle("outbox-janitor");
        loop = Flux.interval(interval, scheduler)
                .onBackpressureDrop()
                .concatMap(tick -> cleanOnce().onErrorResume(e -> {
                    log.warn("Outbox cleanup failed, will retry: {}", e.toString());
                    return Mono.just(0L);
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
