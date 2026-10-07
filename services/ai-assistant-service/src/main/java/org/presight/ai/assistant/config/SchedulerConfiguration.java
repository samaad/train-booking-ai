package org.presight.ai.assistant.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * The two blocking seams of this service, each on its own explicitly bounded pool.
 *
 * <ul>
 *   <li><b>aiScheduler</b> — Spring AI tool callbacks ({@code ToolCallback.call()} returns a bare
 *       String, spring-ai#4434). Spring AI's own ChatClient tool loop hard-codes
 *       {@code Schedulers.boundedElastic()}, so the orchestrator runs its own loop on this pool.</li>
 *   <li><b>vectorScheduler</b> — PgVectorStore, which is JDBC-only, and the blocking embedding
 *       call it makes.</li>
 * </ul>
 *
 * Never {@code Schedulers.boundedElastic()}: its cap is ~10×CPU threads with a 100k task queue —
 * effectively unbounded against a 9.6 GB model. A full queue rejects, which the orchestrator turns
 * into a fallback tier. Backpressure is the feature.
 */
@Configuration(proxyBeanMethods = false)
class SchedulerConfiguration {

    @Bean(destroyMethod = "dispose")
    Scheduler aiScheduler(AssistantProperties properties) {
        return Schedulers.newBoundedElastic(properties.maxConcurrentChats(), properties.schedulerQueue(), "ai");
    }

    @Bean(destroyMethod = "dispose")
    Scheduler vectorScheduler(AssistantProperties properties) {
        return Schedulers.newBoundedElastic(8, properties.schedulerQueue(), "vector");
    }
}
