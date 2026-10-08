package org.presight.ai.common.kafka.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Carries trace context across the outbox's time gap: captured when the event is written (inside the request
 * or listener that caused it) and restored when the relay publishes it, so the producer span — and through
 * Kafka the consumer's — joins the original trace. A no-op when tracing is absent.
 */
public interface TraceContext {

    TraceContext NONE = new TraceContext() {
        @Override
        public Map<String, String> capture() {
            return Map.of();
        }

        @Override
        public <T> T within(Map<String, String> headers, String name, Supplier<T> action) {
            return action.get();
        }
    };

    /** W3C headers of the current span, or empty. */
    Map<String, String> capture();

    /** Runs {@code action} inside a child span of the captured context (the action's sends become its children). */
    <T> T within(Map<String, String> headers, String name, Supplier<T> action);

    static TraceContext of(Tracer tracer, Propagator propagator) {
        return new TraceContext() {
            @Override
            public Map<String, String> capture() {
                Span span = tracer.currentSpan();
                if (span == null) {
                    return Map.of();
                }
                Map<String, String> headers = new HashMap<>();
                propagator.inject(span.context(), headers, Map::put);
                return headers;
            }

            @Override
            public <T> T within(Map<String, String> headers, String name, Supplier<T> action) {
                if (headers == null || headers.isEmpty()) {
                    return action.get();
                }
                Span span = propagator.extract(headers, Map::get).name(name).kind(Span.Kind.PRODUCER).start();
                try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
                    return action.get();
                } finally {
                    span.end();
                }
            }
        };
    }
}
