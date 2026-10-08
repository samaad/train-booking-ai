package org.presight.ai.assistant.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.presight.ai.assistant.event.ChatEvent;
import org.presight.ai.assistant.security.Caller;
import reactor.core.publisher.Sinks;

/**
 * Server-side state of one chat turn, handed to tools through Spring AI's ToolContext — never through
 * model arguments. It carries identity, the tool budget, the holds this conversation was issued, and a
 * sink for streaming tool activity to the client.
 */
public final class Turn {

    /** ToolContext key. */
    public static final String KEY = "turn";

    private final Caller caller;
    private final String conversationId;
    private final long turnSeq;
    private final int maxCalls;
    private final int maxMutating;
    private final Set<UUID> issuedHolds = ConcurrentHashMap.newKeySet();
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger mutating = new AtomicInteger();
    private final List<String> toolResultsJson = Collections.synchronizedList(new ArrayList<>());
    private final List<String> toolNames = Collections.synchronizedList(new ArrayList<>());
    private final Sinks.Many<ChatEvent> events = Sinks.many().unicast().onBackpressureBuffer();

    public Turn(Caller caller, String conversationId, long turnSeq, Set<UUID> previouslyIssuedHolds,
                int maxCalls, int maxMutating) {
        this.caller = caller;
        this.conversationId = conversationId;
        this.turnSeq = turnSeq;
        this.maxCalls = maxCalls;
        this.maxMutating = maxMutating;
        this.issuedHolds.addAll(previouslyIssuedHolds);
    }

    public Caller caller() {
        return caller;
    }

    public String conversationId() {
        return conversationId;
    }

    public long turnSeq() {
        return turnSeq;
    }

    /** Enforced here, in orchestration — a prompt instruction is advisory, this is not. */
    void acquire(String tool, boolean isMutating) {
        // A refused call consumes nothing: undo the reservation before refusing.
        if (calls.incrementAndGet() > maxCalls) {
            calls.decrementAndGet();
            throw new ToolBudgetExceeded("Tool budget exhausted (" + maxCalls + " calls per turn). Answer with what you have.");
        }
        if (isMutating && mutating.incrementAndGet() > maxMutating) {
            mutating.decrementAndGet();
            calls.decrementAndGet();
            throw new ToolBudgetExceeded("At most " + maxMutating + " seat holds or booking actions per turn.");
        }
        toolNames.add(tool);
    }

    void issueHold(UUID holdId) {
        issuedHolds.add(holdId);
    }

    boolean wasIssued(UUID holdId) {
        return issuedHolds.contains(holdId);
    }

    public Set<UUID> issuedHolds() {
        return Set.copyOf(issuedHolds);
    }

    void recordResult(String json) {
        toolResultsJson.add(json);
    }

    /** All tool results of this turn (JSON documents), for the price guardrail. */
    public List<String> toolResults() {
        synchronized (toolResultsJson) {
            return List.copyOf(toolResultsJson);
        }
    }

    public List<String> toolNames() {
        synchronized (toolNames) {
            return List.copyOf(toolNames);
        }
    }

    void emit(ChatEvent event) {
        events.emitNext(event, Sinks.EmitFailureHandler.busyLooping(java.time.Duration.ofMillis(100)));
    }

    public Sinks.Many<ChatEvent> events() {
        return events;
    }

    static final class ToolBudgetExceeded extends RuntimeException {
        ToolBudgetExceeded(String message) {
            super(message);
        }
    }
}
