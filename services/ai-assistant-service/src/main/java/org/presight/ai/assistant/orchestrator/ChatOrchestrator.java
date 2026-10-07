package org.presight.ai.assistant.orchestrator;

import org.presight.ai.assistant.event.ChatEvent;
import org.presight.ai.assistant.security.Caller;
import reactor.core.publisher.Flux;

/**
 * One chat turn as a stream of events. Our interface — Spring AI is the least stable dependency in the
 * stack, so it stays behind this seam.
 */
public interface ChatOrchestrator {

    /**
     * @param caller         identity captured eagerly from the validated JWT
     * @param conversationId {@code null} to start a new conversation
     */
    Flux<ChatEvent> chat(Caller caller, String conversationId, String message);
}
