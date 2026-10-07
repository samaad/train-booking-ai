package org.presight.ai.assistant.event;

import java.util.Map;

/**
 * Everything a chat turn streams to the client, one SSE event each ({@link #name()} is the SSE
 * event name). Structured events carry service data verbatim so the UI renders cards from facts,
 * not from the model's prose.
 */
public sealed interface ChatEvent {

    String name();

    /** First event of every turn. */
    record Conversation(String conversationId) implements ChatEvent {
        public String name() { return "conversation"; }
    }

    /** A chunk of model text. */
    record Token(String text) implements ChatEvent {
        public String name() { return "token"; }
    }

    /** A tool ran; {@code result} is the structured service response (or error) the model also saw. */
    record ToolActivity(String tool, String outcome, Object result) implements ChatEvent {
        public String name() { return "tool"; }
    }

    /**
     * An irreversible action the model proposed. Nothing has happened: the UI shows a confirm button
     * and, only on an explicit tap, calls {@code request} itself with the server-derived idempotency key.
     * The LLM proposes; the user disposes.
     */
    record ConfirmationRequired(String action, String method, String path, String idempotencyKey,
                                Map<String, Object> details) implements ChatEvent {
        public String name() { return "confirmation"; }
    }

    /** The answer may be untrustworthy, e.g. it quotes a price no tool returned. */
    record Warning(String code, String message) implements ChatEvent {
        public String name() { return "warning"; }
    }

    /** The model is unavailable; tier 3 = deterministic search results, tier 4 = use the booking form. */
    record Fallback(int tier, String message, Object results) implements ChatEvent {
        public String name() { return "fallback"; }
    }

    /** The turn failed. Sent after any partial text — a response is never silently truncated. */
    record Error(String code, String message) implements ChatEvent {
        public String name() { return "error"; }
    }

    record Done(String conversationId) implements ChatEvent {
        public String name() { return "done"; }
    }
}
