package org.presight.ai.assistant.memory;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Conversation memory behind our own interface (02-ai-layer: not Spring AI's ChatMemoryRepository,
 * to limit coupling to a young API). Every read is scoped by owner: another user's conversation id
 * behaves exactly like a missing one.
 */
public interface ConversationStore {

    record Conversation(String id, String userId, String title, Set<UUID> issuedHolds, long nextSeq,
                        Instant createdAt, Instant updatedAt) {
    }

    enum Role { USER, ASSISTANT }

    record Message(String id, String conversationId, long seq, Role role, String content, List<String> toolCalls,
                   Integer tokensIn, Integer tokensOut, Long latencyMs, Instant createdAt) {
    }

    Mono<Conversation> create(String userId, String title);

    /** Empty unless the conversation exists <em>and</em> belongs to {@code userId}. */
    Mono<Conversation> findOwned(String conversationId, String userId);

    Flux<Conversation> listOwned(String userId, int limit);

    /** The last {@code turns} user/assistant pairs, oldest first. */
    Flux<Message> recent(String conversationId, int turns);

    Flux<Message> all(String conversationId);

    /** Reserves two sequence numbers (user + assistant) for one turn; returns the first. */
    Mono<Long> reserveTurn(String conversationId);

    Mono<Void> append(Message message);

    Mono<Void> recordIssuedHolds(String conversationId, Set<UUID> holdIds);
}
