package org.presight.ai.assistant;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import java.util.List;
import org.presight.ai.assistant.event.ChatEvent;
import org.presight.ai.assistant.memory.ConversationStore;
import org.presight.ai.assistant.orchestrator.ChatOrchestrator;
import org.presight.ai.assistant.security.Caller;
import org.presight.ai.common.error.DomainException;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Chat over SSE (02-ai-layer: unidirectional, proxy-friendly, auto-reconnecting; WebSocket is reserved
 * for the live seat map). Identity is captured <b>here</b>, eagerly, from the validated JWT — before any
 * scheduler hop — and passed down explicitly.
 */
@RestController
public class ChatController {

    private final ChatOrchestrator orchestrator;
    private final ConversationStore conversations;

    public ChatController(ChatOrchestrator orchestrator, ConversationStore conversations) {
        this.orchestrator = orchestrator;
        this.conversations = conversations;
    }

    public record ChatRequest(String conversationId, @NotBlank @Size(max = 2000) String message) {
    }

    public record ConversationSummary(String conversationId, String title, Instant updatedAt) {
    }

    public record MessageView(long seq, String role, String content, List<String> toolCalls, Instant createdAt) {
    }

    @PostMapping(path = "/chat", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<ChatEvent>> chat(@AuthenticationPrincipal Jwt jwt, @Valid @RequestBody ChatRequest request) {
        Caller caller = Caller.of(jwt);
        return orchestrator.chat(caller, request.conversationId(), request.message())
                .map(event -> ServerSentEvent.builder(event).event(event.name()).build());
    }

    @GetMapping("/conversations")
    public Flux<ConversationSummary> conversations(@AuthenticationPrincipal Jwt jwt) {
        return conversations.listOwned(jwt.getSubject(), 50)
                .map(c -> new ConversationSummary(c.id(), c.title(), c.updatedAt()));
    }

    @GetMapping("/conversations/{conversationId}/messages")
    public Flux<MessageView> messages(@AuthenticationPrincipal Jwt jwt, @PathVariable String conversationId) {
        return conversations.findOwned(conversationId, jwt.getSubject())
                .switchIfEmpty(Mono.error(() -> DomainException.notFound("CONVERSATION_NOT_FOUND", "No conversation " + conversationId)))
                .flatMapMany(c -> conversations.all(c.id()))
                .map(m -> new MessageView(m.seq(), m.role().name(), m.content(), m.toolCalls(), m.createdAt()));
    }
}
