package org.presight.ai.assistant.orchestrator;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.presight.ai.assistant.config.AssistantProperties;
import org.presight.ai.assistant.event.ChatEvent;
import org.presight.ai.assistant.fallback.DeterministicFallback;
import org.presight.ai.assistant.memory.ConversationStore;
import org.presight.ai.assistant.memory.ConversationStore.Conversation;
import org.presight.ai.assistant.memory.ConversationStore.Message;
import org.presight.ai.assistant.memory.ConversationStore.Role;
import org.presight.ai.assistant.rag.RagRetriever;
import org.presight.ai.assistant.security.Caller;
import org.presight.ai.assistant.tools.TrainToolRegistry;
import org.presight.ai.assistant.tools.Turn;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.id.UuidV7;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.metadata.Usage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.model.tool.ToolCallingManager;
import org.springframework.ai.model.tool.ToolExecutionResult;
import org.springframework.ai.ollama.api.OllamaChatOptions;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * The chat turn, end to end. The only class that drives Spring AI's chat API.
 *
 * <p><b>Thread model.</b> Model output streams reactively ({@code Flux<ChatResponse>}) straight to SSE.
 * Tool execution is the one blocking seam: Spring AI's ChatClient tool loop hard-codes
 * {@code Schedulers.boundedElastic()}, so this class runs the loop itself and calls
 * {@link ToolCallingManager#executeToolCalls} on the bounded {@code aiScheduler}. When Spring AI ships
 * reactive tool callbacks, that one {@code subscribeOn} is what goes away.
 *
 * <p><b>Resilience.</b> A semaphore bulkhead caps concurrent turns (never a thread-pool bulkhead with
 * reactive types) and a circuit breaker guards Ollama. Nothing retries a model call: a retried 120 s
 * call turns one slow request into three. A failure before the first token falls back to the
 * deterministic tiers; after it, the client gets the partial text plus an explicit error event.
 */
@Service
class OllamaChatOrchestrator implements ChatOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(OllamaChatOrchestrator.class);

    private final ChatModel chatModel;
    private final ToolCallingManager toolCallingManager;
    private final TrainToolRegistry tools;
    private final RagRetriever rag;
    private final ConversationStore conversations;
    private final DeterministicFallback fallback;
    private final Scheduler aiScheduler;
    private final AssistantProperties properties;
    private final MeterRegistry meters;
    private final String model;
    private final Bulkhead bulkhead;
    private final CircuitBreaker circuitBreaker;
    private final AtomicInteger toolTasksQueued = new AtomicInteger();

    OllamaChatOrchestrator(ChatModel chatModel, ToolCallingManager toolCallingManager, TrainToolRegistry tools,
                           RagRetriever rag, ConversationStore conversations, DeterministicFallback fallback,
                           @Qualifier("aiScheduler") Scheduler aiScheduler, AssistantProperties properties,
                           MeterRegistry meters, @Value("${spring.ai.ollama.chat.model}") String model) {
        this.chatModel = chatModel;
        this.toolCallingManager = toolCallingManager;
        this.tools = tools;
        this.rag = rag;
        this.conversations = conversations;
        this.fallback = fallback;
        this.aiScheduler = aiScheduler;
        this.properties = properties;
        this.meters = meters;
        this.model = model;
        this.bulkhead = Bulkhead.of("ollama-chat", BulkheadConfig.custom()
                .maxConcurrentCalls(properties.maxConcurrentChats())
                .maxWaitDuration(Duration.ZERO) // overflow fails fast into the fallback, never queues behind a 120 s call
                .build());
        this.circuitBreaker = CircuitBreaker.of("ollama", CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(20)
                .minimumNumberOfCalls(10)
                .waitDurationInOpenState(Duration.ofSeconds(60))
                .build());
        meters.gauge("ai.scheduler.queued", toolTasksQueued);
        meters.gauge("ai.chat.active", bulkhead, b -> properties.maxConcurrentChats() - b.getMetrics().getAvailableConcurrentCalls());
    }

    @Override
    public Flux<ChatEvent> chat(Caller caller, String conversationId, String message) {
        return resolve(caller, conversationId, message)
                .flatMapMany(conversation -> conversations.reserveTurn(conversation.id())
                        .flatMapMany(seq -> turn(caller, conversation, seq, message)));
    }

    private Mono<Conversation> resolve(Caller caller, String conversationId, String message) {
        if (conversationId == null) {
            return conversations.create(caller.userId(), message.length() > 60 ? message.substring(0, 60) + "…" : message);
        }
        // Another user's conversation id is indistinguishable from a missing one.
        return conversations.findOwned(conversationId, caller.userId())
                .switchIfEmpty(Mono.error(() -> DomainException.notFound("CONVERSATION_NOT_FOUND", "No conversation " + conversationId)));
    }

    private Flux<ChatEvent> turn(Caller caller, Conversation conversation, long seq, String message) {
        Turn turn = new Turn(caller, conversation.id(), seq, conversation.issuedHolds(),
                properties.maxToolCallsPerTurn(), properties.maxMutatingCallsPerTurn());
        TurnRecord record = new TurnRecord();

        Flux<ChatEvent> modelEvents = Mono.zip(conversations.recent(conversation.id(), properties.historyTurns()).collectList(),
                        rag.retrieve(message))
                .flatMapMany(ctx -> rounds(prompt(caller, ctx.getT1(), ctx.getT2(), message, turn), turn, record, 0))
                // Breaker inside, bulkhead outside: "busy" is load shedding, not an Ollama failure, and must
                // not trip the breaker for everyone.
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .onErrorResume(e -> record.anyToken.get()
                        ? Flux.just(new ChatEvent.Error("RESPONSE_INTERRUPTED",
                                "The assistant stopped mid-answer (" + reason(e) + "). The text above is incomplete."))
                        : fallback.answer(caller, message, reason(e)).flux())
                .doFinally(signal -> turn.events().tryEmitComplete());

        return Flux.concat(
                Mono.just(new ChatEvent.Conversation(conversation.id())),
                Flux.merge(modelEvents, turn.events().asFlux()),
                Flux.defer(() -> finish(turn, seq, message, record)));
    }

    /** One model round; recurses while the model asks for tools, bounded by the tool budget. */
    private Flux<ChatEvent> rounds(Prompt prompt, Turn turn, TurnRecord record, int depth) {
        RoundState round = new RoundState();
        Flux<ChatEvent> tokens = chatModel.stream(prompt)
                .timeout(properties.chatTimeout()) // max silence between chunks, first chunk included
                .doOnNext(response -> round.accept(response, record, meters))
                .mapNotNull(response -> {
                    String text = textOf(response);
                    return text == null || text.isEmpty() ? null : (ChatEvent) new ChatEvent.Token(text);
                });

        return tokens.concatWith(Flux.defer(() -> {
            if (round.toolCalls.isEmpty()) {
                return Flux.empty();
            }
            if (depth >= properties.maxToolCallsPerTurn()) {
                return Flux.just(new ChatEvent.Warning("TOOL_LOOP_STOPPED", "Stopped after too many tool rounds."));
            }
            ChatResponse toolRequest = new ChatResponse(List.of(new Generation(
                    AssistantMessage.builder().content(round.text.toString()).toolCalls(round.toolCalls).build())));
            return executeTools(prompt, toolRequest)
                    .flatMapMany(result -> rounds(new Prompt(result.conversationHistory(), prompt.getOptions()), turn, record, depth + 1));
        }));
    }

    /** THE seam: synchronous Spring AI tool callbacks, on the bounded aiScheduler and nowhere else. */
    private Mono<ToolExecutionResult> executeTools(Prompt prompt, ChatResponse toolRequest) {
        toolTasksQueued.incrementAndGet();
        AtomicBoolean started = new AtomicBoolean();
        return Mono.fromCallable(() -> {
                    started.set(true);
                    toolTasksQueued.decrementAndGet();
                    return toolCallingManager.executeToolCalls(prompt, toolRequest);
                })
                .subscribeOn(aiScheduler)
                .doFinally(s -> {
                    if (!started.get()) {
                        toolTasksQueued.decrementAndGet(); // rejected or cancelled while queued
                    }
                });
    }

    private Prompt prompt(Caller caller, List<Message> history, List<String> reference, String userMessage, Turn turn) {
        List<org.springframework.ai.chat.messages.Message> messages = new ArrayList<>();
        messages.add(new SystemMessage(systemPrompt(caller, reference)));
        for (Message m : history) {
            messages.add(m.role() == Role.USER ? new UserMessage(m.content()) : new AssistantMessage(m.content()));
        }
        messages.add(new UserMessage(userMessage));
        OllamaChatOptions options = OllamaChatOptions.builder()
                .model(model)
                .temperature(properties.temperature())
                .disableThinking() // thinking adds significant time-to-first-token
                .toolCallbacks(tools.callbacks())
                .toolContext(Map.of(Turn.KEY, turn)) // identity travels here, never through model arguments
                .build();
        return new Prompt(messages, options);
    }

    /** Short and concrete: small models follow terse rules far better than policy essays. */
    private String systemPrompt(Caller caller, List<String> reference) {
        return """
                You are the booking assistant for the TrainBook rail service. Today is %s. User: %s.
                Rules:
                - Use tools for ALL facts about trains, seats, prices, bookings. Never guess.
                - Never state a price or availability that did not come from a tool result.
                - To book: searchTrains → checkAvailability → holdSeat → summary → confirmBooking.
                - confirmBooking and cancelBooking only show the user a confirmation card; never say something is booked or cancelled.
                - If a tool fails, say so plainly. Do not invent a result.
                - Reply in the user's language. Be brief.
                Reference material (may be empty):
                %s""".formatted(LocalDate.now(properties.zone()), caller.displayName() == null ? "traveller" : caller.displayName(),
                String.join("\n---\n", reference));
    }

    /** Guardrail check, then persistence. Persistence failures are logged, never surfaced mid-stream. */
    private Flux<ChatEvent> finish(Turn turn, long seq, String userMessage, TurnRecord record) {
        List<ChatEvent> tail = new ArrayList<>();
        String answer = record.answer.toString();
        List<String> unsupported = PriceGuard.unsupportedAmounts(answer, turn.toolResults());
        if (!unsupported.isEmpty()) {
            meters.counter("ai.hallucinated_price").increment(unsupported.size());
            tail.add(new ChatEvent.Warning("UNVERIFIED_PRICE",
                    "These amounts did not come from any service: " + String.join(", ", unsupported) + ". Check the details cards."));
        }
        long latency = Duration.between(record.startedAt, Instant.now()).toMillis();
        meters.timer("llm.request.duration", "hasTools", Boolean.toString(!turn.toolNames().isEmpty()))
                .record(Duration.ofMillis(latency));

        Mono<Void> persist = conversations.append(new Message(UuidV7.generate().toString(), turn.conversationId(), seq,
                        Role.USER, userMessage, List.of(), null, null, null, record.startedAt))
                .then(conversations.append(new Message(UuidV7.generate().toString(), turn.conversationId(), seq + 1,
                        Role.ASSISTANT, answer, turn.toolNames(), record.tokensIn.get(), record.tokensOut.get(), latency, Instant.now())))
                .then(conversations.recordIssuedHolds(turn.conversationId(), turn.issuedHolds()))
                .onErrorResume(e -> {
                    log.error("Failed to persist turn {} of conversation {}", seq, turn.conversationId(), e);
                    return Mono.empty();
                });
        return persist.thenMany(Flux.fromIterable(tail)).concatWith(Mono.just(new ChatEvent.Done(turn.conversationId())));
    }

    private static String reason(Throwable e) {
        return switch (e) {
            case CallNotPermittedException c -> "model circuit open";
            case BulkheadFullException b -> "assistant busy";
            case RejectedExecutionException r -> "assistant busy";
            case TimeoutException t -> "model timed out";
            default -> "model unreachable";
        };
    }

    private static String textOf(ChatResponse response) {
        Generation g = response.getResult();
        return g == null || g.getOutput() == null ? null : g.getOutput().getText();
    }

    /** Accumulates what one turn produced across all rounds. */
    private static final class TurnRecord {
        final Instant startedAt = Instant.now();
        final StringBuilder answer = new StringBuilder();
        final AtomicBoolean anyToken = new AtomicBoolean();
        final AtomicReference<Integer> tokensIn = new AtomicReference<>(0);
        final AtomicReference<Integer> tokensOut = new AtomicReference<>(0);
    }

    /** What one model round produced: its text, any tool calls, and usage. */
    private static final class RoundState {
        final StringBuilder text = new StringBuilder();
        final List<AssistantMessage.ToolCall> toolCalls = new ArrayList<>();

        void accept(ChatResponse response, TurnRecord record, MeterRegistry meters) {
            Generation g = response.getResult();
            if (g != null && g.getOutput() != null) {
                String t = g.getOutput().getText();
                if (t != null && !t.isEmpty()) {
                    if (record.anyToken.compareAndSet(false, true)) {
                        meters.timer("llm.ttft").record(Duration.between(record.startedAt, Instant.now()));
                    }
                    text.append(t);
                    record.answer.append(t);
                }
                if (g.getOutput().hasToolCalls()) {
                    toolCalls.addAll(g.getOutput().getToolCalls());
                }
            }
            Usage usage = response.getMetadata() == null ? null : response.getMetadata().getUsage();
            if (usage != null && usage.getPromptTokens() != null && usage.getPromptTokens() > 0) {
                record.tokensIn.accumulateAndGet(usage.getPromptTokens(), Integer::sum);
                record.tokensOut.accumulateAndGet(usage.getCompletionTokens() == null ? 0 : usage.getCompletionTokens(), Integer::sum);
                meters.counter("llm.tokens", "direction", "prompt").increment(usage.getPromptTokens());
                meters.counter("llm.tokens", "direction", "completion")
                        .increment(usage.getCompletionTokens() == null ? 0 : usage.getCompletionTokens());
            }
        }
    }
}
