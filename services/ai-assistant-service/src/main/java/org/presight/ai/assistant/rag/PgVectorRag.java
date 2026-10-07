package org.presight.ai.assistant.rag;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.presight.ai.assistant.config.AssistantProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * pgvector-backed RAG. PgVectorStore is JDBC-only and its embedding call is blocking, so every call
 * here runs on {@code vectorScheduler} — never inline on an event-loop thread.
 */
@Component
public class PgVectorRag implements RagRetriever {

    private static final Logger log = LoggerFactory.getLogger(PgVectorRag.class);

    /** mxbai-embed-large is trained asymmetrically: queries (not documents) carry this prefix. */
    static final String QUERY_PREFIX = "Represent this sentence for searching relevant passages: ";
    private static final Duration SEARCH_TIMEOUT = Duration.ofSeconds(5);

    private final VectorStore store;
    private final Scheduler vectorScheduler;
    private final ResourcePatternResolver resources;
    private final AssistantProperties properties;
    private final MeterRegistry meters;
    private final MarkdownChunker chunker = new MarkdownChunker();

    PgVectorRag(VectorStore store, @Qualifier("vectorScheduler") Scheduler vectorScheduler,
                ResourcePatternResolver resources, AssistantProperties properties, MeterRegistry meters) {
        this.store = store;
        this.vectorScheduler = vectorScheduler;
        this.resources = resources;
        this.properties = properties;
        this.meters = meters;
    }

    @Override
    public Mono<List<String>> retrieve(String query) {
        AssistantProperties.Rag rag = properties.rag();
        String today = LocalDate.now(properties.zone()).toString();
        Timer.Sample sample = Timer.start(meters);
        return Mono.fromCallable(() -> store.similaritySearch(SearchRequest.builder()
                        .query(QUERY_PREFIX + query)
                        .topK(rag.topK())
                        .similarityThreshold(rag.similarityThreshold())
                        // An expired fare rule is a correctness bug, not a relevance one: filter on validity.
                        .filterExpression("validFrom <= '" + today + "' && validTo >= '" + today + "'")
                        .build()))
                .subscribeOn(vectorScheduler)
                .timeout(SEARCH_TIMEOUT)
                .map(docs -> docs.stream().map(Document::getText).toList())
                .doOnNext(texts -> meters.counter("rag.chunks.returned").increment(texts.size()))
                .doFinally(s -> sample.stop(meters.timer("rag.retrieval.duration")))
                .onErrorResume(e -> {
                    log.warn("RAG retrieval failed, continuing without reference material: {}", e.toString());
                    return Mono.just(List.of());
                });
    }

    /** Chunks the whole corpus first, so an oversized chunk fails before anything is written. */
    public Mono<Integer> ingest() {
        return Mono.fromCallable(() -> {
                    Map<String, List<Document>> bySource = new LinkedHashMap<>();
                    for (Resource r : resources.getResources(properties.rag().corpus())) {
                        String source = r.getFilename();
                        List<Document> docs = new ArrayList<>();
                        for (MarkdownChunker.Chunk c : chunker.chunk(source, read(r))) {
                            Map<String, Object> metadata = new LinkedHashMap<>(c.metadata());
                            metadata.put("chunkIndex", c.index());
                            UUID id = UUID.nameUUIDFromBytes((source + "#" + c.index()).getBytes(StandardCharsets.UTF_8));
                            docs.add(Document.builder().id(id.toString()).text(c.text()).metadata(metadata).build());
                        }
                        bySource.put(source, docs);
                    }
                    int total = 0;
                    for (var entry : bySource.entrySet()) {
                        store.delete("source == '" + entry.getKey() + "'"); // drop chunks a shrunken doc no longer has
                        store.add(entry.getValue());
                        total += entry.getValue().size();
                    }
                    return total;
                })
                .subscribeOn(vectorScheduler);
    }

    /** Non-fatal at startup: Ollama may be down. Booking never depends on the AI path. */
    @EventListener(ApplicationReadyEvent.class)
    void ingestOnStartup() {
        if (properties.rag().ingestOnStartup()) {
            ingest().subscribe(
                    n -> log.info("RAG corpus ingested: {} chunks", n),
                    e -> log.error("RAG ingestion failed; chat continues without reference material", e));
        }
    }

    private static String read(Resource r) {
        try {
            return r.getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
