package org.presight.ai.assistant.rag;

import java.util.List;
import reactor.core.publisher.Mono;

/** Reference-material retrieval for the system prompt. Our interface; Spring AI stays behind it. */
public interface RagRetriever {

    /** Never errors: on any failure it returns no context, and the turn proceeds without RAG. */
    Mono<List<String>> retrieve(String query);
}
