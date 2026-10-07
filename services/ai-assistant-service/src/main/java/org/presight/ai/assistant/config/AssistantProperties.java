package org.presight.ai.assistant.config;

import java.time.Duration;
import java.time.ZoneId;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("assistant")
public record AssistantProperties(
        ZoneId zone,
        int maxConcurrentChats,
        int schedulerQueue,
        Duration chatTimeout,
        double temperature,
        int historyTurns,
        int maxToolCallsPerTurn,
        int maxMutatingCallsPerTurn,
        Rag rag) {

    /**
     * @param corpus resource pattern for the markdown corpus; chunks must stay under the
     *               embedding model's 512-token window (see MarkdownChunker)
     */
    public record Rag(int topK, double similarityThreshold, boolean ingestOnStartup, String corpus) {
    }
}
