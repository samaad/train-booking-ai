package org.presight.ai.admin.dlt;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param replayGroup consumer group whose committed offsets mark how far bulk replay has got per DLT
 * @param maxReplays  replays allowed per message; past it the message is skipped and left for a code fix
 */
@ConfigurationProperties("dlt")
public record DltProperties(String replayGroup, int maxReplays) {
}
