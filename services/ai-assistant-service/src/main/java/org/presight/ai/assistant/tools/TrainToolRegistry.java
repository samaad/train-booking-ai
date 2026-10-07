package org.presight.ai.assistant.tools;

import java.util.List;
import org.springframework.ai.tool.ToolCallback;

/**
 * The tool surface offered to the model. Our interface, so a Spring AI API change touches the
 * implementation only.
 */
public interface TrainToolRegistry {

    List<ToolCallback> callbacks();
}
