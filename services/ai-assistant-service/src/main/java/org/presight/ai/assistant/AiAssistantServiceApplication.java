package org.presight.ai.assistant;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class AiAssistantServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AiAssistantServiceApplication.class, args);
    }
}
