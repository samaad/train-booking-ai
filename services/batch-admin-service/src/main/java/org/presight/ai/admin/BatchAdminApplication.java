package org.presight.ai.admin;

import org.presight.ai.admin.dlt.DltProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Operator tooling. The one non-reactive service: admin work is a handful of blocking Kafka client calls,
 * so servlet + virtual threads is simpler and nothing here sits on a request path passengers wait on.
 * Not routed through Kong — reach it inside the network only.
 */
@SpringBootApplication
@EnableConfigurationProperties(DltProperties.class)
public class BatchAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchAdminApplication.class, args);
    }
}
