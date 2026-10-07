package org.presight.ai.ticketing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class TicketingServiceApplication {

    public static void main(String[] args) {
        System.setProperty("java.awt.headless", "true"); // QR/PDF rendering uses java.awt imaging
        SpringApplication.run(TicketingServiceApplication.class, args);
    }
}
