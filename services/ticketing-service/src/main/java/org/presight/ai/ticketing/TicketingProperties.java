package org.presight.ai.ticketing;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("ticketing")
public record TicketingProperties(ZoneId zone, String qrSecret, ObjectStore objectStore) {

    public TicketingProperties {
        if (qrSecret == null || qrSecret.getBytes(StandardCharsets.UTF_8).length < 32) {
            throw new IllegalArgumentException("ticketing.qr-secret must be at least 32 bytes");
        }
    }

    /** Any S3-compatible store: AWS S3 in production, adobe/s3mock locally. */
    public record ObjectStore(URI endpoint, String region, String bucket, String accessKey, String secretKey,
                              boolean pathStyle) {
    }
}
