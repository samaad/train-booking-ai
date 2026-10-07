package org.presight.ai.ticketing.storage;

import java.time.Duration;
import org.presight.ai.ticketing.TicketingProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/** Non-blocking S3 access (AWS SDK v2 async client). Rendered ticket PDFs live here. */
@Component
public class ObjectStore implements ApplicationRunner, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ObjectStore.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final S3AsyncClient s3;
    private final String bucket;

    public ObjectStore(TicketingProperties properties) {
        TicketingProperties.ObjectStore cfg = properties.objectStore();
        this.bucket = cfg.bucket();
        this.s3 = S3AsyncClient.builder()
                .endpointOverride(cfg.endpoint())
                .region(Region.of(cfg.region()))
                .forcePathStyle(cfg.pathStyle())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(cfg.accessKey(), cfg.secretKey())))
                .httpClientBuilder(NettyNioAsyncHttpClient.builder())
                .build();
    }

    /** Empty when the object does not exist. */
    public Mono<byte[]> get(String key) {
        return Mono.fromFuture(() -> s3.getObject(b -> b.bucket(bucket).key(key), AsyncResponseTransformer.toBytes()))
                .map(response -> response.asByteArray())
                .timeout(TIMEOUT)
                .onErrorResume(NoSuchKeyException.class, e -> Mono.empty());
    }

    public Mono<Void> put(String key, byte[] content, String contentType) {
        return Mono.fromFuture(() -> s3.putObject(b -> b.bucket(bucket).key(key).contentType(contentType),
                        AsyncRequestBody.fromBytes(content)))
                .timeout(TIMEOUT)
                .then();
    }

    /** Creates the bucket if needed. Non-fatal: tickets still render without the store, just uncached. */
    @Override
    public void run(ApplicationArguments args) {
        Mono.fromFuture(() -> s3.createBucket(b -> b.bucket(bucket)))
                .timeout(TIMEOUT)
                .onErrorResume(e -> e instanceof BucketAlreadyOwnedByYouException
                        || e.getCause() instanceof BucketAlreadyOwnedByYouException, e -> Mono.empty())
                .doOnError(e -> log.warn("Object store not ready, PDFs will not be cached: {}", e.toString()))
                .onErrorResume(e -> Mono.empty())
                .block();
    }

    @Override
    public void destroy() {
        s3.close();
    }
}
