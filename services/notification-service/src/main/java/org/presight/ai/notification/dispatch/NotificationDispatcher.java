package org.presight.ai.notification.dispatch;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.presight.ai.common.id.UuidV7;
import org.presight.ai.notification.channel.ChannelSender;
import org.presight.ai.notification.dispatch.Notification.Channel;
import org.presight.ai.notification.dispatch.Notification.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Fans one source event out to every channel the recipient has an address for. This is the entry
 * point for the booking/payment/ticket consumers (events.EventsListener).
 *
 * <p>Idempotent per (eventId, channel): the PENDING record is inserted first and the unique index
 * turns a redelivered event into a no-op, so nobody gets the same SMS twice.
 */
@Service
public class NotificationDispatcher implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(NotificationDispatcher.class);

    private final ReactiveMongoTemplate mongo;
    private final Templates templates;
    private final Map<Channel, ChannelSender> senders = new EnumMap<>(Channel.class);

    NotificationDispatcher(ReactiveMongoTemplate mongo, Templates templates, List<ChannelSender> senders) {
        this.mongo = mongo;
        this.templates = templates;
        senders.forEach(s -> {
            if (this.senders.putIfAbsent(s.channel(), s) != null) {
                throw new IllegalStateException("Two senders for channel " + s.channel());
            }
        });
    }

    /** Addresses per channel; a channel without an address is skipped. */
    public record Recipient(String email, String phone, String pushToken) {

        Map<Channel, String> addresses() {
            Map<Channel, String> out = new EnumMap<>(Channel.class);
            if (email != null) {
                out.put(Channel.EMAIL, email);
            }
            if (phone != null) {
                out.put(Channel.SMS, phone);
            }
            if (pushToken != null) {
                out.put(Channel.PUSH, pushToken);
            }
            return out;
        }
    }

    public record Dispatch(String eventId, String eventType, Recipient recipient, Map<String, String> params) {
    }

    /** Emits the settled notification for each channel; channels already handled for this event are skipped. */
    public Flux<Notification> dispatch(Dispatch d) {
        if (!templates.supports(d.eventType())) {
            log.debug("No notification for event type {}", d.eventType());
            return Flux.empty();
        }
        return Mono.fromCallable(() -> templates.render(d.eventType(), d.params()))
                .flatMapMany(message -> fanOut(d, message));
    }

    private Flux<Notification> fanOut(Dispatch d, Templates.Template message) {
        return Flux.fromIterable(d.recipient().addresses().entrySet())
                .filter(e -> senders.containsKey(e.getKey()))
                .flatMap(e -> claim(new Notification(UuidV7.generate().toString(), d.eventId(), d.eventType(),
                        e.getKey(), e.getValue(), message.subject(), message.body(), d.params(),
                        Status.PENDING, null, Instant.now(), null)))
                .flatMap(this::deliver);
    }

    private Mono<Notification> claim(Notification n) {
        return mongo.insert(n).onErrorResume(DuplicateKeyException.class, e -> Mono.empty());
    }

    private Mono<Notification> deliver(Notification n) {
        return senders.get(n.channel()).send(n)
                .then(settle(n, Status.SENT, null))
                .onErrorResume(e -> settle(n, Status.FAILED, e.getMessage()));
    }

    private Mono<Notification> settle(Notification n, Status status, String failureReason) {
        return mongo.findAndModify(
                Query.query(Criteria.where("_id").is(n.id())),
                new Update().set("status", status).set("failureReason", failureReason).set("settledAt", Instant.now()),
                FindAndModifyOptions.options().returnNew(true),
                Notification.class);
    }

    @Override
    public void run(ApplicationArguments args) {
        mongo.indexOps(Notification.class)
                .createIndex(new Index().on("eventId", Sort.Direction.ASC).on("channel", Sort.Direction.ASC).unique())
                .block();
    }
}
