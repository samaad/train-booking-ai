package org.presight.ai.notification.channel;

import org.presight.ai.notification.dispatch.Notification;
import org.presight.ai.notification.dispatch.Notification.Channel;
import reactor.core.publisher.Mono;

/** A delivery provider for one channel. An error signal marks that delivery FAILED; other channels are unaffected. */
public interface ChannelSender {

    Channel channel();

    Mono<Void> send(Notification notification);
}
