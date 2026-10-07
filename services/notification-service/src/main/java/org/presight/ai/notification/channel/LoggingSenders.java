package org.presight.ai.notification.channel;

import java.util.regex.Pattern;
import org.presight.ai.notification.dispatch.Notification;
import org.presight.ai.notification.dispatch.Notification.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

/**
 * Development senders: validate the address, then log the delivery with the recipient masked.
 * Replace a bean per channel with a real provider (SES, an SMS gateway, FCM) without touching dispatch.
 */
@Configuration(proxyBeanMethods = false)
class LoggingSenders {

    private static final Logger log = LoggerFactory.getLogger(LoggingSenders.class);

    @Bean
    ChannelSender emailSender() {
        return sender(Channel.EMAIL, Pattern.compile("[^@\\s]+@[^@\\s]+\\.[^@\\s]+"));
    }

    @Bean
    ChannelSender smsSender() {
        return sender(Channel.SMS, Pattern.compile("\\+[1-9][0-9]{7,14}")); // E.164
    }

    @Bean
    ChannelSender pushSender() {
        return sender(Channel.PUSH, Pattern.compile("[A-Za-z0-9_:\\-]{16,512}"));
    }

    private static ChannelSender sender(Channel channel, Pattern validAddress) {
        return new ChannelSender() {
            @Override
            public Channel channel() {
                return channel;
            }

            @Override
            public Mono<Void> send(Notification n) {
                if (!validAddress.matcher(n.recipient()).matches()) {
                    return Mono.error(new IllegalArgumentException("Invalid " + channel + " address"));
                }
                return Mono.fromRunnable(() -> log.info("[{}] to {} — {}", channel, mask(n.recipient()), n.subject()));
            }
        };
    }

    private static String mask(String address) {
        return address.length() <= 4 ? "****" : address.substring(0, 2) + "****" + address.substring(address.length() - 2);
    }
}
