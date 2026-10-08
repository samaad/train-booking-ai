package org.presight.ai.inventory.hold;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.presight.ai.common.kafka.TopicDeclarations;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.consumer.IdempotentHandler;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCancelled;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/**
 * Saga compensation: a cancelled booking (by the user, a failed payment or an expired hold) frees its seat.
 * booking.cancelled is the single release path; inventory does not also react to payment.failed, because
 * booking turns every payment failure into a cancellation — two paths would only race.
 */
@Component
class BookingCancelledListener {

    private final HoldService holds;
    private final IdempotentHandler handler;

    BookingCancelledListener(HoldService holds, IdempotentHandler handler) {
        this.holds = holds;
        this.handler = handler;
    }

    @KafkaListener(topics = Topics.BOOKING_CANCELLED)
    void onBookingCancelled(ConsumerRecord<String, String> record) {
        handler.handle(record, BookingCancelled.class, e -> holds.releaseForCancelledBooking(e.holdId()));
    }

    @Configuration(proxyBeanMethods = false)
    static class TopicsConfig {
        @Bean
        KafkaAdmin.NewTopics inventoryTopics(@Value("${spring.kafka.consumer.group-id}") String group,
                                             @Value("${kafka.replicas:1}") int replicas) {
            return TopicDeclarations.of(List.of(Topics.SEAT_HELD, Topics.SEAT_RELEASED, Topics.SEAT_COMMITTED),
                    group, List.of(Topics.BOOKING_CANCELLED), replicas);
        }
    }
}
