package org.presight.ai.payment.payment;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.presight.ai.common.kafka.TopicDeclarations;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.consumer.IdempotentHandler;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCancelled;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCreated;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/** payment-service's side of the saga: learn what a booking costs; refund when it is cancelled. */
@Component
class BookingEventsListener {

    private final PaymentService payments;
    private final IdempotentHandler handler;

    BookingEventsListener(PaymentService payments, IdempotentHandler handler) {
        this.payments = payments;
        this.handler = handler;
    }

    @KafkaListener(topics = Topics.BOOKING_CREATED)
    void onBookingCreated(ConsumerRecord<String, String> record) {
        handler.handle(record, BookingCreated.class, e -> payments.bookingCreated(e.bookingId(), e.userId(), e.amount(), e.currency()));
    }

    @KafkaListener(topics = Topics.BOOKING_CANCELLED)
    void onBookingCancelled(ConsumerRecord<String, String> record) {
        handler.handle(record, BookingCancelled.class, e -> payments.bookingCancelled(e.bookingId()));
    }

    @Configuration(proxyBeanMethods = false)
    static class TopicsConfig {
        @Bean
        KafkaAdmin.NewTopics paymentTopics(@Value("${spring.kafka.consumer.group-id}") String group,
                                           @Value("${kafka.replicas:1}") int replicas) {
            return TopicDeclarations.of(List.of(Topics.PAYMENT_AUTHORIZED, Topics.PAYMENT_FAILED, Topics.PAYMENT_REFUNDED),
                    group, List.of(Topics.BOOKING_CREATED, Topics.BOOKING_CANCELLED), replicas);
        }
    }
}
