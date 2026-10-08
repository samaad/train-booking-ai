package org.presight.ai.ticketing.ticket;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.presight.ai.common.kafka.TopicDeclarations;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.consumer.IdempotentHandler;
import org.presight.ai.common.kafka.events.BookingEvents.BookingCancelled;
import org.presight.ai.common.kafka.events.BookingEvents.BookingConfirmed;
import org.presight.ai.ticketing.ticket.TicketService.IssueTicket;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/**
 * Issue on {@code booking.confirmed}, void on {@code booking.cancelled}. A failing issuance is retried three
 * times by the container's error handler and then dead-lettered for manual follow-up.
 */
@Component
class BookingEventsListener {

    private final TicketService tickets;
    private final IdempotentHandler handler;
    private final Counter voids;

    BookingEventsListener(TicketService tickets, IdempotentHandler handler, MeterRegistry meters) {
        this.tickets = tickets;
        this.handler = handler;
        this.voids = meters.counter("saga.compensation", "type", "ticket_void");
    }

    @KafkaListener(topics = Topics.BOOKING_CONFIRMED)
    void onBookingConfirmed(ConsumerRecord<String, String> record) {
        handler.handle(record, BookingConfirmed.class, e -> tickets.issue(new IssueTicket(e.bookingId(), e.userId(), e.pnr(),
                e.passengerName(), e.trainNumber(), e.travelDate(), e.fareClass(), e.seatNo(), e.fromStation(),
                e.toStation(), e.departure())));
    }

    @KafkaListener(topics = Topics.BOOKING_CANCELLED)
    void onBookingCancelled(ConsumerRecord<String, String> record) {
        handler.handle(record, BookingCancelled.class, e -> tickets.voidTicket(e.bookingId()).doOnSuccess(v -> voids.increment()));
    }

    @Configuration(proxyBeanMethods = false)
    static class TopicsConfig {
        @Bean
        KafkaAdmin.NewTopics ticketingTopics(@Value("${spring.kafka.consumer.group-id}") String group,
                                             @Value("${kafka.replicas:1}") int replicas) {
            return TopicDeclarations.of(List.of(Topics.TICKET_ISSUED, Topics.TICKET_VOIDED),
                    group, List.of(Topics.BOOKING_CONFIRMED, Topics.BOOKING_CANCELLED), replicas);
        }
    }
}
