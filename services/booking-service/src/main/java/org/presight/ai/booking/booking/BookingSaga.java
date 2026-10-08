package org.presight.ai.booking.booking;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.presight.ai.booking.booking.Booking.SagaState;
import org.presight.ai.booking.booking.Booking.Status;
import org.presight.ai.booking.client.InventoryClient;
import org.presight.ai.common.error.DomainException;
import org.presight.ai.common.error.ErrorCategory;
import org.presight.ai.common.id.UuidV7;
import org.presight.ai.common.kafka.TopicDeclarations;
import org.presight.ai.common.kafka.Topics;
import org.presight.ai.common.kafka.consumer.IdempotentHandler;
import org.presight.ai.common.kafka.events.BookingEvents.BookingConfirmed;
import org.presight.ai.common.kafka.events.BookingEvents.CancelReason;
import org.presight.ai.common.kafka.events.OtherEvents.TicketIssued;
import org.presight.ai.common.kafka.events.PaymentEvents.PaymentAuthorized;
import org.presight.ai.common.kafka.events.PaymentEvents.PaymentFailed;
import org.presight.ai.common.kafka.outbox.Outbox;
import org.presight.ai.common.web.RemoteCallErrors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * booking-service's side of the choreographed saga (03-consistency):
 *
 * <pre>
 * PAYMENT_PENDING ──payment.authorized──▶ commit seat ──▶ SEATS_COMMITTED (booking.confirmed)
 *        │                                     └─ hold expired ──▶ CANCELLED (booking.cancelled → refund)
 *        └──────payment.failed───────────────────────────────▶ CANCELLED (booking.cancelled → release)
 * SEATS_COMMITTED ──ticket.issued──▶ TICKETED
 * </pre>
 *
 * Every step is a conditional transition from an expected state, so a stale or out-of-order event (the user
 * cancelled first, a redelivery) changes nothing. Each step's state change and outgoing event commit together,
 * together with the processed_events row that makes redelivery a no-op.
 */
@Component
class BookingSaga {

    private static final Logger log = LoggerFactory.getLogger(BookingSaga.class);
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(5);

    private final BookingRepository bookings;
    private final InventoryClient inventory;
    private final Outbox outbox;
    private final IdempotentHandler handler;

    BookingSaga(BookingRepository bookings, InventoryClient inventory, Outbox outbox, IdempotentHandler handler) {
        this.bookings = bookings;
        this.inventory = inventory;
        this.outbox = outbox;
        this.handler = handler;
    }

    @KafkaListener(topics = Topics.PAYMENT_AUTHORIZED)
    void onPaymentAuthorized(ConsumerRecord<String, String> record) {
        handler.handle(record, PaymentAuthorized.class, this::paymentAuthorized);
    }

    @KafkaListener(topics = Topics.PAYMENT_FAILED)
    void onPaymentFailed(ConsumerRecord<String, String> record) {
        handler.handle(record, PaymentFailed.class, e -> bookings.findById(e.bookingId())
                .flatMap(b -> cancel(b, SagaState.PAYMENT_PENDING, CancelReason.PAYMENT_FAILED, "Payment failed: " + e.reason())));
    }

    @KafkaListener(topics = Topics.TICKET_ISSUED)
    void onTicketIssued(ConsumerRecord<String, String> record) {
        handler.handle(record, TicketIssued.class, e ->
                bookings.advance(e.bookingId(), SagaState.SEATS_COMMITTED, SagaState.TICKETED, Status.CONFIRMED, null));
    }

    private Mono<?> paymentAuthorized(PaymentAuthorized e) {
        return bookings.findById(e.bookingId()).flatMap(b -> {
            if (b.sagaState() != SagaState.PAYMENT_PENDING) {
                // e.g. the user cancelled first: payment refunds from booking.cancelled, nothing to do here.
                log.info("payment.authorized for booking {} in {} ignored", b.id(), b.sagaState());
                return Mono.empty();
            }
            if (e.amount().compareTo(b.amount()) != 0 || !e.currency().equals(b.currency())) {
                // The price is booking's; any other amount must never confirm a seat.
                return cancel(b, SagaState.PAYMENT_PENDING, CancelReason.AMOUNT_MISMATCH,
                        "Authorised " + e.amount() + " " + e.currency() + ", booking is " + b.amount() + " " + b.currency());
            }
            return inventory.commit(b.holdId())
                    .timeout(COMMIT_TIMEOUT)
                    .onErrorMap(RemoteCallErrors.from("inventory"))
                    .then(bookings.advance(b.id(), SagaState.PAYMENT_PENDING, SagaState.SEATS_COMMITTED, Status.CONFIRMED, null)
                            .flatMap(rows -> rows == 0 ? Mono.empty() : outbox.append(Topics.BOOKING_CONFIRMED, confirmed(b))))
                    // The hold lapsed before payment arrived: compensate. Outages propagate and are retried.
                    .onErrorResume(err -> err instanceof DomainException d && d.category() == ErrorCategory.CONFLICT,
                            err -> cancel(b, SagaState.PAYMENT_PENDING, CancelReason.HOLD_EXPIRED, err.getMessage()));
        });
    }

    private Mono<Void> cancel(Booking b, SagaState from, CancelReason reason, String detail) {
        return bookings.advance(b.id(), from, SagaState.CANCELLED, Status.CANCELLED, reason.name())
                .flatMap(rows -> rows == 0
                        ? Mono.<Void>empty()
                        : outbox.append(Topics.BOOKING_CANCELLED, BookingService.cancelled(b, reason, detail)));
    }

    private static BookingConfirmed confirmed(Booking b) {
        return new BookingConfirmed(UuidV7.generate(), b.id(), b.pnr(), b.holdId(), b.trainNumber(), b.travelDate(),
                b.fareClass(), b.seatNo(), b.fromStation(), b.toStation(), b.departure(), b.passengerName(), Instant.now());
    }

    @Configuration(proxyBeanMethods = false)
    static class TopicsConfig {
        @Bean
        KafkaAdmin.NewTopics bookingTopics(@Value("${spring.kafka.consumer.group-id}") String group,
                                           @Value("${kafka.replicas:1}") int replicas) {
            return TopicDeclarations.of(
                    List.of(Topics.BOOKING_CREATED, Topics.BOOKING_CONFIRMED, Topics.BOOKING_CANCELLED),
                    group,
                    List.of(Topics.PAYMENT_AUTHORIZED, Topics.PAYMENT_FAILED, Topics.TICKET_ISSUED),
                    replicas);
        }
    }
}
