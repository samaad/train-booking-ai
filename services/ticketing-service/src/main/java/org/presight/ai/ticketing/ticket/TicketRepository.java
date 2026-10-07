package org.presight.ai.ticketing.ticket;

import io.r2dbc.spi.Readable;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.presight.ai.ticketing.ticket.Ticket.Status;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
class TicketRepository {

    private final DatabaseClient db;

    TicketRepository(DatabaseClient db) {
        this.db = db;
    }

    /** Inserts unless the booking already has a ticket; either way returns the booking's ticket. */
    Mono<Ticket> insertIfAbsent(Ticket t) {
        return db.sql("""
                        INSERT INTO ticket (id, pnr, booking_id, status, passenger_name, train_number, travel_date,
                                            fare_class, seat_no, from_station, to_station, departure, qr_token)
                        VALUES (:id, :pnr, :bookingId, 'ISSUED', :passenger, :train, :date,
                                :fareClass, :seatNo, :from, :to, :departure, :qr)
                        ON CONFLICT ON CONSTRAINT uq_ticket_booking DO NOTHING
                        """)
                .bind("id", t.id())
                .bind("pnr", t.pnr())
                .bind("bookingId", t.bookingId())
                .bind("passenger", t.passengerName())
                .bind("train", t.trainNumber())
                .bind("date", t.travelDate())
                .bind("fareClass", t.fareClass())
                .bind("seatNo", t.seatNo())
                .bind("from", t.fromStation())
                .bind("to", t.toStation())
                .bind("departure", t.departure())
                .bind("qr", t.qrToken())
                .then()
                .then(findByBookingId(t.bookingId()));
    }

    Mono<Ticket> findByPnr(String pnr) {
        return db.sql("SELECT * FROM ticket WHERE pnr = :pnr").bind("pnr", pnr).map(TicketRepository::ticket).one();
    }

    Mono<Ticket> findByBookingId(UUID bookingId) {
        return db.sql("SELECT * FROM ticket WHERE booking_id = :id").bind("id", bookingId).map(TicketRepository::ticket).one();
    }

    Mono<Long> voidByBookingId(UUID bookingId) {
        return db.sql("UPDATE ticket SET status = 'VOIDED', voided_at = now() WHERE booking_id = :id AND status = 'ISSUED'")
                .bind("id", bookingId)
                .fetch()
                .rowsUpdated();
    }

    Mono<Long> recordPdf(UUID id, String objectKey) {
        return db.sql("UPDATE ticket SET pdf_object_key = :key WHERE id = :id")
                .bind("id", id)
                .bind("key", objectKey)
                .fetch()
                .rowsUpdated();
    }

    private static Ticket ticket(Readable row) {
        OffsetDateTime voided = row.get("voided_at", OffsetDateTime.class);
        return new Ticket(
                row.get("id", UUID.class),
                row.get("pnr", String.class),
                row.get("booking_id", UUID.class),
                Status.valueOf(row.get("status", String.class)),
                row.get("passenger_name", String.class),
                row.get("train_number", String.class),
                row.get("travel_date", LocalDate.class),
                row.get("fare_class", String.class),
                row.get("seat_no", String.class),
                row.get("from_station", String.class),
                row.get("to_station", String.class),
                row.get("departure", OffsetDateTime.class),
                row.get("qr_token", String.class),
                row.get("pdf_object_key", String.class),
                row.get("issued_at", OffsetDateTime.class).toInstant(),
                voided == null ? null : voided.toInstant());
    }
}
