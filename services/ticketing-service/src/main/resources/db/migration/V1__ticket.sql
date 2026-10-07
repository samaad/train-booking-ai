-- One ticket per booking. booking_id is UNIQUE so issuance is idempotent: the booking.confirmed
-- consumer is at-least-once, and a redelivered event must not issue a second ticket.

CREATE TABLE ticket (
    id              uuid        PRIMARY KEY,
    pnr             text        NOT NULL,
    booking_id      uuid        NOT NULL,
    status          text        NOT NULL CHECK (status IN ('ISSUED', 'VOIDED')),
    passenger_name  text        NOT NULL,
    train_number    text        NOT NULL,
    travel_date     date        NOT NULL,
    fare_class      text        NOT NULL,
    seat_no         text        NOT NULL,
    from_station    text        NOT NULL,
    to_station      text        NOT NULL,
    departure       timestamptz NOT NULL,
    qr_token        text        NOT NULL,
    pdf_object_key  text,
    issued_at       timestamptz NOT NULL DEFAULT now(),
    voided_at       timestamptz,
    CONSTRAINT uq_ticket_pnr UNIQUE (pnr),
    CONSTRAINT uq_ticket_booking UNIQUE (booking_id)
);
