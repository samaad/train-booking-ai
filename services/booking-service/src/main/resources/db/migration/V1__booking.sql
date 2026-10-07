-- The booking aggregate. status is the saga state; transitions are conditional UPDATEs guarded
-- by version (optimistic concurrency), so a booking can be long-lived without holding locks.

CREATE TABLE booking (
    id              uuid          PRIMARY KEY,
    pnr             text          NOT NULL,
    hold_id         uuid          NOT NULL,
    status          text          NOT NULL CHECK (status IN ('PENDING_PAYMENT', 'CONFIRMED', 'CANCELLED')),
    train_number    text          NOT NULL,
    travel_date     date          NOT NULL,
    fare_class      text          NOT NULL,
    seat_no         text          NOT NULL,
    from_station    text          NOT NULL,
    to_station      text          NOT NULL,
    departure       timestamptz   NOT NULL,
    arrival         timestamptz   NOT NULL,
    passenger_name  text          NOT NULL,
    contact_email   text          NOT NULL,
    amount          numeric(12,2) NOT NULL CHECK (amount >= 0),
    currency        text          NOT NULL CHECK (char_length(currency) = 3),
    cancel_reason   text,
    idempotency_key text          NOT NULL,
    request_hash    text          NOT NULL,
    version         bigint        NOT NULL DEFAULT 0,
    created_at      timestamptz   NOT NULL DEFAULT now(),
    updated_at      timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT uq_booking_pnr UNIQUE (pnr),
    -- A hold is single-use: at most one booking can ever be made from it.
    CONSTRAINT uq_booking_hold UNIQUE (hold_id),
    CONSTRAINT uq_booking_idempotency_key UNIQUE (idempotency_key)
);
