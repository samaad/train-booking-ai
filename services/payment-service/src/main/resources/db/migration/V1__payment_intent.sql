-- A payment intent is written PENDING *before* the PSP is called, so the idempotency key and the
-- one-live-payment-per-booking rule are claimed first. A PENDING row means "outcome unknown":
-- it is re-driven with the same PSP idempotency key, never assumed failed.

CREATE TABLE payment_intent (
    id              uuid          PRIMARY KEY,
    booking_id      uuid          NOT NULL,
    amount          numeric(12,2) NOT NULL CHECK (amount > 0),
    currency        text          NOT NULL CHECK (char_length(currency) = 3),
    status          text          NOT NULL CHECK (status IN ('PENDING', 'AUTHORIZED', 'FAILED', 'REFUNDED')),
    payment_token   text          NOT NULL,
    psp_reference   text,
    failure_reason  text,
    idempotency_key text          NOT NULL,
    request_hash    text          NOT NULL,
    created_at      timestamptz   NOT NULL DEFAULT now(),
    updated_at      timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT uq_payment_idempotency_key UNIQUE (idempotency_key)
);

-- At most one non-failed payment per booking: a declined attempt may be retried with a new
-- intent, but a booking can never hold two live authorisations (no double charge).
CREATE UNIQUE INDEX ux_payment_live_booking ON payment_intent (booking_id) WHERE status <> 'FAILED';
