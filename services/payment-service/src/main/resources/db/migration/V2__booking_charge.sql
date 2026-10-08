-- What each booking may be charged, learned from booking.created.v1. booking-service owns the price;
-- payment only ever authorises exactly this amount, and nothing once the booking is cancelled.
-- booking.created and booking.cancelled are different topics with no ordering between them, so a
-- cancellation can arrive first: it is stored as a CANCELLED row with no amount, and the late
-- booking.created must not reopen it.
CREATE TABLE booking_charge (
    booking_id  uuid          PRIMARY KEY,
    amount      numeric(12,2) CHECK (amount > 0),
    currency    text,
    status      text          NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'CANCELLED')),
    created_at  timestamptz   NOT NULL DEFAULT now(),
    updated_at  timestamptz   NOT NULL DEFAULT now()
);
