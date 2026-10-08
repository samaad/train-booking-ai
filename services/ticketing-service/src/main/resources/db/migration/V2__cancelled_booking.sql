-- booking.confirmed and booking.cancelled are different topics with no ordering between them. A
-- cancellation processed first leaves this tombstone, so the late confirmation issues no ticket.
CREATE TABLE cancelled_booking (
    booking_id   uuid        PRIMARY KEY,
    cancelled_at timestamptz NOT NULL DEFAULT now()
);
