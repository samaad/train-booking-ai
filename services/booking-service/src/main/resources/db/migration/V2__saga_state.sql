-- 03-consistency: carry the saga state explicitly from day one, so moving from choreography to an
-- orchestrator later is mechanical. `status` stays the customer-facing view.
ALTER TABLE booking
    ADD COLUMN saga_state text NOT NULL DEFAULT 'PAYMENT_PENDING'
        CHECK (saga_state IN ('PAYMENT_PENDING', 'SEATS_COMMITTED', 'TICKETED', 'CANCELLED'));

UPDATE booking SET saga_state = 'CANCELLED' WHERE status = 'CANCELLED';
