-- The sweeper records lapsed holds as EXPIRED (previously only derived at read time), so it never
-- revisits them.
ALTER TABLE seat_hold DROP CONSTRAINT seat_hold_status_check;
ALTER TABLE seat_hold ADD CONSTRAINT seat_hold_status_check
    CHECK (status IN ('ACTIVE', 'RELEASED', 'COMMITTED', 'EXPIRED'));
CREATE INDEX ix_seat_hold_active_expiry ON seat_hold (expires_at) WHERE status = 'ACTIVE';
