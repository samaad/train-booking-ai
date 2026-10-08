-- Phase 4: a booking belongs to the user (JWT subject) who made it. Nullable for bookings made before.
ALTER TABLE booking ADD COLUMN user_id text;
CREATE INDEX ix_booking_user ON booking (user_id);
