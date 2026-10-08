-- Phase 4: only the booking's owner (from booking.created.v1) may pay for it.
ALTER TABLE booking_charge ADD COLUMN user_id text;
