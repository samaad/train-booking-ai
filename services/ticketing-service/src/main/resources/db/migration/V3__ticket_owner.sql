-- Phase 4: a ticket is visible to its booking's owner (from booking.confirmed.v1) and staff only.
ALTER TABLE ticket ADD COLUMN user_id text;
