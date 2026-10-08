-- Phase 4: a hold belongs to the user (JWT subject) who placed it. Nullable for holds created before.
ALTER TABLE seat_hold ADD COLUMN user_id text;
