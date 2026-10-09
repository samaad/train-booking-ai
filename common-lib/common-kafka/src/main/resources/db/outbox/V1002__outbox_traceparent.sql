-- The W3C trace parent as its own column. The polling relay reads it from `headers`; Debezium's outbox
-- router (outbox.mode=cdc) can only place whole columns into Kafka headers, not keys inside a jsonb.
ALTER TABLE outbox ADD COLUMN traceparent text;
