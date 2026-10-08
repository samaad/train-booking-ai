-- Trace context (W3C traceparent) of the transaction that wrote the event. The relay publishes later, from
-- its own thread; without this the trace would end at the outbox and restart at every consumer.
ALTER TABLE outbox ADD COLUMN headers jsonb;
