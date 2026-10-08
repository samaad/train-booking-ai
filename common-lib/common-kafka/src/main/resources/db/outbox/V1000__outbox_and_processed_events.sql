-- Shared by every Postgres service (Flyway location classpath:db/outbox). The business row and its
-- outbox row are written in ONE transaction; OutboxRelay publishes afterwards. A crash between a
-- database write and a Kafka send can therefore never lose an event — it can only delay it.

CREATE TABLE outbox (
    id             uuid        PRIMARY KEY,           -- the event id consumers deduplicate on
    seq            bigserial   NOT NULL,              -- publish order (per service)
    topic          text        NOT NULL,
    message_key    text        NOT NULL,
    payload        jsonb       NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz,
    attempts       integer     NOT NULL DEFAULT 0,
    last_error     text
);

CREATE INDEX ix_outbox_unpublished ON outbox (seq) WHERE published_at IS NULL;

-- Delivery is at-least-once, so every consumer is idempotent: the effect of an event and this row are
-- committed together, and a redelivered event finds its row already present.
CREATE TABLE processed_events (
    consumer_group text        NOT NULL,
    event_id       uuid        NOT NULL,
    topic          text        NOT NULL,
    processed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
);
