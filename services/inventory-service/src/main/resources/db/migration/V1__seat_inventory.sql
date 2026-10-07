-- Seat inventory is the source of truth for holds. Redis never decides who holds a seat;
-- the conditional UPDATEs in HoldRepository are correct with zero Redis involvement.

CREATE TABLE seat_inventory (
    id              uuid        PRIMARY KEY DEFAULT uuidv7(),
    train_number    text        NOT NULL,
    travel_date     date        NOT NULL,
    fare_class      text        NOT NULL,
    seat_no         text        NOT NULL,
    status          text        NOT NULL DEFAULT 'AVAILABLE'
                                CHECK (status IN ('AVAILABLE', 'HELD', 'COMMITTED')),
    hold_id         uuid,
    hold_expires_at timestamptz,
    version         bigint      NOT NULL DEFAULT 0,
    CONSTRAINT uq_seat UNIQUE (train_number, travel_date, seat_no),
    CONSTRAINT held_seat_has_hold CHECK (status <> 'HELD' OR (hold_id IS NOT NULL AND hold_expires_at IS NOT NULL))
);

CREATE INDEX ix_seat_inventory_lookup ON seat_inventory (train_number, travel_date, fare_class, status);
CREATE UNIQUE INDEX ux_seat_inventory_hold ON seat_inventory (hold_id) WHERE hold_id IS NOT NULL;

-- One row per hold attempt that won a seat. EXPIRED is derived (ACTIVE and expires_at <= now()),
-- so no sweeper is needed for correctness: an expired seat is simply reclaimable.
CREATE TABLE seat_hold (
    id              uuid        PRIMARY KEY,
    seat_id         uuid        NOT NULL REFERENCES seat_inventory (id),
    train_number    text        NOT NULL,
    travel_date     date        NOT NULL,
    fare_class      text        NOT NULL,
    seat_no         text        NOT NULL,
    status          text        NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'COMMITTED')),
    expires_at      timestamptz NOT NULL,
    idempotency_key text        NOT NULL,
    request_hash    text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_seat_hold_idempotency_key UNIQUE (idempotency_key)
);
