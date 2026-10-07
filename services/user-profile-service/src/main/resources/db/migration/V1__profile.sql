-- user_id is the identity provider's subject (JWT "sub"). It only ever comes from a validated
-- token, never from a request body or path.

CREATE SEQUENCE loyalty_number_seq START WITH 10000001;

CREATE TABLE profile (
    user_id              text        PRIMARY KEY,
    display_name         text,
    email                text,
    phone                text,
    preferred_fare_class text,
    seat_preference      text        NOT NULL DEFAULT 'NO_PREFERENCE'
                                     CHECK (seat_preference IN ('WINDOW', 'AISLE', 'NO_PREFERENCE')),
    email_notifications  boolean     NOT NULL DEFAULT true,
    sms_notifications    boolean     NOT NULL DEFAULT false,
    loyalty_number       text        NOT NULL DEFAULT 'TB' || nextval('loyalty_number_seq'),
    loyalty_points       integer     NOT NULL DEFAULT 0 CHECK (loyalty_points >= 0),
    version              bigint      NOT NULL DEFAULT 0,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_profile_loyalty_number UNIQUE (loyalty_number)
);

CREATE TABLE traveller (
    id              uuid        PRIMARY KEY,
    user_id         text        NOT NULL REFERENCES profile (user_id) ON DELETE CASCADE,
    full_name       text        NOT NULL,
    date_of_birth   date,
    document_type   text        CHECK (document_type IN ('PASSPORT', 'EMIRATES_ID', 'NATIONAL_ID')),
    document_number text,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ix_traveller_user ON traveller (user_id);
