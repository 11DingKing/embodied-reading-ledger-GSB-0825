-- 0001_init.sql
-- Embodied Reading Ledger: initial schema.
-- All timestamps are timestamptz and are stored/serialized as UTC RFC3339Nano.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     integer PRIMARY KEY,
    applied_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS books (
    id          uuid PRIMARY KEY,
    title       text NOT NULL,
    author      text NOT NULL,
    edition     text NOT NULL,
    isbn        text NOT NULL UNIQUE,
    total_pages integer NOT NULL CHECK (total_pages > 0),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reading_sessions (
    id          uuid PRIMARY KEY,
    book_id     uuid NOT NULL REFERENCES books (id),
    reader_name text NOT NULL,
    state       text NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN', 'ENDED')),
    last_seq    bigint NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now(),
    ended_at    timestamptz
);

-- Append-only event log. No UPDATE/DELETE ever allowed (trigger below enforces it).
CREATE TABLE IF NOT EXISTS reading_events (
    id               uuid PRIMARY KEY,
    session_id       uuid NOT NULL REFERENCES reading_sessions (id),
    seq              bigint NOT NULL,
    type             text NOT NULL CHECK (type IN (
                        'SESSION_STARTED',
                        'PAGE_REACHED',
                        'PASSAGE_REACTED',
                        'INTERRUPTED',
                        'SESSION_ENDED'
                     )),
    occurred_at      timestamptz NOT NULL,            -- client-reported, validated
    recorded_at      timestamptz NOT NULL DEFAULT now(), -- server receive time (UTC)
    page             integer CHECK (page IS NULL OR page > 0),
    passage          text,
    reaction         text,
    interrupt_reason text,
    UNIQUE (session_id, seq)
);

CREATE OR REPLACE FUNCTION reject_event_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'reading_events is append-only: UPDATE/DELETE are forbidden';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS reading_events_no_update ON reading_events;
CREATE TRIGGER reading_events_no_update
    BEFORE UPDATE OR DELETE ON reading_events
    FOR EACH ROW EXECUTE FUNCTION reject_event_mutation();

-- Idempotency ledger: first writer commits key + stored response atomically
-- with the business write; replays return the stored response verbatim
-- (plain text, not jsonb, so the bytes are preserved exactly).
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key           text PRIMARY KEY,
    request_hash  text NOT NULL,
    status_code   integer,
    response_body text,
    created_at    timestamptz NOT NULL DEFAULT now()
);
