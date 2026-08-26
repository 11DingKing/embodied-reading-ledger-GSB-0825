-- 0001_init.sql
-- Embodied Reading Event Ledger — initial schema.
--
-- Design invariants enforced at the database layer (not in application code):
--   * events is APPEND-ONLY: a trigger rejects UPDATE and DELETE.
--   * per-session event ordering is guaranteed by UNIQUE (session_id, seq);
--     concurrent appends racing for the same seq resolve to exactly one winner.
--   * idempotency is a first-class table; a UNIQUE key row is the concurrency
--     token that serializes same-key writers via row locks.
--
-- All wall-clock instants are stored as TEXT holding a canonical UTC
-- RFC3339Nano string. TEXT (not timestamptz) is used deliberately so that
-- nanosecond precision round-trips exactly; timestamptz only keeps microseconds.

CREATE TABLE IF NOT EXISTS books (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    isbn            TEXT NOT NULL,
    title           TEXT NOT NULL,
    author          TEXT NOT NULL DEFAULT '',
    edition         TEXT NOT NULL DEFAULT '',
    publisher       TEXT NOT NULL DEFAULT '',
    published_year  INTEGER,
    page_count      INTEGER,
    created_at      TEXT NOT NULL          -- canonical UTC RFC3339Nano
);

CREATE TABLE IF NOT EXISTS sessions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    book_id     UUID NOT NULL REFERENCES books(id),
    reader      TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'open',  -- open | ended (projection cache only)
    created_at  TEXT NOT NULL                  -- canonical UTC RFC3339Nano
);

-- Append-only ledger. seq is 1-based and dense per session.
CREATE TABLE IF NOT EXISTS events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id      UUID NOT NULL REFERENCES sessions(id),
    seq             INTEGER NOT NULL CHECK (seq >= 1),
    type            TEXT NOT NULL CHECK (type IN (
                        'SESSION_STARTED',
                        'PAGE_REACHED',
                        'PASSAGE_REACTED',
                        'INTERRUPTED',
                        'SESSION_ENDED')),
    occurred_at     TEXT NOT NULL,   -- client instant, canonical UTC RFC3339Nano
    recorded_at     TEXT NOT NULL,   -- server instant, canonical UTC RFC3339Nano
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT events_session_seq_uniq UNIQUE (session_id, seq)
);

CREATE INDEX IF NOT EXISTS events_session_seq_idx ON events (session_id, seq);

-- Idempotency ledger. One row per Idempotency-Key. The UNIQUE PK plus row
-- locking (INSERT ... ON CONFLICT DO UPDATE) serializes concurrent same-key
-- writers so a duplicate request replays the first stored response.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key             TEXT PRIMARY KEY,
    method          TEXT NOT NULL,
    path            TEXT NOT NULL,
    request_hash    TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | completed
    response_status INTEGER,
    response_body   TEXT,
    created_at      TEXT NOT NULL
);

-- Enforce append-only semantics for the events table at the DB level.
CREATE OR REPLACE FUNCTION events_block_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'events is append-only: % is not permitted', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS events_no_update ON events;
CREATE TRIGGER events_no_update
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_block_mutation();
