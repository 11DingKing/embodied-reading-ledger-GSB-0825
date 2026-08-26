CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS books (
    id          UUID PRIMARY KEY,
    isbn        TEXT NOT NULL,
    title       TEXT NOT NULL,
    author      TEXT NOT NULL,
    edition     TEXT NOT NULL DEFAULT '',
    total_pages INTEGER NOT NULL CHECK (total_pages > 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reading_sessions (
    id         UUID PRIMARY KEY,
    book_id    UUID NOT NULL REFERENCES books (id),
    reader_tag TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'ended')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
    session_id  UUID NOT NULL REFERENCES reading_sessions (id),
    seq         BIGINT NOT NULL CHECK (seq > 0),
    event_type  TEXT NOT NULL CHECK (event_type IN (
                    'SESSION_STARTED',
                    'PAGE_REACHED',
                    'PASSAGE_REACTED',
                    'INTERRUPTED',
                    'SESSION_ENDED'
                )),
    occurred_at TIMESTAMPTZ NOT NULL,
    payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_events_session_seq ON events (session_id, seq);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key                 TEXT PRIMARY KEY,
    request_fingerprint TEXT NOT NULL,
    response_status     INTEGER,
    response_body       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION events_append_only() RETURNS trigger AS $fn$
BEGIN
    RAISE EXCEPTION 'events table is append-only: UPDATE and DELETE are forbidden';
END;
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_events_append_only ON events;
CREATE TRIGGER trg_events_append_only
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_append_only();
