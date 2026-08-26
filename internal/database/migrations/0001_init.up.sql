-- Embodied Reading Event Ledger — initial schema
-- All timestamps are TIMESTAMPTZ; the application always writes UTC RFC3339Nano.

CREATE TABLE books (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    isbn            TEXT,
    title           TEXT        NOT NULL,
    author          TEXT        NOT NULL,
    publisher       TEXT,
    published_year  INT,
    total_pages     INT,
    format          TEXT        NOT NULL DEFAULT 'PAPERBACK',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT books_format_check CHECK (format IN ('PAPERBACK','HARDCOVER','EBOOK','AUDIOBOOK','OTHER')),
    CONSTRAINT books_pages_check CHECK (total_pages IS NULL OR total_pages > 0),
    CONSTRAINT books_year_check CHECK (published_year IS NULL OR published_year > 0)
);

CREATE TABLE sessions (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    book_id     UUID        NOT NULL REFERENCES books(id) ON DELETE RESTRICT,
    label       TEXT,
    status      TEXT        NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sessions_status_check CHECK (status IN ('pending','active','ended'))
);

CREATE INDEX idx_sessions_book_id ON sessions(book_id);

-- Append-only event ledger. UPDATE/DELETE are forbidden by trigger below.
CREATE TABLE events (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id   UUID        NOT NULL REFERENCES sessions(id) ON DELETE RESTRICT,
    seq          INT         NOT NULL,
    event_type   TEXT        NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    page         INT,
    note         TEXT,
    quote        TEXT,
    reason       TEXT,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT events_session_seq_unique UNIQUE (session_id, seq),
    CONSTRAINT events_seq_positive CHECK (seq > 0),
    CONSTRAINT events_type_check CHECK (event_type IN (
        'SESSION_STARTED','PAGE_REACHED','PASSAGE_REACTED','INTERRUPTED','SESSION_ENDED'
    )),
    CONSTRAINT events_page_check CHECK (page IS NULL OR page > 0)
);

CREATE INDEX idx_events_session_seq ON events(session_id, seq);

-- Append-only enforcement: no UPDATE or DELETE may ever touch the event ledger.
CREATE OR REPLACE FUNCTION events_no_mutate() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'events table is append-only; UPDATE/DELETE is forbidden'
        USING ERRCODE = 'check_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_append_only
BEFORE UPDATE OR DELETE ON events
FOR EACH ROW EXECUTE FUNCTION events_no_mutate();

-- Idempotency store. A unique key guarantees a first response is recorded once.
CREATE TABLE idempotency_keys (
    key             TEXT        PRIMARY KEY,
    request_method  TEXT        NOT NULL,
    request_path    TEXT        NOT NULL,
    request_hash    BYTEA       NOT NULL,
    status_code     INT         NOT NULL,
    response_body   BYTEA       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
