-- Deterministic seed fixture.
-- All UUIDs and timestamps are fixed so the result is reproducible.
-- Safe to re-run (ON CONFLICT DO NOTHING); events are append-only.

INSERT INTO books (id, isbn, title, author, publisher, published_year, total_pages, format)
VALUES (
    'a0000000-0000-0000-0000-000000000001',
    '9780241292556',
    'The Order of Time',
    'Carlo Rovelli',
    'Penguin',
    2018,
    256,
    'PAPERBACK'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO sessions (id, book_id, label, status)
VALUES (
    'b0000000-0000-0000-0000-000000000001',
    'a0000000-0000-0000-0000-000000000001',
    'Morning reading — Chapter 1',
    'ended'
)
ON CONFLICT (id) DO NOTHING;

-- Timeline (UTC):
-- 09:00:00 STARTED
-- 09:10:00 PAGE_REACHED p20   (+10m reading)
-- 09:20:00 PASSAGE_REACTED p34 (+10m reading)
-- 09:25:00 INTERRUPTED         (+5m reading, then interrupted)
-- 09:55:00 PAGE_REACHED p40    (30m interruption gap, NOT counted)
-- 10:05:00 SESSION_ENDED       (+10m reading)
-- Total reading = 35 minutes, lastPage = 40, interruptions = 1.

INSERT INTO events (id, session_id, seq, event_type, occurred_at, page, note, quote, reason)
VALUES
('c0000000-0000-0000-0000-000000000001', 'b0000000-0000-0000-0000-000000000001', 1, 'SESSION_STARTED',  '2026-01-15T09:00:00Z', NULL, NULL, NULL, NULL),
('c0000000-0000-0000-0000-000000000002', 'b0000000-0000-0000-0000-000000000001', 2, 'PAGE_REACHED',     '2026-01-15T09:10:00Z', 20,   NULL, NULL, NULL),
('c0000000-0000-0000-0000-000000000003', 'b0000000-0000-0000-0000-000000000001', 3, 'PASSAGE_REACTED',  '2026-01-15T09:20:00Z', 34,   'Time as the fourth dimension feels less literal than I expected.', 'The distinction between past and future is a curious feature of the universe.', NULL),
('c0000000-0000-0000-0000-000000000004', 'b0000000-0000-0000-0000-000000000001', 4, 'INTERRUPTED',      '2026-01-15T09:25:00Z', NULL, NULL, NULL, 'Phone rang'),
('c0000000-0000-0000-0000-000000000005', 'b0000000-0000-0000-0000-000000000001', 5, 'PAGE_REACHED',     '2026-01-15T09:55:00Z', 40,   NULL, NULL, NULL),
('c0000000-0000-0000-0000-000000000006', 'b0000000-0000-0000-0000-000000000001', 6, 'SESSION_ENDED',    '2026-01-15T10:05:00Z', NULL, NULL, NULL, NULL)
ON CONFLICT (id) DO NOTHING;
