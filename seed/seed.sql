-- Deterministic seed fixtures (fixed UUIDs and fixed UTC timestamps).
-- Apply after the server has run migrations once:
--   docker compose exec -T db psql -U postgres -d reading_ledger < seed/seed.sql
-- Or inside the container: psql -U postgres -d reading_ledger -f /seed/seed.sql

BEGIN;

INSERT INTO books (id, title, author, edition, isbn, total_pages, created_at) VALUES
  ('11111111-1111-1111-1111-111111111111', 'The Peregrine', 'J. A. Baker',
   'NYRB Classics 2005', '978-1590171332', 192, '2026-01-01T00:00:00Z'),
  ('22222222-2222-2222-2222-222222222222', 'The Rings of Saturn', 'W. G. Sebald',
   'New Directions 1998', '978-0811214131', 296, '2026-01-01T00:00:00Z')
ON CONFLICT (id) DO NOTHING;

INSERT INTO reading_sessions (id, book_id, reader_name, state, last_seq, created_at, ended_at) VALUES
  ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '11111111-1111-1111-1111-111111111111',
   'Ada', 'ENDED', 5, '2026-02-01T18:00:00Z', '2026-02-01T19:05:00Z')
ON CONFLICT (id) DO NOTHING;

INSERT INTO reading_events (id, session_id, seq, type, occurred_at, recorded_at, page, passage, reaction, interrupt_reason) VALUES
  ('e0000001-0000-0000-0000-000000000001', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 1,
   'SESSION_STARTED', '2026-02-01T18:00:00Z', '2026-02-01T18:00:00Z', NULL, NULL, NULL, NULL),
  ('e0000002-0000-0000-0000-000000000002', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 2,
   'PAGE_REACHED', '2026-02-01T18:20:00Z', '2026-02-01T18:20:00Z', 17, NULL, NULL, NULL),
  ('e0000003-0000-0000-0000-000000000003', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 3,
   'PASSAGE_REACTED', '2026-02-01T18:35:00Z', '2026-02-01T18:35:00Z', 41,
   'The hawk''s flight over the coastal fog',
   'I slowed down here; the prose feels like wind.', NULL),
  ('e0000004-0000-0000-0000-000000000004', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 4,
   'INTERRUPTED', '2026-02-01T18:50:00Z', '2026-02-01T18:50:00Z', NULL, NULL, NULL,
   'phone call'),
  ('e0000005-0000-0000-0000-000000000005', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 5,
   'SESSION_ENDED', '2026-02-01T19:05:00Z', '2026-02-01T19:05:00Z', 58, NULL, NULL, NULL)
ON CONFLICT (id) DO NOTHING;

COMMIT;
