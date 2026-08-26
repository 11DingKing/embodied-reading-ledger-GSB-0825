INSERT INTO books (id, isbn, title, author, edition, total_pages, created_at) VALUES
    ('00000000-0000-4000-8000-000000000001',
     '978-7-111-00000-1',
     '纸上的钟',
     '林晚',
     '2024年第1版',
     320,
     '2026-08-01T08:00:00Z')
ON CONFLICT (id) DO NOTHING;

INSERT INTO reading_sessions (id, book_id, reader_tag, status, created_at) VALUES
    ('00000000-0000-4000-8000-000000000002',
     '00000000-0000-4000-8000-000000000001',
     'alice',
     'ended',
     '2026-08-20T08:59:00Z'),
    ('00000000-0000-4000-8000-000000000003',
     '00000000-0000-4000-8000-000000000001',
     'alice',
     'open',
     '2026-08-24T13:00:00Z')
ON CONFLICT (id) DO NOTHING;

INSERT INTO events (session_id, seq, event_type, occurred_at, payload) VALUES
    ('00000000-0000-4000-8000-000000000002', 1, 'SESSION_STARTED',  '2026-08-20T09:00:00Z',  '{}'::jsonb),
    ('00000000-0000-4000-8000-000000000002', 2, 'PAGE_REACHED',     '2026-08-20T09:05:00Z',  '{"page": 18}'::jsonb),
    ('00000000-0000-4000-8000-000000000002', 3, 'PASSAGE_REACTED',  '2026-08-20T09:12:30Z',  '{"page": 22, "quote": "钟摆每一次回落，都像一次重读。", "note": "这句话让我停下来，想起昨晚没读完的那一章。"}'::jsonb),
    ('00000000-0000-4000-8000-000000000002', 4, 'INTERRUPTED',      '2026-08-20T09:20:00Z',  '{"reason": "快递敲门"}'::jsonb),
    ('00000000-0000-4000-8000-000000000002', 5, 'PAGE_REACHED',     '2026-08-20T09:35:00Z',  '{"page": 26}'::jsonb),
    ('00000000-0000-4000-8000-000000000002', 6, 'INTERRUPTED',      '2026-08-20T09:48:00Z',  '{"reason": "电话响了"}'::jsonb),
    ('00000000-0000-4000-8000-000000000002', 7, 'PAGE_REACHED',     '2026-08-20T09:55:00Z',  '{"page": 30}'::jsonb),
    ('00000000-0000-4000-8000-000000000002', 8, 'SESSION_ENDED',    '2026-08-20T10:10:00Z',  '{}'::jsonb),
    ('00000000-0000-4000-8000-000000000003', 1, 'SESSION_STARTED',  '2026-08-24T13:10:00Z',  '{}'::jsonb),
    ('00000000-0000-4000-8000-000000000003', 2, 'PAGE_REACHED',     '2026-08-24T13:25:00Z',  '{"page": 41}'::jsonb)
ON CONFLICT (session_id, seq) DO NOTHING;
