DROP TABLE IF EXISTS idempotency_keys;
DROP TRIGGER IF EXISTS events_append_only ON events;
DROP FUNCTION IF EXISTS events_no_mutate();
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF EXISTS books;
