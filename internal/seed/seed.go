// Package seed provides a deterministic fixture: the same book, session, and
// event stream every run, with fixed UUIDs and fixed UTC instants. This gives
// reviewers a stable, inspectable ledger to query and lets tests assert exact
// projected values. It is safe to run repeatedly (ON CONFLICT DO NOTHING).
package seed

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Fixed identifiers so GET /sessions/{id} is reproducible across runs.
const (
	BookID    = "00000000-0000-0000-0000-0000000000b1"
	SessionID = "00000000-0000-0000-0000-0000000000c1"
)

// Apply writes the deterministic fixture. All timestamps are canonical UTC
// RFC3339Nano. The event stream tells one honest reading story:
//
//	09:00:00       SESSION_STARTED
//	09:12:30       PAGE_REACHED page 12
//	09:20:00       PASSAGE_REACTED "the grief chapter" — "it caught in my throat"
//	09:25:00       INTERRUPTED    doorbell               (gap opens)
//	09:33:00       PAGE_REACHED page 18                  (gap closes: 8m interrupted)
//	09:40:00       SESSION_ENDED
//
// Gross span 40m, minus 8m interrupted = 32m reading; last page 18; 1
// interruption; 1 recorded feeling.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	const bookSQL = `
		INSERT INTO books (id, isbn, title, author, edition, publisher, published_year, page_count, created_at)
		VALUES ($1, '9780374528379', 'The Brothers Karamazov', 'Fyodor Dostoevsky',
		        'FSG Classics', 'Farrar, Straus and Giroux', 2002, 796,
		        '2026-08-26T08:59:00Z')
		ON CONFLICT (id) DO NOTHING`
	if _, err := pool.Exec(ctx, bookSQL, BookID); err != nil {
		return fmt.Errorf("seed book: %w", err)
	}

	const sessionSQL = `
		INSERT INTO sessions (id, book_id, reader, status, created_at)
		VALUES ($1, $2, 'Ada', 'ended', '2026-08-26T08:59:30Z')
		ON CONFLICT (id) DO NOTHING`
	if _, err := pool.Exec(ctx, sessionSQL, SessionID, BookID); err != nil {
		return fmt.Errorf("seed session: %w", err)
	}

	events := []struct {
		seq        int
		typ        string
		occurredAt string
		recordedAt string
		payload    string
	}{
		{1, "SESSION_STARTED", "2026-08-26T09:00:00Z", "2026-08-26T09:00:00.010Z", `{}`},
		{2, "PAGE_REACHED", "2026-08-26T09:12:30Z", "2026-08-26T09:12:30.010Z", `{"page":12}`},
		{3, "PASSAGE_REACTED", "2026-08-26T09:20:00Z", "2026-08-26T09:20:00.010Z", `{"passage":"the grief chapter","feeling":"it caught in my throat"}`},
		{4, "INTERRUPTED", "2026-08-26T09:25:00Z", "2026-08-26T09:25:00.010Z", `{"reason":"doorbell"}`},
		{5, "PAGE_REACHED", "2026-08-26T09:33:00Z", "2026-08-26T09:33:00.010Z", `{"page":18}`},
		{6, "SESSION_ENDED", "2026-08-26T09:40:00Z", "2026-08-26T09:40:00.010Z", `{}`},
	}
	const eventSQL = `
		INSERT INTO events (session_id, seq, type, occurred_at, recorded_at, payload)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		ON CONFLICT (session_id, seq) DO NOTHING`
	for _, e := range events {
		if _, err := pool.Exec(ctx, eventSQL, SessionID, e.seq, e.typ, e.occurredAt, e.recordedAt, e.payload); err != nil {
			return fmt.Errorf("seed event seq %d: %w", e.seq, err)
		}
	}
	return nil
}
