package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/embodied-reading/ledger/internal/apperr"
	"github.com/embodied-reading/ledger/internal/clock"
	"github.com/embodied-reading/ledger/internal/domain"
)

// TxFn performs the actual write work inside an already-open transaction and
// returns the HTTP status and JSON body that should be recorded for idempotent
// replay and returned to the client.
type TxFn func(ctx context.Context, tx pgx.Tx) (status int, body []byte, err error)

// DoIdempotent runs fn inside a single serializable-enough transaction guarded
// by the idempotency ledger.
//
//   - If key names a previously completed request with a matching body hash, the
//     stored response is replayed verbatim and fn never runs (no second write).
//   - If the key is fresh (or empty), fn runs; its response is persisted against
//     the key and the transaction commits atomically, so the event write and the
//     idempotency record land together or not at all.
//
// Concurrent callers sharing a key serialize on the idempotency row lock, so the
// second caller replays the first's committed response rather than duplicating
// the write.
func (s *Store) DoIdempotent(ctx context.Context, key, method, path string, reqBody []byte, fn TxFn) (int, []byte, error) {
	hash := HashRequest(method, path, reqBody)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	cached, err := s.claimIdempotency(ctx, tx, key, method, path, hash)
	if err != nil {
		return 0, nil, err
	}
	if cached != nil {
		// Nothing was written; commit is a no-op but keeps the flow uniform.
		if err := tx.Commit(ctx); err != nil {
			return 0, nil, fmt.Errorf("commit replay tx: %w", err)
		}
		return cached.Status, cached.Body, nil
	}

	status, body, err := fn(ctx, tx)
	if err != nil {
		return 0, nil, err
	}

	if err := s.completeIdempotency(ctx, tx, key, status, body); err != nil {
		return 0, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, nil, fmt.Errorf("commit tx: %w", err)
	}
	return status, body, nil
}

// CreateBookInput is the validated input for registering a book edition.
type CreateBookInput struct {
	ISBN          string
	Title         string
	Author        string
	Edition       string
	Publisher     string
	PublishedYear *int
	PageCount     *int
}

// CreateBookTx inserts a book within tx and returns it.
func (s *Store) CreateBookTx(ctx context.Context, tx pgx.Tx, in CreateBookInput) (Book, error) {
	now := clock.Format(s.clock.Now())
	var b Book
	err := tx.QueryRow(ctx, `
		INSERT INTO books (isbn, title, author, edition, publisher, published_year, page_count, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, isbn, title, author, edition, publisher, published_year, page_count, created_at
	`, in.ISBN, in.Title, in.Author, in.Edition, in.Publisher, in.PublishedYear, in.PageCount, now).
		Scan(&b.ID, &b.ISBN, &b.Title, &b.Author, &b.Edition, &b.Publisher, &b.PublishedYear, &b.PageCount, &b.CreatedAt)
	if err != nil {
		return Book{}, fmt.Errorf("insert book: %w", err)
	}
	return b, nil
}

// CreateSessionTx inserts a session within tx after verifying the book exists.
func (s *Store) CreateSessionTx(ctx context.Context, tx pgx.Tx, bookID, reader string) (Session, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM books WHERE id = $1)`, bookID).Scan(&exists); err != nil {
		return Session{}, fmt.Errorf("check book exists: %w", err)
	}
	if !exists {
		return Session{}, apperr.New(apperr.CodeNotFound, "book not found").
			WithDetails(map[string]any{"bookId": bookID})
	}
	now := clock.Format(s.clock.Now())
	var sess Session
	err := tx.QueryRow(ctx, `
		INSERT INTO sessions (book_id, reader, status, created_at)
		VALUES ($1, $2, 'open', $3)
		RETURNING id, book_id, reader, status, created_at
	`, bookID, reader, now).
		Scan(&sess.ID, &sess.BookID, &sess.Reader, &sess.Status, &sess.CreatedAt)
	if err != nil {
		return Session{}, fmt.Errorf("insert session: %w", err)
	}
	return sess, nil
}

// AppendEventInput is the validated input for appending one event.
type AppendEventInput struct {
	SessionID   string
	ExpectedSeq int
	Type        domain.EventType
	OccurredAt  time.Time
	Payload     domain.Payload
}

// loadSessionEvents reads every event for a session ordered by seq, and reports
// whether the session exists.
func (s *Store) loadSessionEvents(ctx context.Context, tx pgx.Tx, sessionID string) (bool, []domain.Event, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sessions WHERE id = $1)`, sessionID).Scan(&exists); err != nil {
		return false, nil, fmt.Errorf("check session exists: %w", err)
	}
	if !exists {
		return false, nil, nil
	}
	rows, err := tx.Query(ctx, `
		SELECT seq, type, occurred_at, recorded_at, payload
		FROM events WHERE session_id = $1 ORDER BY seq ASC
	`, sessionID)
	if err != nil {
		return true, nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var out []domain.Event
	for rows.Next() {
		var (
			seq         int
			typ         string
			occurredStr string
			recordedStr string
			payloadRaw  []byte
		)
		if err := rows.Scan(&seq, &typ, &occurredStr, &recordedStr, &payloadRaw); err != nil {
			return true, nil, fmt.Errorf("scan event: %w", err)
		}
		occurred, err := time.Parse(clock.Layout, occurredStr)
		if err != nil {
			return true, nil, fmt.Errorf("parse occurred_at: %w", err)
		}
		recorded, err := time.Parse(clock.Layout, recordedStr)
		if err != nil {
			return true, nil, fmt.Errorf("parse recorded_at: %w", err)
		}
		var p domain.Payload
		if len(payloadRaw) > 0 {
			if err := json.Unmarshal(payloadRaw, &p); err != nil {
				return true, nil, fmt.Errorf("unmarshal payload: %w", err)
			}
		}
		out = append(out, domain.Event{
			Seq:        seq,
			Type:       domain.EventType(typ),
			OccurredAt: occurred.UTC(),
			RecordedAt: recorded.UTC(),
			Payload:    p,
		})
	}
	if err := rows.Err(); err != nil {
		return true, nil, fmt.Errorf("iterate events: %w", err)
	}
	return true, out, nil
}

// foldState reconstructs the session State from an ordered event slice.
func foldState(events []domain.Event) domain.State {
	var st domain.State
	for _, e := range events {
		st = domain.Apply(st, e)
	}
	return st
}

// AppendEventResult is returned to the handler after a successful append.
type AppendEventResult struct {
	Seq        int
	Type       domain.EventType
	OccurredAt time.Time
	RecordedAt time.Time
	View       SessionView
}

// AppendEventTx validates and appends one event within tx, enforcing optimistic
// sequencing. The (session_id, seq) unique constraint is the concurrency
// arbiter: if two writers race for the same seq, exactly one INSERT succeeds and
// the other surfaces a SEQ_CONFLICT carrying the authoritative current seq.
func (s *Store) AppendEventTx(ctx context.Context, tx pgx.Tx, in AppendEventInput) (AppendEventResult, error) {
	exists, events, err := s.loadSessionEvents(ctx, tx, in.SessionID)
	if err != nil {
		return AppendEventResult{}, err
	}
	if !exists {
		return AppendEventResult{}, apperr.New(apperr.CodeNotFound, "session not found").
			WithDetails(map[string]any{"sessionId": in.SessionID})
	}

	st := foldState(events)

	// Optimistic sequencing: the new event must occupy exactly currentCount+1.
	wantSeq := st.Count + 1
	if in.ExpectedSeq != wantSeq {
		return AppendEventResult{}, apperr.New(apperr.CodeSeqConflict,
			"expectedSeq does not match the current sequence").
			WithDetails(map[string]any{
				"expectedSeq": in.ExpectedSeq,
				"currentSeq":  st.Count,
			})
	}

	candidate := domain.Event{
		Seq:        wantSeq,
		Type:       in.Type,
		OccurredAt: in.OccurredAt,
		Payload:    in.Payload,
	}
	if err := domain.ValidateNext(st, candidate); err != nil {
		return AppendEventResult{}, err
	}

	recordedAt := s.clock.Now().UTC()
	payloadJSON, err := json.Marshal(in.Payload)
	if err != nil {
		return AppendEventResult{}, fmt.Errorf("marshal payload: %w", err)
	}

	// Wrap the INSERT in a SAVEPOINT (pgx nested transaction). A unique-key
	// collision aborts only the savepoint, not the whole transaction, so after
	// rolling back to it we can still run the follow-up query that reports the
	// authoritative current sequence. Without this, the conflicting query would
	// run inside an aborted transaction and fail, yielding a bogus currentSeq: 0.
	sp, err := tx.Begin(ctx)
	if err != nil {
		return AppendEventResult{}, fmt.Errorf("open savepoint: %w", err)
	}
	_, err = sp.Exec(ctx, `
		INSERT INTO events (session_id, seq, type, occurred_at, recorded_at, payload)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, in.SessionID, wantSeq, string(in.Type),
		clock.Format(in.OccurredAt), clock.Format(recordedAt), payloadJSON)
	if err != nil {
		// Roll back to the savepoint so the outer transaction stays usable.
		if rbErr := sp.Rollback(ctx); rbErr != nil {
			return AppendEventResult{}, fmt.Errorf("rollback savepoint: %w", rbErr)
		}
		if isUniqueViolation(err) {
			// A concurrent writer claimed this seq first. Query the authoritative
			// current sequence on the now-recovered transaction so the client can
			// retry from the right place.
			currentSeq, seqErr := s.currentSeq(ctx, tx, in.SessionID)
			if seqErr != nil {
				return AppendEventResult{}, fmt.Errorf("read current seq after conflict: %w", seqErr)
			}
			return AppendEventResult{}, apperr.New(apperr.CodeSeqConflict,
				"another writer already appended at this sequence").
				WithDetails(map[string]any{
					"expectedSeq": in.ExpectedSeq,
					"currentSeq":  currentSeq,
				})
		}
		return AppendEventResult{}, fmt.Errorf("insert event: %w", err)
	}
	// Release the savepoint, folding the successful INSERT into the outer tx.
	if err := sp.Commit(ctx); err != nil {
		return AppendEventResult{}, fmt.Errorf("release savepoint: %w", err)
	}

	// Keep the session status projection cache in step (append-only-safe: this
	// touches sessions, never events).
	if in.Type == domain.EventSessionEnded {
		if _, err := tx.Exec(ctx, `UPDATE sessions SET status = 'ended' WHERE id = $1`, in.SessionID); err != nil {
			return AppendEventResult{}, fmt.Errorf("update session status: %w", err)
		}
	}

	// Recompute the view from the full stream including the new event.
	candidate.RecordedAt = recordedAt
	events = append(events, candidate)
	view, err := s.buildView(ctx, tx, in.SessionID, events)
	if err != nil {
		return AppendEventResult{}, err
	}

	return AppendEventResult{
		Seq:        wantSeq,
		Type:       in.Type,
		OccurredAt: in.OccurredAt,
		RecordedAt: recordedAt,
		View:       view,
	}, nil
}

// currentSeq returns the authoritative max seq for a session, or 0 if it has no
// events yet. It must be called on a live (non-aborted) transaction; the caller
// rolls back to a savepoint before invoking it after a conflicting INSERT.
func (s *Store) currentSeq(ctx context.Context, tx pgx.Tx, sessionID string) (int, error) {
	var maxSeq *int
	if err := tx.QueryRow(ctx, `SELECT MAX(seq) FROM events WHERE session_id = $1`, sessionID).Scan(&maxSeq); err != nil {
		return 0, fmt.Errorf("query current seq: %w", err)
	}
	if maxSeq == nil {
		return 0, nil
	}
	return *maxSeq, nil
}

// SessionView is the read model returned by GET /sessions/{id}.
type SessionView struct {
	Session    Session           `json:"session"`
	Book       Book              `json:"book"`
	Projection domain.Projection `json:"projection"`
	Events     []EventView       `json:"events"`
}

// EventView is one event as presented in the read model.
type EventView struct {
	Seq        int            `json:"seq"`
	Type       string         `json:"type"`
	OccurredAt string         `json:"occurredAt"`
	RecordedAt string         `json:"recordedAt"`
	Payload    domain.Payload `json:"payload"`
}

// buildView assembles the full read model for a session from a preloaded,
// ordered event slice.
func (s *Store) buildView(ctx context.Context, tx pgx.Tx, sessionID string, events []domain.Event) (SessionView, error) {
	var sess Session
	err := tx.QueryRow(ctx, `
		SELECT id, book_id, reader, status, created_at FROM sessions WHERE id = $1
	`, sessionID).Scan(&sess.ID, &sess.BookID, &sess.Reader, &sess.Status, &sess.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SessionView{}, apperr.New(apperr.CodeNotFound, "session not found").
				WithDetails(map[string]any{"sessionId": sessionID})
		}
		return SessionView{}, fmt.Errorf("load session: %w", err)
	}

	var book Book
	err = tx.QueryRow(ctx, `
		SELECT id, isbn, title, author, edition, publisher, published_year, page_count, created_at
		FROM books WHERE id = $1
	`, sess.BookID).Scan(&book.ID, &book.ISBN, &book.Title, &book.Author, &book.Edition,
		&book.Publisher, &book.PublishedYear, &book.PageCount, &book.CreatedAt)
	if err != nil {
		return SessionView{}, fmt.Errorf("load book: %w", err)
	}

	// Defensive: ensure order by seq.
	sort.Slice(events, func(i, j int) bool { return events[i].Seq < events[j].Seq })

	evViews := make([]EventView, 0, len(events))
	for _, e := range events {
		evViews = append(evViews, EventView{
			Seq:        e.Seq,
			Type:       string(e.Type),
			OccurredAt: clock.Format(e.OccurredAt),
			RecordedAt: clock.Format(e.RecordedAt),
			Payload:    e.Payload,
		})
	}

	return SessionView{
		Session:    sess,
		Book:       book,
		Projection: domain.Project(events),
		Events:     evViews,
	}, nil
}

// GetSessionView loads the full read model for a session outside any write path.
func (s *Store) GetSessionView(ctx context.Context, sessionID string) (SessionView, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return SessionView{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	exists, events, err := s.loadSessionEvents(ctx, tx, sessionID)
	if err != nil {
		return SessionView{}, err
	}
	if !exists {
		return SessionView{}, apperr.New(apperr.CodeNotFound, "session not found").
			WithDetails(map[string]any{"sessionId": sessionID})
	}
	view, err := s.buildView(ctx, tx, sessionID, events)
	if err != nil {
		return SessionView{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return SessionView{}, fmt.Errorf("commit read tx: %w", err)
	}
	return view, nil
}
