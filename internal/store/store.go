// Package store owns all PostgreSQL access for the embodied reading ledger.
// Correctness (concurrency, idempotency, append-only) is enforced by the
// database: a CAS UPDATE on reading_sessions.last_seq, a UNIQUE(session_id,
// seq) backstop on the append-only event log, and the idempotency_keys table.
// No Redis, no in-memory locks.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"embodied-reading-ledger/internal/errs"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event types accepted by the ledger.
const (
	EventSessionStarted = "SESSION_STARTED"
	EventPageReached    = "PAGE_REACHED"
	EventPassageReacted = "PASSAGE_REACTED"
	EventInterrupted    = "INTERRUPTED"
	EventSessionEnded   = "SESSION_ENDED"
)

// Session states.
const (
	SessionOpen  = "OPEN"
	SessionEnded = "ENDED"
)

// Book is a registered physical edition.
type Book struct {
	ID         uuid.UUID `json:"id"`
	Title      string    `json:"title"`
	Author     string    `json:"author"`
	Edition    string    `json:"edition"`
	ISBN       string    `json:"isbn"`
	TotalPages int       `json:"total_pages"`
	CreatedAt  time.Time `json:"created_at"`
}

// Session is one embodied reading sitting against a book.
type Session struct {
	ID         uuid.UUID  `json:"id"`
	BookID     uuid.UUID  `json:"book_id"`
	ReaderName string     `json:"reader_name"`
	State      string     `json:"state"`
	LastSeq    int64      `json:"last_seq"`
	CreatedAt  time.Time  `json:"created_at"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
}

// Event is one immutable entry in the append-only log.
type Event struct {
	ID              uuid.UUID `json:"id"`
	SessionID       uuid.UUID `json:"session_id"`
	Seq             int64     `json:"seq"`
	Type            string    `json:"type"`
	OccurredAt      time.Time `json:"occurred_at"` // client-reported, validated, stored as UTC
	RecordedAt      time.Time `json:"recorded_at"` // server receive time, UTC
	Page            *int      `json:"page,omitempty"`
	Passage         *string   `json:"passage,omitempty"`
	Reaction        *string   `json:"reaction,omitempty"`
	InterruptReason *string   `json:"interrupt_reason,omitempty"`
}

// AppendEventInput is the validated payload of POST /sessions/{id}/events.
type AppendEventInput struct {
	SessionID       uuid.UUID
	ExpectedSeq     int64
	Type            string
	OccurredAt      time.Time
	Page            *int
	Passage         *string
	Reaction        *string
	InterruptReason *string
}

// AppendEventResult is returned after a successful append.
type AppendEventResult struct {
	Event   Event
	Session Session
}

// Store wraps a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New connects and verifies the pool.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// HashRequest fingerprints a request body for idempotency mismatch detection.
func HashRequest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// idempotent runs fn inside a transaction guarded by the Idempotency-Key.
//
// Protocol (entirely in PostgreSQL):
//  1. INSERT the key with ON CONFLICT DO NOTHING. If a concurrent first-write
//     is still in flight, this statement blocks until it commits/rolls back.
//  2. If the key already existed, replay the stored response verbatim when the
//     request hash matches; 422 otherwise. No business SQL re-executes and
//     nothing is written twice.
//  3. If we claimed the key, run fn; on success store status+response in the
//     same transaction so key and business writes commit atomically.
//     On a domain error the whole transaction rolls back, so a failed request
//     never consumes its key.
func (s *Store) idempotent(
	ctx context.Context,
	key, requestHash string,
	fn func(ctx context.Context, tx pgx.Tx) (int, any, *errs.AppError),
) (int, []byte, *errs.AppError) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, nil, errs.Internal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit

	tag, err := tx.Exec(ctx,
		`INSERT INTO idempotency_keys (key, request_hash) VALUES ($1, $2)
		 ON CONFLICT (key) DO NOTHING`, key, requestHash)
	if err != nil {
		return 0, nil, errs.Internal(err)
	}

	if tag.RowsAffected() == 0 {
		// Key already committed by the first writer: replay, never re-execute.
		var storedHash string
		var status *int
		var body []byte
		err := tx.QueryRow(ctx,
			`SELECT request_hash, status_code, response_body
			   FROM idempotency_keys WHERE key = $1`, key).
			Scan(&storedHash, &status, &body)
		if err != nil {
			return 0, nil, errs.Internal(err)
		}
		if storedHash != requestHash {
			return 0, nil, errs.WithDetails(http.StatusUnprocessableEntity,
				errs.CodeIdempotencyMismatch,
				"Idempotency-Key was already used with a different request body",
				map[string]any{"key": key})
		}
		if status == nil {
			return 0, nil, errs.New(http.StatusConflict,
				errs.CodeIdempotencyInProgress,
				"the original request with this Idempotency-Key is still in flight")
		}
		return *status, body, nil
	}

	status, resp, appErr := fn(ctx, tx)
	if appErr != nil {
		return 0, nil, appErr // rollback: failed requests do not consume the key
	}
	body, err := json.Marshal(resp)
	if err != nil {
		return 0, nil, errs.Internal(err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE idempotency_keys SET status_code = $2, response_body = $3
		 WHERE key = $1`, key, status, body); err != nil {
		return 0, nil, errs.Internal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, nil, errs.Internal(err)
	}
	return status, body, nil
}

// CreateBook registers a physical edition idempotently.
func (s *Store) CreateBook(ctx context.Context, key, requestHash string, in Book) (int, []byte, *errs.AppError) {
	in.ID = uuid.New()
	return s.idempotent(ctx, key, requestHash, func(ctx context.Context, tx pgx.Tx) (int, any, *errs.AppError) {
		err := tx.QueryRow(ctx,
			`INSERT INTO books (id, title, author, edition, isbn, total_pages)
			 VALUES ($1,$2,$3,$4,$5,$6) RETURNING created_at`,
			in.ID, in.Title, in.Author, in.Edition, in.ISBN, in.TotalPages).
			Scan(&in.CreatedAt)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return 0, nil, errs.WithDetails(http.StatusConflict, errs.CodeConflict,
					"a book with this ISBN is already registered",
					map[string]any{"isbn": in.ISBN})
			}
			return 0, nil, errs.Internal(err)
		}
		in.CreatedAt = in.CreatedAt.UTC()
		return http.StatusCreated, map[string]any{"book": in}, nil
	})
}

// CreateSession opens a reading session idempotently.
func (s *Store) CreateSession(ctx context.Context, key, requestHash string, in Session) (int, []byte, *errs.AppError) {
	in.ID = uuid.New()
	in.State = SessionOpen
	return s.idempotent(ctx, key, requestHash, func(ctx context.Context, tx pgx.Tx) (int, any, *errs.AppError) {
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM books WHERE id = $1)`, in.BookID).
			Scan(&exists); err != nil {
			return 0, nil, errs.Internal(err)
		}
		if !exists {
			return 0, nil, errs.NotFound("book not found: " + in.BookID.String())
		}
		if err := tx.QueryRow(ctx,
			`INSERT INTO reading_sessions (id, book_id, reader_name)
			 VALUES ($1,$2,$3) RETURNING created_at`,
			in.ID, in.BookID, in.ReaderName).Scan(&in.CreatedAt); err != nil {
			return 0, nil, errs.Internal(err)
		}
		in.CreatedAt = in.CreatedAt.UTC()
		return http.StatusCreated, map[string]any{"session": in}, nil
	})
}

// AppendEvent validates the state machine and appends one event. Concurrency
// safety comes from a compare-and-swap UPDATE on reading_sessions.last_seq
// (exactly one concurrent writer wins; losers get 409 + current_seq), with
// UNIQUE(session_id, seq) as the database backstop.
func (s *Store) AppendEvent(ctx context.Context, key, requestHash string, in AppendEventInput) (int, []byte, *errs.AppError) {
	return s.idempotent(ctx, key, requestHash, func(ctx context.Context, tx pgx.Tx) (int, any, *errs.AppError) {
		var (
			state      string
			totalPages int
		)
		err := tx.QueryRow(ctx,
			`SELECT s.state, b.total_pages
			   FROM reading_sessions s JOIN books b ON b.id = s.book_id
			  WHERE s.id = $1`, in.SessionID).Scan(&state, &totalPages)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil, errs.NotFound("session not found: " + in.SessionID.String())
		}
		if err != nil {
			return 0, nil, errs.Internal(err)
		}

		if appErr := validatePayload(in, totalPages); appErr != nil {
			return 0, nil, appErr
		}

		// Compare-and-swap: only succeeds if we observed the expected sequence.
		// The row lock is held until commit/rollback, serializing writers.
		tag, err := tx.Exec(ctx,
			`UPDATE reading_sessions SET last_seq = last_seq + 1
			  WHERE id = $1 AND last_seq = $2`, in.SessionID, in.ExpectedSeq)
		if err != nil {
			return 0, nil, errs.Internal(err)
		}
		if tag.RowsAffected() == 0 {
			var current int64
			if err := tx.QueryRow(ctx,
				`SELECT last_seq FROM reading_sessions WHERE id = $1`,
				in.SessionID).Scan(&current); err != nil {
				return 0, nil, errs.Internal(err)
			}
			return 0, nil, errs.WithDetails(http.StatusConflict, errs.CodeSeqConflict,
				"expectedSeq does not match the session's current sequence",
				map[string]any{"current_seq": current})
		}

		var (
			lastType *string
			lastAt   *time.Time
		)
		err = tx.QueryRow(ctx,
			`SELECT type, occurred_at FROM reading_events
			  WHERE session_id = $1 ORDER BY seq DESC LIMIT 1`,
			in.SessionID).Scan(&lastType, &lastAt)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return 0, nil, errs.Internal(err)
		}

		if appErr := validateTransition(in, lastType, lastAt); appErr != nil {
			return 0, nil, appErr // rollback also undoes the CAS increment
		}

		ev := Event{
			ID:              uuid.New(),
			SessionID:       in.SessionID,
			Seq:             in.ExpectedSeq + 1,
			Type:            in.Type,
			OccurredAt:      in.OccurredAt.UTC(),
			Page:            in.Page,
			Passage:         in.Passage,
			Reaction:        in.Reaction,
			InterruptReason: in.InterruptReason,
		}
		if err := tx.QueryRow(ctx,
			`INSERT INTO reading_events
			   (id, session_id, seq, type, occurred_at, page, passage, reaction, interrupt_reason)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING recorded_at`,
			ev.ID, ev.SessionID, ev.Seq, ev.Type, ev.OccurredAt,
			ev.Page, ev.Passage, ev.Reaction, ev.InterruptReason).
			Scan(&ev.RecordedAt); err != nil {
			return 0, nil, errs.Internal(err) // UNIQUE(session_id, seq) backstop
		}
		ev.RecordedAt = ev.RecordedAt.UTC()

		newState := SessionOpen
		if in.Type == EventSessionEnded {
			newState = SessionEnded
			if _, err := tx.Exec(ctx,
				`UPDATE reading_sessions SET state = 'ENDED', ended_at = $2
				  WHERE id = $1`, in.SessionID, ev.OccurredAt); err != nil {
				return 0, nil, errs.Internal(err)
			}
		}

		return http.StatusCreated, map[string]any{
			"event": ev,
			"session": map[string]any{
				"id":       in.SessionID,
				"state":    newState,
				"last_seq": ev.Seq,
			},
		}, nil
	})
}

func validatePayload(in AppendEventInput, totalPages int) *errs.AppError {
	switch in.Type {
	case EventPageReached:
		if in.Page == nil {
			return errs.Validation("PAGE_REACHED requires a page number")
		}
		if *in.Page < 1 || *in.Page > totalPages {
			return errs.WithDetails(400, errs.CodeValidation,
				"page is outside the edition's page range",
				map[string]any{"page": *in.Page, "total_pages": totalPages})
		}
	case EventPassageReacted:
		if in.Reaction == nil || *in.Reaction == "" {
			return errs.Validation("PASSAGE_REACTED requires the reader's reaction text")
		}
	case EventSessionStarted, EventInterrupted, EventSessionEnded:
	default:
		return errs.WithDetails(400, errs.CodeValidation,
			"unknown event type",
			map[string]any{"allowed": []string{
				EventSessionStarted, EventPageReached,
				EventPassageReacted, EventInterrupted, EventSessionEnded,
			}})
	}
	return nil
}

func validateTransition(in AppendEventInput, lastType *string, lastAt *time.Time) *errs.AppError {
	if lastType != nil && *lastType == EventSessionEnded {
		return errs.New(http.StatusConflict, errs.CodeAppendAfterEnd,
			"the session has ended; no further events may be appended")
	}
	if in.ExpectedSeq == 0 {
		if in.Type == EventPageReached {
			return errs.WithDetails(http.StatusUnprocessableEntity, errs.CodePageBeforeStart,
				"a page was reached before the session started",
				map[string]any{"expected_first_type": EventSessionStarted})
		}
		if in.Type != EventSessionStarted {
			return errs.WithDetails(http.StatusUnprocessableEntity, errs.CodeInvalidTransition,
				"the first event of a session must be SESSION_STARTED",
				map[string]any{"got": in.Type})
		}
	}
	if in.ExpectedSeq > 0 && in.Type == EventSessionStarted {
		return errs.New(http.StatusUnprocessableEntity, errs.CodeInvalidTransition,
			"SESSION_STARTED may only be the first event of a session")
	}
	if lastAt != nil && in.OccurredAt.UTC().Before(lastAt.UTC()) {
		return errs.WithDetails(http.StatusUnprocessableEntity, errs.CodeTimeRegression,
			"occurred_at is earlier than the previous event; client clocks may not move backwards within a session",
			map[string]any{"previous_occurred_at": lastAt.UTC().Format(time.RFC3339Nano)})
	}
	return nil
}

// SessionView is the reconstructed ledger for one session.
type SessionView struct {
	Session           Session
	Book              Book
	Events            []Event
	DurationsSeconds  []float64 // per event, delta vs previous event (0 for the first)
	ReadingSeconds    float64
	ReadingMinutes    float64
	LastPage          *int
	InterruptionCount int
	Reactions         []Reaction
}

// Reaction is a reader-written note recovered from PASSAGE_REACTED events.
type Reaction struct {
	Seq        int64     `json:"seq"`
	OccurredAt time.Time `json:"occurred_at"`
	Page       *int      `json:"page,omitempty"`
	Passage    *string   `json:"passage,omitempty"`
	Reaction   string    `json:"reaction"`
}

// GetSession reconstructs the session: durations are computed server-side
// from adjacent events; no client-computed durations are ever trusted.
func (s *Store) GetSession(ctx context.Context, id uuid.UUID) (*SessionView, *errs.AppError) {
	view := &SessionView{}
	var endedAt *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT s.id, s.book_id, s.reader_name, s.state, s.last_seq, s.created_at, s.ended_at,
		        b.id, b.title, b.author, b.edition, b.isbn, b.total_pages, b.created_at
		   FROM reading_sessions s JOIN books b ON b.id = s.book_id
		  WHERE s.id = $1`, id).
		Scan(&view.Session.ID, &view.Session.BookID, &view.Session.ReaderName,
			&view.Session.State, &view.Session.LastSeq, &view.Session.CreatedAt, &endedAt,
			&view.Book.ID, &view.Book.Title, &view.Book.Author, &view.Book.Edition,
			&view.Book.ISBN, &view.Book.TotalPages, &view.Book.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errs.NotFound("session not found: " + id.String())
	}
	if err != nil {
		return nil, errs.Internal(err)
	}
	if endedAt != nil {
		u := endedAt.UTC()
		view.Session.EndedAt = &u
	}
	view.Session.CreatedAt = view.Session.CreatedAt.UTC()
	view.Book.CreatedAt = view.Book.CreatedAt.UTC()

	rows, err := s.pool.Query(ctx,
		`SELECT id, session_id, seq, type, occurred_at, recorded_at,
		        page, passage, reaction, interrupt_reason
		   FROM reading_events WHERE session_id = $1 ORDER BY seq ASC`, id)
	if err != nil {
		return nil, errs.Internal(err)
	}
	defer rows.Close()

	var prev *time.Time
	for rows.Next() {
		var ev Event
		if err := rows.Scan(&ev.ID, &ev.SessionID, &ev.Seq, &ev.Type,
			&ev.OccurredAt, &ev.RecordedAt, &ev.Page, &ev.Passage,
			&ev.Reaction, &ev.InterruptReason); err != nil {
			return nil, errs.Internal(err)
		}
		ev.OccurredAt = ev.OccurredAt.UTC()
		ev.RecordedAt = ev.RecordedAt.UTC()
		delta := 0.0
		if prev != nil {
			delta = ev.OccurredAt.Sub(*prev).Seconds()
		}
		view.DurationsSeconds = append(view.DurationsSeconds, delta)
		view.ReadingSeconds += delta
		occ := ev.OccurredAt
		prev = &occ

		if ev.Page != nil {
			p := *ev.Page
			view.LastPage = &p
		}
		switch ev.Type {
		case EventInterrupted:
			view.InterruptionCount++
		case EventPassageReacted:
			view.Reactions = append(view.Reactions, Reaction{
				Seq:        ev.Seq,
				OccurredAt: ev.OccurredAt,
				Page:       ev.Page,
				Passage:    ev.Passage,
				Reaction:   *ev.Reaction,
			})
		}
		view.Events = append(view.Events, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, errs.Internal(err)
	}
	view.ReadingMinutes = view.ReadingSeconds / 60
	return view, nil
}
