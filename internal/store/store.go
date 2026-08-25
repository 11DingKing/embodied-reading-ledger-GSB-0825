package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"embodied-reading-ledger/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")

// DBTX is satisfied by *pgxpool.Pool and pgx.Tx.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Store struct {
	db DBTX
}

func New(db DBTX) *Store { return &Store{db: db} }

func (s *Store) DB() DBTX { return s.db }

// --- Books ---

type InsertBookParams struct {
	ISBN          *string
	Title         string
	Author        string
	Publisher     *string
	PublishedYear *int
	TotalPages    *int
	Format        domain.BookFormat
}

func (s *Store) InsertBook(ctx context.Context, p InsertBookParams) (*domain.Book, error) {
	b := &domain.Book{
		ISBN:          p.ISBN,
		Title:         p.Title,
		Author:        p.Author,
		Publisher:     p.Publisher,
		PublishedYear: p.PublishedYear,
		TotalPages:    p.TotalPages,
		Format:        p.Format,
	}
	var createdAt time.Time
	err := s.db.QueryRow(ctx, `
		INSERT INTO books (isbn,title,author,publisher,published_year,total_pages,format)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		RETURNING id::text, created_at`,
		p.ISBN, p.Title, p.Author, p.Publisher, p.PublishedYear, p.TotalPages, string(p.Format),
	).Scan(&b.ID, &createdAt)
	if err != nil {
		return nil, err
	}
	b.CreatedAt = domain.NewTimestamp(createdAt)
	return b, nil
}

func (s *Store) GetBook(ctx context.Context, id string) (*domain.Book, error) {
	b := &domain.Book{}
	var format string
	var createdAt time.Time
	err := s.db.QueryRow(ctx, `
		SELECT id::text, isbn, title, author, publisher, published_year, total_pages, format, created_at
		FROM books WHERE id=$1::uuid`, id,
	).Scan(&b.ID, &b.ISBN, &b.Title, &b.Author, &b.Publisher, &b.PublishedYear,
		&b.TotalPages, &format, &createdAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	b.Format = domain.BookFormat(format)
	b.CreatedAt = domain.NewTimestamp(createdAt)
	return b, nil
}

// --- Sessions ---

type InsertSessionParams struct {
	BookID string
	Label  *string
}

func (s *Store) InsertSession(ctx context.Context, p InsertSessionParams) (*domain.Session, error) {
	sess := &domain.Session{BookID: p.BookID, Label: p.Label, Status: domain.SessionPending}
	var createdAt time.Time
	err := s.db.QueryRow(ctx, `
		INSERT INTO sessions (book_id,label) VALUES ($1::uuid,$2)
		RETURNING id::text, status, created_at`,
		p.BookID, p.Label,
	).Scan(&sess.ID, &sess.Status, &createdAt)
	if err != nil {
		if isFKViolation(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	sess.CreatedAt = domain.NewTimestamp(createdAt)
	return sess, nil
}

// LockSession locks the session row FOR UPDATE and returns it. Must run in a tx.
func (s *Store) LockSession(ctx context.Context, id string) (*domain.Session, error) {
	sess := &domain.Session{}
	var createdAt time.Time
	err := s.db.QueryRow(ctx, `
		SELECT id::text, book_id::text, label, status, created_at
		FROM sessions WHERE id=$1::uuid FOR UPDATE`, id,
	).Scan(&sess.ID, &sess.BookID, &sess.Label, &sess.Status, &createdAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	sess.CreatedAt = domain.NewTimestamp(createdAt)
	return sess, nil
}

func (s *Store) GetSession(ctx context.Context, id string) (*domain.Session, error) {
	sess := &domain.Session{}
	var createdAt time.Time
	err := s.db.QueryRow(ctx, `
		SELECT id::text, book_id::text, label, status, created_at
		FROM sessions WHERE id=$1::uuid`, id,
	).Scan(&sess.ID, &sess.BookID, &sess.Label, &sess.Status, &createdAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	sess.CreatedAt = domain.NewTimestamp(createdAt)
	return sess, nil
}

func (s *Store) UpdateSessionStatus(ctx context.Context, id string, status domain.SessionStatus) error {
	_, err := s.db.Exec(ctx, `UPDATE sessions SET status=$2 WHERE id=$1::uuid`, id, string(status))
	return err
}

// --- Events ---

func (s *Store) MaxSeq(ctx context.Context, sessionID string) (int, error) {
	var seq int
	err := s.db.QueryRow(ctx,
		`SELECT COALESCE(MAX(seq),0) FROM events WHERE session_id=$1::uuid`, sessionID,
	).Scan(&seq)
	return seq, err
}

// LastEvent holds the fields needed to validate the next append.
type LastEvent struct {
	Seq        int
	EventType  domain.EventType
	OccurredAt time.Time
}

func (s *Store) GetLastEvent(ctx context.Context, sessionID string) (*LastEvent, error) {
	var le LastEvent
	var et string
	err := s.db.QueryRow(ctx, `
		SELECT seq, event_type, occurred_at
		FROM events WHERE session_id=$1::uuid
		ORDER BY seq DESC LIMIT 1`, sessionID,
	).Scan(&le.Seq, &et, &le.OccurredAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	le.EventType = domain.EventType(et)
	le.OccurredAt = le.OccurredAt.UTC()
	return &le, nil
}

type InsertEventParams struct {
	SessionID  string
	Seq        int
	EventType  domain.EventType
	OccurredAt time.Time
	Page       *int
	Note       *string
	Quote      *string
	Reason     *string
}

func (s *Store) InsertEvent(ctx context.Context, p InsertEventParams) (*domain.Event, error) {
	e := &domain.Event{
		SessionID:  p.SessionID,
		Seq:        p.Seq,
		EventType:  p.EventType,
		OccurredAt: domain.NewTimestamp(p.OccurredAt),
		Page:       p.Page,
		Note:       p.Note,
		Quote:      p.Quote,
		Reason:     p.Reason,
	}
	var recordedAt time.Time
	err := s.db.QueryRow(ctx, `
		INSERT INTO events (session_id,seq,event_type,occurred_at,page,note,quote,reason)
		VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8)
		RETURNING id::text, recorded_at`,
		p.SessionID, p.Seq, string(p.EventType), p.OccurredAt, p.Page, p.Note, p.Quote, p.Reason,
	).Scan(&e.ID, &recordedAt)
	if err != nil {
		return nil, err
	}
	e.RecordedAt = domain.NewTimestamp(recordedAt)
	return e, nil
}

func (s *Store) ListEvents(ctx context.Context, sessionID string) ([]domain.Event, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text, session_id::text, seq, event_type, occurred_at, page, note, quote, reason, recorded_at
		FROM events WHERE session_id=$1::uuid ORDER BY seq ASC`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []domain.Event
	for rows.Next() {
		var e domain.Event
		var et string
		var occurredAt, recordedAt time.Time
		if err := rows.Scan(&e.ID, &e.SessionID, &e.Seq, &et, &occurredAt,
			&e.Page, &e.Note, &e.Quote, &e.Reason, &recordedAt); err != nil {
			return nil, err
		}
		e.EventType = domain.EventType(et)
		e.OccurredAt = domain.NewTimestamp(occurredAt)
		e.RecordedAt = domain.NewTimestamp(recordedAt)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// --- Idempotency ---

type IdempotentRecord struct {
	Key           string
	RequestMethod string
	RequestPath   string
	RequestHash   []byte
	StatusCode    int
	ResponseBody  []byte
	CreatedAt     time.Time
}

func (s *Store) GetIdempotency(ctx context.Context, key string) (*IdempotentRecord, error) {
	r := &IdempotentRecord{}
	err := s.db.QueryRow(ctx, `
		SELECT key, request_method, request_path, request_hash, status_code, response_body, created_at
		FROM idempotency_keys WHERE key=$1`, key,
	).Scan(&r.Key, &r.RequestMethod, &r.RequestPath, &r.RequestHash, &r.StatusCode, &r.ResponseBody, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return r, nil
}

func (s *Store) InsertIdempotency(ctx context.Context, r IdempotentRecord) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO idempotency_keys (key,request_method,request_path,request_hash,status_code,response_body)
		VALUES ($1,$2,$3,$4,$5,$6)`,
		r.Key, r.RequestMethod, r.RequestPath, r.RequestHash, r.StatusCode, r.ResponseBody)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrIdempotencyRace
		}
		return err
	}
	return nil
}

var ErrIdempotencyRace = errors.New("idempotency record already exists")

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

func isFKViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23503"
	}
	return false
}

// AdvisoryLock blocks until a transaction-scoped advisory lock for the given key is held.
func AdvisoryLock(ctx context.Context, tx pgx.Tx, key string) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", key)
	if err != nil {
		return fmt.Errorf("acquire advisory lock: %w", err)
	}
	return nil
}

var _ DBTX = (*pgxpool.Pool)(nil)
