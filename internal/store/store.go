// Package store is the PostgreSQL persistence layer. It owns all SQL and all
// transactional guarantees: idempotent replay, optimistic per-session
// sequencing, and append-only event storage. Correctness lives in Postgres —
// there are no in-process locks or caches; concurrency is resolved by row locks
// and unique constraints.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/embodied-reading/ledger/internal/apperr"
	"github.com/embodied-reading/ledger/internal/clock"
)

// Store wraps a pgx pool and a clock.
type Store struct {
	pool  *pgxpool.Pool
	clock clock.Clock
}

// New creates a Store from an existing pool.
func New(pool *pgxpool.Pool, clk clock.Clock) *Store {
	return &Store{pool: pool, clock: clk}
}

// Pool exposes the underlying pool (used by migrations and tests).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Book is a persisted physical book edition.
type Book struct {
	ID            string `json:"id"`
	ISBN          string `json:"isbn"`
	Title         string `json:"title"`
	Author        string `json:"author"`
	Edition       string `json:"edition"`
	Publisher     string `json:"publisher"`
	PublishedYear *int   `json:"publishedYear,omitempty"`
	PageCount     *int   `json:"pageCount,omitempty"`
	CreatedAt     string `json:"createdAt"`
}

// Session is a persisted reading session.
type Session struct {
	ID        string `json:"id"`
	BookID    string `json:"bookId"`
	Reader    string `json:"reader"`
	Status    string `json:"status"`
	CreatedAt string `json:"createdAt"`
}

// cachedResponse is a previously stored idempotent response.
type cachedResponse struct {
	Status int
	Body   []byte
}

// HashRequest returns a stable hash of a write request's identity for
// idempotency-key reuse detection.
func HashRequest(method, path string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// isUniqueViolation reports whether err is a Postgres unique_violation (23505).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

// claimIdempotency attempts to claim key within tx. It returns:
//   - (cached, nil) when a completed response already exists → caller replays it;
//   - (nil, nil) when the key was freshly claimed → caller performs the work;
//   - (nil, err) on reuse-with-different-body or infrastructure failure.
//
// Concurrent requests sharing a key serialize on the row lock taken by
// ON CONFLICT DO UPDATE: the second caller blocks until the first commits, then
// observes the completed row and replays it. If key is empty, idempotency is
// skipped and (nil, nil) is returned.
func (s *Store) claimIdempotency(ctx context.Context, tx pgx.Tx, key, method, path, hash string) (*cachedResponse, error) {
	if key == "" {
		return nil, nil
	}
	var (
		inserted   bool
		status     string
		storedHash string
		respStatus *int
		respBody   *string
	)
	err := tx.QueryRow(ctx, `
		INSERT INTO idempotency_keys (key, method, path, request_hash, status, created_at)
		VALUES ($1, $2, $3, $4, 'pending', $5)
		ON CONFLICT (key) DO UPDATE SET key = idempotency_keys.key
		RETURNING (xmax = 0) AS inserted, status, request_hash, response_status, response_body
	`, key, method, path, hash, clock.Format(s.clock.Now())).
		Scan(&inserted, &status, &storedHash, &respStatus, &respBody)
	if err != nil {
		return nil, fmt.Errorf("claim idempotency key: %w", err)
	}
	if inserted {
		return nil, nil
	}
	// Existing key: enforce that the same key is not reused for a different body.
	if storedHash != hash {
		return nil, apperr.New(apperr.CodeIdempotencyKeyReuse,
			"Idempotency-Key was already used for a different request").
			WithDetails(map[string]any{"key": key})
	}
	if respStatus == nil || respBody == nil {
		// Should be unreachable: committed rows are always completed.
		return nil, apperr.New(apperr.CodeInternal, "idempotency record is incomplete")
	}
	return &cachedResponse{Status: *respStatus, Body: []byte(*respBody)}, nil
}

// completeIdempotency records the final response for a claimed key.
func (s *Store) completeIdempotency(ctx context.Context, tx pgx.Tx, key string, status int, body []byte) error {
	if key == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `
		UPDATE idempotency_keys
		SET status = 'completed', response_status = $2, response_body = $3
		WHERE key = $1
	`, key, status, string(body))
	if err != nil {
		return fmt.Errorf("complete idempotency key: %w", err)
	}
	return nil
}
