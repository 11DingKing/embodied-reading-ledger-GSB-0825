package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"embodied-reading-ledger/internal/store"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// ErrorBody is the structured error envelope returned for all non-2xx responses.
type ErrorBody struct {
	Error ErrorBodyInner `json:"error"`
}

type ErrorBodyInner struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// writeTxn executes fn inside a transaction, serializing by idempotency key when present.
// It returns the HTTP status code and the exact response body bytes to write.
func (s *Service) writeTxn(
	ctx context.Context,
	method, path string,
	idemKey string,
	reqHash []byte,
	fn func(ctx context.Context, tx pgx.Tx) (int, any, *APIError),
) (int, []byte, error) {
	if idemKey == "" {
		return s.runWithoutIdempotency(ctx, fn)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := store.AdvisoryLock(ctx, tx, idemKey); err != nil {
		return 0, nil, err
	}

	rec, err := store.New(tx).GetIdempotency(ctx, idemKey)
	if err == nil {
		if rec.RequestMethod != method || rec.RequestPath != path || !bytes.Equal(rec.RequestHash, reqHash) {
			return 0, nil, errIdempotencyReused()
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, nil, err
		}
		return rec.StatusCode, rec.ResponseBody, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return 0, nil, err
	}

	status, body, apiErr := fn(ctx, tx)
	if apiErr != nil {
		return 0, nil, apiErr
	}

	raw, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}

	if err := store.New(tx).InsertIdempotency(ctx, store.IdempotentRecord{
		Key:           idemKey,
		RequestMethod: method,
		RequestPath:   path,
		RequestHash:   reqHash,
		StatusCode:    status,
		ResponseBody:  raw,
	}); err != nil {
		return 0, nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, nil, err
	}
	return status, raw, nil
}

func (s *Service) runWithoutIdempotency(
	ctx context.Context,
	fn func(ctx context.Context, tx pgx.Tx) (int, any, *APIError),
) (int, []byte, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	status, body, apiErr := fn(ctx, tx)
	if apiErr != nil {
		return 0, nil, apiErr
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, nil, err
	}
	return status, raw, nil
}

// apiErrorToStatus is a convenience kept for documentation; handlers use APIError directly.
func apiErrorStatus(err *APIError) int {
	if err == nil {
		return http.StatusOK
	}
	return err.Status
}
