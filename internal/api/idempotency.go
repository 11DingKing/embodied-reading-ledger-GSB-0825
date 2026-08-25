package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/jackc/pgx/v5"
)

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("read random bytes: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func isUUID(s string) bool { return uuidRe.MatchString(s) }

func requestFingerprint(method, path string, rawBody []byte) string {
	h := sha256.Sum256([]byte(method + "\n" + path + "\n" + string(rawBody)))
	return hex.EncodeToString(h[:])
}

func idempotencyEnter(ctx context.Context, tx pgx.Tx, key, fingerprint string) (replayStatus int, replayBody []byte, apiErr *APIError) {
	if key == "" {
		return 0, nil, nil
	}

	var insertedKey string
	err := tx.QueryRow(ctx,
		`INSERT INTO idempotency_keys (key, request_fingerprint)
		 VALUES ($1, $2)
		 ON CONFLICT (key) DO NOTHING
		 RETURNING key`, key, fingerprint).Scan(&insertedKey)
	switch {
	case err == nil:
		return 0, nil, nil
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return 0, nil, &APIError{Status: http.StatusInternalServerError, Code: "INTERNAL", Message: "internal server error"}
	}

	var existingFingerprint string
	var storedStatus *int
	var storedBody []byte
	if err := tx.QueryRow(ctx,
		`SELECT request_fingerprint, response_status, response_body
		 FROM idempotency_keys
		 WHERE key = $1`, key).
		Scan(&existingFingerprint, &storedStatus, &storedBody); err != nil {
		return 0, nil, &APIError{Status: http.StatusInternalServerError, Code: "INTERNAL", Message: "internal server error"}
	}

	if existingFingerprint != fingerprint {
		return 0, nil, &APIError{
			Status:  http.StatusConflict,
			Code:    "IDEMPOTENCY_KEY_REUSED",
			Message: "Idempotency-Key was already used with a different request body",
			Details: map[string]any{"idempotencyKey": key},
		}
	}

	if storedStatus == nil {
		return 0, nil, &APIError{
			Status:  http.StatusConflict,
			Code:    "IDEMPOTENCY_IN_FLIGHT",
			Message: "another request with the same Idempotency-Key is still in progress",
			Details: map[string]any{"idempotencyKey": key},
		}
	}

	return *storedStatus, storedBody, nil
}

func idempotencyComplete(ctx context.Context, tx pgx.Tx, key string, status int, body []byte) error {
	if key == "" {
		return nil
	}
	_, err := tx.Exec(ctx,
		`UPDATE idempotency_keys
		 SET response_status = $2, response_body = $3
		 WHERE key = $1 AND response_status IS NULL`,
		key, status, string(body))
	return err
}
