package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"embodied-reading-ledger/internal/ledger"

	"github.com/jackc/pgx/v5"
)

type appendEventRequest struct {
	ExpectedSeq *int64 `json:"expectedSeq"`
	Event       struct {
		Type       string          `json:"type"`
		OccurredAt string          `json:"occurredAt"`
		Payload    json.RawMessage `json:"payload"`
	} `json:"event"`
}

type eventResponse struct {
	SessionID  string    `json:"sessionId"`
	Seq        int64     `json:"seq"`
	EventType  string    `json:"eventType"`
	OccurredAt time.Time `json:"occurredAt"`
	RecordedAt time.Time `json:"recordedAt"`
}

func (s *Server) appendEvent(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	if !isUUID(sessionID) {
		writeAPIError(w, validationError("session id must be a valid UUID", map[string]any{"sessionId": sessionID}))
		return
	}

	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeAPIError(w, validationError("cannot read request body", nil))
		return
	}

	var req appendEventRequest
	if err := decodeStrict(raw, &req); err != nil {
		writeAPIError(w, validationError("invalid JSON request body: "+err.Error(), nil))
		return
	}

	if req.ExpectedSeq == nil {
		writeAPIError(w, validationError("expectedSeq is required", map[string]any{"expectedSeq": "must be an integer >= 0"}))
		return
	}
	if *req.ExpectedSeq < 0 {
		writeAPIError(w, validationError("expectedSeq must be >= 0", map[string]any{"expectedSeq": *req.ExpectedSeq}))
		return
	}

	evType, ok := ledger.ParseEventType(req.Event.Type)
	if !ok {
		writeAPIError(w, validationError("unknown event type", map[string]any{
			"eventType": req.Event.Type,
			"allowed":   []string{"SESSION_STARTED", "PAGE_REACHED", "PASSAGE_REACTED", "INTERRUPTED", "SESSION_ENDED"},
		}))
		return
	}

	occurredAt, err := time.Parse(time.RFC3339Nano, req.Event.OccurredAt)
	if err != nil {
		writeAPIError(w, &APIError{
			Status:  http.StatusBadRequest,
			Code:    "INVALID_TIMESTAMP",
			Message: "occurredAt must be an RFC3339Nano timestamp, e.g. 2026-08-25T10:00:00.123456789Z",
			Details: map[string]any{"occurredAt": req.Event.OccurredAt},
		})
		return
	}
	occurredAt = occurredAt.UTC()

	payload := req.Event.Payload
	if len(bytes.TrimSpace(payload)) == 0 {
		payload = json.RawMessage("{}")
	}

	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		internalError(w, "begin tx", err)
		return
	}
	defer tx.Rollback(ctx)

	key := r.Header.Get("Idempotency-Key")
	fingerprint := requestFingerprint(r.Method, r.URL.Path, raw)
	replayStatus, replayBody, apiErr := idempotencyEnter(ctx, tx, key, fingerprint)
	if apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}
	if replayStatus != 0 {
		writeReplay(w, replayStatus, replayBody)
		return
	}

	var sessionStatus string
	var totalPages int
	err = tx.QueryRow(ctx,
		`SELECT rs.status, b.total_pages
		 FROM reading_sessions rs
		 JOIN books b ON b.id = rs.book_id
		 WHERE rs.id = $1
		 FOR UPDATE OF rs`, sessionID).
		Scan(&sessionStatus, &totalPages)
	if errors.Is(err, pgx.ErrNoRows) {
		writeAPIError(w, &APIError{
			Status:  http.StatusNotFound,
			Code:    "SESSION_NOT_FOUND",
			Message: "the reading session does not exist",
			Details: map[string]any{"sessionId": sessionID},
		})
		return
	}
	if err != nil {
		internalError(w, "lock session", err)
		return
	}

	var prev *ledger.Event
	var (
		lastSeq        int64
		lastEventType  string
		lastOccurredAt time.Time
	)
	err = tx.QueryRow(ctx,
		`SELECT seq, event_type, occurred_at
		 FROM events
		 WHERE session_id = $1
		 ORDER BY seq DESC
		 LIMIT 1`, sessionID).
		Scan(&lastSeq, &lastEventType, &lastOccurredAt)
	switch {
	case err == nil:
		prev = &ledger.Event{
			Seq:        lastSeq,
			Type:       ledger.EventType(lastEventType),
			OccurredAt: lastOccurredAt.UTC(),
		}
	case errors.Is(err, pgx.ErrNoRows):
	default:
		internalError(w, "load last event", err)
		return
	}

	if ruleErr := ledger.ValidateAppend(prev, totalPages, *req.ExpectedSeq, evType, occurredAt, payload); ruleErr != nil {
		status := http.StatusUnprocessableEntity
		if ruleErr.Code == "SEQUENCE_CONFLICT" {
			status = http.StatusConflict
		}
		writeAPIError(w, &APIError{
			Status:  status,
			Code:    ruleErr.Code,
			Message: ruleErr.Message,
			Details: ruleErr.Details,
		})
		return
	}

	newSeq := int64(1)
	if prev != nil {
		newSeq = prev.Seq + 1
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO events (session_id, seq, event_type, occurred_at, payload)
		 VALUES ($1, $2, $3, $4, $5::jsonb)`,
		sessionID, newSeq, string(evType), occurredAt, string(payload)); err != nil {
		internalError(w, "insert event", err)
		return
	}

	if evType == ledger.SessionEnded {
		if _, err := tx.Exec(ctx,
			`UPDATE reading_sessions SET status = 'ended' WHERE id = $1`,
			sessionID); err != nil {
			internalError(w, "close session", err)
			return
		}
	}

	var recordedAt time.Time
	if err := tx.QueryRow(ctx,
		`SELECT recorded_at FROM events WHERE session_id = $1 AND seq = $2`,
		sessionID, newSeq).Scan(&recordedAt); err != nil {
		internalError(w, "load recorded_at", err)
		return
	}

	resp := eventResponse{
		SessionID:  sessionID,
		Seq:        newSeq,
		EventType:  string(evType),
		OccurredAt: occurredAt,
		RecordedAt: recordedAt.UTC(),
	}
	body := writeJSONBuffer(resp)

	if err := idempotencyComplete(ctx, tx, key, http.StatusCreated, body); err != nil {
		internalError(w, "store idempotent response", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		internalError(w, "commit tx", err)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(body)
}
