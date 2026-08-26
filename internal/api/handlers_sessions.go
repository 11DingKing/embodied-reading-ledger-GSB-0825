package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"embodied-reading-ledger/internal/ledger"

	"github.com/jackc/pgx/v5"
)

type createSessionRequest struct {
	BookID    string `json:"bookId"`
	ReaderTag string `json:"readerTag"`
}

type sessionResponse struct {
	ID        string    `json:"id"`
	BookID    string    `json:"bookId"`
	ReaderTag string    `json:"readerTag"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

type bookSummary struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	Author     string `json:"author"`
	TotalPages int    `json:"totalPages"`
}

type eventView struct {
	Seq        int64           `json:"seq"`
	Type       string          `json:"type"`
	OccurredAt time.Time       `json:"occurredAt"`
	Payload    json.RawMessage `json:"payload"`
}

type sessionDetailResponse struct {
	ID                string            `json:"id"`
	BookID            string            `json:"bookId"`
	ReaderTag         string            `json:"readerTag"`
	Status            string            `json:"status"`
	CreatedAt         time.Time         `json:"createdAt"`
	Book              bookSummary       `json:"book"`
	StartedAt         *time.Time        `json:"startedAt"`
	EndedAt           *time.Time        `json:"endedAt"`
	LastPage          *int              `json:"lastPage"`
	MaxPage           int               `json:"maxPage"`
	InterruptionCount int               `json:"interruptionCount"`
	ReadingMinutes    float64           `json:"readingMinutes"`
	Reactions         []ledger.Reaction `json:"reactions"`
	Events            []eventView       `json:"events"`
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeAPIError(w, validationError("cannot read request body", nil))
		return
	}

	var req createSessionRequest
	if err := decodeStrict(raw, &req); err != nil {
		writeAPIError(w, validationError("invalid JSON request body: "+err.Error(), nil))
		return
	}

	missing := map[string]any{}
	if !isUUID(req.BookID) {
		missing["bookId"] = "bookId must be a valid UUID"
	}
	if isBlank(req.ReaderTag) {
		missing["readerTag"] = "readerTag is required"
	}
	if len(missing) > 0 {
		writeAPIError(w, validationError("invalid session request", missing))
		return
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

	var bookExists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM books WHERE id = $1)`, req.BookID).
		Scan(&bookExists); err != nil {
		internalError(w, "check book", err)
		return
	}
	if !bookExists {
		writeAPIError(w, &APIError{
			Status:  http.StatusNotFound,
			Code:    "BOOK_NOT_FOUND",
			Message: "the referenced book does not exist",
			Details: map[string]any{"bookId": req.BookID},
		})
		return
	}

	session := sessionResponse{
		ID:        newUUID(),
		BookID:    req.BookID,
		ReaderTag: req.ReaderTag,
		Status:    "open",
		CreatedAt: time.Now().UTC(),
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO reading_sessions (id, book_id, reader_tag, status, created_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		session.ID, session.BookID, session.ReaderTag, session.Status, session.CreatedAt); err != nil {
		internalError(w, "insert session", err)
		return
	}

	body := writeJSONBuffer(session)
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

func (s *Server) getSession(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	if !isUUID(sessionID) {
		writeAPIError(w, validationError("session id must be a valid UUID", map[string]any{"sessionId": sessionID}))
		return
	}

	ctx := r.Context()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		internalError(w, "begin tx", err)
		return
	}
	defer tx.Rollback(ctx)

	var (
		bookID, readerTag, status, bookTitle, bookAuthor string
		createdAt                                        time.Time
		book                                             bookSummary
	)
	err = tx.QueryRow(ctx,
		`SELECT rs.id, rs.book_id, rs.reader_tag, rs.status, rs.created_at,
		        b.title, b.author, b.total_pages
		 FROM reading_sessions rs
		 JOIN books b ON b.id = rs.book_id
		 WHERE rs.id = $1`, sessionID).
		Scan(&sessionID, &bookID, &readerTag, &status, &createdAt,
			&bookTitle, &bookAuthor, &book.TotalPages)
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
		internalError(w, "load session", err)
		return
	}
	book.ID = bookID
	book.Title = bookTitle
	book.Author = bookAuthor

	rows, err := tx.Query(ctx,
		`SELECT seq, event_type, occurred_at, payload
		 FROM events
		 WHERE session_id = $1
		 ORDER BY seq ASC`, sessionID)
	if err != nil {
		internalError(w, "load events", err)
		return
	}
	defer rows.Close()

	events := make([]ledger.Event, 0)
	views := make([]eventView, 0)
	for rows.Next() {
		var (
			seq        int64
			eventType  string
			occurredAt time.Time
			payload    []byte
		)
		if err := rows.Scan(&seq, &eventType, &occurredAt, &payload); err != nil {
			internalError(w, "scan event", err)
			return
		}
		occurredAt = occurredAt.UTC()
		events = append(events, ledger.Event{
			Seq:        seq,
			Type:       ledger.EventType(eventType),
			OccurredAt: occurredAt,
			Payload:    payload,
		})
		views = append(views, eventView{
			Seq:        seq,
			Type:       eventType,
			OccurredAt: occurredAt,
			Payload:    payload,
		})
	}
	if err := rows.Err(); err != nil {
		internalError(w, "iterate events", err)
		return
	}

	summary := ledger.Summarize(events)

	resp := sessionDetailResponse{
		ID:                sessionID,
		BookID:            bookID,
		ReaderTag:         readerTag,
		Status:            status,
		CreatedAt:         createdAt.UTC(),
		Book:              book,
		StartedAt:         summary.StartedAt,
		EndedAt:           summary.EndedAt,
		LastPage:          summary.LastPage,
		MaxPage:           summary.MaxPage,
		InterruptionCount: summary.InterruptionCount,
		ReadingMinutes:    summary.ReadingDuration.Minutes(),
		Reactions:         summary.Reactions,
		Events:            views,
	}

	writeJSON(w, http.StatusOK, resp)
}
