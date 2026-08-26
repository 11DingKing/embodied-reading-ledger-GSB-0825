// Package api exposes the HTTP interface of the embodied reading ledger using
// only net/http (Go 1.22+ pattern routing).
package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"embodied-reading-ledger/internal/errs"
	"embodied-reading-ledger/internal/store"

	"github.com/google/uuid"
)

const idempotencyHeader = "Idempotency-Key"

// Server is the HTTP handler set.
type Server struct {
	st  *store.Store
	mux *http.ServeMux
}

// NewServer wires routes.
func NewServer(st *store.Store) *Server {
	s := &Server{st: st, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.healthz)
	s.mux.HandleFunc("POST /books", s.createBook)
	s.mux.HandleFunc("POST /sessions", s.createSession)
	s.mux.HandleFunc("POST /sessions/{id}/events", s.appendEvent)
	s.mux.HandleFunc("GET /sessions/{id}", s.getSession)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- request bodies ----

type createBookBody struct {
	Title      string `json:"title"`
	Author     string `json:"author"`
	Edition    string `json:"edition"`
	ISBN       string `json:"isbn"`
	TotalPages int    `json:"total_pages"`
}

type createSessionBody struct {
	BookID     string `json:"book_id"`
	ReaderName string `json:"reader_name"`
}

type appendEventBody struct {
	Type            string  `json:"type"`
	OccurredAt      string  `json:"occurred_at"`
	ExpectedSeq     *int64  `json:"expected_seq"`
	Page            *int    `json:"page"`
	Passage         *string `json:"passage"`
	Reaction        *string `json:"reaction"`
	InterruptReason *string `json:"interrupt_reason"`
}

// ---- handlers ----

func (s *Server) createBook(w http.ResponseWriter, r *http.Request) {
	key, raw, appErr := readWriteRequest(w, r)
	if appErr != nil {
		writeError(w, appErr)
		return
	}
	var body createBookBody
	if appErr := decode(raw, &body); appErr != nil {
		writeError(w, appErr)
		return
	}
	if strings.TrimSpace(body.Title) == "" || strings.TrimSpace(body.Author) == "" ||
		strings.TrimSpace(body.Edition) == "" || strings.TrimSpace(body.ISBN) == "" {
		writeError(w, errs.Validation("title, author, edition and isbn are required"))
		return
	}
	if body.TotalPages <= 0 {
		writeError(w, errs.Validation("total_pages must be a positive integer"))
		return
	}
	status, resp, appErr := s.st.CreateBook(r.Context(), key, store.HashRequest(raw), store.Book{
		Title:      body.Title,
		Author:     body.Author,
		Edition:    body.Edition,
		ISBN:       body.ISBN,
		TotalPages: body.TotalPages,
	})
	respond(w, status, resp, appErr)
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	key, raw, appErr := readWriteRequest(w, r)
	if appErr != nil {
		writeError(w, appErr)
		return
	}
	var body createSessionBody
	if appErr := decode(raw, &body); appErr != nil {
		writeError(w, appErr)
		return
	}
	bookID, err := uuid.Parse(body.BookID)
	if err != nil {
		writeError(w, errs.Validation("book_id must be a valid UUID"))
		return
	}
	if strings.TrimSpace(body.ReaderName) == "" {
		writeError(w, errs.Validation("reader_name is required"))
		return
	}
	status, resp, appErr := s.st.CreateSession(r.Context(), key, store.HashRequest(raw), store.Session{
		BookID:     bookID,
		ReaderName: body.ReaderName,
	})
	respond(w, status, resp, appErr)
}

func (s *Server) appendEvent(w http.ResponseWriter, r *http.Request) {
	sessionID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, errs.Validation("session id must be a valid UUID"))
		return
	}
	key, raw, appErr := readWriteRequest(w, r)
	if appErr != nil {
		writeError(w, appErr)
		return
	}
	var body appendEventBody
	if appErr := decode(raw, &body); appErr != nil {
		writeError(w, appErr)
		return
	}
	if body.ExpectedSeq == nil {
		writeError(w, errs.Validation("expected_seq is required"))
		return
	}
	if *body.ExpectedSeq < 0 {
		writeError(w, errs.Validation("expected_seq must be >= 0"))
		return
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, body.OccurredAt)
	if err != nil {
		writeError(w, errs.WithDetails(400, errs.CodeValidation,
			"occurred_at must be an RFC3339Nano timestamp",
			map[string]any{"got": body.OccurredAt}))
		return
	}
	status, resp, appErr := s.st.AppendEvent(r.Context(), key, store.HashRequest(raw), store.AppendEventInput{
		SessionID:       sessionID,
		ExpectedSeq:     *body.ExpectedSeq,
		Type:            body.Type,
		OccurredAt:      occurredAt,
		Page:            body.Page,
		Passage:         body.Passage,
		Reaction:        body.Reaction,
		InterruptReason: body.InterruptReason,
	})
	respond(w, status, resp, appErr)
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, errs.Validation("session id must be a valid UUID"))
		return
	}
	view, appErr := s.st.GetSession(r.Context(), id)
	if appErr != nil {
		writeError(w, appErr)
		return
	}
	type eventView struct {
		store.Event
		DurationSincePreviousSeconds float64 `json:"duration_since_previous_seconds"`
	}
	events := make([]eventView, 0, len(view.Events))
	for i, ev := range view.Events {
		events = append(events, eventView{ev, view.DurationsSeconds[i]})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"session":            view.Session,
		"book":               view.Book,
		"events":             events,
		"reading_seconds":    view.ReadingSeconds,
		"reading_minutes":    view.ReadingMinutes,
		"last_page":          view.LastPage,
		"interruption_count": view.InterruptionCount,
		"reactions":          view.Reactions,
	})
}

// ---- helpers ----

// readWriteRequest enforces the Idempotency-Key contract on write endpoints
// and returns the raw body for hashing.
func readWriteRequest(_ http.ResponseWriter, r *http.Request) (string, []byte, *errs.AppError) {
	key := strings.TrimSpace(r.Header.Get(idempotencyHeader))
	if key == "" {
		return "", nil, errs.WithDetails(400, errs.CodeIdempotencyRequired,
			"write requests must carry an Idempotency-Key header",
			map[string]any{"header": idempotencyHeader})
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return "", nil, errs.Internal(err)
	}
	return key, raw, nil
}

func decode(raw []byte, v any) *errs.AppError {
	if err := json.Unmarshal(raw, v); err != nil {
		return errs.Validation("invalid JSON body: " + err.Error())
	}
	return nil
}

func respond(w http.ResponseWriter, status int, body []byte, appErr *errs.AppError) {
	if appErr != nil {
		writeError(w, appErr)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body) //nolint:errcheck
}

func writeError(w http.ResponseWriter, appErr *errs.AppError) {
	writeJSON(w, appErr.Status, map[string]any{"error": appErr})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
