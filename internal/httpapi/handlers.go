package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/embodied-reading/ledger/internal/apperr"
	"github.com/embodied-reading/ledger/internal/clock"
	"github.com/embodied-reading/ledger/internal/domain"
	"github.com/embodied-reading/ledger/internal/store"
)

// maxBodyBytes caps request bodies to a sane size.
const maxBodyBytes = 1 << 20 // 1 MiB

// readBody reads and size-limits the request body so it can be both hashed for
// idempotency and decoded.
func readBody(r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(nil, r.Body, maxBodyBytes)
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, apperr.New(apperr.CodeValidation, "unable to read request body")
	}
	return b, nil
}

// ---- POST /books ----

type createBookRequest struct {
	ISBN          string `json:"isbn"`
	Title         string `json:"title"`
	Author        string `json:"author"`
	Edition       string `json:"edition"`
	Publisher     string `json:"publisher"`
	PublishedYear *int   `json:"publishedYear"`
	PageCount     *int   `json:"pageCount"`
}

func (s *Server) handleCreateBook(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	var req createBookRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeError(w, apperr.New(apperr.CodeValidation, "invalid JSON body: "+err.Error()))
		return
	}
	req.ISBN = strings.TrimSpace(req.ISBN)
	req.Title = strings.TrimSpace(req.Title)
	if req.ISBN == "" {
		s.writeError(w, apperr.New(apperr.CodeValidation, "isbn is required").
			WithDetails(map[string]any{"field": "isbn"}))
		return
	}
	if req.Title == "" {
		s.writeError(w, apperr.New(apperr.CodeValidation, "title is required").
			WithDetails(map[string]any{"field": "title"}))
		return
	}

	key := r.Header.Get("Idempotency-Key")
	status, respBody, err := s.store.DoIdempotent(r.Context(), key, r.Method, r.URL.Path, body,
		func(ctx context.Context, tx pgx.Tx) (int, []byte, error) {
			book, err := s.store.CreateBookTx(ctx, tx, store.CreateBookInput{
				ISBN:          req.ISBN,
				Title:         req.Title,
				Author:        strings.TrimSpace(req.Author),
				Edition:       strings.TrimSpace(req.Edition),
				Publisher:     strings.TrimSpace(req.Publisher),
				PublishedYear: req.PublishedYear,
				PageCount:     req.PageCount,
			})
			if err != nil {
				return 0, nil, err
			}
			out, err := json.Marshal(book)
			if err != nil {
				return 0, nil, err
			}
			return http.StatusCreated, out, nil
		})
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeRawJSON(w, status, respBody)
}

// ---- POST /sessions ----

type createSessionRequest struct {
	BookID string `json:"bookId"`
	Reader string `json:"reader"`
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	var req createSessionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeError(w, apperr.New(apperr.CodeValidation, "invalid JSON body: "+err.Error()))
		return
	}
	req.BookID = strings.TrimSpace(req.BookID)
	req.Reader = strings.TrimSpace(req.Reader)
	if req.BookID == "" {
		s.writeError(w, apperr.New(apperr.CodeValidation, "bookId is required").
			WithDetails(map[string]any{"field": "bookId"}))
		return
	}
	if req.Reader == "" {
		s.writeError(w, apperr.New(apperr.CodeValidation, "reader is required").
			WithDetails(map[string]any{"field": "reader"}))
		return
	}

	key := r.Header.Get("Idempotency-Key")
	status, respBody, err := s.store.DoIdempotent(r.Context(), key, r.Method, r.URL.Path, body,
		func(ctx context.Context, tx pgx.Tx) (int, []byte, error) {
			sess, err := s.store.CreateSessionTx(ctx, tx, req.BookID, req.Reader)
			if err != nil {
				return 0, nil, err
			}
			out, err := json.Marshal(sess)
			if err != nil {
				return 0, nil, err
			}
			return http.StatusCreated, out, nil
		})
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeRawJSON(w, status, respBody)
}

// ---- POST /sessions/{id}/events ----

type appendEventRequest struct {
	ExpectedSeq *int           `json:"expectedSeq"`
	Type        string         `json:"type"`
	OccurredAt  string         `json:"occurredAt"`
	Payload     domain.Payload `json:"payload"`
}

// appendEventResponse is the success envelope for an appended event.
type appendEventResponse struct {
	Seq        int               `json:"seq"`
	Type       string            `json:"type"`
	OccurredAt string            `json:"occurredAt"`
	RecordedAt string            `json:"recordedAt"`
	Session    store.SessionView `json:"session"`
}

func (s *Server) handleAppendEvent(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	body, err := readBody(r)
	if err != nil {
		s.writeError(w, err)
		return
	}
	var req appendEventRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeError(w, apperr.New(apperr.CodeValidation, "invalid JSON body: "+err.Error()))
		return
	}
	if req.ExpectedSeq == nil {
		s.writeError(w, apperr.New(apperr.CodeValidation, "expectedSeq is required").
			WithDetails(map[string]any{"field": "expectedSeq"}))
		return
	}
	if *req.ExpectedSeq < 1 {
		s.writeError(w, apperr.New(apperr.CodeValidation, "expectedSeq must be >= 1").
			WithDetails(map[string]any{"field": "expectedSeq", "value": *req.ExpectedSeq}))
		return
	}
	evType := domain.EventType(strings.TrimSpace(req.Type))
	if !domain.ValidType(evType) {
		s.writeError(w, apperr.New(apperr.CodeValidation, "unknown or missing event type").
			WithDetails(map[string]any{"field": "type", "value": req.Type}))
		return
	}
	occurredAt, err := clock.Parse("occurredAt", req.OccurredAt)
	if err != nil {
		s.writeError(w, err)
		return
	}

	key := r.Header.Get("Idempotency-Key")
	status, respBody, err := s.store.DoIdempotent(r.Context(), key, r.Method, r.URL.Path, body,
		func(ctx context.Context, tx pgx.Tx) (int, []byte, error) {
			res, err := s.store.AppendEventTx(ctx, tx, store.AppendEventInput{
				SessionID:   sessionID,
				ExpectedSeq: *req.ExpectedSeq,
				Type:        evType,
				OccurredAt:  occurredAt,
				Payload:     req.Payload,
			})
			if err != nil {
				return 0, nil, err
			}
			out, err := json.Marshal(appendEventResponse{
				Seq:        res.Seq,
				Type:       string(res.Type),
				OccurredAt: clock.Format(res.OccurredAt),
				RecordedAt: clock.Format(res.RecordedAt),
				Session:    res.View,
			})
			if err != nil {
				return 0, nil, err
			}
			return http.StatusCreated, out, nil
		})
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeRawJSON(w, status, respBody)
}

// ---- GET /sessions/{id} ----

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	view, err := s.store.GetSessionView(r.Context(), sessionID)
	if err != nil {
		s.writeError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, view)
}
