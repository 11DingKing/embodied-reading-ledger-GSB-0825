package api

import (
	"io"
	"net/http"
	"time"
)

type createBookRequest struct {
	ISBN       string `json:"isbn"`
	Title      string `json:"title"`
	Author     string `json:"author"`
	Edition    string `json:"edition"`
	TotalPages int    `json:"totalPages"`
}

type bookResponse struct {
	ID         string    `json:"id"`
	ISBN       string    `json:"isbn"`
	Title      string    `json:"title"`
	Author     string    `json:"author"`
	Edition    string    `json:"edition"`
	TotalPages int       `json:"totalPages"`
	CreatedAt  time.Time `json:"createdAt"`
}

func (s *Server) createBook(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeAPIError(w, validationError("cannot read request body", nil))
		return
	}

	var req createBookRequest
	if err := decodeStrict(raw, &req); err != nil {
		writeAPIError(w, validationError("invalid JSON request body: "+err.Error(), nil))
		return
	}

	missing := map[string]any{}
	if isBlank(req.ISBN) {
		missing["isbn"] = "isbn is required"
	}
	if isBlank(req.Title) {
		missing["title"] = "title is required"
	}
	if isBlank(req.Author) {
		missing["author"] = "author is required"
	}
	if req.TotalPages < 1 {
		missing["totalPages"] = "totalPages must be >= 1"
	}
	if len(missing) > 0 {
		writeAPIError(w, validationError("invalid book request", missing))
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

	book := bookResponse{
		ID:         newUUID(),
		ISBN:       req.ISBN,
		Title:      req.Title,
		Author:     req.Author,
		Edition:    req.Edition,
		TotalPages: req.TotalPages,
		CreatedAt:  time.Now().UTC(),
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO books (id, isbn, title, author, edition, total_pages, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		book.ID, book.ISBN, book.Title, book.Author, book.Edition, book.TotalPages, book.CreatedAt); err != nil {
		internalError(w, "insert book", err)
		return
	}

	body := writeJSONBuffer(book)
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
