package httpapi

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"embodied-reading-ledger/internal/service"
)

const maxBodyBytes = 1 << 20 // 1 MiB

const idempotencyHeader = "Idempotency-Key"

// readRequestJSON reads the raw body (for idempotency hashing) and decodes it into dst.
func readRequestJSON(r *http.Request, dst any) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, errors.New("request body is empty")
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return nil, err
	}
	return body, nil
}

func hashBody(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

func idempotencyKey(r *http.Request) string {
	return strings.TrimSpace(r.Header.Get(idempotencyHeader))
}

func badRequest(msg string) *service.APIError {
	return &service.APIError{Status: 400, Code: service.CodeInvalidRequest, Message: msg}
}

func (s *Server) handleCreateBook(w http.ResponseWriter, r *http.Request) {
	var req service.CreateBookRequest
	body, err := readRequestJSON(r, &req)
	if err != nil {
		writeAPIError(w, badRequest("invalid JSON: "+err.Error()))
		return
	}
	status, raw, err := s.service.CreateBook(r.Context(), req, idempotencyKey(r), hashBody(body))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeBytes(w, status, raw)
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var req service.CreateSessionRequest
	body, err := readRequestJSON(r, &req)
	if err != nil {
		writeAPIError(w, badRequest("invalid JSON: "+err.Error()))
		return
	}
	status, raw, err := s.service.CreateSession(r.Context(), req, idempotencyKey(r), hashBody(body))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeBytes(w, status, raw)
}

func (s *Server) handleAppendEvent(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	var req service.AppendEventRequest
	body, err := readRequestJSON(r, &req)
	if err != nil {
		writeAPIError(w, badRequest("invalid JSON: "+err.Error()))
		return
	}
	status, raw, err := s.service.AppendEvent(r.Context(), sessionID, req, idempotencyKey(r), hashBody(body))
	if err != nil {
		writeAPIError(w, err)
		return
	}
	writeBytes(w, status, raw)
}

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	detail, apiErr := s.service.GetSession(r.Context(), sessionID)
	if apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}
