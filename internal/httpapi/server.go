// Package httpapi wires the reading-ledger HTTP surface using only net/http.
// Handlers are thin: they parse and validate input, delegate to the store's
// transactional idempotent executor, and render structured JSON. All
// correctness guarantees (idempotency, sequencing, append-only) live in the
// store and the database.
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/embodied-reading/ledger/internal/apperr"
	"github.com/embodied-reading/ledger/internal/store"
)

// Server holds handler dependencies.
type Server struct {
	store  *store.Store
	logger *slog.Logger
}

// NewServer constructs a Server.
func NewServer(st *store.Store, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{store: st, logger: logger}
}

// Routes returns the http.Handler for the API using the net/http 1.22+ pattern
// mux (method + path patterns, path wildcards).
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /books", s.handleCreateBook)
	mux.HandleFunc("POST /sessions", s.handleCreateSession)
	mux.HandleFunc("POST /sessions/{id}/events", s.handleAppendEvent)
	mux.HandleFunc("GET /sessions/{id}", s.handleGetSession)
	return withRecovery(s.logger, mux)
}

// errorBody is the stable JSON envelope for every error response.
type errorBody struct {
	Error apperr.Error `json:"error"`
}

// writeJSON renders v as JSON with the given status.
func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Error("encode response", "err", err)
	}
}

// writeError maps err to a structured response with a stable code.
func (s *Server) writeError(w http.ResponseWriter, err error) {
	if ae := apperr.As(err); ae != nil {
		s.writeJSON(w, ae.HTTPStatus(), errorBody{Error: *ae})
		return
	}
	s.logger.Error("unhandled error", "err", err)
	s.writeJSON(w, http.StatusInternalServerError, errorBody{
		Error: apperr.Error{Code: apperr.CodeInternal, Message: "internal server error"},
	})
}

// writeRawJSON writes a pre-serialized JSON body (used to replay stored
// idempotent responses byte-for-byte).
func (s *Server) writeRawJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// decodeJSON strictly decodes a request body into dst, rejecting unknown fields.
// Used by handlers that do not also need the raw bytes for idempotency hashing.
func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return apperr.New(apperr.CodeValidation, "invalid JSON body: "+err.Error())
	}
	return nil
}

var _ = decodeJSON // retained utility; write handlers hash the raw body instead

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// withRecovery converts panics into structured 500s and logs them.
func withRecovery(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				logger.Error("panic recovered", "err", rec, "path", r.URL.Path)
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(errorBody{
					Error: apperr.Error{Code: apperr.CodeInternal, Message: "internal server error"},
				})
			}
		}()
		next.ServeHTTP(w, r)
	})
}
