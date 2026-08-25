package api

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	pool *pgxpool.Pool
	mux  *http.ServeMux
}

func NewServer(pool *pgxpool.Pool) *Server {
	s := &Server{pool: pool, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("POST /books", s.createBook)
	s.mux.HandleFunc("POST /sessions", s.createSession)
	s.mux.HandleFunc("POST /sessions/{id}/events", s.appendEvent)
	s.mux.HandleFunc("GET /sessions/{id}", s.getSession)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) Pool() *pgxpool.Pool { return s.pool }

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func decodeStrict(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func isBlank(s string) bool {
	for _, r := range s {
		if r != ' ' && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
	}
	return true
}
