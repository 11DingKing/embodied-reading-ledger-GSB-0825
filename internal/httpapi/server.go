package httpapi

import (
	"net/http"

	"embodied-reading-ledger/internal/service"
)

type Server struct {
	mux     *http.ServeMux
	service *service.Service
}

func NewServer(svc *service.Service) *Server {
	s := &Server{
		mux:     http.NewServeMux(),
		service: svc,
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /books", s.handleCreateBook)
	s.mux.HandleFunc("POST /sessions", s.handleCreateSession)
	s.mux.HandleFunc("POST /sessions/{id}/events", s.handleAppendEvent)
	s.mux.HandleFunc("GET /sessions/{id}", s.handleGetSession)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

func (s *Server) Handler() http.Handler {
	return recoverMiddleware(loggingMiddleware(s.mux))
}
