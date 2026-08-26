// Command server runs the embodied reading ledger HTTP API. On startup it
// connects to Postgres, applies embedded migrations, and serves the API. A fresh
// clone can run it directly with `go run ./cmd/server` once `docker compose up -d
// db` is healthy.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/embodied-reading/ledger/internal/clock"
	"github.com/embodied-reading/ledger/internal/config"
	"github.com/embodied-reading/ledger/internal/httpapi"
	"github.com/embodied-reading/ledger/internal/store"
	"github.com/embodied-reading/ledger/migrations"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("server exited with error", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg := config.Load()
	logger.Info("starting server", "config", cfg.String())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	st, err := store.Connect(connectCtx, cfg.DatabaseURL, clock.System{})
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.Migrate(connectCtx, migrations.FS); err != nil {
		return err
	}
	logger.Info("migrations applied")

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           httpapi.NewServer(st, logger).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
