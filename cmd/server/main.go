package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"embodied-reading-ledger/internal/api"
	"embodied-reading-ledger/internal/config"
	"embodied-reading-ledger/internal/database"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	pool, err := database.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("connect database: %v", err)
	}
	defer pool.Close()

	if err := database.Migrate(ctx, pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	log.Print("migrations applied")

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           api.NewServer(pool),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("reading ledger server listening on :%s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown: %v", err)
	}
	log.Print("server stopped")
}
