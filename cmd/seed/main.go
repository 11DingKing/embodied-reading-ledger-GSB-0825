package main

import (
	"context"
	"log"
	"os"
	"time"

	"embodied-reading-ledger/internal/config"
	"embodied-reading-ledger/internal/database"
	"embodied-reading-ledger/internal/seed"
)

func main() {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := database.New(ctx, cfg.DatabaseURL, cfg.MaxConns, cfg.MaxConnLife)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer db.Close()

	if err := db.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	if err := seed.Apply(ctx, db.Pool); err != nil {
		log.Fatalf("seed: %v", err)
	}
	log.Printf("seed applied (book=%s session=%s)", seed.BookID, seed.SessionID)
	os.Exit(0)
}
