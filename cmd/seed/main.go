package main

import (
	"context"
	"log"

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

	if err := database.Seed(ctx, pool); err != nil {
		log.Fatalf("seed: %v", err)
	}
	log.Print("deterministic seed fixtures applied (idempotent)")
}
