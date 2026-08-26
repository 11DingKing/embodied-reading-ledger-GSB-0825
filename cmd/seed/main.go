// Command seed loads the deterministic fixture into the database. Run after the
// server (or its migrations) has created the schema:
//
//	go run ./cmd/seed
package main

import (
	"context"
	"log"
	"time"

	"github.com/embodied-reading/ledger/internal/clock"
	"github.com/embodied-reading/ledger/internal/config"
	"github.com/embodied-reading/ledger/internal/seed"
	"github.com/embodied-reading/ledger/internal/store"
	"github.com/embodied-reading/ledger/migrations"
)

func main() {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st, err := store.Connect(ctx, cfg.DatabaseURL, clock.System{})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer st.Close()

	if err := st.Migrate(ctx, migrations.FS); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	if err := seed.Apply(ctx, st.Pool()); err != nil {
		log.Fatalf("seed: %v", err)
	}
	log.Printf("seed applied: book=%s session=%s", seed.BookID, seed.SessionID)
}
