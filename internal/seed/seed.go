package seed

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed seed.sql
var seedSQL string

// Apply inserts the deterministic seed data. It is idempotent.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, seedSQL); err != nil {
		return fmt.Errorf("apply seed: %w", err)
	}
	return nil
}

const (
	BookID    = "a0000000-0000-0000-0000-000000000001"
	SessionID = "b0000000-0000-0000-0000-000000000001"
)
