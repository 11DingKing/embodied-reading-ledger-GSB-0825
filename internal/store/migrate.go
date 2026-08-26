package store

import (
	"context"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/embodied-reading/ledger/internal/clock"
)

// Connect opens a pgx pool against databaseURL and verifies connectivity.
func Connect(ctx context.Context, databaseURL string, clk clock.Clock) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return New(pool, clk), nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Migrate applies every *.sql migration from migFS in lexical filename order.
// Migrations are written to be idempotent (IF NOT EXISTS / OR REPLACE), so
// re-running them on an existing database is safe.
func (s *Store) Migrate(ctx context.Context, migFS fs.FS) error {
	entries, err := fs.ReadDir(migFS, ".")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".sql" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		sqlBytes, err := fs.ReadFile(migFS, name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := s.pool.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	return nil
}
