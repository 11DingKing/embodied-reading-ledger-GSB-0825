// Package integration holds end-to-end tests that run the full HTTP stack
// against a real PostgreSQL 16 instance (the docker-compose db). They exercise
// the guarantees that only surface with a real database: concurrent append
// races, idempotent replay, and illegal state transitions.
//
// Each test run isolates itself in a freshly created, uniquely named schema so
// parallel packages and repeated runs never collide. If DATABASE_URL is unset it
// falls back to the docker-compose default (host port 5439). Tests skip (not
// fail) when no database is reachable, so `go test ./...` stays green on a
// machine without the container — but the acceptance path expects the db up.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/embodied-reading/ledger/internal/clock"
	"github.com/embodied-reading/ledger/internal/config"
	"github.com/embodied-reading/ledger/internal/httpapi"
	"github.com/embodied-reading/ledger/internal/store"
	"github.com/embodied-reading/ledger/migrations"
)

// testEnv bundles a running test server backed by an isolated schema.
type testEnv struct {
	server *httptest.Server
	store  *store.Store
	pool   *pgxpool.Pool
	schema string
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return config.DefaultDatabaseURL
}

// newTestEnv connects to Postgres, creates a unique schema, applies migrations
// into it, and starts the HTTP server. It skips the test if the DB is
// unreachable.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	ctx := context.Background()

	baseCfg, err := pgxpool.ParseConfig(databaseURL())
	if err != nil {
		t.Fatalf("parse db url: %v", err)
	}
	// Unique schema per test for isolation; set as the sole search_path so every
	// pooled connection reads and writes inside it.
	schema := fmt.Sprintf("t_%d", time.Now().UnixNano())
	baseCfg.ConnConfig.RuntimeParams["search_path"] = schema

	// The schema must exist before the pool's connections try to use it as their
	// search_path, so create it via a one-off connection on the public path.
	if err := createSchema(ctx, schema); err != nil {
		t.Skipf("skipping integration test: cannot reach database at %s: %v", databaseURL(), err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, baseCfg)
	if err != nil {
		t.Skipf("skipping integration test: cannot create pool: %v", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		t.Skipf("skipping integration test: database unreachable at %s: %v", databaseURL(), err)
	}

	st := store.New(pool, clock.System{})
	if err := st.Migrate(ctx, migrations.FS); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}

	srv := httptest.NewServer(httpapi.NewServer(st, nil).Routes())

	env := &testEnv{server: srv, store: st, pool: pool, schema: schema}
	t.Cleanup(func() {
		srv.Close()
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
		pool.Close()
	})
	return env
}

// createSchema opens a short-lived connection on the default search_path and
// creates the isolation schema, so the pool can then use it as search_path.
func createSchema(ctx context.Context, schema string) error {
	cfg, err := pgxpool.ParseConfig(databaseURL())
	if err != nil {
		return err
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer p.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := p.Ping(pingCtx); err != nil {
		return err
	}
	_, err = p.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	return err
}

// do performs an HTTP request against the test server and returns status and
// decoded JSON (as raw bytes for flexible assertions).
func (e *testEnv) do(t *testing.T, method, path, idemKey string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, e.server.URL+path, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, out
}

// createBookAndSession is a helper returning a fresh book id and session id.
func (e *testEnv) createBookAndSession(t *testing.T) (string, string) {
	t.Helper()
	status, body := e.do(t, http.MethodPost, "/books", "", map[string]any{
		"isbn": "9780374528379", "title": "The Brothers Karamazov",
	})
	if status != http.StatusCreated {
		t.Fatalf("create book: status %d body %s", status, body)
	}
	var book struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &book); err != nil {
		t.Fatalf("unmarshal book: %v", err)
	}

	status, body = e.do(t, http.MethodPost, "/sessions", "", map[string]any{
		"bookId": book.ID, "reader": "Ada",
	})
	if status != http.StatusCreated {
		t.Fatalf("create session: status %d body %s", status, body)
	}
	var sess struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &sess); err != nil {
		t.Fatalf("unmarshal session: %v", err)
	}
	return book.ID, sess.ID
}
