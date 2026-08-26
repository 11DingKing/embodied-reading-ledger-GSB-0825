package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"embodied-reading-ledger/internal/api"
	"embodied-reading-ledger/internal/store"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	testStore  *store.Store
	testServer *httptest.Server
	adminPool  *pgxpool.Pool
)

func testDatabaseURL() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5433/reading_ledger_test?sslmode=disable"
}

func adminDatabaseURL() string {
	if v := os.Getenv("ADMIN_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	var err error
	adminPool, err = pgxpool.New(ctx, adminDatabaseURL())
	if err != nil {
		fmt.Fprintln(os.Stderr, "SKIP: cannot connect to admin database:", err)
		os.Exit(1)
	}
	if _, err := adminPool.Exec(ctx,
		`SELECT 1 FROM pg_database WHERE datname = 'reading_ledger_test'`); err != nil {
		fmt.Fprintln(os.Stderr, "SKIP: admin database unreachable:", err)
		os.Exit(1)
	}
	if _, err := adminPool.Exec(ctx, `CREATE DATABASE reading_ledger_test`); err != nil &&
		!strings.Contains(err.Error(), "already exists") {
		fmt.Fprintln(os.Stderr, "create test database:", err)
		os.Exit(1)
	}

	testStore, err = store.New(ctx, testDatabaseURL())
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect test database:", err)
		os.Exit(1)
	}
	if err := testStore.Migrate(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}

	testServer = httptest.NewServer(api.NewServer(testStore))
	code := m.Run()
	testServer.Close()
	testStore.Close()
	adminPool.Close()
	os.Exit(code)
}

// reset truncates all tables so each test starts from a clean ledger.
func reset(t *testing.T) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testDatabaseURL())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE reading_events, reading_sessions, books, idempotency_keys`); err != nil {
		t.Fatal(err)
	}
}

type response struct {
	Status int
	Body   []byte
	JSON   map[string]any
}

func do(t *testing.T, method, path string, body any, idemKey string) response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, testServer.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	r := response{Status: resp.StatusCode, Body: raw}
	_ = json.Unmarshal(raw, &r.JSON)
	return r
}

func errorCode(r response) string {
	e, _ := r.JSON["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

func errorDetails(r response) map[string]any {
	e, _ := r.JSON["error"].(map[string]any)
	d, _ := e["details"].(map[string]any)
	return d
}

func createBook(t *testing.T, isbn string, totalPages int) string {
	t.Helper()
	r := do(t, "POST", "/books", map[string]any{
		"title": "The Peregrine", "author": "J. A. Baker",
		"edition": "NYRB 2005", "isbn": isbn, "total_pages": totalPages,
	}, "book-"+isbn)
	if r.Status != http.StatusCreated {
		t.Fatalf("create book: status %d body %s", r.Status, r.Body)
	}
	return r.JSON["book"].(map[string]any)["id"].(string)
}

func createSession(t *testing.T, bookID string) string {
	t.Helper()
	r := do(t, "POST", "/sessions", map[string]any{
		"book_id": bookID, "reader_name": "Ada",
	}, "sess-"+uuid.NewString())
	if r.Status != http.StatusCreated {
		t.Fatalf("create session: status %d body %s", r.Status, r.Body)
	}
	return r.JSON["session"].(map[string]any)["id"].(string)
}

func appendEvent(t *testing.T, sessionID string, seq int64, eventType, occurredAt string,
	extra map[string]any, key string) response {
	t.Helper()
	body := map[string]any{
		"type": eventType, "occurred_at": occurredAt, "expected_seq": seq,
	}
	for k, v := range extra {
		body[k] = v
	}
	return do(t, "POST", "/sessions/"+sessionID+"/events", body, key)
}

// TestFullSessionFlow records a realistic sitting and verifies the
// reconstruction: server-computed durations, reading minutes, last page,
// interruption count and reader reactions.
func TestFullSessionFlow(t *testing.T) {
	reset(t)
	bookID := createBook(t, "978-1", 192)
	sessionID := createSession(t, bookID)

	events := []struct {
		seq   int64
		typ   string
		at    string
		extra map[string]any
	}{
		{0, "SESSION_STARTED", "2026-02-01T18:00:00Z", nil},
		{1, "PAGE_REACHED", "2026-02-01T18:20:00Z", map[string]any{"page": 17}},
		{2, "PASSAGE_REACTED", "2026-02-01T18:35:00Z", map[string]any{
			"page": 41, "passage": "The hawk's flight over the coastal fog",
			"reaction": "I slowed down here; the prose feels like wind."}},
		{3, "INTERRUPTED", "2026-02-01T18:50:00Z", map[string]any{"interrupt_reason": "phone call"}},
		{4, "SESSION_ENDED", "2026-02-01T19:05:00Z", map[string]any{"page": 58}},
	}
	for i, e := range events {
		r := appendEvent(t, sessionID, e.seq, e.typ, e.at, e.extra,
			fmt.Sprintf("flow-%s-%d", sessionID, i))
		if r.Status != http.StatusCreated {
			t.Fatalf("append %s: status %d body %s", e.typ, r.Status, r.Body)
		}
		got := r.JSON["event"].(map[string]any)
		if got["occurred_at"] != e.at {
			t.Fatalf("occurred_at = %v, want %v (UTC RFC3339Nano)", got["occurred_at"], e.at)
		}
	}

	r := do(t, "GET", "/sessions/"+sessionID, nil, "")
	if r.Status != http.StatusOK {
		t.Fatalf("get session: status %d body %s", r.Status, r.Body)
	}
	if got := r.JSON["reading_minutes"].(float64); got != 65 {
		t.Fatalf("reading_minutes = %v, want 65", got)
	}
	if got := r.JSON["reading_seconds"].(float64); got != 3900 {
		t.Fatalf("reading_seconds = %v, want 3900", got)
	}
	if got := r.JSON["last_page"].(float64); got != 58 {
		t.Fatalf("last_page = %v, want 58", got)
	}
	if got := r.JSON["interruption_count"].(float64); got != 1 {
		t.Fatalf("interruption_count = %v, want 1", got)
	}
	reactions, _ := r.JSON["reactions"].([]any)
	if len(reactions) != 1 {
		t.Fatalf("reactions len = %d, want 1", len(reactions))
	}
	if got := reactions[0].(map[string]any)["reaction"].(string); got !=
		"I slowed down here; the prose feels like wind." {
		t.Fatalf("reaction = %q", got)
	}
	durations := []float64{0, 1200, 900, 900, 900}
	evs, _ := r.JSON["events"].([]any)
	if len(evs) != 5 {
		t.Fatalf("events len = %d, want 5", len(evs))
	}
	for i, ev := range evs {
		got := ev.(map[string]any)["duration_since_previous_seconds"].(float64)
		if got != durations[i] {
			t.Fatalf("event %d duration = %v, want %v", i, got, durations[i])
		}
	}
	if s := r.JSON["session"].(map[string]any); s["state"] != "ENDED" || s["last_seq"].(float64) != 5 {
		t.Fatalf("session = %v", s)
	}
}

// TestConcurrentAppendRace fires parallel appends with the same expectedSeq;
// exactly one must win and every loser must get 409 + current_seq.
func TestConcurrentAppendRace(t *testing.T) {
	reset(t)
	bookID := createBook(t, "978-2", 300)
	sessionID := createSession(t, bookID)

	r := appendEvent(t, sessionID, 0, "SESSION_STARTED", "2026-03-01T10:00:00Z", nil, "race-start-"+sessionID)
	if r.Status != http.StatusCreated {
		t.Fatalf("start: %d %s", r.Status, r.Body)
	}

	const writers = 16
	var wg sync.WaitGroup
	results := make([]response, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			at := fmt.Sprintf("2026-03-01T10:%02d:00Z", 10+i)
			body := map[string]any{
				"type": "PAGE_REACHED", "occurred_at": at,
				"expected_seq": 1, "page": 20 + i,
			}
			results[i] = do(t, "POST", "/sessions/"+sessionID+"/events", body,
				fmt.Sprintf("race-%d-%s", i, sessionID))
		}(i)
	}
	wg.Wait()

	var wins, conflicts int
	for i, r := range results {
		switch r.Status {
		case http.StatusCreated:
			wins++
		case http.StatusConflict:
			conflicts++
			if code := errorCode(r); code != "E_SEQ_CONFLICT" {
				t.Fatalf("writer %d conflict code = %s, want E_SEQ_CONFLICT", i, code)
			}
			if cur := errorDetails(r)["current_seq"].(float64); cur != 2 {
				t.Fatalf("writer %d current_seq = %v, want 2", i, cur)
			}
		default:
			t.Fatalf("writer %d: unexpected status %d body %s", i, r.Status, r.Body)
		}
	}
	if wins != 1 || conflicts != writers-1 {
		t.Fatalf("wins=%d conflicts=%d, want 1 and %d", wins, conflicts, writers-1)
	}

	r = do(t, "GET", "/sessions/"+sessionID, nil, "")
	if got := r.JSON["session"].(map[string]any)["last_seq"].(float64); got != 2 {
		t.Fatalf("last_seq = %v, want 2 (exactly one event appended)", got)
	}
}

// TestIdempotentReplay verifies that a replayed key returns the first
// response verbatim and never writes twice.
func TestIdempotentReplay(t *testing.T) {
	reset(t)
	bookID := createBook(t, "978-3", 300)
	sessionID := createSession(t, bookID)

	body := map[string]any{
		"type": "SESSION_STARTED", "occurred_at": "2026-04-01T09:00:00Z", "expected_seq": 0,
	}
	key := "replay-" + sessionID
	first := do(t, "POST", "/sessions/"+sessionID+"/events", body, key)
	if first.Status != http.StatusCreated {
		t.Fatalf("first: %d %s", first.Status, first.Body)
	}
	second := do(t, "POST", "/sessions/"+sessionID+"/events", body, key)
	if second.Status != http.StatusCreated {
		t.Fatalf("replay: %d %s", second.Status, second.Body)
	}
	if !bytes.Equal(first.Body, second.Body) {
		t.Fatalf("replay body differs:\nfirst:  %s\nsecond: %s", first.Body, second.Body)
	}

	r := do(t, "GET", "/sessions/"+sessionID, nil, "")
	if got := r.JSON["session"].(map[string]any)["last_seq"].(float64); got != 1 {
		t.Fatalf("last_seq = %v, want 1: replay must not write twice", got)
	}

	// Same key, different body → 422 E_IDEMPOTENCY_MISMATCH.
	mutated := map[string]any{
		"type": "SESSION_STARTED", "occurred_at": "2026-04-01T09:00:01Z", "expected_seq": 0,
	}
	mismatch := do(t, "POST", "/sessions/"+sessionID+"/events", mutated, key)
	if mismatch.Status != http.StatusUnprocessableEntity ||
		errorCode(mismatch) != "E_IDEMPOTENCY_MISMATCH" {
		t.Fatalf("mismatch: status %d body %s", mismatch.Status, mismatch.Body)
	}

	// Replay must also work for book creation (no duplicate ISBN insert).
	bookBody := map[string]any{
		"title": "The Rings of Saturn", "author": "W. G. Sebald",
		"edition": "ND 1998", "isbn": "978-4", "total_pages": 296,
	}
	b1 := do(t, "POST", "/books", bookBody, "replay-book-1")
	b2 := do(t, "POST", "/books", bookBody, "replay-book-1")
	if b1.Status != http.StatusCreated || b2.Status != http.StatusCreated {
		t.Fatalf("book replay: %d / %d", b1.Status, b2.Status)
	}
	if !bytes.Equal(b1.Body, b2.Body) {
		t.Fatalf("book replay bodies differ")
	}
}

// TestIllegalStateTransitions covers the stable error codes of the state
// machine and clock validation.
func TestIllegalStateTransitions(t *testing.T) {
	reset(t)
	bookID := createBook(t, "978-5", 192)
	sessionID := createSession(t, bookID)

	// Missing Idempotency-Key.
	r := do(t, "POST", "/sessions/"+sessionID+"/events", map[string]any{
		"type": "SESSION_STARTED", "occurred_at": "2026-05-01T10:00:00Z", "expected_seq": 0,
	}, "")
	if r.Status != http.StatusBadRequest || errorCode(r) != "E_IDEMPOTENCY_REQUIRED" {
		t.Fatalf("missing key: %d %s", r.Status, r.Body)
	}

	// Page reached before the session started.
	r = appendEvent(t, sessionID, 0, "PAGE_REACHED", "2026-05-01T10:00:00Z",
		map[string]any{"page": 3}, "t1-"+sessionID)
	if r.Status != http.StatusUnprocessableEntity || errorCode(r) != "E_PAGE_BEFORE_START" {
		t.Fatalf("page before start: %d %s", r.Status, r.Body)
	}

	// PASSAGE_REACTED as first event is also an illegal transition.
	r = appendEvent(t, sessionID, 0, "PASSAGE_REACTED", "2026-05-01T10:00:00Z",
		map[string]any{"reaction": "hi"}, "t2-"+sessionID)
	if r.Status != http.StatusUnprocessableEntity || errorCode(r) != "E_INVALID_STATE_TRANSITION" {
		t.Fatalf("react before start: %d %s", r.Status, r.Body)
	}

	// Happy path start.
	r = appendEvent(t, sessionID, 0, "SESSION_STARTED", "2026-05-01T10:00:00Z", nil, "t3-"+sessionID)
	if r.Status != http.StatusCreated {
		t.Fatalf("start: %d %s", r.Status, r.Body)
	}

	// Duplicate SESSION_STARTED.
	r = appendEvent(t, sessionID, 1, "SESSION_STARTED", "2026-05-01T10:05:00Z", nil, "t4-"+sessionID)
	if r.Status != http.StatusUnprocessableEntity || errorCode(r) != "E_INVALID_STATE_TRANSITION" {
		t.Fatalf("duplicate start: %d %s", r.Status, r.Body)
	}

	// Client clock moving backwards.
	r = appendEvent(t, sessionID, 1, "PAGE_REACHED", "2026-05-01T09:59:00Z",
		map[string]any{"page": 5}, "t5-"+sessionID)
	if r.Status != http.StatusUnprocessableEntity || errorCode(r) != "E_TIME_REGRESSION" {
		t.Fatalf("time regression: %d %s", r.Status, r.Body)
	}

	// Stale expectedSeq (non-concurrent): 409 + current_seq.
	r = appendEvent(t, sessionID, 5, "PAGE_REACHED", "2026-05-01T10:10:00Z",
		map[string]any{"page": 9}, "t6-"+sessionID)
	if r.Status != http.StatusConflict || errorCode(r) != "E_SEQ_CONFLICT" {
		t.Fatalf("seq conflict: %d %s", r.Status, r.Body)
	}
	if cur := errorDetails(r)["current_seq"].(float64); cur != 1 {
		t.Fatalf("current_seq = %v, want 1", cur)
	}

	// Page outside the edition's range.
	r = appendEvent(t, sessionID, 1, "PAGE_REACHED", "2026-05-01T10:10:00Z",
		map[string]any{"page": 9999}, "t7-"+sessionID)
	if r.Status != http.StatusBadRequest || errorCode(r) != "E_VALIDATION" {
		t.Fatalf("page out of range: %d %s", r.Status, r.Body)
	}

	// PASSAGE_REACTED without the reader's reaction.
	r = appendEvent(t, sessionID, 1, "PASSAGE_REACTED", "2026-05-01T10:10:00Z",
		map[string]any{"passage": "some passage"}, "t8-"+sessionID)
	if r.Status != http.StatusBadRequest || errorCode(r) != "E_VALIDATION" {
		t.Fatalf("reaction required: %d %s", r.Status, r.Body)
	}

	// End the session, then try to append after the end.
	r = appendEvent(t, sessionID, 1, "SESSION_ENDED", "2026-05-01T11:00:00Z",
		map[string]any{"page": 27}, "t9-"+sessionID)
	if r.Status != http.StatusCreated {
		t.Fatalf("end: %d %s", r.Status, r.Body)
	}
	r = appendEvent(t, sessionID, 2, "PAGE_REACHED", "2026-05-01T11:05:00Z",
		map[string]any{"page": 28}, "t10-"+sessionID)
	if r.Status != http.StatusConflict || errorCode(r) != "E_APPEND_AFTER_END" {
		t.Fatalf("append after end: %d %s", r.Status, r.Body)
	}

	// Unknown session → 404.
	r = do(t, "GET", "/sessions/"+uuid.NewString(), nil, "")
	if r.Status != http.StatusNotFound || errorCode(r) != "E_NOT_FOUND" {
		t.Fatalf("not found: %d %s", r.Status, r.Body)
	}
}

// TestAppendOnlyTrigger proves the event log rejects UPDATE/DELETE at the
// database level.
func TestAppendOnlyTrigger(t *testing.T) {
	reset(t)
	bookID := createBook(t, "978-6", 192)
	sessionID := createSession(t, bookID)
	r := appendEvent(t, sessionID, 0, "SESSION_STARTED", "2026-06-01T08:00:00Z", nil, "ao-"+sessionID)
	if r.Status != http.StatusCreated {
		t.Fatalf("start: %d %s", r.Status, r.Body)
	}

	pool, err := pgxpool.New(context.Background(), testDatabaseURL())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Exec(context.Background(),
		`UPDATE reading_events SET page = 1 WHERE session_id = $1`, sessionID); err == nil ||
		!strings.Contains(err.Error(), "append-only") {
		t.Fatalf("UPDATE should be rejected by append-only trigger, got %v", err)
	}
	if _, err := pool.Exec(context.Background(),
		`DELETE FROM reading_events WHERE session_id = $1`, sessionID); err == nil ||
		!strings.Contains(err.Error(), "append-only") {
		t.Fatalf("DELETE should be rejected by append-only trigger, got %v", err)
	}
}
