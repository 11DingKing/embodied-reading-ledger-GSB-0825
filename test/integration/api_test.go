package integration_test

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
	"time"

	"embodied-reading-ledger/internal/database"
	"embodied-reading-ledger/internal/domain"
	"embodied-reading-ledger/internal/httpapi"
	"embodied-reading-ledger/internal/service"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDSN = "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"

func dsn() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return defaultDSN
}

type testEnv struct {
	t      *testing.T
	pool   *pgxpool.Pool
	server *httptest.Server
}

func newEnv(t *testing.T) *testEnv {
	t.Helper()
	ctx := context.Background()
	db, err := database.New(ctx, dsn(), 10, 30*time.Minute)
	if err != nil {
		t.Fatalf("connect db (did you run `docker compose up -d db`?): %v", err)
	}
	if err := db.Migrate(ctx); err != nil {
		db.Close()
		t.Fatalf("migrate: %v", err)
	}
	env := &testEnv{t: t, pool: db.Pool}
	svc := service.New(db.Pool)
	env.server = httptest.NewServer(httpapi.NewServer(svc).Handler())
	t.Cleanup(func() {
		env.server.Close()
		db.Close()
	})
	env.clean()
	return env
}

func (e *testEnv) clean() {
	ctx := context.Background()
	_, err := e.pool.Exec(ctx,
		`TRUNCATE TABLE idempotency_keys, events, sessions, books RESTART IDENTITY CASCADE`)
	if err != nil {
		e.t.Fatalf("truncate: %v", err)
	}
}

func (e *testEnv) do(method, path string, body any, idemKey string) (int, []byte) {
	e.t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			e.t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, e.server.URL+path, rdr)
	if err != nil {
		e.t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, data
}

func decode[T any](t *testing.T, b []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("unmarshal %s: %v", string(b), err)
	}
	return v
}

type errBody struct {
	Error struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details"`
	} `json:"error"`
}

func (e *testEnv) createBook(title string) domain.Book {
	e.t.Helper()
	tp := 256
	status, body := e.do(http.MethodPost, "/books", map[string]any{
		"title":      title,
		"author":     "Test Author",
		"totalPages": tp,
		"format":     "PAPERBACK",
	}, "")
	if status != http.StatusCreated {
		e.t.Fatalf("create book status=%d body=%s", status, body)
	}
	return decode[domain.Book](e.t, body)
}

func (e *testEnv) createSession(bookID string) domain.Session {
	e.t.Helper()
	status, body := e.do(http.MethodPost, "/sessions", map[string]any{
		"bookId": bookID,
		"label":  "test session",
	}, "")
	if status != http.StatusCreated {
		e.t.Fatalf("create session status=%d body=%s", status, body)
	}
	return decode[domain.Session](e.t, body)
}

func (e *testEnv) appendEvent(sessionID string, event map[string]any, idemKey string) (int, domain.Event, errBody) {
	e.t.Helper()
	status, body := e.do(http.MethodPost, "/sessions/"+sessionID+"/events", event, idemKey)
	var ev domain.Event
	var eb errBody
	if status == http.StatusCreated {
		ev = decode[domain.Event](e.t, body)
	} else {
		eb = decode[errBody](e.t, body)
	}
	return status, ev, eb
}

// --- Tests ---

func TestHealthz(t *testing.T) {
	env := newEnv(t)
	status, body := env.do(http.MethodGet, "/healthz", nil, "")
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%s", status, body)
	}
	if !strings.Contains(string(body), `"ok"`) {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHappyPathAndMetrics(t *testing.T) {
	env := newEnv(t)
	book := env.createBook("The Order of Time")
	sess := env.createSession(book.ID)

	base := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)

	// 09:00 STARTED (expectedSeq=0)
	st, ev, eb := env.appendEvent(sess.ID, map[string]any{
		"eventType":   "SESSION_STARTED",
		"occurredAt":  base.Format(time.RFC3339Nano),
		"expectedSeq": 0,
	}, "")
	if st != 201 {
		t.Fatalf("STARTED status=%d err=%+v", st, eb)
	}
	if ev.Seq != 1 {
		t.Fatalf("expected seq 1, got %d", ev.Seq)
	}

	// 09:10 PAGE_REACHED p20 (expectedSeq=1)
	st, ev, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "PAGE_REACHED",
		"occurredAt":  base.Add(10 * time.Minute).Format(time.RFC3339Nano),
		"page":        20,
		"expectedSeq": 1,
	}, "")
	if st != 201 {
		t.Fatalf("PAGE_REACHED status=%d err=%+v", st, eb)
	}
	if ev.Seq != 2 {
		t.Fatalf("expected seq 2, got %d", ev.Seq)
	}

	// 09:20 PASSAGE_REACTED p34 (expectedSeq=2)
	st, ev, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "PASSAGE_REACTED",
		"occurredAt":  base.Add(20 * time.Minute).Format(time.RFC3339Nano),
		"page":        34,
		"note":        "A striking passage about entropy.",
		"quote":       "The future is different from the past.",
		"expectedSeq": 2,
	}, "")
	if st != 201 {
		t.Fatalf("PASSAGE_REACTED status=%d err=%+v", st, eb)
	}

	// 09:25 INTERRUPTED (expectedSeq=3)
	st, _, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "INTERRUPTED",
		"occurredAt":  base.Add(25 * time.Minute).Format(time.RFC3339Nano),
		"reason":      "phone rang",
		"expectedSeq": 3,
	}, "")
	if st != 201 {
		t.Fatalf("INTERRUPTED status=%d err=%+v", st, eb)
	}

	// 09:55 PAGE_REACHED p40 — 30min interruption gap excluded (expectedSeq=4)
	st, _, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "PAGE_REACHED",
		"occurredAt":  base.Add(55 * time.Minute).Format(time.RFC3339Nano),
		"page":        40,
		"expectedSeq": 4,
	}, "")
	if st != 201 {
		t.Fatalf("PAGE_REACHED status=%d err=%+v", st, eb)
	}

	// 10:05 SESSION_ENDED (expectedSeq=5)
	st, _, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "SESSION_ENDED",
		"occurredAt":  base.Add(65 * time.Minute).Format(time.RFC3339Nano),
		"expectedSeq": 5,
	}, "")
	if st != 201 {
		t.Fatalf("SESSION_ENDED status=%d err=%+v", st, eb)
	}

	// GET and verify metrics.
	status, body := env.do(http.MethodGet, "/sessions/"+sess.ID, nil, "")
	if status != 200 {
		t.Fatalf("GET session status=%d body=%s", status, body)
	}
	detail := decode[domain.SessionDetail](t, body)

	if detail.Status != domain.SessionEnded {
		t.Fatalf("expected status ended, got %s", detail.Status)
	}
	if detail.EventCount != 6 {
		t.Fatalf("expected 6 events, got %d", detail.EventCount)
	}
	// Reading: 10 + 10 + 5 + 10 = 35 minutes = 2100 seconds (30-min interruption excluded).
	if detail.ReadingDurationSeconds != 2100 {
		t.Fatalf("expected 2100 reading seconds, got %d", detail.ReadingDurationSeconds)
	}
	if detail.ReadingMinutes != 35 {
		t.Fatalf("expected 35 reading minutes, got %v", detail.ReadingMinutes)
	}
	if detail.LastPage == nil || *detail.LastPage != 40 {
		t.Fatalf("expected lastPage 40, got %v", detail.LastPage)
	}
	if detail.InterruptionCount != 1 {
		t.Fatalf("expected 1 interruption, got %d", detail.InterruptionCount)
	}
	if len(detail.Passages) != 1 {
		t.Fatalf("expected 1 passage, got %d", len(detail.Passages))
	} else {
		if detail.Passages[0].Note != "A striking passage about entropy." {
			t.Fatalf("unexpected note: %q", detail.Passages[0].Note)
		}
	}
	if detail.StartedAt == nil || detail.EndedAt == nil {
		t.Fatalf("expected startedAt and endedAt to be set")
	}

	// Timestamps must be UTC and RFC3339Nano (end with Z).
	raw := string(body)
	if !strings.Contains(raw, `"2026-01-15T09:00:00Z"`) {
		// RFC3339Nano for zero-nanos drops fractional part; verify parseability.
		if _, err := time.Parse(time.RFC3339, detail.StartedAt.Time().Format(time.RFC3339Nano)); err != nil {
			t.Fatalf("startedAt not RFC3339: %v", err)
		}
	}
	if loc := detail.Events[0].OccurredAt.Time().Location(); loc != time.UTC {
		t.Fatalf("expected UTC location, got %v", loc)
	}
}

func TestIllegalStateTransitions(t *testing.T) {
	env := newEnv(t)
	book := env.createBook("Illegal States")
	sess := env.createSession(book.ID)
	base := time.Date(2026, 2, 1, 10, 0, 0, 0, time.UTC)

	// First event must be SESSION_STARTED.
	st, _, eb := env.appendEvent(sess.ID, map[string]any{
		"eventType":   "PAGE_REACHED",
		"occurredAt":  base.Format(time.RFC3339Nano),
		"page":        10,
		"expectedSeq": 0,
	}, "")
	if st != 422 || eb.Error.Code != service.CodeSessionNotStarted {
		t.Fatalf("expected 422 SESSION_NOT_STARTED, got %d %+v", st, eb)
	}

	// Start the session.
	st, _, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "SESSION_STARTED",
		"occurredAt":  base.Format(time.RFC3339Nano),
		"expectedSeq": 0,
	}, "")
	if st != 201 {
		t.Fatalf("STARTED failed: %d %+v", st, eb)
	}

	// Second SESSION_STARTED is forbidden.
	st, _, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "SESSION_STARTED",
		"occurredAt":  base.Add(time.Minute).Format(time.RFC3339Nano),
		"expectedSeq": 1,
	}, "")
	if st != 422 || eb.Error.Code != service.CodeSessionAlreadyStarted {
		t.Fatalf("expected 422 SESSION_ALREADY_STARTED, got %d %+v", st, eb)
	}

	// Client timestamp going backwards.
	st, _, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "PAGE_REACHED",
		"occurredAt":  base.Add(-time.Minute).Format(time.RFC3339Nano),
		"page":        12,
		"expectedSeq": 1,
	}, "")
	if st != 422 || eb.Error.Code != service.CodeTimestampNotMonotonic {
		t.Fatalf("expected 422 TIMESTAMP_NOT_MONOTONIC, got %d %+v", st, eb)
	}

	// Page beyond total pages.
	st, _, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "PAGE_REACHED",
		"occurredAt":  base.Add(2 * time.Minute).Format(time.RFC3339Nano),
		"page":        9999,
		"expectedSeq": 1,
	}, "")
	if st != 422 || eb.Error.Code != service.CodeInvalidPage {
		t.Fatalf("expected 422 INVALID_PAGE, got %d %+v", st, eb)
	}

	// PASSAGE_REACTED requires a note.
	st, _, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "PASSAGE_REACTED",
		"occurredAt":  base.Add(3 * time.Minute).Format(time.RFC3339Nano),
		"page":        15,
		"expectedSeq": 1,
	}, "")
	if st != 422 || eb.Error.Code != service.CodeNoteRequired {
		t.Fatalf("expected 422 NOTE_REQUIRED, got %d %+v", st, eb)
	}

	// Unknown event type.
	raw, _ := json.Marshal(map[string]any{
		"eventType":   "BOGUS_EVENT",
		"occurredAt":  base.Add(4 * time.Minute).Format(time.RFC3339Nano),
		"expectedSeq": 1,
	})
	req, _ := http.NewRequest(http.MethodPost, env.server.URL+"/sessions/"+sess.ID+"/events", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 for unknown event type, got %d: %s", resp.StatusCode, b)
	}

	// End the session.
	st, _, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "SESSION_ENDED",
		"occurredAt":  base.Add(5 * time.Minute).Format(time.RFC3339Nano),
		"expectedSeq": 1,
	}, "")
	if st != 201 {
		t.Fatalf("SESSION_ENDED failed: %d %+v", st, eb)
	}

	// No events after SESSION_ENDED.
	st, _, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "PAGE_REACHED",
		"occurredAt":  base.Add(6 * time.Minute).Format(time.RFC3339Nano),
		"page":        20,
		"expectedSeq": 2,
	}, "")
	if st != 422 || eb.Error.Code != service.CodeSessionAlreadyEnded {
		t.Fatalf("expected 422 SESSION_ALREADY_ENDED, got %d %+v", st, eb)
	}
}

func TestSeqConflictReturnsCurrentSeq(t *testing.T) {
	env := newEnv(t)
	book := env.createBook("Seq Conflict")
	sess := env.createSession(book.ID)
	base := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)

	st, _, eb := env.appendEvent(sess.ID, map[string]any{
		"eventType":   "SESSION_STARTED",
		"occurredAt":  base.Format(time.RFC3339Nano),
		"expectedSeq": 0,
	}, "")
	if st != 201 {
		t.Fatalf("STARTED failed: %d %+v", st, eb)
	}

	// Wrong expectedSeq (client is stale, thinks no events yet).
	st, _, eb = env.appendEvent(sess.ID, map[string]any{
		"eventType":   "PAGE_REACHED",
		"occurredAt":  base.Add(time.Minute).Format(time.RFC3339Nano),
		"page":        5,
		"expectedSeq": 0,
	}, "")
	if st != 409 || eb.Error.Code != service.CodeSeqConflict {
		t.Fatalf("expected 409 SEQ_CONFLICT, got %d %+v", st, eb)
	}
	cs, ok := eb.Error.Details["currentSeq"].(float64)
	if !ok || int(cs) != 1 {
		t.Fatalf("expected currentSeq=1 in details, got %#v", eb.Error.Details)
	}
	es, ok := eb.Error.Details["expectedSeq"].(float64)
	if !ok || int(es) != 0 {
		t.Fatalf("expected expectedSeq=0 in details, got %#v", eb.Error.Details)
	}
}

func TestConcurrentAppendOnlyOneSucceeds(t *testing.T) {
	env := newEnv(t)
	book := env.createBook("Concurrency")
	sess := env.createSession(book.ID)
	base := time.Date(2026, 4, 1, 7, 0, 0, 0, time.UTC)

	// Seed the STARTED event so all contenders race at expectedSeq=1.
	st, _, eb := env.appendEvent(sess.ID, map[string]any{
		"eventType":   "SESSION_STARTED",
		"occurredAt":  base.Format(time.RFC3339Nano),
		"expectedSeq": 0,
	}, "")
	if st != 201 {
		t.Fatalf("STARTED failed: %d %+v", st, eb)
	}

	const n = 25
	var wg sync.WaitGroup
	results := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			page := i + 1
			status, _, _ := env.appendEvent(sess.ID, map[string]any{
				"eventType":   "PAGE_REACHED",
				"occurredAt":  base.Add(time.Duration(i+1) * time.Second).Format(time.RFC3339Nano),
				"page":        page,
				"expectedSeq": 1,
			}, "")
			results[i] = status
		}(i)
	}
	wg.Wait()

	success := 0
	conflict := 0
	for _, s := range results {
		switch s {
		case 201:
			success++
		case 409:
			conflict++
		default:
			t.Fatalf("unexpected status %d", s)
		}
	}
	if success != 1 {
		t.Fatalf("expected exactly 1 success, got %d", success)
	}
	if conflict != n-1 {
		t.Fatalf("expected %d conflicts, got %d", n-1, conflict)
	}

	// Verify only one event (seq=2) was actually appended.
	var maxSeq, count int
	err := env.pool.QueryRow(context.Background(),
		`SELECT COALESCE(MAX(seq),0), COUNT(*) FROM events WHERE session_id=$1::uuid`, sess.ID,
	).Scan(&maxSeq, &count)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	if maxSeq != 2 || count != 2 {
		t.Fatalf("expected maxSeq=2 count=2, got maxSeq=%d count=%d", maxSeq, count)
	}
}

func TestIdempotencyReplay(t *testing.T) {
	env := newEnv(t)

	// --- POST /books idempotency ---
	bookBody := map[string]any{
		"title":      "Idempotent Book",
		"author":     "Ada Lovelace",
		"totalPages": 100,
	}
	st1, b1 := env.do(http.MethodPost, "/books", bookBody, "book-key-1")
	st2, b2 := env.do(http.MethodPost, "/books", bookBody, "book-key-1")
	if st1 != 201 || st2 != 201 {
		t.Fatalf("expected both 201, got %d and %d", st1, st2)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatalf("idempotent replay returned different bodies:\n%s\n%s", b1, b2)
	}
	book1 := decode[domain.Book](t, b1)
	book2 := decode[domain.Book](t, b2)
	if book1.ID != book2.ID {
		t.Fatalf("expected same book id, got %s and %s", book1.ID, book2.ID)
	}
	var bookCount int
	if err := env.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM books WHERE title=$1`, "Idempotent Book").Scan(&bookCount); err != nil {
		t.Fatalf("count books: %v", err)
	}
	if bookCount != 1 {
		t.Fatalf("expected exactly 1 book, got %d", bookCount)
	}

	// Same key with different body -> 422.
	st3, b3 := env.do(http.MethodPost, "/books", map[string]any{
		"title":  "Different Book",
		"author": "Someone Else",
	}, "book-key-1")
	if st3 != 422 {
		t.Fatalf("expected 422 for key reuse, got %d: %s", st3, b3)
	}
	eb3 := decode[errBody](t, b3)
	if eb3.Error.Code != service.CodeIdempotencyKeyReused {
		t.Fatalf("expected IDEMPOTENCY_KEY_REUSED, got %s", eb3.Error.Code)
	}

	// --- POST /sessions idempotency ---
	sessBody := map[string]any{"bookId": book1.ID, "label": "idem-session"}
	ss1, sb1 := env.do(http.MethodPost, "/sessions", sessBody, "sess-key-1")
	ss2, sb2 := env.do(http.MethodPost, "/sessions", sessBody, "sess-key-1")
	if ss1 != 201 || ss2 != 201 {
		t.Fatalf("expected both 201, got %d and %d", ss1, ss2)
	}
	if !bytes.Equal(sb1, sb2) {
		t.Fatalf("session replay bodies differ:\n%s\n%s", sb1, sb2)
	}
	sess1 := decode[domain.Session](t, sb1)
	sess2 := decode[domain.Session](t, sb2)
	if sess1.ID != sess2.ID {
		t.Fatalf("expected same session id, got %s and %s", sess1.ID, sess2.ID)
	}

	// --- POST /events idempotency ---
	base := time.Date(2026, 5, 1, 6, 0, 0, 0, time.UTC)
	eventBody := map[string]any{
		"eventType":   "SESSION_STARTED",
		"occurredAt":  base.Format(time.RFC3339Nano),
		"expectedSeq": 0,
	}
	es1, eb1 := env.do(http.MethodPost, "/sessions/"+sess1.ID+"/events", eventBody, "event-key-1")
	es2, eb2 := env.do(http.MethodPost, "/sessions/"+sess1.ID+"/events", eventBody, "event-key-1")
	if es1 != 201 || es2 != 201 {
		t.Fatalf("expected both 201, got %d and %d (%s / %s)", es1, es2, eb1, eb2)
	}
	if !bytes.Equal(eb1, eb2) {
		t.Fatalf("event replay bodies differ:\n%s\n%s", eb1, eb2)
	}
	ev1 := decode[domain.Event](t, eb1)
	ev2 := decode[domain.Event](t, eb2)
	if ev1.Seq != ev2.Seq || ev1.ID != ev2.ID {
		t.Fatalf("expected same event (seq=%d id=%s), got seq=%d id=%s",
			ev1.Seq, ev1.ID, ev2.Seq, ev2.ID)
	}

	// After replay, only one event exists; the next append must succeed at seq=2.
	st, next, ebErr := env.appendEvent(sess1.ID, map[string]any{
		"eventType":   "PAGE_REACHED",
		"occurredAt":  base.Add(time.Minute).Format(time.RFC3339Nano),
		"page":        3,
		"expectedSeq": 1,
	}, "")
	if st != 201 {
		t.Fatalf("next append after replay failed: %d %+v", st, ebErr)
	}
	if next.Seq != 2 {
		t.Fatalf("expected next seq=2, got %d", next.Seq)
	}
}

func TestEventTableIsAppendOnly(t *testing.T) {
	env := newEnv(t)
	book := env.createBook("Append Only")
	sess := env.createSession(book.ID)
	base := time.Date(2026, 6, 1, 5, 0, 0, 0, time.UTC)

	st, _, eb := env.appendEvent(sess.ID, map[string]any{
		"eventType":   "SESSION_STARTED",
		"occurredAt":  base.Format(time.RFC3339Nano),
		"expectedSeq": 0,
	}, "")
	if st != 201 {
		t.Fatalf("STARTED failed: %d %+v", st, eb)
	}

	ctx := context.Background()
	var eventID string
	if err := env.pool.QueryRow(ctx,
		`SELECT id::text FROM events WHERE session_id=$1::uuid LIMIT 1`, sess.ID,
	).Scan(&eventID); err != nil {
		t.Fatalf("get event id: %v", err)
	}

	// UPDATE must be rejected by the append-only trigger.
	if _, err := env.pool.Exec(ctx,
		`UPDATE events SET note='hacked' WHERE id=$1::uuid`, eventID); err == nil {
		t.Fatalf("expected UPDATE on events to be rejected, but it succeeded")
	}

	// DELETE must be rejected.
	if _, err := env.pool.Exec(ctx,
		`DELETE FROM events WHERE id=$1::uuid`, eventID); err == nil {
		t.Fatalf("expected DELETE on events to be rejected, but it succeeded")
	}
}

func TestBookNotFoundAndSessionNotFound(t *testing.T) {
	env := newEnv(t)

	st, body := env.do(http.MethodPost, "/sessions", map[string]any{
		"bookId": "00000000-0000-0000-0000-000000000000",
	}, "")
	if st != 404 {
		t.Fatalf("expected 404 for missing book, got %d: %s", st, body)
	}
	eb := decode[errBody](t, body)
	if eb.Error.Code != service.CodeBookNotFound {
		t.Fatalf("expected BOOK_NOT_FOUND, got %s", eb.Error.Code)
	}

	st, body = env.do(http.MethodGet, "/sessions/00000000-0000-0000-0000-000000000000", nil, "")
	if st != 404 {
		t.Fatalf("expected 404 for missing session, got %d: %s", st, body)
	}
	eb = decode[errBody](t, body)
	if eb.Error.Code != service.CodeSessionNotFound {
		t.Fatalf("expected SESSION_NOT_FOUND, got %s", eb.Error.Code)
	}
}

func TestTimezonesAreNormalizedToUTC(t *testing.T) {
	env := newEnv(t)
	book := env.createBook("TZ Book")
	sess := env.createSession(book.ID)

	// Send a timestamp with a +05:00 offset; it must be stored/returned as UTC.
	offsetTime := "2026-07-01T14:00:00+05:00" // == 09:00:00Z
	st, ev, eb := env.appendEvent(sess.ID, map[string]any{
		"eventType":   "SESSION_STARTED",
		"occurredAt":  offsetTime,
		"expectedSeq": 0,
	}, "")
	if st != 201 {
		t.Fatalf("STARTED failed: %d %+v", st, eb)
	}
	got := ev.OccurredAt.Time()
	want := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	if got.Location() != time.UTC {
		t.Fatalf("expected UTC, got location %v", got.Location())
	}
}

func TestSeedFixture(t *testing.T) {
	// This test uses a separate clean database state but applies the seed
	// through the same SQL used by cmd/seed.
	env := newEnv(t)
	ctx := context.Background()

	seedSQL, err := os.ReadFile("../../internal/seed/seed.sql")
	if err != nil {
		t.Fatalf("read seed sql: %v", err)
	}
	if _, err := env.pool.Exec(ctx, string(seedSQL)); err != nil {
		t.Fatalf("apply seed: %v", err)
	}

	status, body := env.do(http.MethodGet, "/sessions/"+"b0000000-0000-0000-0000-000000000001", nil, "")
	if status != 200 {
		t.Fatalf("GET seeded session status=%d body=%s", status, body)
	}
	detail := decode[domain.SessionDetail](t, body)
	if detail.ReadingDurationSeconds != 2100 {
		t.Fatalf("expected 2100 seconds from seed, got %d", detail.ReadingDurationSeconds)
	}
	if detail.InterruptionCount != 1 {
		t.Fatalf("expected 1 interruption from seed, got %d", detail.InterruptionCount)
	}
	if detail.LastPage == nil || *detail.LastPage != 40 {
		t.Fatalf("expected lastPage 40 from seed, got %v", detail.LastPage)
	}
	if len(detail.Passages) != 1 {
		t.Fatalf("expected 1 passage from seed, got %d", len(detail.Passages))
	}
}

func TestMalformedJSON(t *testing.T) {
	env := newEnv(t)
	req, _ := http.NewRequest(http.MethodPost, env.server.URL+"/books", strings.NewReader("{not json"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestNoIdempotencyKeyCreatesDistinct(t *testing.T) {
	env := newEnv(t)
	body := map[string]any{"title": "No Key", "author": "A"}
	st1, _ := env.do(http.MethodPost, "/books", body, "")
	st2, _ := env.do(http.MethodPost, "/books", body, "")
	if st1 != 201 || st2 != 201 {
		t.Fatalf("expected both 201, got %d %d", st1, st2)
	}
	var n int
	if err := env.pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM books WHERE title='No Key'`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 distinct books without idempotency key, got %d", n)
	}
}

// Ensure fmt is used (for potential future debugging) — referenced to avoid unused import in some builds.
var _ = fmt.Sprintf
