package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"embodied-reading-ledger/internal/api"
	"embodied-reading-ledger/internal/database"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultDatabaseURL = "postgres://ledger:ledger@localhost:5433/ledger?sslmode=disable"

func databaseURL() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return defaultDatabaseURL
}

type harness struct {
	t       *testing.T
	pool    *pgxpool.Pool
	baseURL string
	client  *http.Client
	server  *http.Server
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		t.Fatalf("cannot connect to PostgreSQL at %s: %v\nstart it with: docker compose up -d db", databaseURL(), err)
	}

	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: api.NewServer(pool)}
	go func() { _ = srv.Serve(ln) }()

	h := &harness{
		t:       t,
		pool:    pool,
		baseURL: "http://" + ln.Addr().String(),
		client:  &http.Client{Timeout: 30 * time.Second},
		server:  srv,
	}
	h.truncate()

	t.Cleanup(func() {
		_ = srv.Close()
		pool.Close()
	})
	return h
}

func (h *harness) truncate() {
	h.t.Helper()
	_, err := h.pool.Exec(context.Background(),
		`TRUNCATE TABLE idempotency_keys, events, reading_sessions, books RESTART IDENTITY CASCADE`)
	if err != nil {
		h.t.Fatalf("truncate: %v", err)
	}
}

func (h *harness) do(method, path string, body any, idempotencyKey string) (int, map[string]any, []byte) {
	h.t.Helper()

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequest(method, h.baseURL+path, reader)
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatalf("http request: %v", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("read response: %v", err)
	}
	var parsed map[string]any
	_ = json.Unmarshal(raw, &parsed)
	return resp.StatusCode, parsed, raw
}

func (h *harness) createBook(idempotencyKey string) string {
	h.t.Helper()
	body := map[string]any{
		"isbn":       fmt.Sprintf("978-7-000-%06d", time.Now().UnixNano()%1000000),
		"title":      "纸上的钟",
		"author":     "林晚",
		"edition":    "2024年第1版",
		"totalPages": 320,
	}
	status, resp, _ := h.do(http.MethodPost, "/books", body, idempotencyKey)
	if status != http.StatusCreated {
		h.t.Fatalf("create book: status=%d body=%v", status, resp)
	}
	id, _ := resp["id"].(string)
	if id == "" {
		h.t.Fatalf("create book: missing id: %v", resp)
	}
	return id
}

func (h *harness) createSession(bookID, idempotencyKey string) string {
	h.t.Helper()
	body := map[string]any{"bookId": bookID, "readerTag": "alice"}
	status, resp, _ := h.do(http.MethodPost, "/sessions", body, idempotencyKey)
	if status != http.StatusCreated {
		h.t.Fatalf("create session: status=%d body=%v", status, resp)
	}
	id, _ := resp["id"].(string)
	if id == "" {
		h.t.Fatalf("create session: missing id: %v", resp)
	}
	return id
}

func appendBody(expectedSeq int64, eventType string, when time.Time, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	return map[string]any{
		"expectedSeq": expectedSeq,
		"event": map[string]any{
			"type":       eventType,
			"occurredAt": when.UTC().Format(time.RFC3339Nano),
			"payload":    payload,
		},
	}
}

func (h *harness) appendEvent(sessionID string, expectedSeq int64, eventType string, when time.Time, payload map[string]any, idempotencyKey string) (int, map[string]any) {
	h.t.Helper()
	path := "/sessions/" + sessionID + "/events"
	status, resp, _ := h.do(http.MethodPost, path, appendBody(expectedSeq, eventType, when, payload), idempotencyKey)
	return status, resp
}

func errCode(resp map[string]any) string {
	e, ok := resp["error"].(map[string]any)
	if !ok {
		return ""
	}
	code, _ := e["code"].(string)
	return code
}

func TestFullSessionLifecycleAndSummary(t *testing.T) {
	h := newHarness(t)
	bookID := h.createBook("book-life")
	sessionID := h.createSession(bookID, "sess-life")

	t0 := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)
	steps := []struct {
		seq     int64
		typ     string
		when    time.Time
		payload map[string]any
	}{
		{0, "SESSION_STARTED", t0, map[string]any{}},
		{1, "PAGE_REACHED", t0.Add(5 * time.Minute), map[string]any{"page": 18}},
		{2, "PASSAGE_REACTED", t0.Add(12*time.Minute + 30*time.Second), map[string]any{"page": 22, "quote": "钟摆每一次回落，都像一次重读。", "note": "这句话让我停下来。"}},
		{3, "INTERRUPTED", t0.Add(20 * time.Minute), map[string]any{"reason": "快递敲门"}},
		{4, "PAGE_REACHED", t0.Add(35 * time.Minute), map[string]any{"page": 26}},
		{5, "INTERRUPTED", t0.Add(48 * time.Minute), map[string]any{"reason": "电话响了"}},
		{6, "PAGE_REACHED", t0.Add(55 * time.Minute), map[string]any{"page": 30}},
		{7, "SESSION_ENDED", t0.Add(70 * time.Minute), map[string]any{}},
	}
	for i, st := range steps {
		status, resp := h.appendEvent(sessionID, st.seq, st.typ, st.when, st.payload, fmt.Sprintf("ev-life-%d", i))
		if status != http.StatusCreated {
			t.Fatalf("step %d (%s): status=%d body=%v", i, st.typ, status, resp)
		}
		if got := resp["seq"].(float64); int64(got) != st.seq+1 {
			t.Fatalf("step %d: seq=%v want %d", i, resp["seq"], st.seq+1)
		}
	}

	status, session, _ := h.do(http.MethodGet, "/sessions/"+sessionID, nil, "")
	if status != http.StatusOK {
		t.Fatalf("get session: status=%d body=%v", status, session)
	}

	if session["status"] != "ended" {
		t.Fatalf("status = %v, want ended", session["status"])
	}
	if got := session["interruptionCount"].(float64); got != 2 {
		t.Fatalf("interruptionCount = %v, want 2", got)
	}
	if got := session["lastPage"].(float64); got != 30 {
		t.Fatalf("lastPage = %v, want 30", got)
	}
	if got := session["maxPage"].(float64); got != 30 {
		t.Fatalf("maxPage = %v, want 30", got)
	}
	readingMinutes := session["readingMinutes"].(float64)
	if math.Abs(readingMinutes-48.0) > 0.001 {
		t.Fatalf("readingMinutes = %v, want ~48 (interruption gaps excluded)", readingMinutes)
	}

	reactions, _ := session["reactions"].([]any)
	if len(reactions) != 1 {
		t.Fatalf("reactions len = %d, want 1", len(reactions))
	}
	first := reactions[0].(map[string]any)
	if first["note"] != "这句话让我停下来。" {
		t.Fatalf("reaction note = %v", first["note"])
	}

	events, _ := session["events"].([]any)
	if len(events) != 8 {
		t.Fatalf("events len = %d, want 8", len(events))
	}
	if session["startedAt"] == nil || session["endedAt"] == nil {
		t.Fatalf("startedAt/endedAt should be set: %v", session)
	}

	var eventCount int
	if err := h.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM events WHERE session_id = $1`, sessionID).Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 8 {
		t.Fatalf("events in db = %d, want 8", eventCount)
	}
}

func TestIdempotencyReplay(t *testing.T) {
	h := newHarness(t)

	bookBody := map[string]any{
		"isbn":       "978-7-000-111111",
		"title":      "初版标题",
		"author":     "林晚",
		"edition":    "1",
		"totalPages": 100,
	}

	status, first, rawFirst := h.do(http.MethodPost, "/books", bookBody, "book-key-1")
	if status != http.StatusCreated {
		t.Fatalf("first create: status=%d body=%v", status, first)
	}
	bookID := first["id"].(string)

	status, replay, rawReplay := h.do(http.MethodPost, "/books", bookBody, "book-key-1")
	if status != http.StatusCreated {
		t.Fatalf("replay create: status=%d body=%v", status, replay)
	}
	if replay["id"] != bookID {
		t.Fatalf("replay returned different id: %v vs %v", replay["id"], bookID)
	}
	if !bytes.Equal(rawFirst, rawReplay) {
		t.Fatalf("replay response body differs from first response")
	}

	var bookCount int
	if err := h.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM books WHERE isbn = $1`, "978-7-000-111111").Scan(&bookCount); err != nil {
		t.Fatalf("count books: %v", err)
	}
	if bookCount != 1 {
		t.Fatalf("books in db = %d, want 1 (replay must not double-write)", bookCount)
	}

	differentBody := map[string]any{
		"isbn":       "978-7-000-111111",
		"title":      "被篡改的标题",
		"author":     "林晚",
		"totalPages": 100,
	}
	status, resp, _ := h.do(http.MethodPost, "/books", differentBody, "book-key-1")
	if status != http.StatusConflict || errCode(resp) != "IDEMPOTENCY_KEY_REUSED" {
		t.Fatalf("same key different body: status=%d body=%v", status, resp)
	}

	sessionBody := map[string]any{"bookId": bookID, "readerTag": "alice"}
	status, firstSess, _ := h.do(http.MethodPost, "/sessions", sessionBody, "sess-key-1")
	if status != http.StatusCreated {
		t.Fatalf("first session: status=%d body=%v", status, firstSess)
	}
	sessionID := firstSess["id"].(string)
	status, replaySess, _ := h.do(http.MethodPost, "/sessions", sessionBody, "sess-key-1")
	if status != http.StatusCreated || replaySess["id"] != sessionID {
		t.Fatalf("session replay: status=%d body=%v", status, replaySess)
	}

	t0 := time.Date(2026, 8, 21, 8, 0, 0, 0, time.UTC)
	eventBody := appendBody(0, "SESSION_STARTED", t0, nil)
	status, firstEv, rawFirstEv := h.do(http.MethodPost, "/sessions/"+sessionID+"/events", eventBody, "event-key-1")
	if status != http.StatusCreated {
		t.Fatalf("first event: status=%d body=%v", status, firstEv)
	}
	status, replayEv, rawReplayEv := h.do(http.MethodPost, "/sessions/"+sessionID+"/events", eventBody, "event-key-1")
	if status != http.StatusCreated {
		t.Fatalf("replay event: status=%d body=%v", status, replayEv)
	}
	if replayEv["seq"].(float64) != 1 {
		t.Fatalf("replay event seq = %v, want 1", replayEv["seq"])
	}
	if !bytes.Equal(rawFirstEv, rawReplayEv) {
		t.Fatalf("replay event response body differs")
	}

	var eventCount int
	if err := h.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM events WHERE session_id = $1`, sessionID).Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("events in db = %d, want 1 (replay must not double-write)", eventCount)
	}

	conflictingEvent := appendBody(0, "SESSION_STARTED", t0.Add(time.Hour), nil)
	status, resp, _ = h.do(http.MethodPost, "/sessions/"+sessionID+"/events", conflictingEvent, "event-key-1")
	if status != http.StatusConflict || errCode(resp) != "IDEMPOTENCY_KEY_REUSED" {
		t.Fatalf("same event key different body: status=%d body=%v", status, resp)
	}
}

func TestConcurrentAppendsOnlyOneWins(t *testing.T) {
	h := newHarness(t)
	bookID := h.createBook("")
	sessionID := h.createSession(bookID, "")

	t0 := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	if status, resp := h.appendEvent(sessionID, 0, "SESSION_STARTED", t0, nil, "start"); status != http.StatusCreated {
		t.Fatalf("start: status=%d body=%v", status, resp)
	}

	const n = 24
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		statuses = make([]int, 0, n)
		codes    = make([]string, 0, n)
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			body := appendBody(1, "PAGE_REACHED", t0.Add(time.Duration(i+1)*time.Second),
				map[string]any{"page": 10 + i})
			status, resp, _ := h.do(http.MethodPost, "/sessions/"+sessionID+"/events", body,
				fmt.Sprintf("race-%d", i))
			mu.Lock()
			statuses = append(statuses, status)
			codes = append(codes, errCode(resp))
			mu.Unlock()
		}()
	}
	wg.Wait()

	created, conflicts := 0, 0
	for i, status := range statuses {
		switch status {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicts++
			if codes[i] != "SEQUENCE_CONFLICT" {
				t.Fatalf("conflict %d code = %q, want SEQUENCE_CONFLICT", i, codes[i])
			}
		default:
			t.Fatalf("unexpected status %d (code %s)", status, codes[i])
		}
	}
	if created != 1 {
		t.Fatalf("created = %d, want exactly 1", created)
	}
	if conflicts != n-1 {
		t.Fatalf("conflicts = %d, want %d", conflicts, n-1)
	}

	status, session, _ := h.do(http.MethodGet, "/sessions/"+sessionID, nil, "")
	if status != http.StatusOK {
		t.Fatalf("get session: %d", status)
	}
	events := session["events"].([]any)
	if len(events) != 2 {
		t.Fatalf("events after race = %d, want 2", len(events))
	}
	last := events[1].(map[string]any)
	if last["seq"].(float64) != 2 || last["type"] != "PAGE_REACHED" {
		t.Fatalf("unexpected last event: %v", last)
	}
}

func TestIllegalStateTransitions(t *testing.T) {
	h := newHarness(t)
	bookID := h.createBook("")

	t0 := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
	sessionA := h.createSession(bookID, "")

	status, resp := h.appendEvent(sessionA, 0, "PAGE_REACHED", t0, map[string]any{"page": 1}, "")
	if status != http.StatusUnprocessableEntity || errCode(resp) != "SESSION_NOT_STARTED" {
		t.Fatalf("page before start: status=%d body=%v", status, resp)
	}

	if status, resp = h.appendEvent(sessionA, 0, "SESSION_STARTED", t0, nil, ""); status != http.StatusCreated {
		t.Fatalf("start: status=%d body=%v", status, resp)
	}
	status, resp = h.appendEvent(sessionA, 1, "SESSION_STARTED", t0.Add(time.Minute), nil, "")
	if status != http.StatusUnprocessableEntity || errCode(resp) != "SESSION_ALREADY_STARTED" {
		t.Fatalf("double start: status=%d body=%v", status, resp)
	}
	if status, resp = h.appendEvent(sessionA, 1, "PAGE_REACHED", t0.Add(5*time.Minute), map[string]any{"page": 10}, ""); status != http.StatusCreated {
		t.Fatalf("page 10: status=%d body=%v", status, resp)
	}
	if status, resp = h.appendEvent(sessionA, 2, "SESSION_ENDED", t0.Add(10*time.Minute), nil, ""); status != http.StatusCreated {
		t.Fatalf("end: status=%d body=%v", status, resp)
	}
	status, resp = h.appendEvent(sessionA, 3, "PAGE_REACHED", t0.Add(15*time.Minute), map[string]any{"page": 11}, "")
	if status != http.StatusUnprocessableEntity || errCode(resp) != "SESSION_ALREADY_ENDED" {
		t.Fatalf("append after end: status=%d body=%v", status, resp)
	}

	sessionB := h.createSession(bookID, "")
	t1 := time.Date(2026, 8, 23, 14, 0, 0, 0, time.UTC)
	if status, resp = h.appendEvent(sessionB, 0, "SESSION_STARTED", t1, nil, ""); status != http.StatusCreated {
		t.Fatalf("start B: status=%d body=%v", status, resp)
	}

	status, resp = h.appendEvent(sessionB, 1, "PAGE_REACHED", t1.Add(-time.Minute), map[string]any{"page": 5}, "")
	if status != http.StatusUnprocessableEntity || errCode(resp) != "CLOCK_WENT_BACKWARDS" {
		t.Fatalf("clock backwards: status=%d body=%v", status, resp)
	}
	status, resp = h.appendEvent(sessionB, 1, "PAGE_REACHED", t1, map[string]any{"page": 5}, "")
	if status != http.StatusUnprocessableEntity || errCode(resp) != "CLOCK_WENT_BACKWARDS" {
		t.Fatalf("equal timestamps: status=%d body=%v", status, resp)
	}

	status, resp = h.appendEvent(sessionB, 1, "PAGE_REACHED", t1.Add(time.Minute), map[string]any{"page": 9999}, "")
	if status != http.StatusUnprocessableEntity || errCode(resp) != "PAGE_OUT_OF_RANGE" {
		t.Fatalf("page too big: status=%d body=%v", status, resp)
	}
	status, resp = h.appendEvent(sessionB, 1, "PAGE_REACHED", t1.Add(time.Minute), map[string]any{"page": 0}, "")
	if status != http.StatusUnprocessableEntity || errCode(resp) != "PAGE_OUT_OF_RANGE" {
		t.Fatalf("page zero: status=%d body=%v", status, resp)
	}

	status, resp = h.appendEvent(sessionB, 1, "PASSAGE_REACTED", t1.Add(2*time.Minute),
		map[string]any{"page": 5, "note": "   "}, "")
	if status != http.StatusUnprocessableEntity || errCode(resp) != "INVALID_EVENT_PAYLOAD" {
		t.Fatalf("blank note: status=%d body=%v", status, resp)
	}

	status, resp = h.appendEvent(sessionB, 99, "PAGE_REACHED", t1.Add(3*time.Minute), map[string]any{"page": 6}, "")
	if status != http.StatusConflict || errCode(resp) != "SEQUENCE_CONFLICT" {
		t.Fatalf("wrong expectedSeq: status=%d body=%v", status, resp)
	}
	details := resp["error"].(map[string]any)["details"].(map[string]any)
	if details["currentSeq"].(float64) != 1 {
		t.Fatalf("currentSeq = %v, want 1", details["currentSeq"])
	}

	unknownID := "11111111-1111-4111-8111-111111111111"
	status, resp, _ = h.do(http.MethodPost, "/sessions/"+unknownID+"/events",
		appendBody(0, "SESSION_STARTED", t1, nil), "")
	if status != http.StatusNotFound || errCode(resp) != "SESSION_NOT_FOUND" {
		t.Fatalf("event unknown session: status=%d body=%v", status, resp)
	}
	status, resp, _ = h.do(http.MethodGet, "/sessions/"+unknownID, nil, "")
	if status != http.StatusNotFound || errCode(resp) != "SESSION_NOT_FOUND" {
		t.Fatalf("get unknown session: status=%d body=%v", status, resp)
	}

	status, resp, _ = h.do(http.MethodPost, "/sessions/not-a-uuid/events",
		appendBody(0, "SESSION_STARTED", t1, nil), "")
	if status != http.StatusBadRequest || errCode(resp) != "VALIDATION_ERROR" {
		t.Fatalf("malformed uuid: status=%d body=%v", status, resp)
	}

	status, resp, _ = h.do(http.MethodPost, "/sessions/"+sessionB+"/events",
		map[string]any{"event": map[string]any{"type": "SESSION_STARTED", "occurredAt": t1.Format(time.RFC3339Nano)}}, "")
	if status != http.StatusBadRequest || errCode(resp) != "VALIDATION_ERROR" {
		t.Fatalf("missing expectedSeq: status=%d body=%v", status, resp)
	}

	status, resp, _ = h.do(http.MethodPost, "/sessions/"+sessionB+"/events",
		map[string]any{"expectedSeq": 1, "event": map[string]any{"type": "PAGE_REACHED", "occurredAt": "not-a-time", "payload": map[string]any{"page": 7}}}, "")
	if status != http.StatusBadRequest || errCode(resp) != "INVALID_TIMESTAMP" {
		t.Fatalf("bad timestamp: status=%d body=%v", status, resp)
	}

	status, resp, _ = h.do(http.MethodPost, "/sessions",
		map[string]any{"bookId": unknownID, "readerTag": "alice"}, "")
	if status != http.StatusNotFound || errCode(resp) != "BOOK_NOT_FOUND" {
		t.Fatalf("session for missing book: status=%d body=%v", status, resp)
	}

	status, resp, _ = h.do(http.MethodPost, "/books",
		map[string]any{"isbn": "x", "title": "y", "author": "z", "totalPages": 0}, "")
	if status != http.StatusBadRequest || errCode(resp) != "VALIDATION_ERROR" {
		t.Fatalf("invalid book: status=%d body=%v", status, resp)
	}
}

func TestEventsTableAppendOnly(t *testing.T) {
	h := newHarness(t)
	bookID := h.createBook("")
	sessionID := h.createSession(bookID, "")
	t0 := time.Date(2026, 8, 24, 9, 0, 0, 0, time.UTC)
	if status, resp := h.appendEvent(sessionID, 0, "SESSION_STARTED", t0, nil, ""); status != http.StatusCreated {
		t.Fatalf("start: status=%d body=%v", status, resp)
	}

	if _, err := h.pool.Exec(context.Background(),
		`UPDATE events SET event_type = 'SESSION_ENDED' WHERE session_id = $1`, sessionID); err == nil {
		t.Fatal("UPDATE on events must be rejected by the append-only trigger")
	}

	if _, err := h.pool.Exec(context.Background(),
		`DELETE FROM events WHERE session_id = $1`, sessionID); err == nil {
		t.Fatal("DELETE on events must be rejected by the append-only trigger")
	}
}

func TestSeedFixturesDeterministic(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	if err := database.Seed(ctx, h.pool); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := database.Seed(ctx, h.pool); err != nil {
		t.Fatalf("seed re-run should be idempotent: %v", err)
	}

	endedSession := "00000000-0000-4000-8000-000000000002"
	status, session, _ := h.do(http.MethodGet, "/sessions/"+endedSession, nil, "")
	if status != http.StatusOK {
		t.Fatalf("get seeded session: status=%d body=%v", status, session)
	}
	if session["status"] != "ended" {
		t.Fatalf("seeded session status = %v, want ended", session["status"])
	}
	readingMinutes := session["readingMinutes"].(float64)
	if math.Abs(readingMinutes-48.0) > 0.001 {
		t.Fatalf("seeded readingMinutes = %v, want 48", readingMinutes)
	}
	if session["interruptionCount"].(float64) != 2 {
		t.Fatalf("seeded interruptionCount = %v, want 2", session["interruptionCount"])
	}
	if session["lastPage"].(float64) != 30 {
		t.Fatalf("seeded lastPage = %v, want 30", session["lastPage"])
	}
	reactions := session["reactions"].([]any)
	if len(reactions) != 1 {
		t.Fatalf("seeded reactions = %d, want 1", len(reactions))
	}
	events := session["events"].([]any)
	if len(events) != 8 {
		t.Fatalf("seeded events = %d, want 8", len(events))
	}

	openSession := "00000000-0000-4000-8000-000000000003"
	status, session, _ = h.do(http.MethodGet, "/sessions/"+openSession, nil, "")
	if status != http.StatusOK {
		t.Fatalf("get open seeded session: status=%d", status)
	}
	if session["status"] != "open" {
		t.Fatalf("open session status = %v", session["status"])
	}
	if len(session["events"].([]any)) != 2 {
		t.Fatalf("open session events = %v, want 2", session["events"])
	}

	var bookCount int
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM books`).Scan(&bookCount); err != nil {
		t.Fatalf("count books: %v", err)
	}
	if bookCount != 1 {
		t.Fatalf("books after re-seed = %d, want 1", bookCount)
	}
}
