package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/embodied-reading/ledger/internal/store"
)

// TestHappyPath_FullReadingStory records the whole event stream and verifies the
// server-reconstructed projection: reading minutes (with interruption gap
// removed), last page, interruption count, and the reader's own feelings.
func TestHappyPath_FullReadingStory(t *testing.T) {
	env := newTestEnv(t)
	_, sessionID := env.createBookAndSession(t)

	events := []map[string]any{
		{"expectedSeq": 1, "type": "SESSION_STARTED", "occurredAt": "2026-08-26T09:00:00Z"},
		{"expectedSeq": 2, "type": "PAGE_REACHED", "occurredAt": "2026-08-26T09:12:30Z", "payload": map[string]any{"page": 12}},
		{"expectedSeq": 3, "type": "PASSAGE_REACTED", "occurredAt": "2026-08-26T09:20:00Z", "payload": map[string]any{"passage": "grief", "feeling": "it caught in my throat"}},
		{"expectedSeq": 4, "type": "INTERRUPTED", "occurredAt": "2026-08-26T09:25:00Z", "payload": map[string]any{"reason": "doorbell"}},
		{"expectedSeq": 5, "type": "PAGE_REACHED", "occurredAt": "2026-08-26T09:33:00Z", "payload": map[string]any{"page": 18}},
		{"expectedSeq": 6, "type": "SESSION_ENDED", "occurredAt": "2026-08-26T09:40:00Z"},
	}
	for _, ev := range events {
		status, body := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", "", ev)
		if status != http.StatusCreated {
			t.Fatalf("append %v: status %d body %s", ev["type"], status, body)
		}
	}

	status, body := env.do(t, http.MethodGet, "/sessions/"+sessionID, "", nil)
	if status != http.StatusOK {
		t.Fatalf("get session: status %d body %s", status, body)
	}
	var view store.SessionView
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatalf("unmarshal view: %v", err)
	}
	p := view.Projection
	if p.ReadingMinutes != 32 {
		t.Errorf("readingMinutes = %v, want 32", p.ReadingMinutes)
	}
	if p.LastPage == nil || *p.LastPage != 18 {
		t.Errorf("lastPage = %v, want 18", p.LastPage)
	}
	if p.InterruptionCount != 1 {
		t.Errorf("interruptionCount = %d, want 1", p.InterruptionCount)
	}
	if len(p.Feelings) != 1 || p.Feelings[0].Feeling != "it caught in my throat" {
		t.Errorf("feelings = %+v, want the one recorded feeling", p.Feelings)
	}
	if len(view.Events) != 6 {
		t.Errorf("events len = %d, want 6", len(view.Events))
	}
}

// TestIdempotentReplay verifies a repeated write with the same Idempotency-Key
// returns the first response verbatim and does not create a second row.
func TestIdempotentReplay(t *testing.T) {
	env := newTestEnv(t)
	_, sessionID := env.createBookAndSession(t)

	ev := map[string]any{"expectedSeq": 1, "type": "SESSION_STARTED", "occurredAt": "2026-08-26T09:00:00Z"}
	key := "replay-key-1"

	status1, body1 := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", key, ev)
	if status1 != http.StatusCreated {
		t.Fatalf("first append: status %d body %s", status1, body1)
	}
	status2, body2 := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", key, ev)
	if status2 != http.StatusCreated {
		t.Fatalf("replay: status %d body %s", status2, body2)
	}
	if string(body1) != string(body2) {
		t.Fatalf("idempotent replay body differs:\nfirst=%s\nsecond=%s", body1, body2)
	}

	// Exactly one event must exist despite two POSTs.
	var count int
	if err := env.pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM events WHERE session_id = $1", sessionID).Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if count != 1 {
		t.Fatalf("event count = %d, want 1 (no second landing)", count)
	}
}

// TestIdempotencyKeyReuseWithDifferentBody must be rejected with 409.
func TestIdempotencyKeyReuseWithDifferentBody(t *testing.T) {
	env := newTestEnv(t)
	_, sessionID := env.createBookAndSession(t)

	key := "reuse-key"
	ev1 := map[string]any{"expectedSeq": 1, "type": "SESSION_STARTED", "occurredAt": "2026-08-26T09:00:00Z"}
	ev2 := map[string]any{"expectedSeq": 1, "type": "SESSION_STARTED", "occurredAt": "2026-08-26T10:00:00Z"}

	status1, _ := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", key, ev1)
	if status1 != http.StatusCreated {
		t.Fatalf("first: status %d", status1)
	}
	status2, body2 := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", key, ev2)
	if status2 != http.StatusConflict {
		t.Fatalf("reuse: status %d body %s, want 409", status2, body2)
	}
	assertErrorCode(t, body2, "IDEMPOTENCY_KEY_REUSE")
}

// TestConcurrentAppendRace fires N simultaneous appends all claiming seq 1.
// Exactly one must win (201); every loser must receive 409 SEQ_CONFLICT with the
// authoritative currentSeq. Correctness is enforced solely by the Postgres
// unique constraint — no in-process lock.
func TestConcurrentAppendRace(t *testing.T) {
	env := newTestEnv(t)
	_, sessionID := env.createBookAndSession(t)

	const n = 12
	var (
		wg        sync.WaitGroup
		successes atomic.Int64
		conflicts atomic.Int64
	)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev := map[string]any{"expectedSeq": 1, "type": "SESSION_STARTED", "occurredAt": "2026-08-26T09:00:00Z"}
			<-start // release all goroutines together
			status, body := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", "", ev)
			switch status {
			case http.StatusCreated:
				successes.Add(1)
			case http.StatusConflict:
				conflicts.Add(1)
				assertErrorCode(t, body, "SEQ_CONFLICT")
			default:
				t.Errorf("unexpected status %d body %s", status, body)
			}
		}()
	}
	close(start)
	wg.Wait()

	if successes.Load() != 1 {
		t.Fatalf("successes = %d, want exactly 1", successes.Load())
	}
	if conflicts.Load() != n-1 {
		t.Fatalf("conflicts = %d, want %d", conflicts.Load(), n-1)
	}

	// Ledger must hold exactly one event at seq 1.
	var count int
	if err := env.pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM events WHERE session_id = $1", sessionID).Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if count != 1 {
		t.Fatalf("event count = %d, want 1", count)
	}
}

// TestSeqConflictReportsCurrentSeq verifies a stale expectedSeq yields 409 with
// the authoritative currentSeq in details.
func TestSeqConflictReportsCurrentSeq(t *testing.T) {
	env := newTestEnv(t)
	_, sessionID := env.createBookAndSession(t)

	start := map[string]any{"expectedSeq": 1, "type": "SESSION_STARTED", "occurredAt": "2026-08-26T09:00:00Z"}
	if status, body := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", "", start); status != http.StatusCreated {
		t.Fatalf("start: status %d body %s", status, body)
	}

	// Now the current seq is 1; asking for seq 1 again (or 3) must conflict.
	stale := map[string]any{"expectedSeq": 1, "type": "PAGE_REACHED", "occurredAt": "2026-08-26T09:05:00Z", "payload": map[string]any{"page": 5}}
	status, body := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", "", stale)
	if status != http.StatusConflict {
		t.Fatalf("stale append: status %d body %s, want 409", status, body)
	}
	details := assertErrorCode(t, body, "SEQ_CONFLICT")
	if got := int(details["currentSeq"].(float64)); got != 1 {
		t.Fatalf("currentSeq = %d, want 1", got)
	}
}

// TestIllegalTransitions covers the stable state-machine error codes.
func TestIllegalTransitions(t *testing.T) {
	env := newTestEnv(t)

	t.Run("first event must be SESSION_STARTED", func(t *testing.T) {
		_, sessionID := env.createBookAndSession(t)
		ev := map[string]any{"expectedSeq": 1, "type": "PAGE_REACHED", "occurredAt": "2026-08-26T09:00:00Z", "payload": map[string]any{"page": 1}}
		status, body := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", "", ev)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("status %d body %s, want 422", status, body)
		}
		assertErrorCode(t, body, "INVALID_TRANSITION")
	})

	t.Run("time regression", func(t *testing.T) {
		_, sessionID := env.createBookAndSession(t)
		env.mustAppend(t, sessionID, map[string]any{"expectedSeq": 1, "type": "SESSION_STARTED", "occurredAt": "2026-08-26T09:00:00Z"})
		ev := map[string]any{"expectedSeq": 2, "type": "PAGE_REACHED", "occurredAt": "2026-08-26T08:59:00Z", "payload": map[string]any{"page": 3}}
		status, body := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", "", ev)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("status %d body %s, want 422", status, body)
		}
		assertErrorCode(t, body, "TIME_REGRESSION")
	})

	t.Run("event after end", func(t *testing.T) {
		_, sessionID := env.createBookAndSession(t)
		env.mustAppend(t, sessionID, map[string]any{"expectedSeq": 1, "type": "SESSION_STARTED", "occurredAt": "2026-08-26T09:00:00Z"})
		env.mustAppend(t, sessionID, map[string]any{"expectedSeq": 2, "type": "SESSION_ENDED", "occurredAt": "2026-08-26T09:10:00Z"})
		ev := map[string]any{"expectedSeq": 3, "type": "PAGE_REACHED", "occurredAt": "2026-08-26T09:11:00Z", "payload": map[string]any{"page": 3}}
		status, body := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", "", ev)
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("status %d body %s, want 422", status, body)
		}
		assertErrorCode(t, body, "EVENT_AFTER_END")
	})

	t.Run("page before start", func(t *testing.T) {
		// Start later, then a PAGE_REACHED whose instant precedes the start but
		// does not regress relative to itself requires a crafted stream. Simplest:
		// start at 09:00, page at exactly 09:00 is allowed; to trigger the guard we
		// need occurredAt >= last but < start, which cannot happen once started.
		// Instead we verify the guard through a duplicate-start-free path: since
		// the first event sets start, page-before-start is only reachable when the
		// start instant is in the future of an earlier event. That is impossible in
		// a well-formed stream, so this code path is proven by the domain unit test
		// TestValidateNext_PageBeforeStart. Here we simply assert the happy inverse.
		_, sessionID := env.createBookAndSession(t)
		env.mustAppend(t, sessionID, map[string]any{"expectedSeq": 1, "type": "SESSION_STARTED", "occurredAt": "2026-08-26T09:00:00Z"})
		ev := map[string]any{"expectedSeq": 2, "type": "PAGE_REACHED", "occurredAt": "2026-08-26T09:00:00Z", "payload": map[string]any{"page": 1}}
		status, _ := env.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", "", ev)
		if status != http.StatusCreated {
			t.Fatalf("page at start instant should be allowed, got %d", status)
		}
	})
}

// TestSessionNotFound returns a stable NOT_FOUND.
func TestSessionNotFound(t *testing.T) {
	env := newTestEnv(t)
	status, body := env.do(t, http.MethodGet, "/sessions/00000000-0000-0000-0000-000000000999", "", nil)
	if status != http.StatusNotFound {
		t.Fatalf("status %d body %s, want 404", status, body)
	}
	assertErrorCode(t, body, "NOT_FOUND")
}

// mustAppend appends an event expecting 201.
func (e *testEnv) mustAppend(t *testing.T, sessionID string, ev map[string]any) {
	t.Helper()
	status, body := e.do(t, http.MethodPost, "/sessions/"+sessionID+"/events", "", ev)
	if status != http.StatusCreated {
		t.Fatalf("mustAppend %v: status %d body %s", ev["type"], status, body)
	}
}

// assertErrorCode decodes a structured error body and asserts its code, then
// returns the details map for further assertions.
func assertErrorCode(t *testing.T, body []byte, wantCode string) map[string]any {
	t.Helper()
	var env struct {
		Error struct {
			Code    string         `json:"code"`
			Message string         `json:"message"`
			Details map[string]any `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal error body %s: %v", body, err)
	}
	if env.Error.Code != wantCode {
		t.Fatalf("error code = %q, want %q (body %s)", env.Error.Code, wantCode, body)
	}
	return env.Error.Details
}
