package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/embodied-reading/ledger/internal/domain"
	"github.com/embodied-reading/ledger/internal/store"
)

// TestAppendEventTx_ConflictRecoversAbortedTx deterministically drives the
// PostgreSQL unique-key collision branch of AppendEventTx — the exact path the
// savepoint fix targets — and asserts the returned SEQ_CONFLICT carries the true
// currentSeq (1), never a bogus 0, and that the losing transaction remains
// usable afterwards.
//
// The interleaving is forced with two live transactions and a handshake:
//
//	A: begin → AppendEventTx (INSERT seq 1, uncommitted) → signal → wait → COMMIT
//	B: begin → wait for A's insert → AppendEventTx:
//	       loadSessionEvents sees an EMPTY ledger (A uncommitted, READ COMMITTED),
//	       so B computes wantSeq == 1 and passes every pre-check, then its INSERT
//	       blocks on A's uncommitted unique-index entry.
//	main: once B is blocking, let A COMMIT → B's INSERT fails with unique_violation.
//
// Before the fix, B's follow-up currentSeq query executed inside the now-aborted
// transaction and failed, so the handler fell back to currentSeq: 0. With the
// savepoint, B rolls back only the savepoint, the transaction survives, and the
// fresh SELECT MAX(seq) (READ COMMITTED) sees A's committed row → returns 1.
func TestAppendEventTx_ConflictRecoversAbortedTx(t *testing.T) {
	env := newTestEnv(t)
	_, sessionID := env.createBookAndSession(t)

	ctx := context.Background()
	occurred := time.Date(2026, 8, 26, 9, 0, 0, 0, time.UTC)
	in := store.AppendEventInput{
		SessionID:   sessionID,
		ExpectedSeq: 1,
		Type:        domain.EventSessionStarted,
		OccurredAt:  occurred,
	}

	txA, err := env.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin txA: %v", err)
	}
	defer txA.Rollback(ctx) //nolint:errcheck
	txB, err := env.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin txB: %v", err)
	}
	defer txB.Rollback(ctx) //nolint:errcheck

	// A inserts seq 1 but does not yet commit.
	if _, err := env.store.AppendEventTx(ctx, txA, in); err != nil {
		t.Fatalf("txA append should succeed: %v", err)
	}

	// B runs concurrently; its INSERT will block on A's uncommitted row.
	type bResult struct {
		err error
	}
	done := make(chan bResult, 1)
	go func() {
		_, bErr := env.store.AppendEventTx(ctx, txB, in)
		done <- bResult{err: bErr}
	}()

	// Give B time to reach loadSessionEvents (sees empty) and block on the INSERT
	// against A's uncommitted unique-index entry, then release A by committing.
	select {
	case r := <-done:
		// B finished before A committed. This can only happen if B did NOT block on
		// the INSERT, i.e. it took the pre-check path. That still must report
		// currentSeq 1, but it does not exercise the DB-collision recovery branch,
		// so fail loudly to keep the test meaningful.
		t.Fatalf("txB returned before A committed (pre-check path, not the DB collision): %v", r.err)
	case <-time.After(300 * time.Millisecond):
		// B is blocked on the unique-index lock as intended.
	}

	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit txA: %v", err)
	}

	var r bResult
	select {
	case r = <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("txB did not return after A committed (possible deadlock)")
	}

	if r.err == nil {
		t.Fatalf("txB append should conflict, got success")
	}
	ae := asAppErr(t, r.err)
	if ae.Code != "SEQ_CONFLICT" {
		t.Fatalf("error code = %q, want SEQ_CONFLICT (err %v)", ae.Code, r.err)
	}
	cs, ok := ae.Details["currentSeq"]
	if !ok {
		t.Fatalf("conflict details missing currentSeq: %+v", ae.Details)
	}
	if got := toInt(cs); got != 1 {
		t.Fatalf("currentSeq = %d, want 1 (details %+v)", got, ae.Details)
	}

	// The transaction must still be usable after the recovered conflict — a plain
	// query should succeed rather than fail with "current transaction is aborted".
	var n int
	if err := txB.QueryRow(ctx, "SELECT COUNT(*) FROM events WHERE session_id = $1", sessionID).Scan(&n); err != nil {
		t.Fatalf("transaction unusable after conflict (savepoint not applied?): %v", err)
	}
	if n != 1 {
		t.Fatalf("event count via txB = %d, want 1", n)
	}
}

// appErr mirrors the structured error shape for decoding without importing the
// internal apperr concrete type.
type appErr struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

// asAppErr extracts the structured error via its JSON representation. The
// store's application error marshals to {code,message,details}.
func asAppErr(t *testing.T, err error) appErr {
	t.Helper()
	b, mErr := json.Marshal(err)
	if mErr != nil {
		t.Fatalf("marshal error: %v", mErr)
	}
	var ae appErr
	if uErr := json.Unmarshal(b, &ae); uErr != nil {
		t.Fatalf("unmarshal error %s: %v", b, uErr)
	}
	if ae.Code == "" {
		t.Fatalf("error did not carry a structured code: %s (%v)", b, err)
	}
	return ae
}

func toInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	default:
		return -1
	}
}
