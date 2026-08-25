package ledger

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSummarizeExcludesInterruptionGaps(t *testing.T) {
	t0 := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)
	ev := func(seq int64, typ EventType, when time.Time, payload string) Event {
		return Event{Seq: seq, Type: typ, OccurredAt: when, Payload: json.RawMessage(payload)}
	}

	events := []Event{
		ev(1, SessionStarted, t0, `{}`),
		ev(2, PageReached, t0.Add(5*time.Minute), `{"page":18}`),
		ev(3, PassageReacted, t0.Add(12*time.Minute+30*time.Second), `{"page":22,"note":"停下来"}`),
		ev(4, Interrupted, t0.Add(20*time.Minute), `{"reason":"快递"}`),
		ev(5, PageReached, t0.Add(35*time.Minute), `{"page":26}`),
		ev(6, Interrupted, t0.Add(48*time.Minute), `{"reason":"电话"}`),
		ev(7, PageReached, t0.Add(55*time.Minute), `{"page":30}`),
		ev(8, SessionEnded, t0.Add(70*time.Minute), `{}`),
	}

	s := Summarize(events)

	want := 5*time.Minute + 7*time.Minute + 30*time.Second + 7*time.Minute + 30*time.Second +
		13*time.Minute + 15*time.Minute
	if s.ReadingDuration != want {
		t.Fatalf("readingDuration = %v, want %v (interruption gaps excluded)", s.ReadingDuration, want)
	}
	if s.InterruptionCount != 2 {
		t.Fatalf("interruptionCount = %d, want 2", s.InterruptionCount)
	}
	if s.LastPage == nil || *s.LastPage != 30 {
		t.Fatalf("lastPage = %v, want 30", s.LastPage)
	}
	if s.MaxPage != 30 {
		t.Fatalf("maxPage = %d, want 30", s.MaxPage)
	}
	if len(s.Reactions) != 1 || s.Reactions[0].Note != "停下来" {
		t.Fatalf("reactions = %+v", s.Reactions)
	}
	if s.StartedAt == nil || !s.StartedAt.Equal(t0) {
		t.Fatalf("startedAt = %v", s.StartedAt)
	}
	if s.EndedAt == nil || !s.EndedAt.Equal(t0.Add(70*time.Minute)) {
		t.Fatalf("endedAt = %v", s.EndedAt)
	}
}

func TestValidateAppendRules(t *testing.T) {
	t0 := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)
	started := &Event{Seq: 1, Type: SessionStarted, OccurredAt: t0}
	payload := func(s string) json.RawMessage { return json.RawMessage(s) }

	cases := []struct {
		name     string
		prev     *Event
		pages    int
		expSeq   int64
		evType   EventType
		when     time.Time
		payload  json.RawMessage
		wantCode string
	}{
		{"first event must be start", nil, 100, 0, PageReached, t0, payload(`{"page":1}`), "SESSION_NOT_STARTED"},
		{"start ok", nil, 100, 0, SessionStarted, t0, payload(`{}`), ""},
		{"seq mismatch", started, 100, 5, PageReached, t0.Add(time.Minute), payload(`{"page":1}`), "SEQUENCE_CONFLICT"},
		{"double start", started, 100, 1, SessionStarted, t0.Add(time.Minute), payload(`{}`), "SESSION_ALREADY_STARTED"},
		{"clock backwards", started, 100, 1, PageReached, t0.Add(-time.Minute), payload(`{"page":1}`), "CLOCK_WENT_BACKWARDS"},
		{"clock equal", started, 100, 1, PageReached, t0, payload(`{"page":1}`), "CLOCK_WENT_BACKWARDS"},
		{"page zero", started, 100, 1, PageReached, t0.Add(time.Minute), payload(`{"page":0}`), "PAGE_OUT_OF_RANGE"},
		{"page beyond book", started, 100, 1, PageReached, t0.Add(time.Minute), payload(`{"page":101}`), "PAGE_OUT_OF_RANGE"},
		{"reaction needs note", started, 100, 1, PassageReacted, t0.Add(time.Minute), payload(`{"page":1,"note":""}`), "INVALID_EVENT_PAYLOAD"},
		{"unknown payload field", started, 100, 1, PageReached, t0.Add(time.Minute), payload(`{"page":1,"x":2}`), "INVALID_EVENT_PAYLOAD"},
		{"started payload must be empty", nil, 100, 0, SessionStarted, t0, payload(`{"page":1}`), "INVALID_EVENT_PAYLOAD"},
		{"valid page", started, 100, 1, PageReached, t0.Add(time.Minute), payload(`{"page":50}`), ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAppend(tc.prev, tc.pages, tc.expSeq, tc.evType, tc.when, tc.payload)
			if tc.wantCode == "" {
				if err != nil {
					t.Fatalf("expected no error, got %s: %s", err.Code, err.Message)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error %s, got nil", tc.wantCode)
			}
			if err.Code != tc.wantCode {
				t.Fatalf("code = %s, want %s", err.Code, tc.wantCode)
			}
		})
	}

	ended := &Event{Seq: 3, Type: SessionEnded, OccurredAt: t0.Add(3 * time.Minute)}
	if err := ValidateAppend(ended, 100, 3, PageReached, t0.Add(4*time.Minute), payload(`{"page":1}`)); err == nil || err.Code != "SESSION_ALREADY_ENDED" {
		t.Fatalf("after end: err=%v", err)
	}
}
