package domain

import (
	"testing"
	"time"

	"github.com/embodied-reading/ledger/internal/apperr"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("parse time %q: %v", s, err)
	}
	return tm.UTC()
}

func ptr(i int) *int { return &i }

// fold applies a slice of events to build state, asserting each is valid.
func fold(t *testing.T, events []Event) State {
	t.Helper()
	var st State
	for _, e := range events {
		if err := ValidateNext(st, e); err != nil {
			t.Fatalf("unexpected validation error at seq %d: %v", e.Seq, err)
		}
		st = Apply(st, e)
	}
	return st
}

func TestValidateNext_FirstEventMustBeStart(t *testing.T) {
	var st State
	e := Event{Seq: 1, Type: EventPageReached, OccurredAt: mustTime(t, "2026-08-26T09:00:00Z"), Payload: Payload{Page: ptr(1)}}
	err := ValidateNext(st, e)
	ae := apperr.As(err)
	if ae == nil || ae.Code != apperr.CodeInvalidTransition {
		t.Fatalf("want INVALID_TRANSITION, got %v", err)
	}
}

func TestValidateNext_DuplicateStart(t *testing.T) {
	st := fold(t, []Event{
		{Seq: 1, Type: EventSessionStarted, OccurredAt: mustTime(t, "2026-08-26T09:00:00Z")},
	})
	err := ValidateNext(st, Event{Seq: 2, Type: EventSessionStarted, OccurredAt: mustTime(t, "2026-08-26T09:01:00Z")})
	ae := apperr.As(err)
	if ae == nil || ae.Code != apperr.CodeInvalidTransition {
		t.Fatalf("want INVALID_TRANSITION for duplicate start, got %v", err)
	}
}

func TestValidateNext_TimeRegression(t *testing.T) {
	st := fold(t, []Event{
		{Seq: 1, Type: EventSessionStarted, OccurredAt: mustTime(t, "2026-08-26T09:00:00Z")},
	})
	err := ValidateNext(st, Event{Seq: 2, Type: EventPageReached,
		OccurredAt: mustTime(t, "2026-08-26T08:59:59Z"), Payload: Payload{Page: ptr(5)}})
	ae := apperr.As(err)
	if ae == nil || ae.Code != apperr.CodeTimeRegression {
		t.Fatalf("want TIME_REGRESSION, got %v", err)
	}
}

func TestValidateNext_PageBeforeStart(t *testing.T) {
	// Start at 09:00; a PAGE_REACHED whose occurredAt equals start is fine, but
	// before start must fail. Since time regression also guards backwards moves,
	// we craft a case where lastOccurredAt == start and page == start-safe path
	// isn't triggered: use equal start but page instant earlier than start is
	// impossible without regression. Instead assert the dedicated code fires when
	// the page equals the previous instant yet precedes start — construct via a
	// state whose StartAt is later than LastOccurredAt is not natural, so we test
	// the direct guard: page exactly at start passes.
	st := State{Started: true, StartAt: mustTime(t, "2026-08-26T09:00:00Z"),
		LastOccurredAt: mustTime(t, "2026-08-26T08:00:00Z"), Count: 1}
	err := ValidateNext(st, Event{Seq: 2, Type: EventPageReached,
		OccurredAt: mustTime(t, "2026-08-26T08:30:00Z"), Payload: Payload{Page: ptr(3)}})
	ae := apperr.As(err)
	if ae == nil || ae.Code != apperr.CodePageBeforeStart {
		t.Fatalf("want PAGE_BEFORE_START, got %v", err)
	}
}

func TestValidateNext_EventAfterEnd(t *testing.T) {
	st := fold(t, []Event{
		{Seq: 1, Type: EventSessionStarted, OccurredAt: mustTime(t, "2026-08-26T09:00:00Z")},
		{Seq: 2, Type: EventSessionEnded, OccurredAt: mustTime(t, "2026-08-26T09:10:00Z")},
	})
	err := ValidateNext(st, Event{Seq: 3, Type: EventPageReached,
		OccurredAt: mustTime(t, "2026-08-26T09:11:00Z"), Payload: Payload{Page: ptr(9)}})
	ae := apperr.As(err)
	if ae == nil || ae.Code != apperr.CodeEventAfterEnd {
		t.Fatalf("want EVENT_AFTER_END, got %v", err)
	}
}

func TestValidateNext_PassageRequiresFeeling(t *testing.T) {
	st := fold(t, []Event{
		{Seq: 1, Type: EventSessionStarted, OccurredAt: mustTime(t, "2026-08-26T09:00:00Z")},
	})
	err := ValidateNext(st, Event{Seq: 2, Type: EventPassageReacted,
		OccurredAt: mustTime(t, "2026-08-26T09:05:00Z"), Payload: Payload{Passage: "p.1"}})
	ae := apperr.As(err)
	if ae == nil || ae.Code != apperr.CodeValidation {
		t.Fatalf("want VALIDATION for missing feeling, got %v", err)
	}
}

func TestProject_ReadingStory(t *testing.T) {
	events := []Event{
		{Seq: 1, Type: EventSessionStarted, OccurredAt: mustTime(t, "2026-08-26T09:00:00Z")},
		{Seq: 2, Type: EventPageReached, OccurredAt: mustTime(t, "2026-08-26T09:12:30Z"), Payload: Payload{Page: ptr(12)}},
		{Seq: 3, Type: EventPassageReacted, OccurredAt: mustTime(t, "2026-08-26T09:20:00Z"), Payload: Payload{Passage: "grief", Feeling: "it caught in my throat"}},
		{Seq: 4, Type: EventInterrupted, OccurredAt: mustTime(t, "2026-08-26T09:25:00Z"), Payload: Payload{Reason: "doorbell"}},
		{Seq: 5, Type: EventPageReached, OccurredAt: mustTime(t, "2026-08-26T09:33:00Z"), Payload: Payload{Page: ptr(18)}},
		{Seq: 6, Type: EventSessionEnded, OccurredAt: mustTime(t, "2026-08-26T09:40:00Z")},
	}
	p := Project(events)

	if !p.Started || !p.Ended {
		t.Fatalf("expected started and ended")
	}
	// Gross 40m minus 8m interruption gap = 32m.
	if p.ReadingMinutes != 32 {
		t.Fatalf("readingMinutes = %v, want 32", p.ReadingMinutes)
	}
	if p.LastPage == nil || *p.LastPage != 18 {
		t.Fatalf("lastPage = %v, want 18", p.LastPage)
	}
	if p.InterruptionCount != 1 {
		t.Fatalf("interruptionCount = %d, want 1", p.InterruptionCount)
	}
	if len(p.Feelings) != 1 || p.Feelings[0].Feeling != "it caught in my throat" {
		t.Fatalf("feelings = %+v, want one recorded feeling", p.Feelings)
	}
	if p.EventCount != 6 {
		t.Fatalf("eventCount = %d, want 6", p.EventCount)
	}
}

func TestProject_MultipleInterruptions(t *testing.T) {
	events := []Event{
		{Seq: 1, Type: EventSessionStarted, OccurredAt: mustTime(t, "2026-08-26T10:00:00Z")},
		{Seq: 2, Type: EventInterrupted, OccurredAt: mustTime(t, "2026-08-26T10:05:00Z")},
		{Seq: 3, Type: EventPageReached, OccurredAt: mustTime(t, "2026-08-26T10:15:00Z"), Payload: Payload{Page: ptr(3)}}, // 10m gap
		{Seq: 4, Type: EventInterrupted, OccurredAt: mustTime(t, "2026-08-26T10:20:00Z")},
		{Seq: 5, Type: EventSessionEnded, OccurredAt: mustTime(t, "2026-08-26T10:30:00Z")}, // 10m gap
	}
	p := Project(events)
	// Gross 30m minus (10m + 10m) = 10m.
	if p.ReadingMinutes != 10 {
		t.Fatalf("readingMinutes = %v, want 10", p.ReadingMinutes)
	}
	if p.InterruptionCount != 2 {
		t.Fatalf("interruptionCount = %d, want 2", p.InterruptionCount)
	}
}
