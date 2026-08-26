// Package domain holds the core reading-ledger model: event types, the session
// state machine that validates each appended event, and the projection that
// reconstructs reading minutes, last page, interruption count, and the reader's
// own written feelings from the raw event stream.
//
// The domain layer is pure: it takes already-parsed events and prior state and
// returns decisions or projections. It performs no I/O, so its rules are unit
// testable and identical whether invoked from HTTP handlers or replays.
package domain

import (
	"time"

	"github.com/embodied-reading/ledger/internal/apperr"
)

// EventType enumerates the five ledger event kinds.
type EventType string

const (
	EventSessionStarted EventType = "SESSION_STARTED"
	EventPageReached    EventType = "PAGE_REACHED"
	EventPassageReacted EventType = "PASSAGE_REACTED"
	EventInterrupted    EventType = "INTERRUPTED"
	EventSessionEnded   EventType = "SESSION_ENDED"
)

// validTypes is the set of accepted event types.
var validTypes = map[EventType]bool{
	EventSessionStarted: true,
	EventPageReached:    true,
	EventPassageReacted: true,
	EventInterrupted:    true,
	EventSessionEnded:   true,
}

// ValidType reports whether t is a known event type.
func ValidType(t EventType) bool { return validTypes[t] }

// Payload carries the event-specific fields. Only the subset relevant to a
// given EventType is populated; the state machine validates the pairing.
type Payload struct {
	// Page is the physical page number for PAGE_REACHED.
	Page *int `json:"page,omitempty"`
	// Passage identifies the passage reacted to (e.g. "p.42 ¶2") for
	// PASSAGE_REACTED.
	Passage string `json:"passage,omitempty"`
	// Feeling is the reader's own written reaction for PASSAGE_REACTED.
	Feeling string `json:"feeling,omitempty"`
	// Reason is the reader's note on why they were INTERRUPTED.
	Reason string `json:"reason,omitempty"`
}

// Event is a fully-parsed ledger event ready for validation or projection.
type Event struct {
	Seq        int
	Type       EventType
	OccurredAt time.Time
	RecordedAt time.Time
	Payload    Payload
}

// State is the running state of a session used to validate the next event.
type State struct {
	// Count is the number of events already recorded (i.e. the current max seq).
	Count int
	// Started reports whether SESSION_STARTED has been seen.
	Started bool
	// Ended reports whether SESSION_ENDED has been seen.
	Ended bool
	// StartAt is the occurredAt of SESSION_STARTED (zero until started).
	StartAt time.Time
	// LastOccurredAt is the occurredAt of the most recent event, used to reject
	// client time regression.
	LastOccurredAt time.Time
}

// ValidateNext checks that candidate is a legal next event given the current
// state. It returns a stable application error otherwise. The rules:
//
//   - The first event MUST be SESSION_STARTED; no other first event is legal.
//   - SESSION_STARTED may occur only once, and only as the first event.
//   - No event may follow SESSION_ENDED (EVENT_AFTER_END).
//   - occurredAt must be monotonic non-decreasing (TIME_REGRESSION otherwise).
//   - PAGE_REACHED must not occur before the session start instant
//     (PAGE_BEFORE_START).
//   - Payload must be well-formed for the event type.
func ValidateNext(s State, e Event) error {
	if !ValidType(e.Type) {
		return apperr.New(apperr.CodeValidation, "unknown event type").
			WithDetails(map[string]any{"type": string(e.Type)})
	}

	// Once ended, the ledger is sealed.
	if s.Ended {
		return apperr.New(apperr.CodeEventAfterEnd,
			"session has ended; no further events may be appended")
	}

	// First event gate.
	if !s.Started {
		if e.Type != EventSessionStarted {
			return apperr.New(apperr.CodeInvalidTransition,
				"first event must be SESSION_STARTED").
				WithDetails(map[string]any{"type": string(e.Type)})
		}
	} else if e.Type == EventSessionStarted {
		return apperr.New(apperr.CodeInvalidTransition,
			"SESSION_STARTED may occur only once")
	}

	// Client clock must never move backwards across the ledger.
	if s.Started && e.OccurredAt.Before(s.LastOccurredAt) {
		return apperr.New(apperr.CodeTimeRegression,
			"occurredAt is earlier than the previous event").
			WithDetails(map[string]any{
				"occurredAt": e.OccurredAt.Format(time.RFC3339Nano),
				"previous":   s.LastOccurredAt.Format(time.RFC3339Nano),
			})
	}

	// Per-type payload and ordering rules.
	switch e.Type {
	case EventSessionStarted:
		// nothing extra to validate
	case EventPageReached:
		if e.Payload.Page == nil {
			return apperr.New(apperr.CodeValidation, "page is required for PAGE_REACHED").
				WithDetails(map[string]any{"field": "payload.page"})
		}
		if *e.Payload.Page < 1 {
			return apperr.New(apperr.CodeValidation, "page must be >= 1").
				WithDetails(map[string]any{"field": "payload.page", "value": *e.Payload.Page})
		}
		if e.OccurredAt.Before(s.StartAt) {
			return apperr.New(apperr.CodePageBeforeStart,
				"page reached before the session started").
				WithDetails(map[string]any{
					"occurredAt": e.OccurredAt.Format(time.RFC3339Nano),
					"startedAt":  s.StartAt.Format(time.RFC3339Nano),
				})
		}
	case EventPassageReacted:
		if e.Payload.Feeling == "" {
			return apperr.New(apperr.CodeValidation,
				"feeling is required for PASSAGE_REACTED").
				WithDetails(map[string]any{"field": "payload.feeling"})
		}
	case EventInterrupted:
		// reason is optional; the reader may not always say why.
	case EventSessionEnded:
		// nothing extra to validate
	}

	return nil
}

// Apply folds an event into the state, returning the updated state. It assumes
// the event has already passed ValidateNext.
func Apply(s State, e Event) State {
	s.Count = e.Seq
	s.LastOccurredAt = e.OccurredAt
	switch e.Type {
	case EventSessionStarted:
		s.Started = true
		s.StartAt = e.OccurredAt
	case EventSessionEnded:
		s.Ended = true
	}
	return s
}
