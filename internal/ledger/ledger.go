package ledger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

type EventType string

const (
	SessionStarted EventType = "SESSION_STARTED"
	PageReached    EventType = "PAGE_REACHED"
	PassageReacted EventType = "PASSAGE_REACTED"
	Interrupted    EventType = "INTERRUPTED"
	SessionEnded   EventType = "SESSION_ENDED"
)

func ParseEventType(s string) (EventType, bool) {
	switch EventType(s) {
	case SessionStarted, PageReached, PassageReacted, Interrupted, SessionEnded:
		return EventType(s), true
	default:
		return "", false
	}
}

type Event struct {
	Seq        int64
	Type       EventType
	OccurredAt time.Time
	Payload    json.RawMessage
}

type PageReachedPayload struct {
	Page int `json:"page"`
}

type PassageReactedPayload struct {
	Page  int    `json:"page,omitempty"`
	Quote string `json:"quote,omitempty"`
	Note  string `json:"note"`
}

type InterruptedPayload struct {
	Reason string `json:"reason,omitempty"`
}

type RuleError struct {
	Code    string
	Message string
	Details map[string]any
}

func (e *RuleError) Error() string { return e.Code + ": " + e.Message }

func ruleError(code, message string, details map[string]any) *RuleError {
	return &RuleError{Code: code, Message: message, Details: details}
}

func ValidateAppend(prev *Event, totalPages int, expectedSeq int64, evType EventType, occurredAt time.Time, payload json.RawMessage) *RuleError {
	currentSeq := int64(0)
	if prev != nil {
		currentSeq = prev.Seq
	}

	if expectedSeq != currentSeq {
		return ruleError(
			"SEQUENCE_CONFLICT",
			fmt.Sprintf("expected seq %d but session is at seq %d", expectedSeq, currentSeq),
			map[string]any{"expectedSeq": expectedSeq, "currentSeq": currentSeq},
		)
	}

	if prev == nil {
		if evType != SessionStarted {
			return ruleError(
				"SESSION_NOT_STARTED",
				"the first event of a session must be SESSION_STARTED",
				map[string]any{"eventType": string(evType)},
			)
		}
	} else {
		if prev.Type == SessionEnded {
			return ruleError(
				"SESSION_ALREADY_ENDED",
				"cannot append events after SESSION_ENDED",
				map[string]any{"lastEventType": string(SessionEnded)},
			)
		}
		if evType == SessionStarted {
			return ruleError(
				"SESSION_ALREADY_STARTED",
				"SESSION_STARTED can only be appended once",
				map[string]any{"lastEventType": string(prev.Type)},
			)
		}
		if !occurredAt.After(prev.OccurredAt) {
			return ruleError(
				"CLOCK_WENT_BACKWARDS",
				"occurredAt must be strictly later than the previous event",
				map[string]any{
					"previousOccurredAt": prev.OccurredAt.UTC().Format(time.RFC3339Nano),
					"occurredAt":         occurredAt.UTC().Format(time.RFC3339Nano),
				},
			)
		}
	}

	switch evType {
	case PageReached:
		var p PageReachedPayload
		if err := decodePayload(payload, &p); err != nil {
			return ruleError("INVALID_EVENT_PAYLOAD", err.Error(), map[string]any{"eventType": string(evType)})
		}
		if p.Page < 1 || p.Page > totalPages {
			return ruleError(
				"PAGE_OUT_OF_RANGE",
				fmt.Sprintf("page %d is outside the book range 1..%d", p.Page, totalPages),
				map[string]any{"page": p.Page, "totalPages": totalPages},
			)
		}
	case PassageReacted:
		var p PassageReactedPayload
		if err := decodePayload(payload, &p); err != nil {
			return ruleError("INVALID_EVENT_PAYLOAD", err.Error(), map[string]any{"eventType": string(evType)})
		}
		if isBlank(p.Note) {
			return ruleError(
				"INVALID_EVENT_PAYLOAD",
				"note is required for PASSAGE_REACTED",
				map[string]any{"eventType": string(evType)},
			)
		}
		if p.Page < 0 || p.Page > totalPages {
			return ruleError(
				"PAGE_OUT_OF_RANGE",
				fmt.Sprintf("page %d is outside the book range 0..%d", p.Page, totalPages),
				map[string]any{"page": p.Page, "totalPages": totalPages},
			)
		}
	case Interrupted:
		var p InterruptedPayload
		if err := decodePayload(payload, &p); err != nil {
			return ruleError("INVALID_EVENT_PAYLOAD", err.Error(), map[string]any{"eventType": string(evType)})
		}
	case SessionStarted, SessionEnded:
		var empty struct{}
		if err := decodePayload(payload, &empty); err != nil {
			return ruleError("INVALID_EVENT_PAYLOAD", err.Error(), map[string]any{"eventType": string(evType)})
		}
	}

	return nil
}

func decodePayload(raw json.RawMessage, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid payload for event: %w", err)
	}
	return nil
}

func isBlank(s string) bool {
	for _, r := range s {
		if r != ' ' && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
	}
	return true
}

type Reaction struct {
	Seq        int64     `json:"seq"`
	OccurredAt time.Time `json:"occurredAt"`
	Page       int       `json:"page,omitempty"`
	Quote      string    `json:"quote,omitempty"`
	Note       string    `json:"note"`
}

type Summary struct {
	StartedAt         *time.Time
	EndedAt           *time.Time
	LastPage          *int
	MaxPage           int
	InterruptionCount int
	ReadingDuration   time.Duration
	Reactions         []Reaction
}

func Summarize(events []Event) Summary {
	s := Summary{Reactions: []Reaction{}}

	for i, ev := range events {
		switch ev.Type {
		case SessionStarted:
			t := ev.OccurredAt.UTC()
			s.StartedAt = &t
		case SessionEnded:
			t := ev.OccurredAt.UTC()
			s.EndedAt = &t
		case PageReached:
			var p PageReachedPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				page := p.Page
				s.LastPage = &page
				if p.Page > s.MaxPage {
					s.MaxPage = p.Page
				}
			}
		case Interrupted:
			s.InterruptionCount++
		case PassageReacted:
			var p PassageReactedPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				s.Reactions = append(s.Reactions, Reaction{
					Seq:        ev.Seq,
					OccurredAt: ev.OccurredAt.UTC(),
					Page:       p.Page,
					Quote:      p.Quote,
					Note:       p.Note,
				})
			}
		}

		if i > 0 {
			prev := events[i-1]
			if prev.Type != Interrupted {
				s.ReadingDuration += ev.OccurredAt.Sub(prev.OccurredAt)
			}
		}
	}

	return s
}
