package domain

import (
	"strings"
	"time"
)

// Timestamp is a time.Time that always serializes to UTC RFC3339Nano.
type Timestamp time.Time

func NewTimestamp(t time.Time) Timestamp { return Timestamp(t.UTC()) }

func (t Timestamp) Time() time.Time { return time.Time(t).UTC() }

func (t Timestamp) IsZero() bool { return time.Time(t).IsZero() }

func (t Timestamp) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.Time().Format(time.RFC3339Nano) + `"`), nil
}

func (t *Timestamp) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	parsed, err := ParseTimestamp(s)
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}

// ParseTimestamp parses an RFC3339/RFC3339Nano string and normalizes to UTC.
func ParseTimestamp(s string) (Timestamp, error) {
	s = strings.TrimSpace(s)
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return Timestamp{}, err
		}
	}
	return NewTimestamp(t), nil
}

type BookFormat string

const (
	FormatPaperback BookFormat = "PAPERBACK"
	FormatHardcover BookFormat = "HARDCOVER"
	FormatEbook     BookFormat = "EBOOK"
	FormatAudiobook BookFormat = "AUDIOBOOK"
	FormatOther     BookFormat = "OTHER"
)

func (f BookFormat) Valid() bool {
	switch f {
	case FormatPaperback, FormatHardcover, FormatEbook, FormatAudiobook, FormatOther:
		return true
	}
	return false
}

type Book struct {
	ID            string     `json:"id"`
	ISBN          *string    `json:"isbn,omitempty"`
	Title         string     `json:"title"`
	Author        string     `json:"author"`
	Publisher     *string    `json:"publisher,omitempty"`
	PublishedYear *int       `json:"publishedYear,omitempty"`
	TotalPages    *int       `json:"totalPages,omitempty"`
	Format        BookFormat `json:"format"`
	CreatedAt     Timestamp  `json:"createdAt"`
}

type SessionStatus string

const (
	SessionPending SessionStatus = "pending"
	SessionActive  SessionStatus = "active"
	SessionEnded   SessionStatus = "ended"
)

type Session struct {
	ID        string        `json:"id"`
	BookID    string        `json:"bookId"`
	Label     *string       `json:"label,omitempty"`
	Status    SessionStatus `json:"status"`
	CreatedAt Timestamp     `json:"createdAt"`
}

type EventType string

const (
	EventSessionStarted EventType = "SESSION_STARTED"
	EventPageReached    EventType = "PAGE_REACHED"
	EventPassageReacted EventType = "PASSAGE_REACTED"
	EventInterrupted    EventType = "INTERRUPTED"
	EventSessionEnded   EventType = "SESSION_ENDED"
)

func (e EventType) Valid() bool {
	switch e {
	case EventSessionStarted, EventPageReached, EventPassageReacted, EventInterrupted, EventSessionEnded:
		return true
	}
	return false
}

type Event struct {
	ID         string    `json:"id"`
	SessionID  string    `json:"sessionId"`
	Seq        int       `json:"seq"`
	EventType  EventType `json:"eventType"`
	OccurredAt Timestamp `json:"occurredAt"`
	Page       *int      `json:"page,omitempty"`
	Note       *string   `json:"note,omitempty"`
	Quote      *string   `json:"quote,omitempty"`
	Reason     *string   `json:"reason,omitempty"`
	RecordedAt Timestamp `json:"recordedAt"`
}

type Passage struct {
	Seq        int       `json:"seq"`
	Page       *int      `json:"page,omitempty"`
	Note       string    `json:"note"`
	Quote      *string   `json:"quote,omitempty"`
	OccurredAt Timestamp `json:"occurredAt"`
}

type SessionDetail struct {
	Session
	Book                   Book       `json:"book"`
	StartedAt              *Timestamp `json:"startedAt,omitempty"`
	EndedAt                *Timestamp `json:"endedAt,omitempty"`
	ReadingDurationSeconds int64      `json:"readingDurationSeconds"`
	ReadingMinutes         float64    `json:"readingMinutes"`
	LastPage               *int       `json:"lastPage,omitempty"`
	InterruptionCount      int        `json:"interruptionCount"`
	Passages               []Passage  `json:"passages"`
	EventCount             int        `json:"eventCount"`
	Events                 []Event    `json:"events"`
}
