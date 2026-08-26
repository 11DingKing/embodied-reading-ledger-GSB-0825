package domain

import "time"

// Projection is the reconstructed reading story for a session. Every field is
// derived purely from the event stream — the server computes durations from
// adjacent event instants, so no client ever asserts elapsed minutes.
type Projection struct {
	Started           bool       `json:"started"`
	Ended             bool       `json:"ended"`
	StartedAt         *time.Time `json:"startedAt,omitempty"`
	EndedAt           *time.Time `json:"endedAt,omitempty"`
	ReadingMinutes    float64    `json:"readingMinutes"`
	LastPage          *int       `json:"lastPage,omitempty"`
	InterruptionCount int        `json:"interruptionCount"`
	EventCount        int        `json:"eventCount"`
	// Feelings are the reader's own written reactions, in ledger order.
	Feelings []Feeling `json:"feelings"`
}

// Feeling is one reader-authored reaction captured by PASSAGE_REACTED.
type Feeling struct {
	Seq        int       `json:"seq"`
	Passage    string    `json:"passage,omitempty"`
	Feeling    string    `json:"feeling"`
	OccurredAt time.Time `json:"occurredAt"`
}

// Project folds an ordered event slice into a Projection.
//
// Reading minutes are the span between SESSION_STARTED and the last recorded
// event, MINUS every interval a reader was interrupted. An INTERRUPTED event
// opens a gap; the next event closes it. This yields the time actually spent
// with the book rather than raw wall-clock elapsed. Events must be pre-sorted by
// seq ascending (the caller reads them ordered).
func Project(events []Event) Projection {
	p := Projection{Feelings: []Feeling{}}
	p.EventCount = len(events)

	var (
		lastInstant     time.Time
		interruptedFrom *time.Time // set when currently inside an interruption
		totalGap        time.Duration
	)

	for i := range events {
		e := events[i]
		switch e.Type {
		case EventSessionStarted:
			p.Started = true
			st := e.OccurredAt
			p.StartedAt = &st
		case EventPageReached:
			if e.Payload.Page != nil {
				pg := *e.Payload.Page
				p.LastPage = &pg
			}
		case EventPassageReacted:
			p.Feelings = append(p.Feelings, Feeling{
				Seq:        e.Seq,
				Passage:    e.Payload.Passage,
				Feeling:    e.Payload.Feeling,
				OccurredAt: e.OccurredAt,
			})
		case EventInterrupted:
			// Open an interruption gap starting now, if not already open.
			if interruptedFrom == nil {
				from := e.OccurredAt
				interruptedFrom = &from
			}
		case EventSessionEnded:
			p.Ended = true
			et := e.OccurredAt
			p.EndedAt = &et
		}

		// Any event after an INTERRUPTED closes the interruption gap.
		if interruptedFrom != nil && e.Type != EventInterrupted {
			totalGap += e.OccurredAt.Sub(*interruptedFrom)
			interruptedFrom = nil
		}
		if e.Type == EventInterrupted {
			p.InterruptionCount++
		}

		lastInstant = e.OccurredAt
	}

	if p.Started {
		gross := lastInstant.Sub(*p.StartedAt) - totalGap
		if gross < 0 {
			gross = 0
		}
		p.ReadingMinutes = gross.Minutes()
	}

	return p
}
