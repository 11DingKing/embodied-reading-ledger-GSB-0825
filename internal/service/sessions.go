package service

import (
	"context"
	"errors"
	"time"

	"embodied-reading-ledger/internal/domain"
	"embodied-reading-ledger/internal/store"
)

// GetSession returns a session with derived reading metrics computed from the event ledger.
func (s *Service) GetSession(ctx context.Context, id string) (*domain.SessionDetail, *APIError) {
	if !isValidUUID(id) {
		return nil, errSessionNotFound()
	}
	st := store.New(s.pool)

	sess, err := st.GetSession(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, errSessionNotFound()
		}
		return nil, &APIError{Status: 500, Code: CodeInternal, Message: err.Error()}
	}

	book, err := st.GetBook(ctx, sess.BookID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, errBookNotFound()
		}
		return nil, &APIError{Status: 500, Code: CodeInternal, Message: err.Error()}
	}

	events, err := st.ListEvents(ctx, id)
	if err != nil {
		return nil, &APIError{Status: 500, Code: CodeInternal, Message: err.Error()}
	}

	detail := &domain.SessionDetail{
		Session:  *sess,
		Book:     *book,
		Passages: []domain.Passage{},
		Events:   events,
	}

	var startedAt, endedAt *domain.Timestamp
	var lastPage *int
	interruptions := 0
	var readingDuration time.Duration
	passages := make([]domain.Passage, 0)

	for i, e := range events {
		switch e.EventType {
		case domain.EventSessionStarted:
			ts := e.OccurredAt
			startedAt = &ts
		case domain.EventSessionEnded:
			ts := e.OccurredAt
			endedAt = &ts
		case domain.EventInterrupted:
			interruptions++
		case domain.EventPassageReacted:
			note := ""
			if e.Note != nil {
				note = *e.Note
			}
			passages = append(passages, domain.Passage{
				Seq:        e.Seq,
				Page:       e.Page,
				Note:       note,
				Quote:      e.Quote,
				OccurredAt: e.OccurredAt,
			})
		case domain.EventPageReached:
			// lastPage is the page of the most recent PAGE_REACHED event,
			// not the maximum — readers may page back to re-read. PASSAGE_REACTED
			// deliberately does not update it.
			if e.Page != nil {
				v := *e.Page
				lastPage = &v
			}
		}

		// Reading duration accumulates across adjacent events, excluding the
		// gap that follows an INTERRUPTED event (the interruption itself).
		if i > 0 {
			prev := events[i-1]
			if prev.EventType != domain.EventInterrupted {
				readingDuration += e.OccurredAt.Time().Sub(prev.OccurredAt.Time())
			}
		}
	}

	detail.StartedAt = startedAt
	detail.EndedAt = endedAt
	detail.LastPage = lastPage
	detail.InterruptionCount = interruptions
	detail.Passages = passages
	detail.EventCount = len(events)
	detail.ReadingDurationSeconds = int64(readingDuration / time.Second)
	detail.ReadingMinutes = readingDuration.Minutes()

	return detail, nil
}
