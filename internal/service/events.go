package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"embodied-reading-ledger/internal/domain"
	"embodied-reading-ledger/internal/store"

	"github.com/jackc/pgx/v5"
)

// AppendEventRequest is the payload for POST /sessions/{id}/events.
type AppendEventRequest struct {
	EventType   domain.EventType `json:"eventType"`
	OccurredAt  string           `json:"occurredAt"`
	Page        *int             `json:"page"`
	Note        *string          `json:"note"`
	Quote       *string          `json:"quote"`
	Reason      *string          `json:"reason"`
	ExpectedSeq int              `json:"expectedSeq"`
}

func (s *Service) AppendEvent(
	ctx context.Context,
	sessionID string,
	req AppendEventRequest,
	idemKey string,
	reqHash []byte,
) (int, []byte, error) {
	if !isValidUUID(sessionID) {
		// Return 404-style validation; keep stable code via errSessionNotFound path.
		return 0, nil, errSessionNotFound()
	}
	occurred, err := domain.ParseTimestamp(req.OccurredAt)
	if err != nil {
		return 0, nil, errInvalidRequest("occurredAt must be an RFC3339/RFC3339Nano timestamp")
	}
	if req.Note != nil {
		v := strings.TrimSpace(*req.Note)
		req.Note = &v
	}
	if req.Quote != nil {
		v := strings.TrimSpace(*req.Quote)
		req.Quote = &v
	}
	if req.Reason != nil {
		v := strings.TrimSpace(*req.Reason)
		req.Reason = &v
	}
	if req.EventType == "" {
		return 0, nil, errInvalidRequest("eventType is required")
	}
	if !req.EventType.Valid() {
		return 0, nil, errInvalidEventType(string(req.EventType))
	}

	path := "/sessions/" + sessionID + "/events"

	return s.writeTxn(ctx, http.MethodPost, path, idemKey, reqHash,
		func(ctx context.Context, tx pgx.Tx) (int, any, *APIError) {
			st := store.New(tx)

			// 1. Lock the session row to serialize all writes for this session.
			sess, err := st.LockSession(ctx, sessionID)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					return 0, nil, errSessionNotFound()
				}
				return 0, nil, &APIError{Status: 500, Code: CodeInternal, Message: err.Error()}
			}

			// 2. Determine current max sequence and check expectedSeq (optimistic concurrency).
			currentSeq, err := st.MaxSeq(ctx, sessionID)
			if err != nil {
				return 0, nil, &APIError{Status: 500, Code: CodeInternal, Message: err.Error()}
			}
			if req.ExpectedSeq != currentSeq {
				apiErr := errSeqConflict(currentSeq)
				apiErr.Details["expectedSeq"] = req.ExpectedSeq
				return 0, nil, apiErr
			}

			// 3. State machine: enforce allowed transitions.
			if currentSeq == 0 {
				if req.EventType != domain.EventSessionStarted {
					return 0, nil, errSessionNotStarted()
				}
			} else {
				if sess.Status == domain.SessionEnded {
					return 0, nil, errSessionAlreadyEnded()
				}
				if req.EventType == domain.EventSessionStarted {
					return 0, nil, errSessionAlreadyStarted()
				}
			}

			// 4. Monotonic client timestamps.
			last, err := st.GetLastEvent(ctx, sessionID)
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return 0, nil, &APIError{Status: 500, Code: CodeInternal, Message: err.Error()}
			}
			if last != nil {
				newT := occurred.Time()
				if newT.Before(last.OccurredAt) {
					return 0, nil, errTimestampNotMonotonic(
						last.OccurredAt.UTC().Format(time.RFC3339Nano),
						newT.UTC().Format(time.RFC3339Nano),
					)
				}
			}

			// 5. Type-specific field validation.
			book, berr := st.GetBook(ctx, sess.BookID)
			if berr != nil {
				if errors.Is(berr, store.ErrNotFound) {
					return 0, nil, errBookNotFound()
				}
				return 0, nil, &APIError{Status: 500, Code: CodeInternal, Message: berr.Error()}
			}
			if apiErr := validateEventFields(req, book); apiErr != nil {
				return 0, nil, apiErr
			}

			// 6. Append the event (append-only table).
			e, err := st.InsertEvent(ctx, store.InsertEventParams{
				SessionID:  sessionID,
				Seq:        currentSeq + 1,
				EventType:  req.EventType,
				OccurredAt: occurred.Time(),
				Page:       req.Page,
				Note:       req.Note,
				Quote:      req.Quote,
				Reason:     req.Reason,
			})
			if err != nil {
				return 0, nil, &APIError{Status: 500, Code: CodeInternal, Message: err.Error()}
			}

			// 7. Advance session status.
			switch req.EventType {
			case domain.EventSessionStarted:
				err = st.UpdateSessionStatus(ctx, sessionID, domain.SessionActive)
			case domain.EventSessionEnded:
				err = st.UpdateSessionStatus(ctx, sessionID, domain.SessionEnded)
			}
			if err != nil {
				return 0, nil, &APIError{Status: 500, Code: CodeInternal, Message: err.Error()}
			}

			return http.StatusCreated, e, nil
		})
}

func validateEventFields(req AppendEventRequest, book *domain.Book) *APIError {
	switch req.EventType {
	case domain.EventPageReached:
		if req.Page == nil {
			return errInvalidPage("page is required for PAGE_REACHED")
		}
		if *req.Page <= 0 {
			return errInvalidPage("page must be a positive integer")
		}
		if book.TotalPages != nil && *req.Page > *book.TotalPages {
			return errInvalidPage("page exceeds the book's total pages")
		}
	case domain.EventPassageReacted:
		if req.Note == nil || *req.Note == "" {
			return errNoteRequired()
		}
		if req.Page != nil {
			if *req.Page <= 0 {
				return errInvalidPage("page must be a positive integer")
			}
			if book.TotalPages != nil && *req.Page > *book.TotalPages {
				return errInvalidPage("page exceeds the book's total pages")
			}
		}
	case domain.EventSessionStarted, domain.EventSessionEnded, domain.EventInterrupted:
		// no required fields beyond occurredAt
	}
	return nil
}
