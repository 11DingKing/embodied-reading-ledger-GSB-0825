package service

import "fmt"

// Stable error codes returned in the structured error body.
const (
	CodeInvalidRequest        = "INVALID_REQUEST"
	CodeValidationError       = "VALIDATION_ERROR"
	CodeBookNotFound          = "BOOK_NOT_FOUND"
	CodeSessionNotFound       = "SESSION_NOT_FOUND"
	CodeIdempotencyKeyReused  = "IDEMPOTENCY_KEY_REUSED"
	CodeSeqConflict           = "SEQ_CONFLICT"
	CodeSessionNotStarted     = "SESSION_NOT_STARTED"
	CodeSessionAlreadyStarted = "SESSION_ALREADY_STARTED"
	CodeSessionAlreadyEnded   = "SESSION_ALREADY_ENDED"
	CodeTimestampNotMonotonic = "TIMESTAMP_NOT_MONOTONIC"
	CodeInvalidPage           = "INVALID_PAGE"
	CodeInvalidEventType      = "INVALID_EVENT_TYPE"
	CodeNoteRequired          = "NOTE_REQUIRED"
	CodeInternal              = "INTERNAL"
)

// APIError is a controlled error that maps to an HTTP status and stable code.
type APIError struct {
	Status  int
	Code    string
	Message string
	Details map[string]any
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func newAPIError(status int, code, message string, details map[string]any) *APIError {
	return &APIError{Status: status, Code: code, Message: message, Details: details}
}

func errInvalidRequest(message string) *APIError {
	return newAPIError(400, CodeInvalidRequest, message, nil)
}

func errValidation(message string, details map[string]any) *APIError {
	return newAPIError(422, CodeValidationError, message, details)
}

func errBookNotFound() *APIError {
	return newAPIError(404, CodeBookNotFound, "book not found", nil)
}

func errSessionNotFound() *APIError {
	return newAPIError(404, CodeSessionNotFound, "session not found", nil)
}

func errIdempotencyReused() *APIError {
	return newAPIError(422, CodeIdempotencyKeyReused,
		"idempotency key was already used with a different request", nil)
}

func errSeqConflict(currentSeq int) *APIError {
	return newAPIError(409, CodeSeqConflict,
		"expectedSeq does not match the current sequence",
		map[string]any{"currentSeq": currentSeq, "expectedSeq": nil})
}

func errSessionNotStarted() *APIError {
	return newAPIError(422, CodeSessionNotStarted,
		"the first event of a session must be SESSION_STARTED", nil)
}

func errSessionAlreadyStarted() *APIError {
	return newAPIError(422, CodeSessionAlreadyStarted,
		"SESSION_STARTED may only occur once and must be the first event", nil)
}

func errSessionAlreadyEnded() *APIError {
	return newAPIError(422, CodeSessionAlreadyEnded,
		"session has ended; no further events may be appended", nil)
}

func errTimestampNotMonotonic(previous, next string) *APIError {
	return newAPIError(422, CodeTimestampNotMonotonic,
		"occurredAt must be equal to or later than the previous event",
		map[string]any{"previousOccurredAt": previous, "attemptedOccurredAt": next})
}

func errInvalidPage(message string) *APIError {
	return newAPIError(422, CodeInvalidPage, message, nil)
}

func errInvalidEventType(t string) *APIError {
	return newAPIError(400, CodeInvalidEventType, "unknown event type: "+t, map[string]any{"eventType": t})
}

func errNoteRequired() *APIError {
	return newAPIError(422, CodeNoteRequired, "note is required for PASSAGE_REACTED", nil)
}
