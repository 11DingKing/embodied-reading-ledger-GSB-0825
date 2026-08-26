// Package apperr defines the stable, structured error vocabulary shared by the
// domain and HTTP layers. Every failure the API can return maps to exactly one
// Code, so clients can branch on a string that never changes even if wording or
// HTTP status details evolve.
package apperr

import (
	"errors"
	"fmt"
	"net/http"
)

// Code is a stable machine-readable error identifier.
type Code string

const (
	// CodeValidation covers malformed or missing request input.
	CodeValidation Code = "VALIDATION"
	// CodeNotFound is returned when a referenced entity does not exist.
	CodeNotFound Code = "NOT_FOUND"
	// CodeSeqConflict is returned when expectedSeq does not match the ledger,
	// including the losing side of a concurrent append race. Details carry the
	// authoritative currentSeq.
	CodeSeqConflict Code = "SEQ_CONFLICT"
	// CodeInvalidTransition is returned when an event is illegal for the current
	// session state (e.g. first event is not SESSION_STARTED, or a duplicate
	// start).
	CodeInvalidTransition Code = "INVALID_TRANSITION"
	// CodeTimeRegression is returned when a client occurredAt goes backwards
	// relative to the previous event.
	CodeTimeRegression Code = "TIME_REGRESSION"
	// CodePageBeforeStart is returned when a page is reached before the session
	// start instant.
	CodePageBeforeStart Code = "PAGE_BEFORE_START"
	// CodeEventAfterEnd is returned when any event is appended after
	// SESSION_ENDED.
	CodeEventAfterEnd Code = "EVENT_AFTER_END"
	// CodeIdempotencyKeyReuse is returned when an Idempotency-Key is replayed
	// with a different request body.
	CodeIdempotencyKeyReuse Code = "IDEMPOTENCY_KEY_REUSE"
	// CodeInternal is an unexpected server-side failure.
	CodeInternal Code = "INTERNAL"
)

// Error is a structured application error carrying a stable Code, a
// human-readable Message, and optional machine-readable Details.
type Error struct {
	Code    Code           `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// New builds an Error with the given code and message.
func New(code Code, message string) *Error {
	return &Error{Code: code, Message: message}
}

// WithDetails attaches machine-readable details and returns the error for
// fluent construction.
func (e *Error) WithDetails(details map[string]any) *Error {
	e.Details = details
	return e
}

// HTTPStatus maps a Code to its canonical HTTP status.
func (e *Error) HTTPStatus() int {
	switch e.Code {
	case CodeValidation:
		return http.StatusBadRequest
	case CodeNotFound:
		return http.StatusNotFound
	case CodeSeqConflict, CodeIdempotencyKeyReuse:
		return http.StatusConflict
	case CodeInvalidTransition, CodeTimeRegression, CodePageBeforeStart, CodeEventAfterEnd:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

// As extracts an *Error from an error chain, or nil if none is present.
func As(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return nil
}
