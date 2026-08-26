// Package errs defines the structured application error type shared by the
// store and HTTP layers. Every failure is rendered as
// {"error":{"code","message","details?"}} with a stable machine-readable code.
package errs

import "fmt"

// Stable error codes returned by the API.
const (
	CodeValidation            = "E_VALIDATION"
	CodeNotFound              = "E_NOT_FOUND"
	CodeIdempotencyRequired   = "E_IDEMPOTENCY_REQUIRED"
	CodeIdempotencyMismatch   = "E_IDEMPOTENCY_MISMATCH"
	CodeIdempotencyInProgress = "E_IDEMPOTENCY_IN_PROGRESS"
	CodeSeqConflict           = "E_SEQ_CONFLICT"
	CodeTimeRegression        = "E_TIME_REGRESSION"
	CodePageBeforeStart       = "E_PAGE_BEFORE_START"
	CodeAppendAfterEnd        = "E_APPEND_AFTER_END"
	CodeInvalidTransition     = "E_INVALID_STATE_TRANSITION"
	CodeConflict              = "E_CONFLICT"
	CodeInternal              = "E_INTERNAL"
)

// AppError is a structured application error safe to serialize to clients.
type AppError struct {
	Status  int            `json:"-"`
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (e *AppError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func New(status int, code, message string) *AppError {
	return &AppError{Status: status, Code: code, Message: message}
}

func WithDetails(status int, code, message string, details map[string]any) *AppError {
	return &AppError{Status: status, Code: code, Message: message, Details: details}
}

func Validation(message string) *AppError { return New(400, CodeValidation, message) }

func NotFound(message string) *AppError { return New(404, CodeNotFound, message) }

func Internal(err error) *AppError {
	return New(500, CodeInternal, "internal error: "+err.Error())
}
