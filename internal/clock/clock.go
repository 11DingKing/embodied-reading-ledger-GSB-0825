// Package clock centralizes time handling so the whole service speaks one
// dialect: UTC instants serialized as RFC3339Nano. A pluggable Clock lets tests
// drive deterministic server timestamps.
package clock

import (
	"time"

	"github.com/embodied-reading/ledger/internal/apperr"
)

// Layout is the canonical wire and storage format for every instant.
const Layout = time.RFC3339Nano

// Clock provides the current time. Production uses System; tests inject a fake.
type Clock interface {
	Now() time.Time
}

// System is the real wall clock, normalized to UTC.
type System struct{}

// Now returns the current time in UTC.
func (System) Now() time.Time { return time.Now().UTC() }

// Format renders t as a canonical UTC RFC3339Nano string.
func Format(t time.Time) string {
	return t.UTC().Format(Layout)
}

// Parse parses a canonical RFC3339Nano instant and returns it in UTC. A parse
// failure yields a stable VALIDATION application error naming the field.
func Parse(field, value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, apperr.New(apperr.CodeValidation, field+" is required").
			WithDetails(map[string]any{"field": field})
	}
	t, err := time.Parse(Layout, value)
	if err != nil {
		return time.Time{}, apperr.New(apperr.CodeValidation,
			field+" must be an RFC3339Nano timestamp").
			WithDetails(map[string]any{"field": field, "value": value})
	}
	return t.UTC(), nil
}
