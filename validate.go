package wisptrace

import (
	"encoding/json"
	"math"
	"time"
)

const (
	// MaxDimensionLen caps the dimension strings (ids, model, tool, team, status).
	MaxDimensionLen = 256
	// MaxPayloadBytes caps Span.Payload.
	MaxPayloadBytes = 64 * 1024
)

// Valid timestamps are Unix nanoseconds between 2000-01-01 and 2100-01-01.
// A millisecond or second timestamp falls far below this range, so it is
// rejected instead of silently landing in 1970.
var (
	minStart = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	maxStart = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
)

// Validate reports the first rule the span breaks, as a *ValidationError.
func (s Span) Validate() error {
	bad := func(field, reason string) error { return &ValidationError{Field: field, Reason: reason} }

	if s.TraceID == "" {
		return bad("TraceID", "must not be empty")
	}
	if s.SpanID == "" {
		return bad("SpanID", "must not be empty")
	}
	for _, f := range []struct{ name, v string }{
		{"TraceID", s.TraceID}, {"SpanID", s.SpanID}, {"ParentSpanID", s.ParentSpanID},
		{"Agent", s.Agent}, {"Model", s.Model}, {"Tool", s.Tool}, {"Team", s.Team},
		{"Status", string(s.Status)},
	} {
		if len(f.v) > MaxDimensionLen {
			return bad(f.name, "longer than 256 bytes")
		}
	}
	if s.Start.IsZero() || s.Start.Before(minStart) || !s.Start.Before(maxStart) {
		return bad("Start", "must be between years 2000 and 2100")
	}
	if s.Duration < 0 {
		return bad("Duration", "must not be negative")
	}
	if s.TokensIn < 0 || s.TokensOut < 0 {
		return bad("Tokens", "must not be negative")
	}
	if s.CostUSD < 0 || math.IsNaN(s.CostUSD) || math.IsInf(s.CostUSD, 0) {
		return bad("CostUSD", "must be a finite, non-negative number")
	}
	if len(s.Payload) > MaxPayloadBytes {
		return bad("Payload", "larger than 64 KiB")
	}
	if len(s.Payload) > 0 && !json.Valid(s.Payload) {
		return bad("Payload", "must be valid JSON")
	}
	return nil
}
