package wisptrace

import (
	"encoding/json"
	"time"

	"github.com/GuhaneshT/WispTraceDB/internal/wal"
)

// Status is a span's outcome. Any string is accepted; these are the
// conventional values (matching the OTLP status codes).
type Status string

const (
	StatusOK    Status = "ok"
	StatusError Status = "error"
	StatusUnset Status = "unset"
)

// Span is one traced operation, such as an LLM call or a tool invocation.
type Span struct {
	TraceID      string
	SpanID       string
	ParentSpanID string // empty for a root span

	Start    time.Time
	Duration time.Duration // stored in whole milliseconds

	Agent  string
	Model  string
	Tool   string
	Team   string
	Status Status

	TokensIn  int32
	TokensOut int32
	CostUSD   float64

	// Payload is optional caller-defined JSON metadata.
	Payload json.RawMessage
}

func (s Span) toPayload() wal.SpanPayload {
	return wal.SpanPayload{
		TraceID:      s.TraceID,
		SpanID:       s.SpanID,
		ParentSpanID: s.ParentSpanID,
		Timestamp:    s.Start.UnixNano(),
		AgentID:      s.Agent,
		Model:        s.Model,
		ToolName:     s.Tool,
		Team:         s.Team,
		Status:       string(s.Status),
		TokensIn:     s.TokensIn,
		TokensOut:    s.TokensOut,
		Cost:         s.CostUSD,
		LatencyMs:    s.Duration.Milliseconds(),
		Payload:      []byte(s.Payload),
	}
}

func fromPayload(p wal.SpanPayload) Span {
	return Span{
		TraceID:      p.TraceID,
		SpanID:       p.SpanID,
		ParentSpanID: p.ParentSpanID,
		Start:        time.Unix(0, p.Timestamp).UTC(),
		Duration:     time.Duration(p.LatencyMs) * time.Millisecond,
		Agent:        p.AgentID,
		Model:        p.Model,
		Tool:         p.ToolName,
		Team:         p.Team,
		Status:       Status(p.Status),
		TokensIn:     p.TokensIn,
		TokensOut:    p.TokensOut,
		CostUSD:      p.Cost,
		Payload:      json.RawMessage(p.Payload),
	}
}

func fromPayloads(ps []wal.SpanPayload) []Span {
	out := make([]Span, len(ps))
	for i, p := range ps {
		out[i] = fromPayload(p)
	}
	return out
}
