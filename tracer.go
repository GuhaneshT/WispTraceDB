package wisptrace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// ErrEnded is returned when End is called on a span that was already ended
// and stored.
var ErrEnded = errors.New("wisptrace: span already ended")

// NewTraceID returns a random 16-byte trace id as 32 hex characters, the
// OpenTelemetry format.
func NewTraceID() string { return randomHex(16) }

// NewSpanID returns a random 8-byte span id as 16 hex characters.
func NewSpanID() string { return randomHex(8) }

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("wisptrace: no randomness available: " + err.Error())
	}
	return hex.EncodeToString(b)
}

type spanCtxKey struct{}

// SpanFromContext returns the innermost span started with StartSpan, or nil.
func SpanFromContext(ctx context.Context) *ActiveSpan {
	s, _ := ctx.Value(spanCtxKey{}).(*ActiveSpan)
	return s
}

type startConfig struct {
	traceID, parentID string
	start             time.Time
}

// SpanOption customises StartSpan.
type SpanOption func(*startConfig)

// WithTraceID joins an existing trace, for example one whose id arrived from
// another service. The span has no parent unless WithParentSpanID is also set.
func WithTraceID(id string) SpanOption { return func(c *startConfig) { c.traceID = id } }

// WithParentSpanID sets the parent explicitly instead of taking it from the
// context.
func WithParentSpanID(id string) SpanOption { return func(c *startConfig) { c.parentID = id } }

// WithStartTime backdates the span; the default is the moment of StartSpan.
func WithStartTime(t time.Time) SpanOption { return func(c *startConfig) { c.start = t } }

// ActiveSpan is a span being recorded. Set its fields as the operation
// proceeds, then call End to store it. It is safe for concurrent use.
type ActiveSpan struct {
	db *DB

	mu        sync.Mutex
	span      Span
	name      string
	statusMsg string
	attrs     map[string]any
	ended     bool
}

// StartSpan begins a span. If ctx already carries a span (from an enclosing
// StartSpan) the new span joins its trace as a child, unless WithTraceID or
// WithParentSpanID say otherwise. The returned context carries the new span,
// so pass it to the code that should nest underneath.
//
//	ctx, span := db.StartSpan(ctx, "summarize")
//	defer span.End(ctx)
//	span.SetModel("gpt-4")
//	...
//	span.SetTokens(in, out)
//	span.SetCost(0.0021)
func (db *DB) StartSpan(ctx context.Context, name string, opts ...SpanOption) (context.Context, *ActiveSpan) {
	cfg := startConfig{start: time.Now()}
	for _, o := range opts {
		o(&cfg)
	}
	if parent := SpanFromContext(ctx); parent != nil && cfg.traceID == "" {
		cfg.traceID = parent.TraceID()
		if cfg.parentID == "" {
			cfg.parentID = parent.SpanID()
		}
	}
	if cfg.traceID == "" {
		cfg.traceID = NewTraceID()
	}
	s := &ActiveSpan{
		db:   db,
		name: name,
		span: Span{TraceID: cfg.traceID, SpanID: NewSpanID(), ParentSpanID: cfg.parentID, Start: cfg.start},
	}
	return context.WithValue(ctx, spanCtxKey{}, s), s
}

// TraceID is the span's trace id, for propagating to other services.
func (s *ActiveSpan) TraceID() string { return s.span.TraceID }

// SpanID is the span's own id.
func (s *ActiveSpan) SpanID() string { return s.span.SpanID }

func (s *ActiveSpan) update(f func(*Span)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(&s.span)
}

// SetAgent records which agent ran the operation.
func (s *ActiveSpan) SetAgent(agent string) { s.update(func(sp *Span) { sp.Agent = agent }) }

// SetModel records the model used.
func (s *ActiveSpan) SetModel(model string) { s.update(func(sp *Span) { sp.Model = model }) }

// SetTool records the tool invoked.
func (s *ActiveSpan) SetTool(tool string) { s.update(func(sp *Span) { sp.Tool = tool }) }

// SetTeam records the owning team.
func (s *ActiveSpan) SetTeam(team string) { s.update(func(sp *Span) { sp.Team = team }) }

// SetTokens records input and output token counts.
func (s *ActiveSpan) SetTokens(in, out int32) {
	s.update(func(sp *Span) { sp.TokensIn, sp.TokensOut = in, out })
}

// SetCost records the cost in US dollars.
func (s *ActiveSpan) SetCost(usd float64) { s.update(func(sp *Span) { sp.CostUSD = usd }) }

// SetStatus records the outcome. End uses StatusOK if none was set.
func (s *ActiveSpan) SetStatus(st Status) { s.update(func(sp *Span) { sp.Status = st }) }

// SetError marks the span failed and keeps the error text. A nil error is a no-op.
func (s *ActiveSpan) SetError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.span.Status = StatusError
	s.statusMsg = err.Error()
}

// SetAttribute attaches a key/value to the span's JSON payload. The value
// must be JSON-encodable; End reports an error if it is not.
func (s *ActiveSpan) SetAttribute(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attrs == nil {
		s.attrs = make(map[string]any)
	}
	s.attrs[key] = value
}

// End stops the clock and stores the span. It returns after the span is
// durable. Pass a context that outlives the operation: if the operation was
// cancelled, ctx may be too, and the span would not be stored (use
// context.WithoutCancel). If End fails, the span stays open so it can be
// retried; after a successful End, further calls return ErrEnded.
func (s *ActiveSpan) End(ctx context.Context) error { return s.EndAt(ctx, time.Now()) }

// EndAt is End with an explicit end time.
func (s *ActiveSpan) EndAt(ctx context.Context, end time.Time) error {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return ErrEnded
	}
	s.ended = true
	span := s.span
	name, msg := s.name, s.statusMsg
	attrs := make(map[string]any, len(s.attrs))
	for k, v := range s.attrs {
		attrs[k] = v
	}
	s.mu.Unlock()

	err := func() error {
		if span.Duration = end.Sub(span.Start); span.Duration < 0 {
			span.Duration = 0
		}
		if span.Status == "" {
			span.Status = StatusOK
		}
		payload, err := buildPayload(name, msg, attrs)
		if err != nil {
			return err
		}
		span.Payload = payload
		return s.db.Ingest(ctx, span)
	}()
	if err != nil {
		s.mu.Lock()
		s.ended = false
		s.mu.Unlock()
	}
	return err
}

// buildPayload renders the JSON stored in Span.Payload: the span name, the
// status message and free-form attributes. IngestOTLP produces the same shape.
func buildPayload(name, statusMessage string, attrs map[string]any) (json.RawMessage, error) {
	payload := map[string]any{}
	if name != "" {
		payload["name"] = name
	}
	if statusMessage != "" {
		payload["status_message"] = statusMessage
	}
	if len(attrs) > 0 {
		payload["attributes"] = attrs
	}
	if len(payload) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, &ValidationError{Field: "payload", Reason: err.Error()}
	}
	return b, nil
}
