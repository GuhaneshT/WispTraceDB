package wisptrace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// OTLP/JSON request shapes, reduced to what the mapping reads. Unknown fields
// are ignored because OTLP evolves and exporters send plenty we do not use.
type otlpRequest struct {
	ResourceSpans []struct {
		Resource struct {
			Attributes []keyValue `json:"attributes"`
		} `json:"resource"`
		ScopeSpans []struct {
			Spans []otlpSpan `json:"spans"`
		} `json:"scopeSpans"`
	} `json:"resourceSpans"`
}

type otlpSpan struct {
	TraceID           string     `json:"traceId"`
	SpanID            string     `json:"spanId"`
	ParentSpanID      string     `json:"parentSpanId"`
	Name              string     `json:"name"`
	StartTimeUnixNano numLike    `json:"startTimeUnixNano"`
	EndTimeUnixNano   numLike    `json:"endTimeUnixNano"`
	Attributes        []keyValue `json:"attributes"`
	Status            struct {
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
	} `json:"status"`
}

type keyValue struct {
	Key   string   `json:"key"`
	Value anyValue `json:"value"`
}

// anyValue is OTLP's AnyValue. Arrays and key-value lists are kept raw.
type anyValue struct {
	StringValue *string         `json:"stringValue"`
	IntValue    *numLike        `json:"intValue"`
	DoubleValue *float64        `json:"doubleValue"`
	BoolValue   *bool           `json:"boolValue"`
	ArrayValue  json.RawMessage `json:"arrayValue"`
	KvlistValue json.RawMessage `json:"kvlistValue"`
}

// numLike reads a JSON number that protobuf-JSON may also encode as a string
// (int64 fields are strings in the spec; many exporters send numbers).
type numLike string

func (n *numLike) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" {
		*n = ""
		return nil
	}
	*n = numLike(strings.Trim(s, `"`))
	return nil
}

func (n numLike) int64() (int64, error) {
	if n == "" {
		return 0, nil
	}
	if v, err := strconv.ParseInt(string(n), 10, 64); err == nil {
		return v, nil
	}
	f, err := strconv.ParseFloat(string(n), 64) // tolerate "1.7e18"
	if err != nil || f < math.MinInt64 || f > math.MaxInt64 {
		return 0, fmt.Errorf("%q is not an integer", string(n))
	}
	return int64(f), nil
}

func (v anyValue) asString() (string, bool) {
	switch {
	case v.StringValue != nil:
		return *v.StringValue, true
	case v.IntValue != nil:
		return string(*v.IntValue), true
	}
	return "", false
}

func (v anyValue) asInt() (int64, bool) {
	switch {
	case v.IntValue != nil:
		n, err := v.IntValue.int64()
		return n, err == nil
	case v.DoubleValue != nil:
		return int64(*v.DoubleValue), true
	case v.StringValue != nil:
		n, err := strconv.ParseInt(*v.StringValue, 10, 64)
		return n, err == nil
	}
	return 0, false
}

func (v anyValue) asFloat() (float64, bool) {
	switch {
	case v.DoubleValue != nil:
		return *v.DoubleValue, true
	case v.IntValue != nil:
		n, err := v.IntValue.int64()
		return float64(n), err == nil
	case v.StringValue != nil:
		f, err := strconv.ParseFloat(*v.StringValue, 64)
		return f, err == nil
	}
	return 0, false
}

// toJSON is the value as it is stored in a span's payload.
func (v anyValue) toJSON() any {
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.IntValue != nil:
		if n, err := v.IntValue.int64(); err == nil {
			return n
		}
		return string(*v.IntValue)
	case v.DoubleValue != nil:
		return *v.DoubleValue
	case v.BoolValue != nil:
		return *v.BoolValue
	case len(v.ArrayValue) > 0:
		return v.ArrayValue
	case len(v.KvlistValue) > 0:
		return v.KvlistValue
	}
	return nil
}

// Attribute keys the mapping consumes. Everything else goes to the payload.
// gen_ai.* follows the OpenTelemetry GenAI semantic conventions; wisp.* covers
// what those conventions do not define.
var (
	modelKeys    = []string{"gen_ai.response.model", "gen_ai.request.model"}
	tokensInKeys = []string{"gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens"}
	tokensOutKey = []string{"gen_ai.usage.output_tokens", "gen_ai.usage.completion_tokens"}
	agentKeys    = []string{"gen_ai.agent.id", "gen_ai.agent.name"}
	toolKeys     = []string{"gen_ai.tool.name"}
	teamKeys     = []string{"wisp.team"}
	costKeys     = []string{"wisp.cost_usd"}
)

func consumed(key string) bool {
	for _, set := range [][]string{modelKeys, tokensInKeys, tokensOutKey, agentKeys, toolKeys, teamKeys, costKeys} {
		for _, k := range set {
			if k == key {
				return true
			}
		}
	}
	return false
}

func attrMap(kvs []keyValue) map[string]anyValue {
	m := make(map[string]anyValue, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value
	}
	return m
}

func firstString(m map[string]anyValue, keys []string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.asString(); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

func firstInt(m map[string]anyValue, keys []string) (int64, bool) {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if n, ok := v.asInt(); ok {
				return n, true
			}
		}
	}
	return 0, false
}

func clampInt32(n int64) int32 {
	switch {
	case n > math.MaxInt32:
		return math.MaxInt32
	case n < 0:
		return -1 // fails validation instead of being silently fixed
	}
	return int32(n)
}

func statusFrom(raw json.RawMessage) Status {
	s := strings.ToUpper(strings.Trim(strings.TrimSpace(string(raw)), `"`))
	switch {
	case s == "1" || strings.HasSuffix(s, "OK"):
		return StatusOK
	case s == "2" || strings.HasSuffix(s, "ERROR"):
		return StatusError
	}
	return StatusUnset
}

// mapSpan converts one OTLP span, given its resource's attributes.
func mapSpan(resource map[string]anyValue, s otlpSpan) (Span, error) {
	start, err := s.StartTimeUnixNano.int64()
	if err != nil {
		return Span{}, &ValidationError{Field: "startTimeUnixNano", Reason: err.Error()}
	}
	end, err := s.EndTimeUnixNano.int64()
	if err != nil {
		return Span{}, &ValidationError{Field: "endTimeUnixNano", Reason: err.Error()}
	}
	var dur time.Duration
	if end > start && start > 0 {
		dur = time.Duration(end - start)
	}

	attrs := attrMap(s.Attributes)
	out := Span{
		TraceID:      s.TraceID,
		SpanID:       s.SpanID,
		ParentSpanID: s.ParentSpanID,
		Start:        time.Unix(0, start),
		Duration:     dur,
		Agent:        firstString(attrs, agentKeys),
		Model:        firstString(attrs, modelKeys),
		Tool:         firstString(attrs, toolKeys),
		Team:         firstString(attrs, teamKeys),
		Status:       statusFrom(s.Status.Code),
	}
	if out.Team == "" {
		out.Team = firstString(resource, append(append([]string{}, teamKeys...), "service.namespace"))
	}
	if n, ok := firstInt(attrs, tokensInKeys); ok {
		out.TokensIn = clampInt32(n)
	}
	if n, ok := firstInt(attrs, tokensOutKey); ok {
		out.TokensOut = clampInt32(n)
	}
	for _, k := range costKeys {
		if v, ok := attrs[k]; ok {
			if f, ok := v.asFloat(); ok {
				out.CostUSD = f
			}
		}
	}

	extra := map[string]any{}
	for _, kv := range s.Attributes {
		if !consumed(kv.Key) {
			extra[kv.Key] = kv.Value.toJSON()
		}
	}
	payload, err := buildPayload(s.Name, s.Status.Message, extra)
	if err != nil {
		return Span{}, err
	}
	out.Payload = payload
	return out, nil
}

// MaxOTLPBytes caps the request IngestOTLP will read.
const MaxOTLPBytes = 16 << 20

// ErrInvalidOTLP reports a request that is not a valid OTLP/JSON
// ExportTraceServiceRequest. It is wrapped with the decoder's detail.
var ErrInvalidOTLP = errors.New("wisptrace: invalid OTLP/JSON")

// ErrTooLarge reports an OTLP request over MaxOTLPBytes.
var ErrTooLarge = errors.New("wisptrace: request too large")

// IngestOTLP ingests an OTLP/JSON ExportTraceServiceRequest, the body an
// OpenTelemetry exporter configured for http/json would POST. Embedders call
// it from their own transport, for example an http.Handler of their own.
//
// Spans map as follows (OpenTelemetry GenAI semantic conventions):
//
//	traceId, spanId, parentSpanId      -> TraceID, SpanID, ParentSpanID (hex as sent)
//	startTimeUnixNano                  -> Start
//	endTimeUnixNano - start            -> Duration
//	status.code                        -> Status ("ok", "error", "unset")
//	gen_ai.response/request.model      -> Model
//	gen_ai.usage.input/output_tokens   -> TokensIn, TokensOut
//	gen_ai.agent.id / .name            -> Agent
//	gen_ai.tool.name                   -> Tool
//	wisp.team, resource wisp.team,
//	  resource service.namespace       -> Team
//	wisp.cost_usd                      -> CostUSD
//	name, status.message, other attrs  -> Payload (JSON)
//
// Spans are durable once IngestOTLP returns, with the same guarantees as
// Ingest. Rejection indexes count spans in request order across all
// resourceSpans and scopeSpans; each Rejection.Err matches ErrInvalidSpan.
// A request that is not valid OTLP/JSON returns ErrInvalidOTLP and stores
// nothing.
func (db *DB) IngestOTLP(ctx context.Context, r io.Reader) (BatchResult, error) {
	if err := db.check(ctx); err != nil {
		return BatchResult{}, err
	}
	body, err := io.ReadAll(io.LimitReader(r, MaxOTLPBytes+1))
	if err != nil {
		return BatchResult{}, fmt.Errorf("wisptrace: read OTLP request: %w", err)
	}
	if len(body) > MaxOTLPBytes {
		return BatchResult{}, ErrTooLarge
	}
	var req otlpRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return BatchResult{}, fmt.Errorf("%w: %v", ErrInvalidOTLP, err)
	}

	var (
		spans    []Span
		origin   []int // request-order index of each entry in spans
		rejected []Rejection
		n        int
	)
	for _, rs := range req.ResourceSpans {
		resource := attrMap(rs.Resource.Attributes)
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				span, err := mapSpan(resource, sp)
				if err != nil {
					rejected = append(rejected, Rejection{Index: n, Err: err})
				} else {
					spans = append(spans, span)
					origin = append(origin, n)
				}
				n++
			}
		}
	}

	res, err := db.IngestBatch(ctx, spans)
	for _, rj := range res.Rejected {
		rejected = append(rejected, Rejection{Index: origin[rj.Index], Err: rj.Err})
	}
	sort.Slice(rejected, func(i, j int) bool { return rejected[i].Index < rejected[j].Index })
	res.Rejected = rejected
	return res, err
}
