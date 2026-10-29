package wisptrace_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	wisptrace "github.com/GuhaneshT/WispTraceDB"
)

func TestIngestOTLPMapsGenAIAttributes(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	start := time.Now().UnixNano()
	end := start + int64(300*time.Millisecond)
	body := fmt.Sprintf(`{"resourceSpans":[{
	  "resource":{"attributes":[{"key":"service.namespace","value":{"stringValue":"platform"}}]},
	  "scopeSpans":[{"spans":[{
	    "traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331","parentSpanId":"",
	    "name":"chat","startTimeUnixNano":"%d","endTimeUnixNano":"%d",
	    "attributes":[
	      {"key":"gen_ai.request.model","value":{"stringValue":"gpt-4"}},
	      {"key":"gen_ai.usage.input_tokens","value":{"intValue":"120"}},
	      {"key":"gen_ai.usage.output_tokens","value":{"intValue":30}},
	      {"key":"gen_ai.agent.id","value":{"stringValue":"agent-7"}},
	      {"key":"wisp.cost_usd","value":{"doubleValue":0.002}},
	      {"key":"custom.flag","value":{"boolValue":true}}
	    ],
	    "status":{"code":"STATUS_CODE_ERROR","message":"rate limited"}
	  }]}]}]}`, start, end)

	res, err := db.IngestOTLP(ctx, strings.NewReader(body))
	if err != nil || res.Accepted != 1 || len(res.Rejected) != 0 {
		t.Fatalf("IngestOTLP: %v %+v", err, res)
	}
	s, err := db.Span(ctx, "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331")
	if err != nil {
		t.Fatal(err)
	}
	if s.Model != "gpt-4" || s.TokensIn != 120 || s.TokensOut != 30 || s.Agent != "agent-7" ||
		s.Team != "platform" || s.CostUSD != 0.002 || s.Status != wisptrace.StatusError ||
		s.Duration != 300*time.Millisecond {
		t.Fatalf("mapping wrong: %+v", s)
	}
	var payload struct {
		Name          string         `json:"name"`
		StatusMessage string         `json:"status_message"`
		Attributes    map[string]any `json:"attributes"`
	}
	if err := json.Unmarshal(s.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Name != "chat" || payload.StatusMessage != "rate limited" || payload.Attributes["custom.flag"] != true {
		t.Fatalf("payload wrong: %s", s.Payload)
	}
	if _, leaked := payload.Attributes["gen_ai.request.model"]; leaked {
		t.Fatalf("mapped attribute leaked into payload: %s", s.Payload)
	}
}

func TestIngestOTLPRejectionsKeepRequestOrderIndexes(t *testing.T) {
	db, ctx := open(t), context.Background()
	now := time.Now().UnixNano()
	body := fmt.Sprintf(`{"resourceSpans":[{"scopeSpans":[{"spans":[
	  {"traceId":"a","spanId":"1","startTimeUnixNano":"%d"},
	  {"traceId":"","spanId":"2","startTimeUnixNano":"%d"},
	  {"traceId":"a","spanId":"3","startTimeUnixNano":"not-a-number"}
	]}]}]}`, now, now)
	res, err := db.IngestOTLP(ctx, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 1 || len(res.Rejected) != 2 || res.Rejected[0].Index != 1 || res.Rejected[1].Index != 2 {
		t.Fatalf("unexpected result: %+v", res)
	}
	for _, rj := range res.Rejected {
		if !errors.Is(rj.Err, wisptrace.ErrInvalidSpan) {
			t.Fatalf("rejection %d: want ErrInvalidSpan, got %v", rj.Index, rj.Err)
		}
	}
}

func TestIngestOTLPRejectsMalformedAndOversize(t *testing.T) {
	db, ctx := open(t), context.Background()
	if _, err := db.IngestOTLP(ctx, strings.NewReader(`{nope`)); !errors.Is(err, wisptrace.ErrInvalidOTLP) {
		t.Fatalf("malformed: want ErrInvalidOTLP, got %v", err)
	}
	huge := strings.NewReader(`{"resourceSpans":[],"pad":"` + strings.Repeat("x", wisptrace.MaxOTLPBytes) + `"}`)
	if _, err := db.IngestOTLP(ctx, huge); !errors.Is(err, wisptrace.ErrTooLarge) {
		t.Fatalf("oversize: want ErrTooLarge, got %v", err)
	}
}

func TestIngestOTLPAcceptsNumericTimestampsAndEmptyRequest(t *testing.T) {
	db, ctx := open(t), context.Background()
	if res, err := db.IngestOTLP(ctx, strings.NewReader(`{}`)); err != nil || res.Accepted != 0 {
		t.Fatalf("empty request: %v %+v", err, res)
	}
	body := fmt.Sprintf(`{"resourceSpans":[{"scopeSpans":[{"spans":[
	  {"traceId":"n","spanId":"1","startTimeUnixNano":%d,"endTimeUnixNano":%d}]}]}]}`,
		time.Now().UnixNano(), time.Now().UnixNano()+1_000_000)
	if res, err := db.IngestOTLP(ctx, strings.NewReader(body)); err != nil || res.Accepted != 1 {
		t.Fatalf("numeric timestamps: %v %+v", err, res)
	}
}
