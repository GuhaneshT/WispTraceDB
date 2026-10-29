package wisptrace_test

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	wisptrace "github.com/GuhaneshT/WispTraceDB"
)

func TestGeneratedIDsHaveOTelShape(t *testing.T) {
	traceRe, spanRe := regexp.MustCompile(`^[0-9a-f]{32}$`), regexp.MustCompile(`^[0-9a-f]{16}$`)
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		tid, sid := wisptrace.NewTraceID(), wisptrace.NewSpanID()
		if !traceRe.MatchString(tid) || !spanRe.MatchString(sid) {
			t.Fatalf("bad id shape: %q %q", tid, sid)
		}
		if seen[tid] || seen[sid] {
			t.Fatal("duplicate id generated")
		}
		seen[tid], seen[sid] = true, true
	}
}

func TestStartSpanRecordsAndStores(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()

	_, sp := db.StartSpan(ctx, "summarize")
	sp.SetAgent("agent-1")
	sp.SetModel("gpt-4")
	sp.SetTool("search")
	sp.SetTeam("platform")
	sp.SetTokens(120, 30)
	sp.SetCost(0.002)
	sp.SetAttribute("prompt_version", 7)
	time.Sleep(20 * time.Millisecond)
	if err := sp.End(ctx); err != nil {
		t.Fatal(err)
	}

	got, err := db.Span(ctx, sp.TraceID(), sp.SpanID())
	if err != nil {
		t.Fatal(err)
	}
	if got.Model != "gpt-4" || got.Agent != "agent-1" || got.Tool != "search" || got.Team != "platform" ||
		got.TokensIn != 120 || got.TokensOut != 30 || got.CostUSD != 0.002 ||
		got.Status != wisptrace.StatusOK || got.ParentSpanID != "" {
		t.Fatalf("recorded span wrong: %+v", got)
	}
	if got.Duration < 20*time.Millisecond {
		t.Fatalf("duration %v should cover the 20ms sleep", got.Duration)
	}
	var payload struct {
		Name       string         `json:"name"`
		Attributes map[string]any `json:"attributes"`
	}
	if err := json.Unmarshal(got.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Name != "summarize" || payload.Attributes["prompt_version"] != float64(7) {
		t.Fatalf("payload wrong: %s", got.Payload)
	}
}

func TestNestedSpansShareTraceAndLinkParents(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()

	ctx, root := db.StartSpan(ctx, "agent-run")
	ctx2, child := db.StartSpan(ctx, "llm-call")
	_, grandchild := db.StartSpan(ctx2, "tool-call")

	if wisptrace.SpanFromContext(ctx2) != child {
		t.Fatal("SpanFromContext should return the innermost span")
	}
	if child.TraceID() != root.TraceID() || grandchild.TraceID() != root.TraceID() {
		t.Fatal("nested spans must share the trace id")
	}
	for _, s := range []*wisptrace.ActiveSpan{grandchild, child, root} {
		if err := s.End(ctx); err != nil {
			t.Fatal(err)
		}
	}

	tr, err := db.Trace(ctx, root.TraceID())
	if err != nil || len(tr) != 3 {
		t.Fatalf("Trace: %v len=%d", err, len(tr))
	}
	parents := map[string]string{}
	for _, s := range tr {
		parents[s.SpanID] = s.ParentSpanID
	}
	if parents[root.SpanID()] != "" || parents[child.SpanID()] != root.SpanID() ||
		parents[grandchild.SpanID()] != child.SpanID() {
		t.Fatalf("parent links wrong: %v", parents)
	}
}

func TestSetErrorMarksSpanFailed(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	_, sp := db.StartSpan(ctx, "call")
	sp.SetModel("gpt-4")
	sp.SetError(errors.New("rate limited"))
	if err := sp.End(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := db.Span(ctx, sp.TraceID(), sp.SpanID())
	if err != nil || got.Status != wisptrace.StatusError {
		t.Fatalf("want error status: %v %+v", err, got)
	}
	if rate, err := db.ErrorRate(ctx, "gpt-4", wisptrace.Last(time.Hour)); err != nil || rate != 1 {
		t.Fatalf("ErrorRate: %v %v", err, rate)
	}
}

func TestStartSpanOptions(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	start := time.Now().Add(-2 * time.Second)

	_, sp := db.StartSpan(ctx, "joined",
		wisptrace.WithTraceID("upstream-trace"), wisptrace.WithParentSpanID("upstream-span"), wisptrace.WithStartTime(start))
	if err := sp.End(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := db.Span(ctx, "upstream-trace", sp.SpanID())
	if err != nil {
		t.Fatal(err)
	}
	if got.ParentSpanID != "upstream-span" || got.Duration < 2*time.Second {
		t.Fatalf("options ignored: %+v", got)
	}
}

func TestEndTwiceAndEndFailureRetry(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	_, sp := db.StartSpan(ctx, "once")
	if err := sp.End(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sp.End(ctx); !errors.Is(err, wisptrace.ErrEnded) {
		t.Fatalf("second End: want ErrEnded, got %v", err)
	}

	_, sp2 := db.StartSpan(ctx, "retry")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := sp2.End(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if err := sp2.End(ctx); err != nil {
		t.Fatalf("a failed End must be retryable: %v", err)
	}
	if _, err := db.Span(ctx, sp2.TraceID(), sp2.SpanID()); err != nil {
		t.Fatalf("retried span not stored: %v", err)
	}
}

func TestUnencodableAttributeFailsEnd(t *testing.T) {
	db, ctx := open(t), context.Background()
	_, sp := db.StartSpan(ctx, "bad")
	sp.SetAttribute("ch", make(chan int))
	if err := sp.End(ctx); !errors.Is(err, wisptrace.ErrInvalidSpan) {
		t.Fatalf("want ErrInvalidSpan, got %v", err)
	}
}
