package wisptrace_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	wisptrace "github.com/GuhaneshT/WispTraceDB"
)

func goodSpan(trace, span string) wisptrace.Span {
	return wisptrace.Span{
		TraceID: trace, SpanID: span,
		Start:    time.Now(),
		Duration: 245 * time.Millisecond,
		Agent:    "agent-1", Model: "gpt-4", Tool: "search", Team: "platform",
		Status:   wisptrace.StatusOK,
		TokensIn: 100, TokensOut: 50, CostUSD: 0.0015,
		Payload: json.RawMessage(`{"k":"v"}`),
	}
}

func open(t *testing.T, opts ...wisptrace.Option) *wisptrace.DB {
	t.Helper()
	db, err := wisptrace.Open(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestValidateRejectsEachRule(t *testing.T) {
	cases := map[string]func(*wisptrace.Span){
		"empty trace":     func(s *wisptrace.Span) { s.TraceID = "" },
		"empty span":      func(s *wisptrace.Span) { s.SpanID = "" },
		"zero start":      func(s *wisptrace.Span) { s.Start = time.Time{} },
		"epoch start":     func(s *wisptrace.Span) { s.Start = time.Unix(0, 0) },
		"far future":      func(s *wisptrace.Span) { s.Start = time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC) },
		"negative dur":    func(s *wisptrace.Span) { s.Duration = -1 },
		"negative tokens": func(s *wisptrace.Span) { s.TokensIn = -1 },
		"negative cost":   func(s *wisptrace.Span) { s.CostUSD = -0.01 },
		"nan cost":        func(s *wisptrace.Span) { s.CostUSD = math.NaN() },
		"inf cost":        func(s *wisptrace.Span) { s.CostUSD = math.Inf(1) },
		"long model":      func(s *wisptrace.Span) { s.Model = strings.Repeat("m", wisptrace.MaxDimensionLen+1) },
		"huge payload": func(s *wisptrace.Span) {
			s.Payload = json.RawMessage(`"` + strings.Repeat("x", wisptrace.MaxPayloadBytes) + `"`)
		},
		"payload not json": func(s *wisptrace.Span) { s.Payload = json.RawMessage(`{nope`) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := goodSpan("t", "s")
			mutate(&s)
			err := s.Validate()
			if !errors.Is(err, wisptrace.ErrInvalidSpan) {
				t.Fatalf("want ErrInvalidSpan, got %v", err)
			}
			var ve *wisptrace.ValidationError
			if !errors.As(err, &ve) || ve.Field == "" {
				t.Fatalf("want *ValidationError with a field, got %v", err)
			}
		})
	}
	if err := goodSpan("t", "s").Validate(); err != nil {
		t.Fatalf("good span rejected: %v", err)
	}
}

func TestIngestSyncReadBack(t *testing.T) {
	db, ctx := open(t), context.Background()
	in := goodSpan("trace-1", "span-1")
	if err := db.Ingest(ctx, in); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := db.Span(ctx, "trace-1", "span-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Model != "gpt-4" || got.Duration != 245*time.Millisecond || got.CostUSD != 0.0015 ||
		got.TokensIn != 100 || string(got.Payload) != `{"k":"v"}` || !got.Start.Equal(in.Start.Truncate(time.Nanosecond)) {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	tr, err := db.Trace(ctx, "trace-1")
	if err != nil || len(tr) != 1 {
		t.Fatalf("Trace: %v len=%d", err, len(tr))
	}
}

func TestNotFound(t *testing.T) {
	db, ctx := open(t), context.Background()
	if _, err := db.Span(ctx, "nope", "nope"); !errors.Is(err, wisptrace.ErrNotFound) {
		t.Fatalf("Span: want ErrNotFound, got %v", err)
	}
	if _, err := db.Trace(ctx, "nope"); !errors.Is(err, wisptrace.ErrNotFound) {
		t.Fatalf("Trace: want ErrNotFound, got %v", err)
	}
}

func TestReadYourWritesOption(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	for i := 0; i < 50; i++ {
		id := string(rune('a'+i%26)) + string(rune('A'+i/26))
		if err := db.Ingest(ctx, goodSpan("t-"+id, "s")); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Span(ctx, "t-"+id, "s"); err != nil {
			t.Fatalf("span %d not visible right after Ingest: %v", i, err)
		}
	}
}

func TestIngestBatchReportsRejections(t *testing.T) {
	db, ctx := open(t), context.Background()
	bad := goodSpan("", "x")
	res, err := db.IngestBatch(ctx, []wisptrace.Span{goodSpan("t", "1"), bad, goodSpan("t", "2")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted != 2 || len(res.Rejected) != 1 || res.Rejected[0].Index != 1 ||
		!errors.Is(res.Rejected[0].Err, wisptrace.ErrInvalidSpan) {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestDeleteSpanAndTrace(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	for _, s := range []wisptrace.Span{goodSpan("t", "1"), goodSpan("t", "2"), goodSpan("other", "1")} {
		if err := db.Ingest(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.DeleteSpan(ctx, "t", "1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Span(ctx, "t", "1"); !errors.Is(err, wisptrace.ErrNotFound) {
		t.Fatalf("deleted span still readable: %v", err)
	}
	if err := db.DeleteTrace(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Trace(ctx, "t"); !errors.Is(err, wisptrace.ErrNotFound) {
		t.Fatalf("deleted trace still readable: %v", err)
	}
	if _, err := db.Span(ctx, "other", "1"); err != nil {
		t.Fatalf("unrelated trace lost: %v", err)
	}
	if err := db.DeleteSpan(ctx, "", "x"); !errors.Is(err, wisptrace.ErrInvalidSpan) {
		t.Fatalf("empty id: want ErrInvalidSpan, got %v", err)
	}
}

func TestQueryAndAggregates(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	a, b := goodSpan("t1", "a"), goodSpan("t2", "b")
	b.Model, b.Team, b.Status = "claude", "ml", wisptrace.StatusError
	for _, s := range []wisptrace.Span{a, b} {
		if err := db.Ingest(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	page, err := db.Query(ctx, wisptrace.Query{Range: wisptrace.Last(time.Hour), Model: "claude"})
	if err != nil || len(page.Spans) != 1 || page.Spans[0].SpanID != "b" {
		t.Fatalf("Query: %v %+v", err, page)
	}
	costs, err := db.Costs(ctx, wisptrace.ByTeam, wisptrace.Last(time.Hour))
	if err != nil || len(costs) != 2 {
		t.Fatalf("Costs: %v %v", err, costs)
	}
	rate, err := db.ErrorRate(ctx, "claude", wisptrace.Last(time.Hour))
	if err != nil || rate != 1 {
		t.Fatalf("ErrorRate: %v %v", err, rate)
	}
}

func TestClosedAndContext(t *testing.T) {
	db, err := wisptrace.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.Ingest(cancelled, goodSpan("t", "s")); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close should be a no-op: %v", err)
	}
	if err := db.Ingest(context.Background(), goodSpan("t", "s")); !errors.Is(err, wisptrace.ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestAcknowledgedSpansSurviveReopen(t *testing.T) {
	dir, ctx := t.TempDir(), context.Background()
	db, err := wisptrace.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ingest(ctx, goodSpan("durable", "1")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = wisptrace.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Span(ctx, "durable", "1"); err != nil {
		t.Fatalf("span lost across reopen: %v", err)
	}
	if rep, err := db.Check(ctx); err != nil || !rep.OK() {
		t.Fatalf("consistency: %v %+v", err, rep)
	}
}
