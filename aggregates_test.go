package wisptrace_test

import (
	"context"
	"errors"
	"testing"
	"time"

	wisptrace "github.com/GuhaneshT/WispTraceDB"
)

// aggSeed ingests spans into two hourly windows (base and base+1h):
//
//	base+10m  gpt-4   $0.010  100ms  in 10 out 5
//	base+20m  gpt-4   $0.020  300ms  in 20 out 15
//	base+70m  claude  $0.050  50ms   in 7  out 3
//	base+75m  (none)  $0.001  10ms
func aggSeed(t *testing.T) (*wisptrace.DB, context.Context, time.Time) {
	t.Helper()
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	base := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	rows := []struct {
		at        time.Duration
		model     string
		cost      float64
		lat       time.Duration
		in, out   int32
		traceSpan string
	}{
		{10 * time.Minute, "gpt-4", 0.010, 100 * time.Millisecond, 10, 5, "1"},
		{20 * time.Minute, "gpt-4", 0.020, 300 * time.Millisecond, 20, 15, "2"},
		{70 * time.Minute, "claude", 0.050, 50 * time.Millisecond, 7, 3, "3"},
		{75 * time.Minute, "", 0.001, 10 * time.Millisecond, 0, 0, "4"},
	}
	for _, r := range rows {
		s := goodSpan("agg-"+r.traceSpan, "s")
		s.Start, s.Model, s.CostUSD, s.Duration, s.TokensIn, s.TokensOut = base.Add(r.at), r.model, r.cost, r.lat, r.in, r.out
		if err := db.Ingest(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	return db, ctx, base
}

func TestAggregatesHourlyBucketsPerModel(t *testing.T) {
	db, ctx, base := aggSeed(t)
	got, err := db.Aggregates(ctx, wisptrace.WindowHour, wisptrace.Between(base, base.Add(2*time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 buckets (gpt-4 in hour 0; \"\" and claude in hour 1), got %d: %+v", len(got), got)
	}

	// Ordered by start, then model: hour 0 first, then "" before "claude".
	first, noModel, claude := got[0], got[1], got[2]
	if !first.Start.Equal(base) || first.Model != "gpt-4" || first.Spans != 2 ||
		first.TokensIn != 30 || first.TokensOut != 20 {
		t.Fatalf("hour 0 gpt-4 bucket wrong: %+v", first)
	}
	if d := first.CostUSD - 0.03; d > 1e-9 || d < -1e-9 {
		t.Fatalf("cost = %v, want 0.03", first.CostUSD)
	}
	if first.LatencyAvg != 200*time.Millisecond || first.LatencyMin != 100*time.Millisecond ||
		first.LatencyMax != 300*time.Millisecond {
		t.Fatalf("latency stats wrong: %+v", first)
	}
	if noModel.Model != "" || noModel.Spans != 1 || !noModel.Start.Equal(base.Add(time.Hour)) {
		t.Fatalf("spans without a model must bucket under the empty name: %+v", noModel)
	}
	if claude.Model != "claude" || claude.Spans != 1 || claude.LatencyMax != 50*time.Millisecond {
		t.Fatalf("claude bucket wrong: %+v", claude)
	}
}

func TestAggregatesWindowSizesAndAlignmentRule(t *testing.T) {
	db, ctx, base := aggSeed(t)

	// A day bucket holds all four spans; the base hour is inside one UTC day
	// or straddles two, so only check the grand total across buckets.
	days, err := db.Aggregates(ctx, wisptrace.WindowDay, wisptrace.Between(base.Add(-48*time.Hour), base.Add(48*time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	var spans int64
	for _, b := range days {
		spans += b.Spans
	}
	if spans != 4 {
		t.Fatalf("day buckets should hold all 4 spans, got %d: %+v", spans, days)
	}

	// A range starting mid-hour leaves out the hour bucket that began before it.
	got, err := db.Aggregates(ctx, wisptrace.WindowHour, wisptrace.Between(base.Add(30*time.Minute), base.Add(2*time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range got {
		if b.Start.Equal(base) {
			t.Fatalf("a bucket starting before From must be excluded: %+v", b)
		}
	}
	if len(got) != 2 {
		t.Fatalf("want the two hour-1 buckets, got %+v", got)
	}
}

func TestAggregatesRejectUnsupportedWindowAndClosedDB(t *testing.T) {
	db, ctx := open(t), context.Background()
	if _, err := db.Aggregates(ctx, wisptrace.Window(7*time.Minute), wisptrace.Last(time.Hour)); !errors.Is(err, wisptrace.ErrUnsupportedWindow) {
		t.Fatalf("want ErrUnsupportedWindow, got %v", err)
	}
	if got, err := db.Aggregates(ctx, wisptrace.WindowMinute, wisptrace.Last(time.Hour)); err != nil || len(got) != 0 {
		t.Fatalf("empty db: %v %+v", err, got)
	}
}

func TestAggregatesSurviveReopen(t *testing.T) {
	dir, ctx := t.TempDir(), context.Background()
	db, err := wisptrace.Open(dir, wisptrace.WithReadYourWrites())
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	s := goodSpan("persist", "s")
	s.Start, s.Model, s.CostUSD = base.Add(5*time.Minute), "gpt-4", 0.25
	if err := db.Ingest(ctx, s); err != nil {
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
	got, err := db.Aggregates(ctx, wisptrace.WindowHour, wisptrace.Between(base, base.Add(time.Hour)))
	if err != nil || len(got) != 1 || got[0].Spans != 1 || got[0].CostUSD != 0.25 {
		t.Fatalf("rollups must persist across a restart: %v %+v", err, got)
	}
}
