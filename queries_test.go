package wisptrace_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	wisptrace "github.com/GuhaneshT/WispTraceDB"
)

// seed stores spans across two teams and three models:
//
//	t1: gpt-4   platform ok      $0.010
//	t1: gpt-4   platform error   $0.010
//	t2: claude  ml       unset   $0.050
//	t3: gpt-4   platform ok      $0.010
func seed(t *testing.T) (*wisptrace.DB, context.Context) {
	t.Helper()
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	rows := []struct {
		trace, span, model, team, agent string
		status                          wisptrace.Status
		cost                            float64
	}{
		{"t1", "a", "gpt-4", "platform", "bot", wisptrace.StatusOK, 0.01},
		{"t1", "b", "gpt-4", "platform", "bot", wisptrace.StatusError, 0.01},
		{"t2", "a", "claude", "ml", "researcher", wisptrace.StatusUnset, 0.05},
		{"t3", "a", "gpt-4", "platform", "bot", wisptrace.StatusOK, 0.01},
	}
	for _, r := range rows {
		s := goodSpan(r.trace, r.span)
		s.Model, s.Team, s.Agent, s.Status, s.CostUSD = r.model, r.team, r.agent, r.status, r.cost
		if err := db.Ingest(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	return db, ctx
}

func TestErrorRateTreatsUnsetAsNotAFailure(t *testing.T) {
	db, ctx := seed(t)
	all := wisptrace.Last(time.Hour)

	// 1 error of 4 spans. The unset span must not count as a failure.
	rate, err := db.ErrorRate(ctx, "", all)
	if err != nil || rate != 0.25 {
		t.Fatalf("ErrorRate(all) = %v, %v; want 0.25", rate, err)
	}
	if rate, err := db.ErrorRate(ctx, "claude", all); err != nil || rate != 0 {
		t.Fatalf("ErrorRate(claude) = %v, %v; want 0 (only span is unset)", rate, err)
	}
}

func TestFailedSpansExcludesUnset(t *testing.T) {
	db, ctx := seed(t)
	failed, err := db.FailedSpans(ctx, wisptrace.Last(time.Hour), 10)
	if err != nil || len(failed) != 1 || failed[0].SpanID != "b" {
		t.Fatalf("FailedSpans: %v %+v", err, failed)
	}
}

func TestTracesListsDistinctIDs(t *testing.T) {
	db, ctx := seed(t)
	ids, err := db.Traces(ctx, wisptrace.Query{Range: wisptrace.Last(time.Hour), Model: "gpt-4"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	if len(ids) != 2 || !got["t1"] || !got["t3"] {
		t.Fatalf("want t1 and t3 once each, got %v", ids)
	}
}

func TestTopTeamsAndModels(t *testing.T) {
	db, ctx := seed(t)
	r := wisptrace.Last(time.Hour)

	teams, err := db.TopTeamsByCost(ctx, 1, r)
	if err != nil || len(teams) != 1 || teams[0].Team != "ml" {
		t.Fatalf("TopTeamsByCost: %v %+v", err, teams)
	}
	models, err := db.TopModelsBySpanCount(ctx, 5, r)
	if err != nil || len(models) != 2 || models[0].Model != "gpt-4" || models[0].Spans != 3 {
		t.Fatalf("TopModelsBySpanCount: %v %+v", err, models)
	}
}

func TestDistinctDimensions(t *testing.T) {
	db, ctx := seed(t)
	r := wisptrace.Last(time.Hour)
	for dim, want := range map[wisptrace.Dimension][]string{
		wisptrace.DimModel: {"claude", "gpt-4"},
		wisptrace.DimTeam:  {"ml", "platform"},
		wisptrace.DimAgent: {"bot", "researcher"},
	} {
		got, err := db.Distinct(ctx, dim, r, 0)
		if err != nil || len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Fatalf("Distinct(%d) = %v, %v; want %v", dim, got, err, want)
		}
	}
	if got, err := db.Distinct(ctx, wisptrace.DimModel, r, 1); err != nil || len(got) != 1 {
		t.Fatalf("Distinct with limit 1: %v %v", got, err)
	}
}

// spansAt ingests one span per offset, `ago` before now, and returns the db.
func spansAt(t *testing.T, ago ...time.Duration) (*wisptrace.DB, context.Context) {
	t.Helper()
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	for i, d := range ago {
		s := goodSpan(fmt.Sprintf("t%d", i), "s")
		s.Start = time.Now().Add(-d)
		if err := db.Ingest(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	return db, ctx
}

func TestQueryOrderAndPagingAreStable(t *testing.T) {
	// t0 is the oldest, t4 the newest.
	db, ctx := spansAt(t, 50*time.Minute, 40*time.Minute, 30*time.Minute, 20*time.Minute, 10*time.Minute)
	q := wisptrace.Query{Range: wisptrace.Last(time.Hour), Limit: 2}

	var got []string
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("paging did not terminate")
		}
		page, err := db.Query(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range page.Spans {
			got = append(got, s.TraceID)
		}
		if page.NextCursor == "" {
			break
		}
		q.Cursor = page.NextCursor
	}
	want := []string{"t0", "t1", "t2", "t3", "t4"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("oldest-first paging: got %v want %v", got, want)
	}

	newest, err := db.Query(ctx, wisptrace.Query{Range: wisptrace.Last(time.Hour), NewestFirst: true, Limit: 2})
	if err != nil || len(newest.Spans) != 2 || newest.Spans[0].TraceID != "t4" || newest.Spans[1].TraceID != "t3" {
		t.Fatalf("newest-first: %v %+v", err, newest)
	}
	if _, err := db.Query(ctx, wisptrace.Query{Cursor: "garbage"}); !errors.Is(err, wisptrace.ErrInvalidCursor) {
		t.Fatalf("bad cursor: want ErrInvalidCursor, got %v", err)
	}
}

func TestTraceSpansComeBackInStartOrder(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	base := time.Now().Add(-time.Minute)
	// Span ids sort the opposite way to their start times.
	for i, id := range []string{"z", "m", "a"} {
		s := goodSpan("one-trace", id)
		s.Start = base.Add(time.Duration(i) * time.Second)
		if err := db.Ingest(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	tr, err := db.Trace(ctx, "one-trace")
	if err != nil || len(tr) != 3 || tr[0].SpanID != "z" || tr[1].SpanID != "m" || tr[2].SpanID != "a" {
		t.Fatalf("Trace order: %v %+v", err, tr)
	}
}

func TestFailedSpansAreNewestFirstAndTracesFollowQueryOrder(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	for i, ago := range []time.Duration{30 * time.Minute, 20 * time.Minute, 10 * time.Minute} {
		s := goodSpan(fmt.Sprintf("t%d", i), "s")
		s.Start, s.Status = time.Now().Add(-ago), wisptrace.StatusError
		if err := db.Ingest(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	r := wisptrace.Last(time.Hour)
	failed, err := db.FailedSpans(ctx, r, 2)
	if err != nil || len(failed) != 2 || failed[0].TraceID != "t2" || failed[1].TraceID != "t1" {
		t.Fatalf("FailedSpans: %v %+v", err, failed)
	}
	ids, err := db.Traces(ctx, wisptrace.Query{Range: r, NewestFirst: true, Limit: 2})
	if err != nil || fmt.Sprint(ids) != "[t2 t1]" {
		t.Fatalf("Traces newest-first: %v %v", err, ids)
	}
}

func TestTopTiesBreakByName(t *testing.T) {
	db, ctx := open(t, wisptrace.WithReadYourWrites()), context.Background()
	for i, team := range []string{"b", "a", "c"} {
		s := goodSpan(fmt.Sprintf("t%d", i), "s")
		s.Team, s.CostUSD = team, 0.01 // identical cost
		if err := db.Ingest(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	teams, err := db.TopTeamsByCost(ctx, 2, wisptrace.Last(time.Hour))
	if err != nil || len(teams) != 2 || teams[0].Team != "a" || teams[1].Team != "b" {
		t.Fatalf("ties must resolve by name, got %v %+v", err, teams)
	}
}

func TestFlushAndCloseDrainQueuedSpans(t *testing.T) {
	dir, ctx := t.TempDir(), context.Background()
	db, err := wisptrace.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := db.Ingest(ctx, goodSpan(fmt.Sprintf("t%d", i), "s")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(ctx); err != nil {
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
	page, err := db.Query(ctx, wisptrace.Query{Range: wisptrace.Last(time.Hour)})
	if err != nil || len(page.Spans) != 20 {
		t.Fatalf("after reopen: %v %d spans, want 20", err, len(page.Spans))
	}
}
