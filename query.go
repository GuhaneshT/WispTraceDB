package wisptrace

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/GuhaneshT/WispTraceDB/internal/engine"
)

// TimeRange is a closed interval. A zero From means "from the beginning" and a
// zero To means "no upper bound".
type TimeRange struct{ From, To time.Time }

// Last is the range from d ago until now.
func Last(d time.Duration) TimeRange {
	now := time.Now()
	return TimeRange{From: now.Add(-d), To: now}
}

// Between is the range [from, to].
func Between(from, to time.Time) TimeRange { return TimeRange{From: from, To: to} }

func (r TimeRange) bounds() (start, end int64) {
	end = math.MaxInt64
	if !r.From.IsZero() {
		start = r.From.UnixNano()
	}
	if !r.To.IsZero() {
		end = r.To.UnixNano()
	}
	return start, end
}

// Query selects spans by time range and optional exact-match dimensions.
type Query struct {
	Range  TimeRange
	Agent  string
	Model  string
	Tool   string
	Team   string
	Status Status

	// NewestFirst returns the most recent spans first. The default is
	// chronological order, oldest first.
	NewestFirst bool

	// Limit is the page size (default 100, at most 1000).
	Limit int
	// Cursor continues from a previous Page.NextCursor.
	Cursor string
}

// Page is one page of query results.
type Page struct {
	Spans      []Span
	NextCursor string // empty on the last page
}

const (
	defaultPageSize = 100
	maxPageSize     = 1000
)

func clampLimit(n int) int {
	if n <= 0 {
		return defaultPageSize
	}
	if n > maxPageSize {
		return maxPageSize
	}
	return n
}

// matching returns every span matching q, ordered by start time (then trace
// and span id, so the order is total and repeatable).
func (db *DB) matching(q Query) ([]Span, error) {
	start, end := q.Range.bounds()
	ps, err := db.eng.RangeQuery(engine.RangeFilter{
		StartTS: start, EndTS: end,
		AgentID: q.Agent, Model: q.Model, ToolName: q.Tool, Team: q.Team, Status: string(q.Status),
	})
	if err != nil {
		return nil, err
	}
	spans := fromPayloads(ps)
	sortSpans(spans, q.NewestFirst)
	return spans, nil
}

func sortSpans(spans []Span, newestFirst bool) {
	sort.Slice(spans, func(i, j int) bool {
		a, b := spans[i], spans[j]
		if !a.Start.Equal(b.Start) {
			if newestFirst {
				return a.Start.After(b.Start)
			}
			return a.Start.Before(b.Start)
		}
		if a.TraceID != b.TraceID {
			return a.TraceID < b.TraceID
		}
		return a.SpanID < b.SpanID
	})
}

// Query returns one page of spans matching q in a fixed order (see
// Query.NewestFirst).
//
// The cost of a call grows with the number of spans in q.Range, not with the
// page size: every call evaluates the whole range, then returns one page of
// it. Use a bounded Range. Cursors are positions in that ordering, so they
// stay valid while new spans arrive after the current position (oldest-first
// order), but a span inserted or deleted earlier in the ordering, or removed
// by retention, can shift later pages by one.
func (db *DB) Query(ctx context.Context, q Query) (Page, error) {
	if err := db.check(ctx); err != nil {
		return Page{}, err
	}
	offset := 0
	if q.Cursor != "" {
		n, err := strconv.Atoi(q.Cursor)
		if err != nil || n < 0 {
			return Page{}, ErrInvalidCursor
		}
		offset = n
	}
	spans, err := db.matching(q)
	if err != nil {
		return Page{}, err
	}
	limit := clampLimit(q.Limit)
	if offset >= len(spans) {
		return Page{}, nil
	}
	end := offset + limit
	page := Page{}
	if end < len(spans) {
		page.NextCursor = strconv.Itoa(end)
	} else {
		end = len(spans)
	}
	page.Spans = spans[offset:end]
	return page, nil
}

// Traces returns the distinct trace ids that have at least one span matching
// q, in the order their first matching span appears (see Query.NewestFirst),
// up to q.Limit (default 100, at most 1000). q.Cursor is ignored.
func (db *DB) Traces(ctx context.Context, q Query) ([]string, error) {
	if err := db.check(ctx); err != nil {
		return nil, err
	}
	spans, err := db.matching(q)
	if err != nil {
		return nil, err
	}
	limit := clampLimit(q.Limit)
	seen := make(map[string]bool)
	ids := []string{}
	for _, s := range spans {
		if seen[s.TraceID] {
			continue
		}
		seen[s.TraceID] = true
		ids = append(ids, s.TraceID)
		if len(ids) == limit {
			break
		}
	}
	return ids, nil
}

// isFailure reports whether a status counts as a failed span. "ok",
// "success", "unset" and an empty status do not: OpenTelemetry leaves
// successful spans unset.
func isFailure(st Status) bool {
	switch strings.ToLower(string(st)) {
	case "", "ok", "success", "unset":
		return false
	}
	return true
}

// FailedSpans returns the most recent spans with a failure status in the
// range, newest first, up to limit (default 100, at most 1000).
func (db *DB) FailedSpans(ctx context.Context, r TimeRange, limit int) ([]Span, error) {
	if err := db.check(ctx); err != nil {
		return nil, err
	}
	spans, err := db.matching(Query{Range: r, NewestFirst: true})
	if err != nil {
		return nil, err
	}
	limit = clampLimit(limit)
	failed := []Span{}
	for _, s := range spans {
		if isFailure(s.Status) {
			failed = append(failed, s)
			if len(failed) == limit {
				break
			}
		}
	}
	return failed, nil
}

// GroupBy is the dimension an aggregate is grouped by.
type GroupBy int

const (
	ByModel GroupBy = iota
	ByTeam
)

// Costs returns total dollar cost per group over the range.
func (db *DB) Costs(ctx context.Context, by GroupBy, r TimeRange) (map[string]float64, error) {
	if err := db.check(ctx); err != nil {
		return nil, err
	}
	start, end := r.bounds()
	switch by {
	case ByModel:
		return db.eng.GetCostByModel(start, end)
	case ByTeam:
		return db.eng.GetCostByTeam(start, end)
	}
	return nil, errors.New("wisptrace: unsupported GroupBy")
}

// Tokens is a token total.
type Tokens struct{ In, Out, Total int64 }

// TokensByModel returns token totals per model over the range.
func (db *DB) TokensByModel(ctx context.Context, r TimeRange) (map[string]Tokens, error) {
	if err := db.check(ctx); err != nil {
		return nil, err
	}
	start, end := r.bounds()
	m, err := db.eng.GetTokensByModel(start, end)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Tokens, len(m))
	for k, v := range m {
		out[k] = Tokens{In: v.TokensIn, Out: v.TokensOut, Total: v.Total}
	}
	return out, nil
}

// ErrorRate returns the fraction (0 to 1) of spans with a failure status,
// over all models when model is empty. Statuses "ok", "unset" and "success"
// are not failures. It is 0 when no spans match.
func (db *DB) ErrorRate(ctx context.Context, model string, r TimeRange) (float64, error) {
	if err := db.check(ctx); err != nil {
		return 0, err
	}
	start, end := r.bounds()
	pct, err := db.eng.GetErrorRate(model, start, end)
	return pct / 100, err
}

// TeamCost is one team's total cost.
type TeamCost struct {
	Team    string
	CostUSD float64
}

// TopTeamsByCost returns the n most expensive teams over the range, highest
// first (default 100, at most 1000). Ties are ordered by team name.
func (db *DB) TopTeamsByCost(ctx context.Context, n int, r TimeRange) ([]TeamCost, error) {
	if err := db.check(ctx); err != nil {
		return nil, err
	}
	start, end := r.bounds()
	rows, err := db.eng.GetTopTeamsByCost(0, start, end) // all teams; ranked and cut below
	if err != nil {
		return nil, err
	}
	out := make([]TeamCost, len(rows))
	for i, t := range rows {
		out[i] = TeamCost{Team: t.Team, CostUSD: t.Cost}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CostUSD != out[j].CostUSD {
			return out[i].CostUSD > out[j].CostUSD
		}
		return out[i].Team < out[j].Team
	})
	if n = clampLimit(n); len(out) > n {
		out = out[:n]
	}
	return out, nil
}

// ModelCount is the number of spans recorded for one model.
type ModelCount struct {
	Model string
	Spans int64
}

// TopModelsBySpanCount returns the n most-used models over the range, most
// used first (default 100, at most 1000). Ties are ordered by model name.
func (db *DB) TopModelsBySpanCount(ctx context.Context, n int, r TimeRange) ([]ModelCount, error) {
	if err := db.check(ctx); err != nil {
		return nil, err
	}
	start, end := r.bounds()
	rows, err := db.eng.GetTopModelsBySpanCount(0, start, end) // all models; ranked and cut below
	if err != nil {
		return nil, err
	}
	out := make([]ModelCount, len(rows))
	for i, m := range rows {
		out[i] = ModelCount{Model: m.Model, Spans: m.Count}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Spans != out[j].Spans {
			return out[i].Spans > out[j].Spans
		}
		return out[i].Model < out[j].Model
	})
	if n = clampLimit(n); len(out) > n {
		out = out[:n]
	}
	return out, nil
}

// Dimension is a span field whose distinct values can be listed.
type Dimension int

const (
	DimModel Dimension = iota
	DimTeam
	DimAgent
)

// Distinct returns the sorted distinct values of a dimension seen in the
// range, up to limit (default 1000, at most 10000).
func (db *DB) Distinct(ctx context.Context, dim Dimension, r TimeRange, limit int) ([]string, error) {
	if err := db.check(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 1000
	}
	if limit > engine.MaxCardinalityPerQuery {
		limit = engine.MaxCardinalityPerQuery
	}
	start, end := r.bounds()
	switch dim {
	case DimModel:
		return db.eng.GetDistinctModels(start, end, limit)
	case DimTeam:
		return db.eng.GetDistinctTeams(start, end, limit)
	case DimAgent:
		return db.eng.GetDistinctAgents(start, end, limit)
	}
	return nil, fmt.Errorf("wisptrace: unsupported Dimension %d", dim)
}
