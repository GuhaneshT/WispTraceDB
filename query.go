package wisptrace

import (
	"context"
	"errors"
	"math"
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

// Query returns one page of spans matching q. Cursors are positional, not a
// snapshot: spans flushed or compacted between pages can shift results.
func (db *DB) Query(ctx context.Context, q Query) (Page, error) {
	if err := db.check(ctx); err != nil {
		return Page{}, err
	}
	limit := q.Limit
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	start, end := q.Range.bounds()
	ps, next, err := db.eng.QueryWithCursor(engine.RangeFilter{
		StartTS: start, EndTS: end,
		AgentID: q.Agent, Model: q.Model, ToolName: q.Tool, Team: q.Team, Status: string(q.Status),
	}, limit, q.Cursor)
	if err != nil {
		return Page{}, err
	}
	return Page{Spans: fromPayloads(ps), NextCursor: next}, nil
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

// ErrorRate returns the fraction (0..1) of a model's spans with an error status.
func (db *DB) ErrorRate(ctx context.Context, model string, r TimeRange) (float64, error) {
	if err := db.check(ctx); err != nil {
		return 0, err
	}
	start, end := r.bounds()
	return db.eng.GetErrorRate(model, start, end)
}
