package wisptrace

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/GuhaneshT/WispTraceDB/internal/rollup"
)

// Window is the size of one aggregation bucket.
type Window time.Duration

const (
	WindowMinute      = Window(time.Minute)
	WindowFiveMinutes = Window(5 * time.Minute)
	WindowHour        = Window(time.Hour)
	WindowDay         = Window(24 * time.Hour)
)

// ErrUnsupportedWindow reports a Window other than the four constants.
var ErrUnsupportedWindow = errors.New("wisptrace: unsupported aggregation window")

func (w Window) engineName() (string, error) {
	switch w {
	case WindowMinute:
		return rollup.WindowMinute, nil
	case WindowFiveMinutes:
		return rollup.WindowFiveMin, nil
	case WindowHour:
		return rollup.WindowHour, nil
	case WindowDay:
		return rollup.WindowDay, nil
	}
	return "", fmt.Errorf("%w: %v", ErrUnsupportedWindow, time.Duration(w))
}

// Bucket is the precomputed totals for one model in one time window.
type Bucket struct {
	Start  time.Time // start of the window, a multiple of Window since the Unix epoch, in UTC
	Window Window
	Model  string // "" collects spans recorded without a model

	Spans     int64
	CostUSD   float64
	TokensIn  int64
	TokensOut int64

	// Latency statistics, in whole milliseconds.
	LatencyAvg time.Duration
	LatencyMin time.Duration
	LatencyMax time.Duration
}

// Aggregates returns precomputed per-model totals (span count, cost, tokens
// and latency) for every window of the given size that starts inside the
// range, ordered by window start and then model.
//
// These buckets are maintained as spans arrive, so the call costs the same
// however many spans there are, unlike Costs, TokensByModel and the other
// scanning queries. In exchange they are coarser:
//
//   - Grouping is by model only, by design. There are no per-team, per-agent
//     or per-tool buckets; use Costs, Query and friends for those.
//   - Buckets are whole windows aligned to the Unix epoch (UTC). A bucket is
//     returned when its start time is in [From, To); a bucket the range only
//     partly covers is returned whole if it starts inside the range and is
//     left out if it starts before From. Align the range to the window to get
//     complete buckets, for example
//     Between(t.Truncate(time.Hour), t.Add(24*time.Hour).Truncate(time.Hour)).
//   - A span is counted when it is ingested. Deleting it later does not
//     remove its contribution, and ingesting the same span ID twice counts it
//     twice. Cost is kept to a millionth of a dollar per span.
//   - Buckets older than the retention window are dropped.
//
// Like every read, buckets reflect a span only once it is visible (see Sync).
func (db *DB) Aggregates(ctx context.Context, w Window, r TimeRange) ([]Bucket, error) {
	if err := db.check(ctx); err != nil {
		return nil, err
	}
	name, err := w.engineName()
	if err != nil {
		return nil, err
	}
	start, end := r.bounds()
	m, err := db.eng.QueryAggregations(name, start, end)
	if err != nil {
		return nil, err
	}

	out := make([]Bucket, 0, len(m))
	for key, v := range m {
		b := Bucket{
			Start:     time.Unix(0, key.WindowStart).UTC(),
			Window:    w,
			Model:     key.Model,
			Spans:     v.Count,
			CostUSD:   rollup.UnscaleCost(v.SumCost),
			TokensIn:  v.SumTokensIn,
			TokensOut: v.SumTokensOut,

			LatencyMin: time.Duration(v.MinLatencyMs) * time.Millisecond,
			LatencyMax: time.Duration(v.MaxLatencyMs) * time.Millisecond,
		}
		if v.Count > 0 {
			b.LatencyAvg = time.Duration(v.SumLatencyMs/v.Count) * time.Millisecond
		}
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Start.Equal(out[j].Start) {
			return out[i].Start.Before(out[j].Start)
		}
		return out[i].Model < out[j].Model
	})
	return out, nil
}
