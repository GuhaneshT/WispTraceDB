package rollup

import (
	"fmt"
	"math"
)

const (
	WindowMinute  = "1m"
	WindowFiveMin = "5m"
	WindowHour    = "1h"
	WindowDay     = "1d"
)


const (
	WindowSizeMinute  int64 = 60 * 1_000_000_000
	WindowSizeFiveMin int64 = 5 * 60 * 1_000_000_000
	WindowSizeHour    int64 = 60 * 60 * 1_000_000_000
	WindowSizeDay     int64 = 24 * 60 * 60 * 1_000_000_000
)

// CostScale converts a float64 dollar amount to the fixed-point int64 units
// SumCost is stored in (micro-dollars). SumCost stays an integer rather than
// a float so that millions of incremental additions across a bucket's
// lifetime don't accumulate floating-point rounding drift — the same reason
// money is conventionally counted in a small integer subunit instead of a
// float. Callers must scale on the way in (ScaleCost) and back on the way
// out (UnscaleCost); never pass a raw float64 dollar amount through int64()
// directly — that truncates any cost under $1.00 to zero.
const CostScale = 1_000_000

// ScaleCost converts a float64 dollar amount into CostScale units for
// storage in a Value's SumCost.
func ScaleCost(dollars float64) int64 {
	return int64(math.Round(dollars * CostScale))
}

// UnscaleCost converts a SumCost total (in CostScale units) back into a
// float64 dollar amount for display/query results.
func UnscaleCost(scaled int64) float64 {
	return float64(scaled) / CostScale
}


type RollupManager struct {
	Minute  *Store // 1-minute buckets
	FiveMin *Store // 5-minute buckets
	Hour    *Store // 1-hour buckets
	Day     *Store // 1-day buckets
}

func NewRollupManager() *RollupManager {
	return &RollupManager{
		Minute:  NewStore(WindowSizeMinute),
		FiveMin: NewStore(WindowSizeFiveMin),
		Hour:    NewStore(WindowSizeHour),
		Day:     NewStore(WindowSizeDay),
	}
}

func (m *RollupManager) Add(timestamp int64, model string, cost, tokensIn, tokensOut, latencyMs int64) {
	m.Minute.Add(timestamp, model, cost, tokensIn, tokensOut, latencyMs)
	m.FiveMin.Add(timestamp, model, cost, tokensIn, tokensOut, latencyMs)
	m.Hour.Add(timestamp, model, cost, tokensIn, tokensOut, latencyMs)
	m.Day.Add(timestamp, model, cost, tokensIn, tokensOut, latencyMs)
}

func (m *RollupManager) GetBucket(window string, key BucketKey) (*Value, bool, error) {
	s, err := m.storeFor(window)
	if err != nil {
		return nil, false, err
	}
	v, ok := s.GetBucket(key)
	return v, ok, nil
}

func (m *RollupManager) GetBucketInRange(window string, start, end int64) (map[BucketKey]*Value, error) {
	s, err := m.storeFor(window)
	if err != nil {
		return nil, err
	}
	return s.GetBucketInRange(start, end), nil
}

func (m *RollupManager) storeFor(window string) (*Store, error) {
	switch window {
	case WindowMinute:
		return m.Minute, nil
	case WindowFiveMin:
		return m.FiveMin, nil
	case WindowHour:
		return m.Hour, nil
	case WindowDay:
		return m.Day, nil
	default:
		return nil, fmt.Errorf(
			"rollup: unknown window %q; valid: %s, %s, %s, %s",
			window, WindowMinute, WindowFiveMin, WindowHour, WindowDay,
		)
	}
}
