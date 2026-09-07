package rollup

import "fmt"

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
