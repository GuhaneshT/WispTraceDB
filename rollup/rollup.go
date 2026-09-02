package rollup

import "sync"

type BucketKey struct {
    WindowStart int64
    Model       string
}

type Value struct {
    Count int64

    SumCost      int64
    SumTokensIn  int64
    SumTokensOut int64

    SumLatencyMs int64
    MinLatencyMs int64
    MaxLatencyMs int64
}

type Store struct {
    WindowSize int64
    mu         sync.Mutex
    buckets    map[BucketKey]*Value
}

func NewStore(windowSize int64) *Store {
    return &Store{
        WindowSize: windowSize,
        buckets:    make(map[BucketKey]*Value),
    }
}

func (s *Store) Add(timestamp int64,model string,cost int64,tokensIn int64,tokensOut int64,latencyMs int64,) {
    s.mu.Lock()
    defer s.mu.Unlock()

    bucketStart := (timestamp / s.WindowSize) * s.WindowSize

    key := BucketKey{
        WindowStart: bucketStart,
        Model:       model,
    }

    if val, ok := s.buckets[key]; ok {
        val.Count++
        val.SumCost += cost
        val.SumTokensIn += tokensIn
        val.SumTokensOut += tokensOut
        val.SumLatencyMs += latencyMs

        if latencyMs < val.MinLatencyMs {
            val.MinLatencyMs = latencyMs
        }

        if latencyMs > val.MaxLatencyMs {
            val.MaxLatencyMs = latencyMs
        }

        return
    }

    s.buckets[key] = &Value{
        Count:        1,
        SumCost:      cost,
        SumTokensIn:  tokensIn,
        SumTokensOut: tokensOut,
        SumLatencyMs: latencyMs,
        MinLatencyMs: latencyMs,
        MaxLatencyMs: latencyMs,
    }
}



func (s *Store) GetAllBuckets() map[BucketKey]*Value {
    s.mu.Lock()
    defer s.mu.Unlock()

    result := make(map[BucketKey]*Value, len(s.buckets))

    for key, value := range s.buckets {
        valueCopy := *value
        result[key] = &valueCopy
    }

    return result
}

func (s *Store) GetBucket(key BucketKey) (*Value, bool) {
    s.mu.Lock()
    defer s.mu.Unlock()

    value, ok := s.buckets[key]
    if !ok {
        return nil, false
    }

    valueCopy := *value
    return &valueCopy, true
}

func (s *Store) GetBucketInRange(start, end int64) map[BucketKey]*Value {
    s.mu.Lock()
    defer s.mu.Unlock()

    result := make(map[BucketKey]*Value)

    for key, value := range s.buckets {
        if key.WindowStart >= start && key.WindowStart < end {
            valueCopy := *value
            result[key] = &valueCopy
        }
    }

    return result
}

func (s *Store) Evict(start int64) map[BucketKey]*Value {
	s.mu.Lock()
	defer s.mu.Unlock()

	evicted := make(map[BucketKey]*Value)

	for key,value := range s.buckets {
		if key.WindowStart < start {
			valueCopy := *value
			evicted[key] = &valueCopy
			delete(s.buckets,key)
		}
	}
	return evicted
}

