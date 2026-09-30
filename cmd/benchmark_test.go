package cmd

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/GuhaneshT/WispTraceDB/wal"
)

func benchConfig(b *testing.B) WispTraceConfig {
	dir := b.TempDir()
	cfg := DefaultWispTraceConfig()
	cfg.WALPath = filepath.Join(dir, "wal.log")
	cfg.PebblePath = filepath.Join(dir, "lsm")
	cfg.SegmentDir = filepath.Join(dir, "segments")
	cfg.CheckpointPath = filepath.Join(dir, "checkpoint.dat")
	cfg.ManifestPath = filepath.Join(dir, "manifest.dat")
	cfg.SegmentFlushThreshold = 1000 // reasonable batch size for benchmarks
	return cfg
}

func generateBenchSpan(traceID, spanID string, ts int64) wal.SpanPayload {
	return wal.SpanPayload{
		TraceID:   traceID,
		SpanID:    spanID,
		Timestamp: ts,
		AgentID:   "bench-agent",
		Model:     "claude-sonnet-5",
		Status:    "ok",
		TokensIn:  150,
		TokensOut: 50,
		Cost:      0.01,
		LatencyMs: 120,
		Payload:   []byte("test payload data for benchmarking"),
	}
}

func BenchmarkInsertSpan_Sequential(b *testing.B) {
	wt, err := CreateWispTraceWithConfig(benchConfig(b))
	if err != nil {
		b.Fatalf("create wt: %v", err)
	}
	defer wt.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		span := generateBenchSpan("trace-seq", fmt.Sprintf("span-%d", i), int64(i))
		if err := wt.InsertSpan(span); err != nil {
			b.Fatalf("insert error: %v", err)
		}
	}
}

func BenchmarkInsertSpan_Parallel(b *testing.B) {
	wt, err := CreateWispTraceWithConfig(benchConfig(b))
	if err != nil {
		b.Fatalf("create wt: %v", err)
	}
	defer wt.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			span := generateBenchSpan("trace-par", fmt.Sprintf("span-%d", i), int64(i))
			if err := wt.InsertSpan(span); err != nil {
				b.Fatalf("insert error: %v", err)
			}
			i++
		}
	})
}

func BenchmarkGetSpan(b *testing.B) {
	wt, err := CreateWispTraceWithConfig(benchConfig(b))
	if err != nil {
		b.Fatalf("create wt: %v", err)
	}
	defer wt.Close()

	// Preload spans
	const numSpans = 5000
	for i := 0; i < numSpans; i++ {
		span := generateBenchSpan("trace-get", fmt.Sprintf("span-%d", i), int64(i))
		if err := wt.InsertSpan(span); err != nil {
			b.Fatalf("insert error: %v", err)
		}
	}
	if err := wt.Flush(); err != nil {
		b.Fatalf("flush error: %v", err)
	}
	wt.WaitForIngest()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		spanID := fmt.Sprintf("span-%d", i%numSpans)
		_, found, err := wt.GetSpan("trace-get", spanID)
		if err != nil || !found {
			b.Fatalf("get error: %v, found: %v", err, found)
		}
	}
}

func BenchmarkGetTrace(b *testing.B) {
	wt, err := CreateWispTraceWithConfig(benchConfig(b))
	if err != nil {
		b.Fatalf("create wt: %v", err)
	}
	defer wt.Close()

	// Preload traces
	const numTraces = 500
	const spansPerTrace = 10
	for t := 0; t < numTraces; t++ {
		traceID := fmt.Sprintf("trace-%d", t)
		for s := 0; s < spansPerTrace; s++ {
			span := generateBenchSpan(traceID, fmt.Sprintf("span-%d", s), int64(s))
			if err := wt.InsertSpan(span); err != nil {
				b.Fatalf("insert error: %v", err)
			}
		}
	}
	if err := wt.Flush(); err != nil {
		b.Fatalf("flush error: %v", err)
	}
	wt.WaitForIngest()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		traceID := fmt.Sprintf("trace-%d", i%numTraces)
		spans, found, err := wt.GetTrace(traceID)
		if err != nil || !found || len(spans) != spansPerTrace {
			b.Fatalf("get trace error: %v, found: %v, len: %d", err, found, len(spans))
		}
	}
}

func BenchmarkRangeQuery(b *testing.B) {
	wt, err := CreateWispTraceWithConfig(benchConfig(b))
	if err != nil {
		b.Fatalf("create wt: %v", err)
	}
	defer wt.Close()

	// Preload 10000 spans spread across timestamps (0 to 100000)
	const numSpans = 10000
	for i := 0; i < numSpans; i++ {
		span := generateBenchSpan(fmt.Sprintf("trace-%d", i), "span-1", int64(i*10))
		if err := wt.InsertSpan(span); err != nil {
			b.Fatalf("insert error: %v", err)
		}
	}
	if err := wt.Flush(); err != nil {
		b.Fatalf("flush error: %v", err)
	}
	wt.WaitForIngest()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		startTS := int64((i * 10) % 90000)
		filter := RangeFilter{StartTS: startTS, EndTS: startTS + 1000} // small time window
		_, err := wt.RangeQuery(filter)
		if err != nil {
			b.Fatalf("range query error: %v", err)
		}
	}
}
