package cmd

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/GuhaneshT/WispTraceDB/segment"
)

// TestCompactionDiagnostic inspects the post-compaction segment set: live
// segment count, per-segment span count, zone-map range, and bloom section
// size, plus what a few sample RangeQueries actually return. It was written to
// explain the Phase 0 compaction benchmark and immediately caught a benchmark
// bug: sharing a trace id across spans with a fixed span id made every write
// supersede the previous one, so the scan never saw a real range.
//
// Run with:
//
//	go test -run TestCompactionDiagnostic -v ./cmd
func TestCompactionDiagnostic(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWispTraceConfig()
	cfg.WALPath = filepath.Join(dir, "wal.log")
	cfg.PebblePath = filepath.Join(dir, "lsm")
	cfg.SegmentDir = filepath.Join(dir, "segments")
	cfg.CheckpointPath = filepath.Join(dir, "checkpoint.dat")
	cfg.ManifestPath = filepath.Join(dir, "manifest.dat")
	cfg.SegmentFlushThreshold = 1000

	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("create wt: %v", err)
	}
	defer wt.Close()

	const numSpans = 15000
	for i := 0; i < numSpans; i++ {
		span := generateBenchSpan(fmt.Sprintf("trace-%d", i), "span-1", int64(i*10))
		if err := wt.InsertSpan(span); err != nil {
			t.Fatalf("insert error: %v", err)
		}
	}
	if err := wt.Flush(); err != nil {
		t.Fatalf("flush error: %v", err)
	}
	wt.WaitForIngest()

	live, err := wt.LiveSegments()
	if err != nil {
		t.Fatalf("live segments: %v", err)
	}
	t.Logf("compaction threshold=%d flush threshold=%d", cfg.CompactionSegmentThreshold, cfg.SegmentFlushThreshold)
	t.Logf("live segments: %d", len(live))

	total := 0
	for _, id := range live {
		r, err := segment.OpenReader(segment.SegmentPath(cfg.SegmentDir, id))
		if err != nil {
			t.Fatalf("open segment %d: %v", id, err)
		}
		t.Logf("  seg %d: spans=%d ts=[%d,%d] bloomSection=%d bytes",
			id, r.Header.SpanCount, r.Header.MinTimestamp, r.Header.MaxTimestamp, r.Header.BloomSectionLength)
		total += int(r.Header.SpanCount)
		r.Close()
	}
	t.Logf("total spans across live segments: %d (inserted %d)", total, numSpans)

	for _, start := range []int64{0, 30000, 60000, 90000, 120000} {
		filter := RangeFilter{StartTS: start, EndTS: start + 1000}
		res, err := wt.RangeQuery(filter)
		if err != nil {
			t.Fatalf("range query [%d,%d]: %v", start, start+1000, err)
		}
		t.Logf("query [%d,%d]: returned=%d", start, start+1000, len(res))
	}
}
