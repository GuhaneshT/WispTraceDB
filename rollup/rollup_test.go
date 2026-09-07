package rollup

import (
	"os"
	"testing"
)

// ─── RollupManager tests ────────────────────────────────────────────────────

// TestManagerRoutesToAllWindows verifies that a single Add call lands in all
// four window stores.
func TestManagerRoutesToAllWindows(t *testing.T) {
	mgr := NewRollupManager()

	// 90 seconds in nanoseconds — falls into the 1m bucket starting at 60s,
	// and into the 0s bucket for 5m / 1h / 1d.
	ts := int64(90 * 1_000_000_000)
	mgr.Add(ts, "gpt-4o", 100, 10, 20, 500)

	for _, window := range []string{WindowMinute, WindowFiveMin, WindowHour, WindowDay} {
		buckets, err := mgr.GetBucketInRange(window, 0, ts+1)
		if err != nil {
			t.Fatalf("window %s: GetBucketInRange: %v", window, err)
		}
		if len(buckets) != 1 {
			t.Fatalf("window %s: want 1 bucket, got %d", window, len(buckets))
		}
	}
}

// TestBucketAlignment verifies that a timestamp lands in the correct bucket
// for the 1m window.
func TestBucketAlignment(t *testing.T) {
	mgr := NewRollupManager()

	// Timestamp 90s → 1m bucket must start at 60s.
	ts := int64(90 * 1_000_000_000)
	mgr.Add(ts, "gpt-4o", 0, 0, 0, 0)

	expectedStart := int64(60 * 1_000_000_000)
	key := BucketKey{WindowStart: expectedStart, Model: "gpt-4o"}
	v, ok, err := mgr.GetBucket(WindowMinute, key)
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if !ok {
		t.Fatalf("expected bucket at WindowStart=%d, not found", expectedStart)
	}
	if v.Count != 1 {
		t.Fatalf("want Count=1, got %d", v.Count)
	}
}

// TestManagerUnknownWindowReturnsError verifies that an invalid window string
// returns an error rather than panicking.
func TestManagerUnknownWindowReturnsError(t *testing.T) {
	mgr := NewRollupManager()
	if _, _, err := mgr.GetBucket("2h", BucketKey{}); err == nil {
		t.Fatal("want error for unknown window, got nil")
	}
	if _, err := mgr.GetBucketInRange("2h", 0, 1); err == nil {
		t.Fatal("want error for unknown window in GetBucketInRange, got nil")
	}
}

// TestEvictRemovesOldBuckets verifies that Evict removes buckets strictly
// older than the given cutoff and returns them as the evicted set.
func TestEvictRemovesOldBuckets(t *testing.T) {
	store := NewStore(WindowSizeMinute)

	// Bucket at t=0 (WindowStart 0) and bucket at t=90s (WindowStart 60s).
	store.Add(0, "gpt-4o", 10, 1, 2, 100)
	store.Add(int64(90*1_000_000_000), "gpt-4o", 20, 2, 4, 200)

	// Evict everything with WindowStart < 60s  → only the t=0 bucket.
	evicted := store.Evict(int64(60 * 1_000_000_000))
	if len(evicted) != 1 {
		t.Fatalf("want 1 evicted bucket, got %d", len(evicted))
	}

	remaining := store.GetAllBuckets()
	if len(remaining) != 1 {
		t.Fatalf("want 1 remaining bucket after eviction, got %d", len(remaining))
	}
}

// ─── Writer / Reader round-trip ─────────────────────────────────────────────

// TestWriterRoundTrip is the end-to-end correctness gate: flush a store to
// disk, reload it, and verify that every bucket key and value is identical.
func TestWriterRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Populate a store with two models in the same 1m bucket.
	store := NewStore(WindowSizeMinute)
	store.Add(int64(90*1_000_000_000), "gpt-4o", 100, 10, 20, 500)
	store.Add(int64(90*1_000_000_000), "claude-3", 200, 30, 40, 600)
	// Also add to a second bucket to exercise min/max WindowStart in the header.
	store.Add(int64(200*1_000_000_000), "gpt-4o", 50, 5, 10, 300)

	original := store.GetAllBuckets()

	w := NewWriter()
	for key, val := range original {
		w.Add(AggregatedMetrics{BucketKey: key, Value: *val})
	}

	if err := w.Flush(dir, 1, WindowSizeMinute); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	loaded, err := ReadSnapshot(RollupPath(dir, 1), WindowSizeMinute)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}

	got := loaded.GetAllBuckets()
	if len(got) != len(original) {
		t.Fatalf("bucket count mismatch: want %d, got %d", len(original), len(got))
	}
	for key, want := range original {
		v, ok := got[key]
		if !ok {
			t.Fatalf("missing bucket %+v after reload", key)
		}
		if *v != *want {
			t.Fatalf("bucket %+v: want %+v, got %+v", key, *want, *v)
		}
	}
}

// TestWriterWrongWindowSizeRejected verifies that ReadSnapshot refuses to load
// a file whose stored WindowSize doesn't match the requested one.
func TestWriterWrongWindowSizeRejected(t *testing.T) {
	dir := t.TempDir()

	w := NewWriter()
	w.Add(AggregatedMetrics{
		BucketKey: BucketKey{WindowStart: 0, Model: "gpt-4o"},
		Value:     Value{Count: 1, MinLatencyMs: 100, MaxLatencyMs: 100},
	})
	if err := w.Flush(dir, 1, WindowSizeMinute); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Try loading with the wrong window size (1h instead of 1m).
	if _, err := ReadSnapshot(RollupPath(dir, 1), WindowSizeHour); err == nil {
		t.Fatal("want error when loading with mismatched windowSize, got nil")
	}
}

// TestWriterEmptyFlushErrors verifies that Flush on an empty Writer returns an
// error and leaves no orphan file on disk.
func TestWriterEmptyFlushErrors(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter()

	if err := w.Flush(dir, 1, WindowSizeMinute); err == nil {
		t.Fatal("want error flushing empty writer, got nil")
	}
	if _, err := os.Stat(RollupPath(dir, 1)); !os.IsNotExist(err) {
		t.Fatal("want no rollup file after failed flush, but file exists")
	}
}
