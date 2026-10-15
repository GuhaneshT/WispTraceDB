package cmd

import (
	"os"
	"testing"
	"time"

	"github.com/GuhaneshT/WispTraceDB/segment"
	"github.com/GuhaneshT/WispTraceDB/wal"
)

// The segment reader cache hands its handles to the compactor as a Purger, and
// the compactor must call it BEFORE unlinking: on Windows os.Remove fails
// while any handle to the file is open (see segment's
// TestDeleteWhileOpenProbe). This test holds a cached reader across a real
// Compact call and asserts Compact blocks on the purge instead of racing it,
// then finishes cleanly once the reader is released. It catches a missing or
// reordered purge on every platform: without the purge, the unlink either
// succeeds immediately (test fails the "still blocked" check) or fails after
// its retry budget (test fails the error check).
func TestSegmentCachePurgeWiresThroughCompaction(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 1 << 30 // drive flushes explicitly, one segment each
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	if wt.segCache == nil {
		t.Fatal("segCache is nil: the default config must enable the reader cache, because this test exists to exercise its purge wiring")
	}

	// Segment 1: [a1 a2], segment 2: [b1 b2].
	batches := [][]wal.SpanPayload{
		{testSpan("t1", "a1", 100), testSpan("t1", "a2", 101)},
		{testSpan("t1", "b1", 110), testSpan("t1", "b2", 111)},
	}
	for _, batch := range batches {
		for _, s := range batch {
			if err := wt.InsertSpan(s); err != nil {
				t.Fatalf("InsertSpan(%s) error = %v", s.SpanID, err)
			}
		}
		wt.WaitForIngest()
		if err := wt.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
	}

	// Populate the cache: each GetSpan acquires through it, leaving the
	// segment handles open after the call returns (the mechanism itself is
	// covered by segment's cache unit tests).
	for _, id := range []string{"a1", "b1"} {
		if _, found, err := wt.GetSpan("t1", id); err != nil || !found {
			t.Fatalf("GetSpan(t1/%s) before compaction: found=%v err=%v", id, found, err)
		}
	}

	// Hold segment 1's reader across the compaction. Purge must wait for this
	// release — it may neither close the handle out from under us nor unlink
	// the file while it is open.
	_, release, err := wt.segCache.Acquire(1)
	if err != nil {
		t.Fatalf("segCache.Acquire(1) error = %v", err)
	}

	compacted := make(chan error, 1)
	go func() {
		// wt.nextSegmentID is the id the inline compactor would carve out
		// next; using it directly keeps the manual call collision-free.
		_, err := wt.Compactor().Compact([]uint64{1, 2}, wt.nextSegmentID)
		compacted <- err
	}()

	select {
	case err := <-compacted:
		t.Fatalf("Compact returned (err=%v) while a cached reader for segment 1 was still held: the purge must wait for in-flight readers before unlinking", err)
	case <-time.After(300 * time.Millisecond):
		// Still blocked on the held reader — purge is wired in and waiting.
	}

	if err := release.Release(); err != nil {
		t.Fatalf("release.Release() error = %v", err)
	}
	select {
	case err := <-compacted:
		if err != nil {
			t.Fatalf("Compact() error = %v after the held reader was released (purge-before-unlink broken?)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Compact did not finish after the held reader was released")
	}

	// The direct consequence of handles being closed first: the old files are
	// actually gone. On Windows an open handle would have blocked every retry
	// of os.Remove and left them behind.
	for _, id := range []uint64{1, 2} {
		if _, err := os.Stat(segment.SegmentPath(cfg.SegmentDir, id)); !os.IsNotExist(err) {
			t.Fatalf("segment %d still on disk after compaction (stat err = %v): purge did not close its handles before unlink", id, err)
		}
	}

	// Reads now re-acquire through the cache: segments 1 and 2 are dead ids
	// (the index points at the merged segment), so GetSpan must succeed on
	// the new segment rather than fail on the purged ones.
	for _, id := range []string{"a1", "a2", "b1", "b2"} {
		if _, found, err := wt.GetSpan("t1", id); err != nil || !found {
			t.Fatalf("GetSpan(t1/%s) after compaction: found=%v err=%v", id, found, err)
		}
	}
	spans, found, err := wt.GetTrace("t1")
	if err != nil || !found || len(spans) != 4 {
		t.Fatalf("GetTrace(t1) after compaction = (len %d, found %v, err %v), want 4 spans", len(spans), found, err)
	}

	assertConsistent(t, wt)
}

// A negative SegmentCacheSize disables the cache entirely: every read opens
// and closes its own light handle, exactly the pre-cache path, and compaction
// runs with no purger at all.
func TestSegmentCacheDisabledStillReadsAndCompacts(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentCacheSize = -1
	cfg.SegmentFlushThreshold = 1 << 30
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	if wt.segCache != nil {
		t.Fatal("SegmentCacheSize < 0 must disable the cache, but segCache was created")
	}

	for _, s := range []wal.SpanPayload{testSpan("t1", "s1", 100), testSpan("t1", "s2", 110)} {
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan(%s) error = %v", s.SpanID, err)
		}
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if _, found, err := wt.GetSpan("t1", "s1"); err != nil || !found {
		t.Fatalf("GetSpan(t1/s1) with cache disabled: found=%v err=%v", found, err)
	}
	if spans, found, err := wt.GetTrace("t1"); err != nil || !found || len(spans) != 2 {
		t.Fatalf("GetTrace(t1) with cache disabled = (len %d, found %v, err %v), want 2 spans", len(spans), found, err)
	}

	// Compaction without a purger must still unlink (no reader handles exist
	// to worry about in this configuration).
	if _, err := wt.Compactor().Compact([]uint64{1}, wt.nextSegmentID); err != nil {
		t.Fatalf("Compact() with cache disabled error = %v", err)
	}
	if _, err := os.Stat(segment.SegmentPath(cfg.SegmentDir, 1)); !os.IsNotExist(err) {
		t.Fatalf("segment 1 still on disk after compaction (stat err = %v)", err)
	}

	assertConsistent(t, wt)
}
