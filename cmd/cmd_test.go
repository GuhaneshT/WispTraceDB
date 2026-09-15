package cmd

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/GuhaneshT/WispTraceDB/pebble"
	"github.com/GuhaneshT/WispTraceDB/segment"
	"github.com/GuhaneshT/WispTraceDB/wal"
)

func testConfig(t *testing.T) WispTraceConfig {
	dir := t.TempDir()
	cfg := DefaultWispTraceConfig()
	cfg.WALPath = filepath.Join(dir, "wal.log")
	cfg.PebblePath = filepath.Join(dir, "lsm")
	cfg.SegmentDir = filepath.Join(dir, "segments")
	cfg.CheckpointPath = filepath.Join(dir, "checkpoint.dat")
	cfg.ManifestPath = filepath.Join(dir, "manifest.dat")
	cfg.SegmentFlushThreshold = 3
	return cfg
}

func testSpan(traceID, spanID string, ts int64) wal.SpanPayload {
	return wal.SpanPayload{
		TraceID:   traceID,
		SpanID:    spanID,
		Timestamp: ts,
		AgentID:   "agent-1",
		Model:     "claude-sonnet-5",
		Status:    "ok",
		TokensIn:  5,
		TokensOut: 10,
		Cost:      0.002,
		LatencyMs: 42,
		Payload:   []byte("payload"),
	}
}

func TestInsertSpanTriggersFlushAtThreshold(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	spans := []wal.SpanPayload{
		testSpan("t1", "s1", 100),
		testSpan("t1", "s2", 110),
		testSpan("t1", "s3", 120), // hits threshold of 3, triggers flush
	}
	for _, s := range spans {
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan() error = %v", err)
		}
	}
	wt.WaitForIngest()

	// After the flush, segment 1 should exist and be indexed in Pebble.
	if wt.nextSegmentID != 2 {
		t.Fatalf("nextSegmentID = %d, want 2 (one segment flushed)", wt.nextSegmentID)
	}

	for _, s := range spans {
		key := segment.CompositeKey(s.TraceID, s.SpanID)
		loc, err := wt.index.GetSpan([]byte(key))
		if err != nil {
			t.Fatalf("GetSpan(%s) error = %v", key, err)
		}
		if loc.SegmentID != 1 {
			t.Fatalf("GetSpan(%s).SegmentID = %d, want 1", key, loc.SegmentID)
		}
	}

	// Checkpoint should reflect the confirmed segment.
	checkpointVal, err := NewCheckpoint(wt.config.CheckpointPath).Load()
	if err != nil {
		t.Fatalf("checkpoint Load() error = %v", err)
	}
	if checkpointVal != 1 {
		t.Fatalf("checkpoint = %d, want 1", checkpointVal)
	}
}

func TestInsertSpanBelowThresholdDoesNotFlush(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	if err := wt.InsertSpan(testSpan("t1", "s1", 100)); err != nil {
		t.Fatalf("InsertSpan() error = %v", err)
	}

	if wt.nextSegmentID != 1 {
		t.Fatalf("nextSegmentID = %d, want 1 (no flush yet)", wt.nextSegmentID)
	}

	// Not yet indexed — still only buffered.
	key := segment.CompositeKey("t1", "s1")
	if _, err := wt.index.GetSpan([]byte(key)); err == nil {
		t.Fatal("GetSpan() should not find a span before its segment is flushed")
	}
}

func TestFlushIsIdempotentWhenBufferEmpty(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() on empty buffer should be a no-op, got error = %v", err)
	}
	if wt.nextSegmentID != 1 {
		t.Fatalf("nextSegmentID = %d, want 1 (nothing flushed)", wt.nextSegmentID)
	}
}

func TestExplicitFlushIndexesBufferedSpans(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	if err := wt.InsertSpan(testSpan("t1", "s1", 100)); err != nil {
		t.Fatalf("InsertSpan() error = %v", err)
	}
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	key := segment.CompositeKey("t1", "s1")
	if _, err := wt.index.GetSpan([]byte(key)); err != nil {
		t.Fatalf("GetSpan() after explicit Flush() error = %v", err)
	}
}

func TestCloseFlushesRemainingBufferedSpans(t *testing.T) {
	cfg := testConfig(t)
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}

	if err := wt.InsertSpan(testSpan("t1", "s1", 100)); err != nil {
		t.Fatalf("InsertSpan() error = %v", err)
	}
	if err := wt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Verify the checkpoint reflects the flush that happened during Close,
	// by reopening a fresh index at the same path.
	idx, err := pebble.OpenDB(cfg.PebblePath)
	if err != nil {
		t.Fatalf("reopen pebble error = %v", err)
	}
	defer idx.Close()

	key := segment.CompositeKey("t1", "s1")
	if _, err := idx.GetSpan([]byte(key)); err != nil {
		t.Fatalf("GetSpan() after Close()'s final flush error = %v", err)
	}
}

// crashClose closes the WAL and Pebble index directly, bypassing wt.Close()
// (which would flush any buffered spans and hide exactly the gap these tests
// exist to catch). This is what a real crash looks like from recover()'s
// perspective: the WAL is durable up to its last fsynced append, but nothing
// buffered only in segmentWriter's memory survives.
func crashClose(t *testing.T, wt *WispTrace) {
	t.Helper()
	if err := wt.wal.Close(); err != nil {
		t.Fatalf("wal.Close() error = %v", err)
	}
	if err := wt.index.Close(); err != nil {
		t.Fatalf("index.Close() error = %v", err)
	}
}

func TestRecoverReplaysUnflushedSpansAfterRestart(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 100 // high enough that inserts below won't auto-flush

	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}

	spans := []wal.SpanPayload{
		testSpan("t1", "s1", 100),
		testSpan("t1", "s2", 110),
	}
	for _, s := range spans {
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan() error = %v", err)
		}
	}
	wt.WaitForIngest()

	// Sanity check: nothing has been flushed or indexed yet — these spans
	// only exist in the WAL and in segmentWriter's in-memory buffer.
	if wt.nextSegmentID != 1 {
		t.Fatalf("nextSegmentID = %d, want 1 (nothing flushed before crash)", wt.nextSegmentID)
	}

	crashClose(t, wt)

	// "Restart": open a fresh WispTrace against the same on-disk paths.
	wt2, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() after crash error = %v", err)
	}
	defer wt2.Close()

	if wt2.nextSegmentID != 2 {
		t.Fatalf("nextSegmentID = %d, want 2 (recovery should have flushed one segment)", wt2.nextSegmentID)
	}

	for _, s := range spans {
		key := segment.CompositeKey(s.TraceID, s.SpanID)
		loc, err := wt2.index.GetSpan([]byte(key))
		if err != nil {
			t.Fatalf("GetSpan(%s) after recovery error = %v", key, err)
		}
		if loc.SegmentID != 1 {
			t.Fatalf("GetSpan(%s).SegmentID = %d, want 1", key, loc.SegmentID)
		}

		// The span must actually be readable from the recovered segment file,
		// not just present as a dangling index entry.
		reader, err := segment.OpenReader(segment.SegmentPath(cfg.SegmentDir, loc.SegmentID))
		if err != nil {
			t.Fatalf("OpenReader() error = %v", err)
		}
		got, err := reader.ReadAt(loc.Offset)
		reader.Close()
		if err != nil {
			t.Fatalf("ReadAt() error = %v", err)
		}
		if got.TraceID != s.TraceID || got.SpanID != s.SpanID {
			t.Fatalf("recovered span = %+v, want %+v", got, s)
		}
	}

	checkpointVal, err := NewCheckpoint(cfg.CheckpointPath).Load()
	if err != nil {
		t.Fatalf("checkpoint Load() error = %v", err)
	}
	if checkpointVal != 1 {
		t.Fatalf("checkpoint = %d, want 1", checkpointVal)
	}
}

func TestRecoverIsNoopWithNoUnflushedRecords(t *testing.T) {
	cfg := testConfig(t)

	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	if err := wt.InsertSpan(testSpan("t1", "s1", 100)); err != nil {
		t.Fatalf("InsertSpan() error = %v", err)
	}
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if wt.nextSegmentID != 2 {
		t.Fatalf("nextSegmentID = %d, want 2 after explicit flush", wt.nextSegmentID)
	}

	crashClose(t, wt)

	wt2, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() after restart error = %v", err)
	}
	defer wt2.Close()

	// Everything was already confirmed before the "crash" — RemoveSegmentsUpTo
	// should have pruned the WAL, so replay finds nothing and recover() must
	// not manufacture an extra, empty segment.
	if wt2.nextSegmentID != 2 {
		t.Fatalf("nextSegmentID = %d, want 2 (recovery should be a no-op)", wt2.nextSegmentID)
	}
	if _, err := os.Stat(segment.SegmentPath(cfg.SegmentDir, 2)); !os.IsNotExist(err) {
		t.Fatalf("segment 2 should not exist, stat err = %v", err)
	}
}

func TestAutoCompactionTriggersPastThreshold(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 1          // flush a new segment on every insert
	cfg.CompactionSegmentThreshold = 4     // compact once live count exceeds 4

	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	// Insert enough spans, each in its own segment (threshold=1), to cross
	// the compaction threshold at least once.
	for i := 0; i < 6; i++ {
		s := testSpan("t1", fmt.Sprintf("s%d", i), int64(i*10))
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan(%d) error = %v", i, err)
		}
	}

	live, err := wt.WaitForIngest().LiveSegments()
	if err != nil {
		t.Fatalf("LiveSegments() error = %v", err)
	}
	// 6 segments flushed; once count exceeded 4, the oldest half (of
	// whatever the live count was at that trigger point) got merged into
	// one. The live count afterward must be strictly less than 6 — proof a
	// merge actually happened rather than every segment just accumulating.
	if len(live) >= 6 {
		t.Fatalf("LiveSegments() = %v (len %d), want fewer than 6 — compaction should have merged some", live, len(live))
	}

	// Every span must still be findable regardless of which segment (merged
	// or original) now holds it — compaction must be invisible to readers.
	for i := 0; i < 6; i++ {
		spanID := fmt.Sprintf("s%d", i)
		key := segment.CompositeKey("t1", spanID)
		loc, err := wt.index.GetSpan([]byte(key))
		if err != nil {
			t.Fatalf("GetSpan(%s) error = %v", spanID, err)
		}
		reader, err := segment.OpenReader(segment.SegmentPath(cfg.SegmentDir, loc.SegmentID))
		if err != nil {
			t.Fatalf("OpenReader(segment %d) for %s error = %v", loc.SegmentID, spanID, err)
		}
		got, err := reader.ReadAt(loc.Offset)
		reader.Close()
		if err != nil {
			t.Fatalf("ReadAt() for %s error = %v", spanID, err)
		}
		if got.SpanID != spanID {
			t.Fatalf("ReadAt() for %s = %+v, want SpanID %s", spanID, got, spanID)
		}
	}
}

func TestGetSpanReturnsInsertedSpan(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 1
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	want := testSpan("trace-x", "span-x", 555)
	if err := wt.InsertSpan(want); err != nil {
		t.Fatalf("InsertSpan() error = %v", err)
	}

	got, found, err := wt.WaitForIngest().GetSpan("trace-x", "span-x")
	if err != nil {
		t.Fatalf("GetSpan() error = %v", err)
	}
	if !found {
		t.Fatal("GetSpan() found = false, want true")
	}
	if got.TraceID != want.TraceID || got.SpanID != want.SpanID || got.Cost != want.Cost {
		t.Fatalf("GetSpan() = %+v, want %+v", got, want)
	}
}

func TestGetSpanNotFoundReturnsFalseNoError(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	got, found, err := wt.WaitForIngest().GetSpan("nonexistent", "nonexistent")
	if err != nil {
		t.Fatalf("GetSpan() error = %v, want nil for a missing key", err)
	}
	if found {
		t.Fatalf("GetSpan() found = true, want false, got %+v", got)
	}
}

func TestGetTraceReconstructsSpansScatteredAcrossSegments(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 1 // every InsertSpan flushes its own segment
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	// Interleave spans from two traces across separate flushes, mirroring
	// real arrival order — trace-1's spans must end up in different segments.
	spans := []wal.SpanPayload{
		testSpan("trace-1", "span-1", 100),
		testSpan("trace-2", "span-1", 105),
		testSpan("trace-1", "span-2", 110),
		testSpan("trace-1", "span-3", 120),
	}
	for _, s := range spans {
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan() error = %v", err)
		}
	}
	wt.WaitForIngest()

	got, found, err := wt.WaitForIngest().GetTrace("trace-1")
	if err != nil {
		t.Fatalf("GetTrace() error = %v", err)
	}
	if !found {
		t.Fatal("GetTrace() found = false, want true")
	}
	if len(got) != 3 {
		t.Fatalf("GetTrace() returned %d spans, want 3", len(got))
	}

	gotIDs := make(map[string]bool, len(got))
	for _, s := range got {
		if s.TraceID != "trace-1" {
			t.Fatalf("GetTrace(trace-1) returned a span from %s", s.TraceID)
		}
		gotIDs[s.SpanID] = true
	}
	for _, want := range []string{"span-1", "span-2", "span-3"} {
		if !gotIDs[want] {
			t.Fatalf("GetTrace() missing %s, got %v", want, gotIDs)
		}
	}
}

func TestGetTraceMultipleSpansInSameSegment(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 10 // all spans land in one segment
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	for i := 0; i < 3; i++ {
		s := testSpan("trace-1", fmt.Sprintf("span-%d", i), int64(i*10))
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan(%d) error = %v", i, err)
		}
	}
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, found, err := wt.WaitForIngest().GetTrace("trace-1")
	if err != nil {
		t.Fatalf("GetTrace() error = %v", err)
	}
	if !found || len(got) != 3 {
		t.Fatalf("GetTrace() = (found=%v, len=%d), want (true, 3)", found, len(got))
	}
}

func TestGetTraceNotFoundReturnsFalseNoError(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	got, found, err := wt.WaitForIngest().GetTrace("nonexistent-trace")
	if err != nil {
		t.Fatalf("GetTrace() error = %v, want nil for a missing trace", err)
	}
	if found {
		t.Fatalf("GetTrace() found = true, want false, got %v", got)
	}
}

func TestRangeQueryFiltersByTimeWindow(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 1
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	for i, ts := range []int64{100, 200, 300, 400} {
		s := testSpan("t1", fmt.Sprintf("s%d", i), ts)
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan() error = %v", err)
		}
	}

	got, err := wt.WaitForIngest().RangeQuery(RangeFilter{StartTS: 150, EndTS: 350})
	if err != nil {
		t.Fatalf("RangeQuery() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("RangeQuery() len = %d, want 2 (ts 200 and 300)", len(got))
	}
	for _, s := range got {
		if s.Timestamp < 150 || s.Timestamp > 350 {
			t.Fatalf("RangeQuery() returned out-of-window span: %+v", s)
		}
	}
}

func TestRangeQueryFiltersByDimension(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 1
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	a := testSpan("t1", "s1", 100)
	a.Model = "claude-sonnet-5"
	b := testSpan("t1", "s2", 110)
	b.Model = "claude-haiku-4-5"

	if err := wt.InsertSpan(a); err != nil {
		t.Fatalf("InsertSpan(a) error = %v", err)
	}
	if err := wt.InsertSpan(b); err != nil {
		t.Fatalf("InsertSpan(b) error = %v", err)
	}

	got, err := wt.WaitForIngest().RangeQuery(RangeFilter{StartTS: 0, EndTS: 1000, Model: "claude-haiku-4-5"})
	if err != nil {
		t.Fatalf("RangeQuery() error = %v", err)
	}
	if len(got) != 1 || got[0].SpanID != "s2" {
		t.Fatalf("RangeQuery(Model=claude-haiku-4-5) = %+v, want just s2", got)
	}
}

func TestRangeQueryExcludesTombstones(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 1
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	live := testSpan("t1", "s1", 100)
	tombstone := testSpan("t1", "s2", 110)
	tombstone.Deleted = true

	if err := wt.InsertSpan(live); err != nil {
		t.Fatalf("InsertSpan(live) error = %v", err)
	}
	if err := wt.InsertSpan(tombstone); err != nil {
		t.Fatalf("InsertSpan(tombstone) error = %v", err)
	}

	got, err := wt.WaitForIngest().RangeQuery(RangeFilter{StartTS: 0, EndTS: 1000})
	if err != nil {
		t.Fatalf("RangeQuery() error = %v", err)
	}
	if len(got) != 1 || got[0].SpanID != "s1" {
		t.Fatalf("RangeQuery() = %+v, want just the live span s1", got)
	}
}

func TestRangeQueryPrunesSegmentsOutsideWindow(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 1
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	// Segment 1: entirely before the query window.
	if err := wt.InsertSpan(testSpan("t1", "old", 10)); err != nil {
		t.Fatalf("InsertSpan(old) error = %v", err)
	}
	// Segment 2: inside the window.
	if err := wt.InsertSpan(testSpan("t1", "in-window", 500)); err != nil {
		t.Fatalf("InsertSpan(in-window) error = %v", err)
	}

	got, err := wt.WaitForIngest().RangeQuery(RangeFilter{StartTS: 400, EndTS: 600})
	if err != nil {
		t.Fatalf("RangeQuery() error = %v", err)
	}
	if len(got) != 1 || got[0].SpanID != "in-window" {
		t.Fatalf("RangeQuery() = %+v, want just in-window", got)
	}
}

func TestRangeQueryNoMatchesReturnsEmpty(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	if err := wt.InsertSpan(testSpan("t1", "s1", 100)); err != nil {
		t.Fatalf("InsertSpan() error = %v", err)
	}
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	got, err := wt.WaitForIngest().RangeQuery(RangeFilter{StartTS: 900, EndTS: 1000})
	if err != nil {
		t.Fatalf("RangeQuery() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("RangeQuery() = %+v, want empty", got)
	}
}

func TestRangeQueryBloomPruningDoesNotAffectCorrectness(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 1
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	a := testSpan("t1", "s1", 100)
	a.AgentID = "agent-real"
	if err := wt.InsertSpan(a); err != nil {
		t.Fatalf("InsertSpan() error = %v", err)
	}

	// AgentID never inserted anywhere — every segment's bloom filter should
	// definitively rule it out, so this must return empty without erroring,
	// regardless of whether the timestamp window would otherwise match.
	got, err := wt.WaitForIngest().RangeQuery(RangeFilter{StartTS: 0, EndTS: 1000, AgentID: "agent-that-does-not-exist"})
	if err != nil {
		t.Fatalf("RangeQuery() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("RangeQuery(AgentID=nonexistent) = %+v, want empty", got)
	}

	// Sanity: the real value still matches — bloom pruning must never
	// produce a false negative on an actually-present value.
	got, err = wt.WaitForIngest().RangeQuery(RangeFilter{StartTS: 0, EndTS: 1000, AgentID: "agent-real"})
	if err != nil {
		t.Fatalf("RangeQuery() error = %v", err)
	}
	if len(got) != 1 || got[0].AgentID != "agent-real" {
		t.Fatalf("RangeQuery(AgentID=agent-real) = %+v, want just the real span", got)
	}
}

func TestGetSpanFindsTombstonedSpanWithDeletedSet(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 1
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	tombstone := testSpan("trace-x", "span-x", 555)
	tombstone.Deleted = true
	if err := wt.InsertSpan(tombstone); err != nil {
		t.Fatalf("InsertSpan() error = %v", err)
	}

	got, found, err := wt.WaitForIngest().GetSpan("trace-x", "span-x")
	if err != nil {
		t.Fatalf("GetSpan() error = %v", err)
	}
	if !found {
		t.Fatal("GetSpan() found = false, want true — a tombstone is still an indexed record, not absent")
	}
	if !got.Deleted {
		t.Fatal("GetSpan().Deleted = false, want true")
	}
}

func TestPointLookupThroughSegmentReader(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 1 // flush every span, simplest case
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	want := testSpan("trace-x", "span-x", 555)
	if err := wt.InsertSpan(want); err != nil {
		t.Fatalf("InsertSpan() error = %v", err)
	}

	// Ensure the span is flushed and indexed before the raw index read: the
	// index is written by the background ingest goroutine, and without a sync
	// point this races it.
	wt.WaitForIngest()

	key := segment.CompositeKey(want.TraceID, want.SpanID)
	loc, err := wt.index.GetSpan([]byte(key))
	if err != nil {
		t.Fatalf("GetSpan() error = %v", err)
	}

	reader, err := segment.OpenReader(segment.SegmentPath(cfg.SegmentDir, loc.SegmentID))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer reader.Close()

	got, err := reader.ReadAt(loc.Offset)
	if err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	if got.TraceID != want.TraceID || got.SpanID != want.SpanID {
		t.Fatalf("ReadAt() = %+v, want %+v", got, want)
	}
}

func TestSegmentIDsSurviveRestartAfterCompaction(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 3 // drive flushes explicitly, not at threshold
	cfg.CompactionSegmentThreshold = 2

	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}

	// Four 2-span batches: segments 1..4 (checkpoint 4), then the inline
	// compaction after segment 4 merges [1,2] into segment 5, leaving the
	// manifest as [3,4,5]. Compaction consumed id 5 purely in memory — it is
	// never written to the checkpoint, which is what the restart below tests.
	batches := [][]wal.SpanPayload{
		{testSpan("t-old", "a1", 100), testSpan("t-old", "a2", 101)},
		{testSpan("t-old", "b1", 110), testSpan("t-old", "b2", 111)},
		{testSpan("t-old", "c1", 120), testSpan("t-old", "c2", 121)},
		{testSpan("t-old", "d1", 130), testSpan("t-old", "d2", 131)},
	}
	for _, batch := range batches {
		for _, s := range batch {
			if err := wt.InsertSpan(s); err != nil {
				t.Fatalf("InsertSpan() error = %v", err)
			}
		}
		wt.WaitForIngest()
		if err := wt.Flush(); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
	}

	if wt.nextSegmentID != 6 {
		t.Fatalf("nextSegmentID = %d, want 6 (segs 1-4 flushed, [1,2] merged into 5)", wt.nextSegmentID)
	}
	live, err := wt.LiveSegments()
	if err != nil {
		t.Fatalf("LiveSegments() error = %v", err)
	}
	if fmt.Sprint(live) != "[3 4 5]" {
		t.Fatalf("LiveSegments() = %v, want [3 4 5]", live)
	}

	if err := wt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// ----- restart boundary -----
	// The checkpoint reads 4, but merged segment 5 is live on disk and in the
	// manifest. Seeding the next id from the checkpoint alone would hand out 5
	// again and O_TRUNC the merged segment (now a loud refusal).
	wt2, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("reopen: CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt2.Close()

	if wt2.nextSegmentID != 6 {
		t.Fatalf("after restart nextSegmentID = %d, want 6", wt2.nextSegmentID)
	}

	for _, s := range []wal.SpanPayload{
		testSpan("t-new", "n1", 200),
		testSpan("t-new", "n2", 201),
	} {
		if err := wt2.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan() error = %v", err)
		}
	}
	wt2.WaitForIngest()
	if err := wt2.Flush(); err != nil {
		t.Fatalf("Flush() after restart error = %v", err)
	}

	// Old spans living in merged segment 5 must survive untouched.
	got, found, err := wt2.GetSpan("t-old", "a1")
	if err != nil {
		t.Fatalf("GetSpan(t-old/a1) error = %v", err)
	}
	if !found || got.SpanID != "a1" || got.Timestamp != 100 {
		t.Fatalf("GetSpan(t-old/a1) = (%+v, %v), want original a1", got, found)
	}

	// Direct proof the merged file itself was never truncated.
	reader, err := segment.OpenReader(segment.SegmentPath(cfg.SegmentDir, 5))
	if err != nil {
		t.Fatalf("OpenReader(segment 5) error = %v", err)
	}
	scanned, scanErr := reader.ScanAll()
	reader.Close()
	if scanErr != nil {
		t.Fatalf("ScanAll(segment 5) error = %v", scanErr)
	}
	spanIDs := make(map[string]bool)
	for _, s := range scanned {
		spanIDs[s.Span.SpanID] = true
	}
	for _, id := range []string{"a1", "a2", "b1", "b2"} {
		if !spanIDs[id] {
			t.Fatalf("segment 5 lost span %s after restart: got %v", id, spanIDs)
		}
	}

	got2, found2, err := wt2.GetSpan("t-new", "n1")
	if err != nil {
		t.Fatalf("GetSpan(t-new/n1) error = %v", err)
	}
	if !found2 || got2.SpanID != "n1" {
		t.Fatalf("GetSpan(t-new/n1) = (%+v, %v), want n1 found", got2, found2)
	}
}

func TestRecoveryPreservesDeleteAfterFlush(t *testing.T) {
	cfg := testConfig(t)
	// Create engine and insert a span, then flush it to segment & index.
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}

	span := testSpan("t1", "s1", 100)
	if err := wt.InsertSpan(span); err != nil {
		t.Fatalf("InsertSpan() error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Now append a delete record for the same key to WAL, but close without flushing session buffer.
	deleteSpan := testSpan("t1", "s1", 100)
	deleteSpan.Deleted = true
	if err := wt.InsertSpan(deleteSpan); err != nil {
		t.Fatalf("InsertSpan(delete) error = %v", err)
	}
	wt.WaitForIngest()

	// Simulate crash: stop engine background loops without calling wt.Flush().
	if err := wt.CloseWithoutFlush(); err != nil {
		t.Fatalf("CloseWithoutFlush() error = %v", err)
	}

	// Reopen engine — recover() runs and replays the WAL.
	wt2, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("reopen CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt2.Close()

	// Post-recovery, GetSpan("t1", "s1") must NOT return a live, un-deleted span.
	got, found, err := wt2.GetSpan("t1", "s1")
	if err != nil {
		t.Fatalf("GetSpan(t1, s1) error = %v", err)
	}
	if found && !got.Deleted {
		t.Fatalf("GetSpan(t1, s1) returned live span %+v after crash recovery, want deleted / not resurrected", got)
	}
}

func TestTier1AndTier2QueryAPIs(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	// Ingest sample spans across distinct traces and dimensions
	spans := []wal.SpanPayload{
		{TraceID: "t1", SpanID: "s1", Timestamp: 100, Model: "gpt-4o", Team: "team-a", Status: "ok", AgentID: "agent-1", ToolName: "search"},
		{TraceID: "t1", SpanID: "s2", Timestamp: 110, Model: "gpt-4o", Team: "team-a", Status: "ok", AgentID: "agent-1", ToolName: "calculator"},
		{TraceID: "t2", SpanID: "s3", Timestamp: 120, Model: "claude-3-5-sonnet", Team: "team-b", Status: "error", AgentID: "agent-2", ToolName: "python"},
		{TraceID: "t3", SpanID: "s4", Timestamp: 130, Model: "gpt-4o", Team: "team-a", Status: "error", AgentID: "agent-1", ToolName: "search"},
	}

	for _, s := range spans {
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan() error = %v", err)
		}
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// 1. Test ListTraces
	traceIDs, err := wt.ListTraces(RangeFilter{StartTS: 90, EndTS: 150}, 2)
	if err != nil {
		t.Fatalf("ListTraces() error = %v", err)
	}
	if len(traceIDs) != 2 {
		t.Fatalf("ListTraces() len = %d, want 2 (capped by limit)", len(traceIDs))
	}

	allTraceIDs, err := wt.ListTraces(RangeFilter{StartTS: 90, EndTS: 150}, 0)
	if err != nil {
		t.Fatalf("ListTraces(all) error = %v", err)
	}
	if len(allTraceIDs) != 3 {
		t.Fatalf("ListTraces(all) len = %d, want 3 distinct trace IDs", len(allTraceIDs))
	}

	// 2. Test QueryWithCursor
	page1, cursor1, err := wt.QueryWithCursor(RangeFilter{StartTS: 90, EndTS: 150}, 2, "")
	if err != nil {
		t.Fatalf("QueryWithCursor(page1) error = %v", err)
	}
	if len(page1) != 2 || cursor1 == "" {
		t.Fatalf("QueryWithCursor(page1) len = %d, cursor = %q, want len=2 and non-empty cursor", len(page1), cursor1)
	}

	page2, cursor2, err := wt.QueryWithCursor(RangeFilter{StartTS: 90, EndTS: 150}, 2, cursor1)
	if err != nil {
		t.Fatalf("QueryWithCursor(page2) error = %v", err)
	}
	if len(page2) != 2 || cursor2 != "" {
		t.Fatalf("QueryWithCursor(page2) len = %d, cursor = %q, want len=2 and empty cursor", len(page2), cursor2)
	}

	// 3. Test Tier 2 Convenience Filters
	byModel, err := wt.QueryByModel("gpt-4o", 90, 150)
	if err != nil || len(byModel) != 3 {
		t.Fatalf("QueryByModel(gpt-4o) len = %d, err = %v, want 3", len(byModel), err)
	}

	byTeam, err := wt.QueryByTeam("team-b", 90, 150)
	if err != nil || len(byTeam) != 1 {
		t.Fatalf("QueryByTeam(team-b) len = %d, err = %v, want 1", len(byTeam), err)
	}

	byStatus, err := wt.QueryByStatus("error", 90, 150)
	if err != nil || len(byStatus) != 2 {
		t.Fatalf("QueryByStatus(error) len = %d, err = %v, want 2", len(byStatus), err)
	}

	byAgent, err := wt.QueryByAgentID("agent-1", 90, 150)
	if err != nil || len(byAgent) != 3 {
		t.Fatalf("QueryByAgentID(agent-1) len = %d, err = %v, want 3", len(byAgent), err)
	}

	byTool, err := wt.QueryByToolName("search", 90, 150)
	if err != nil || len(byTool) != 2 {
		t.Fatalf("QueryByToolName(search) len = %d, err = %v, want 2", len(byTool), err)
	}
}

func TestTier3AndTier4QueryAPIs(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	spans := []wal.SpanPayload{
		{TraceID: "t1", SpanID: "s1", Timestamp: 100, Model: "gpt-4o", Team: "team-a", AgentID: "agent-1", Status: "ok", Cost: 0.10, TokensIn: 100, TokensOut: 200},
		{TraceID: "t1", SpanID: "s2", Timestamp: 110, Model: "gpt-4o", Team: "team-a", AgentID: "agent-1", Status: "ok", Cost: 0.20, TokensIn: 200, TokensOut: 300},
		{TraceID: "t2", SpanID: "s3", Timestamp: 120, Model: "claude-3-5-sonnet", Team: "team-b", AgentID: "agent-2", Status: "error", Cost: 0.50, TokensIn: 500, TokensOut: 500},
		{TraceID: "t3", SpanID: "s4", Timestamp: 130, Model: "gpt-4o", Team: "team-a", AgentID: "agent-1", Status: "error", Cost: 0.15, TokensIn: 150, TokensOut: 150},
	}

	for _, s := range spans {
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan() error = %v", err)
		}
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// 1. Cost By Model & Team
	costByModel, err := wt.GetCostByModel(90, 150)
	if err != nil || math.Abs(costByModel["gpt-4o"]-0.45) > 1e-6 || math.Abs(costByModel["claude-3-5-sonnet"]-0.50) > 1e-6 {
		t.Fatalf("GetCostByModel() = %v, err = %v", costByModel, err)
	}

	costByTeam, err := wt.GetCostByTeam(90, 150)
	if err != nil || math.Abs(costByTeam["team-a"]-0.45) > 1e-6 || math.Abs(costByTeam["team-b"]-0.50) > 1e-6 {
		t.Fatalf("GetCostByTeam() = %v, err = %v", costByTeam, err)
	}

	// 2. Tokens By Model
	tokensByModel, err := wt.GetTokensByModel(90, 150)
	if err != nil {
		t.Fatalf("GetTokensByModel() error = %v", err)
	}
	if tokensByModel["gpt-4o"].TokensIn != 450 || tokensByModel["gpt-4o"].TokensOut != 650 || tokensByModel["gpt-4o"].Total != 1100 {
		t.Fatalf("GetTokensByModel(gpt-4o) = %+v, want TokensIn=450, TokensOut=650, Total=1100", tokensByModel["gpt-4o"])
	}

	// 3. Top Rankings
	topTeams, err := wt.GetTopTeamsByCost(1, 90, 150)
	if err != nil || len(topTeams) != 1 || topTeams[0].Team != "team-b" {
		t.Fatalf("GetTopTeamsByCost() = %+v, err = %v, want top team-b", topTeams, err)
	}

	topModels, err := wt.GetTopModelsBySpanCount(1, 90, 150)
	if err != nil || len(topModels) != 1 || topModels[0].Model != "gpt-4o" || topModels[0].Count != 3 {
		t.Fatalf("GetTopModelsBySpanCount() = %+v, err = %v, want top gpt-4o with count 3", topModels, err)
	}

	// 4. QueryAggregations
	buckets, err := wt.QueryAggregations("1m", 0, 200000000000)
	if err != nil {
		t.Fatalf("QueryAggregations() error = %v", err)
	}
	if len(buckets) == 0 {
		t.Fatalf("QueryAggregations() returned 0 buckets, want >0")
	}

	// 5. Error Analytics
	failed, err := wt.GetFailedSpans(0, 90, 150)
	if err != nil || len(failed) != 2 {
		t.Fatalf("GetFailedSpans() len = %d, err = %v, want 2", len(failed), err)
	}

	errRate, err := wt.GetErrorRate("gpt-4o", 90, 150)
	if err != nil || errRate < 33.3 || errRate > 33.4 {
		t.Fatalf("GetErrorRate(gpt-4o) = %f, err = %v, want ~33.33%%", errRate, err)
	}

	// 6. Distinct Dimensions
	models, err := wt.GetDistinctModels(90, 150)
	if err != nil || len(models) != 2 || models[0] != "claude-3-5-sonnet" || models[1] != "gpt-4o" {
		t.Fatalf("GetDistinctModels() = %v, err = %v", models, err)
	}

	teams, err := wt.GetDistinctTeams(90, 150)
	if err != nil || len(teams) != 2 || teams[0] != "team-a" || teams[1] != "team-b" {
		t.Fatalf("GetDistinctTeams() = %v, err = %v", teams, err)
	}

	agents, err := wt.GetDistinctAgents(90, 150)
	if err != nil || len(agents) != 2 || agents[0] != "agent-1" || agents[1] != "agent-2" {
		t.Fatalf("GetDistinctAgents() = %v, err = %v", agents, err)
	}
}
