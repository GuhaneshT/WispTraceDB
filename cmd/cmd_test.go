package cmd

import (
	"fmt"
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

	live, err := wt.LiveSegments()
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

	got, found, err := wt.GetSpan("trace-x", "span-x")
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

	got, found, err := wt.GetSpan("nonexistent", "nonexistent")
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

	got, found, err := wt.GetTrace("trace-1")
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

	got, found, err := wt.GetTrace("trace-1")
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

	got, found, err := wt.GetTrace("nonexistent-trace")
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

	got, err := wt.RangeQuery(RangeFilter{StartTS: 150, EndTS: 350})
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

	got, err := wt.RangeQuery(RangeFilter{StartTS: 0, EndTS: 1000, Model: "claude-haiku-4-5"})
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

	got, err := wt.RangeQuery(RangeFilter{StartTS: 0, EndTS: 1000})
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

	got, err := wt.RangeQuery(RangeFilter{StartTS: 400, EndTS: 600})
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

	got, err := wt.RangeQuery(RangeFilter{StartTS: 900, EndTS: 1000})
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
	got, err := wt.RangeQuery(RangeFilter{StartTS: 0, EndTS: 1000, AgentID: "agent-that-does-not-exist"})
	if err != nil {
		t.Fatalf("RangeQuery() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("RangeQuery(AgentID=nonexistent) = %+v, want empty", got)
	}

	// Sanity: the real value still matches — bloom pruning must never
	// produce a false negative on an actually-present value.
	got, err = wt.RangeQuery(RangeFilter{StartTS: 0, EndTS: 1000, AgentID: "agent-real"})
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

	got, found, err := wt.GetSpan("trace-x", "span-x")
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
