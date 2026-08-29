package cmd

import (
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
