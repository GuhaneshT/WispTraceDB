package engine

import (
	"errors"
	"testing"

	"github.com/GuhaneshT/WispTraceDB/internal/pebble"
	"github.com/GuhaneshT/WispTraceDB/internal/segment"
	"github.com/GuhaneshT/WispTraceDB/internal/wal"
)

// assertNotReadable checks that one deleted span is absent from every read path:
// the point lookup, the prefix-scanned trace reconstruction, and the
// segment-walking range query. These are three independent code paths and each
// one has previously leaked a tombstone.
//
// It deliberately does NOT assert that the trace is empty: a deleted span can
// share a trace with live siblings, and GetTrace is right to return those.
func assertNotReadable(t *testing.T, wt *WispTrace, traceID, spanID string) {
	t.Helper()

	if span, found, err := wt.GetSpan(traceID, spanID); err != nil {
		t.Fatalf("GetSpan(%s,%s) error = %v", traceID, spanID, err)
	} else if found {
		t.Fatalf("GetSpan(%s,%s) returned a deleted span (Deleted=%v): %+v", traceID, spanID, span.Deleted, span)
	}

	traceSpans, _, err := wt.GetTrace(traceID)
	if err != nil {
		t.Fatalf("GetTrace(%s) error = %v", traceID, err)
	}
	for _, s := range traceSpans {
		if s.SpanID == spanID {
			t.Fatalf("GetTrace(%s) returned the deleted span %s: %+v", traceID, spanID, s)
		}
	}

	results, err := wt.RangeQuery(RangeFilter{StartTS: 0, EndTS: 1 << 40})
	if err != nil {
		t.Fatalf("RangeQuery() error = %v", err)
	}
	for _, s := range results {
		if s.TraceID == traceID && s.SpanID == spanID {
			t.Fatalf("RangeQuery returned a deleted span: %+v", s)
		}
	}
}

// assertTraceGone asserts that a trace whose every span was deleted reports
// not-found, rather than returning an empty-but-present result.
func assertTraceGone(t *testing.T, wt *WispTrace, traceID string) {
	t.Helper()
	spans, found, err := wt.GetTrace(traceID)
	if err != nil {
		t.Fatalf("GetTrace(%s) error = %v", traceID, err)
	}
	if found {
		t.Fatalf("GetTrace(%s) found = true for a fully deleted trace: %+v", traceID, spans)
	}
}

func assertNotIndexed(t *testing.T, wt *WispTrace, traceID, spanID string) {
	t.Helper()
	key := segment.CompositeKey(traceID, spanID)
	if _, err := wt.index.GetSpan([]byte(key)); err == nil {
		t.Fatalf("tombstoned key %q is still present in the index", key)
	} else if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("index lookup for %q returned an unexpected error: %v", key, err)
	}
}

func assertConsistent(t *testing.T, wt *WispTrace) {
	t.Helper()
	rep := wt.CheckConsistency()
	for _, w := range rep.Warnings {
		t.Logf("consistency warning: %s", w)
	}
	if !rep.OK() {
		t.Fatalf("consistency errors: %v", rep.Errors)
	}
}

// A span inserted and then deleted inside one flush window arrives at
// writeSegmentBatch as two records for the same key. The batch must collapse to
// the last write, leaving nothing live to persist.
func TestTombstoneInSingleFlushWindowCollapsesBatch(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	live := testSpan("t1", "s1", 100)
	if err := wt.InsertSpan(live); err != nil {
		t.Fatalf("InsertSpan(live) error = %v", err)
	}
	tombstone := live
	tombstone.Deleted = true
	if err := wt.InsertSpan(tombstone); err != nil {
		t.Fatalf("InsertSpan(tombstone) error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// The batch collapsed to the tombstone alone, so there is nothing live to
	// persist and no segment is written. A segment of pure tombstones would
	// carry no index entries, so reconciliation would drop it from the manifest
	// and it would sit on disk unreferenced forever.
	files, err := listSegmentFiles(wt.config.SegmentDir)
	if err != nil {
		t.Fatalf("listSegmentFiles() error = %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("tombstone-only batch wrote %d segment(s) (%v), want none", len(files), files)
	}
	if wt.nextSegmentID != 1 {
		t.Fatalf("nextSegmentID = %d, want 1 — a tombstone-only batch must not consume a segment id", wt.nextSegmentID)
	}

	assertNotReadable(t, wt, "t1", "s1")
	assertTraceGone(t, wt, "t1")
	assertNotIndexed(t, wt, "t1", "s1")
	assertConsistent(t, wt)
}

// A mixed batch keeps its live spans and still applies the tombstones, in one
// atomic index batch. The segment must not contain the superseded live record.
func TestMixedBatchPersistsLiveAndDropsTombstone(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	keeper := testSpan("t1", "s1", 100)
	doomed := testSpan("t1", "s2", 110)
	if err := wt.InsertSpan(doomed); err != nil {
		t.Fatalf("InsertSpan(s2) error = %v", err)
	}
	if err := wt.InsertSpan(keeper); err != nil {
		t.Fatalf("InsertSpan(s1) error = %v", err)
	}
	tombstone := doomed
	tombstone.Deleted = true
	if err := wt.InsertSpan(tombstone); err != nil {
		t.Fatalf("InsertSpan(tombstone) error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	recs, err := wt.scanSegmentRecords(1)
	if err != nil {
		t.Fatalf("scanSegmentRecords(1) error = %v", err)
	}
	if len(recs) != 1 || recs[0].Span.SpanID != "s1" {
		t.Fatalf("segment 1 holds %+v, want only the live span s1", recs)
	}

	if _, found, err := wt.GetSpan("t1", "s1"); err != nil || !found {
		t.Fatalf("live span s1 should be readable: found=%v err=%v", found, err)
	}
	assertNotReadable(t, wt, "t1", "s2")
	assertNotIndexed(t, wt, "t1", "s2")
	assertConsistent(t, wt)
}

// The common production shape: the span is live and durable in a segment, and
// the delete arrives in a later flush. The index entry must be removed rather
// than repointed at the tombstone.
func TestTombstoneInLaterFlushRemovesIndexEntry(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	live := testSpan("t1", "s1", 100)
	if err := wt.InsertSpan(live); err != nil {
		t.Fatalf("InsertSpan(live) error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if _, found, err := wt.GetSpan("t1", "s1"); err != nil || !found {
		t.Fatalf("span should be readable before the delete: found=%v err=%v", found, err)
	}

	tombstone := live
	tombstone.Deleted = true
	if err := wt.InsertSpan(tombstone); err != nil {
		t.Fatalf("InsertSpan(tombstone) error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// The original live record is still in segment 1 and still on disk. The
	// index entry is what makes it unreachable, which is the point.
	recs, err := wt.scanSegmentRecords(1)
	if err != nil {
		t.Fatalf("scanSegmentRecords(1) error = %v", err)
	}
	if len(recs) != 1 || recs[0].Span.Deleted {
		t.Fatalf("segment 1 should still hold the original live record: %+v", recs)
	}

	assertNotReadable(t, wt, "t1", "s1")
	assertTraceGone(t, wt, "t1")
	assertNotIndexed(t, wt, "t1", "s1")
	assertConsistent(t, wt)
}

// A tombstone that has not been flushed yet must still suppress the older live
// record sitting in a segment. Returning the indexed copy would resurrect a
// deleted span for the whole flush interval.
func TestSessionTombstoneSupersedesFlushedLiveRecord(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	live := testSpan("t1", "s1", 100)
	if err := wt.InsertSpan(live); err != nil {
		t.Fatalf("InsertSpan(live) error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	tombstone := live
	tombstone.Deleted = true
	if err := wt.InsertSpan(tombstone); err != nil {
		t.Fatalf("InsertSpan(tombstone) error = %v", err)
	}
	wt.WaitForIngest()
	// Deliberately no Flush: the tombstone is still in the session buffer.

	span, found, err := wt.GetSpan("t1", "s1")
	if err != nil {
		t.Fatalf("GetSpan() error = %v", err)
	}
	if found {
		t.Fatalf("unflushed tombstone did not supersede the indexed live record: %+v", span)
	}

	// GetTrace reads through the index, which still holds the pre-delete
	// location, so without the buffered-writes-take-precedence rule it would
	// resurrect the span here even though GetSpan reports it gone.
	assertNotReadable(t, wt, "t1", "s1")
	assertTraceGone(t, wt, "t1")

	// The index still points at the live record; only the read path's
	// knowledge of the newer buffered write keeps it hidden.
	if _, err := wt.index.GetSpan([]byte(segment.CompositeKey("t1", "s1"))); err != nil {
		t.Fatalf("index entry should survive until flush: %v", err)
	}
}

// A deleted span must not inflate cost, token, or latency aggregates. The rollup
// stores are additive with no decrement path, so a tombstone that reaches Add
// is counted for the lifetime of the window.
func TestTombstonedSpanNotCountedInRollups(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	live := testSpan("t1", "s1", 100)
	if err := wt.InsertSpan(live); err != nil {
		t.Fatalf("InsertSpan(live) error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Control: the live span is counted. Without this the assertions below
	// would also pass if the window were misaligned and returned nothing.
	if got := rollupSpanCount(t, wt); got != 1 {
		t.Fatalf("rollup count after live insert = %d, want 1 (check the window alignment)", got)
	}

	tombstone := live
	tombstone.Deleted = true
	if err := wt.InsertSpan(tombstone); err != nil {
		t.Fatalf("InsertSpan(tombstone) error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if got := rollupSpanCount(t, wt); got != 1 {
		t.Fatalf("rollup count after tombstone = %d, want 1 — the deleted span was counted", got)
	}
}

// rollupSpanCount sums span counts across every bucket of the 1m window
// containing ts=100, i.e. window start 0.
func rollupSpanCount(t *testing.T, wt *WispTrace) int64 {
	t.Helper()
	buckets, err := wt.QueryAggregations("1m", 0, 60000)
	if err != nil {
		t.Fatalf("QueryAggregations() error = %v", err)
	}
	var n int64
	for _, v := range buckets {
		n += v.Count
	}
	return n
}

// A live span in the same trace must survive while its deleted sibling does not,
// so the assertions above cannot be satisfied by GetTrace failing wholesale.
func TestGetTraceKeepsLiveSiblingsOfDeletedSpan(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	keeper := testSpan("t1", "s1", 100)
	doomed := testSpan("t1", "s2", 110)
	for _, s := range []wal.SpanPayload{keeper, doomed} {
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan(%s) error = %v", s.SpanID, err)
		}
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	tombstone := doomed
	tombstone.Deleted = true
	if err := wt.InsertSpan(tombstone); err != nil {
		t.Fatalf("InsertSpan(tombstone) error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	spans, found, err := wt.GetTrace("t1")
	if err != nil {
		t.Fatalf("GetTrace() error = %v", err)
	}
	if !found {
		t.Fatal("GetTrace() found = false, want true — the live sibling disappeared")
	}
	if len(spans) != 1 || spans[0].SpanID != "s1" {
		t.Fatalf("GetTrace() = %+v, want exactly the live span s1", spans)
	}

	results, err := wt.RangeQuery(RangeFilter{StartTS: 0, EndTS: 1 << 40})
	if err != nil {
		t.Fatalf("RangeQuery() error = %v", err)
	}
	if len(results) != 1 || results[0].SpanID != "s1" {
		t.Fatalf("RangeQuery() = %+v, want exactly the live span s1", results)
	}

	assertConsistent(t, wt)
}

// The public DeleteSpan must remove a flushed span from all three read paths and
// from the index — the same guarantee the tests above check for a hand-built
// tombstone, now reachable through the API.
func TestDeleteSpanRemovesSpanFromAllReadPaths(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	live := testSpan("t1", "s1", 100)
	if err := wt.InsertSpan(live); err != nil {
		t.Fatalf("InsertSpan(live) error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if _, found, err := wt.GetSpan("t1", "s1"); err != nil || !found {
		t.Fatalf("span should be readable before delete: found=%v err=%v", found, err)
	}

	if err := wt.DeleteSpan("t1", "s1"); err != nil {
		t.Fatalf("DeleteSpan() error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() after delete error = %v", err)
	}

	assertNotReadable(t, wt, "t1", "s1")
	assertTraceGone(t, wt, "t1")
	assertNotIndexed(t, wt, "t1", "s1")
	assertConsistent(t, wt)
}

// DeleteTrace must remove every span of the named trace and nothing else: a
// live span in a different trace must survive untouched.
func TestDeleteTraceRemovesOnlyThatTrace(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	doomed := []wal.SpanPayload{
		testSpan("t1", "s1", 100),
		testSpan("t1", "s2", 110),
	}
	keeper := testSpan("t2", "s1", 120)
	for _, s := range append(append([]wal.SpanPayload{}, doomed...), keeper) {
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan(%s/%s) error = %v", s.TraceID, s.SpanID, err)
		}
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if err := wt.DeleteTrace("t1"); err != nil {
		t.Fatalf("DeleteTrace() error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() after delete error = %v", err)
	}

	assertTraceGone(t, wt, "t1")
	for _, s := range doomed {
		assertNotIndexed(t, wt, "t1", s.SpanID)
	}

	got, found, err := wt.GetSpan("t2", "s1")
	if err != nil || !found || got.SpanID != "s1" {
		t.Fatalf("live span in an untouched trace was affected: found=%v err=%v span=%+v", found, err, got)
	}
	assertConsistent(t, wt)
}

// The delete API must reject empty identifiers rather than writing a tombstone
// whose composite key ("||", "t1||") could collide across traces.
func TestDeleteEmptyIdentifiersError(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	if err := wt.DeleteSpan("", "s1"); err == nil {
		t.Fatal("DeleteSpan(empty traceID) = nil, want error")
	}
	if err := wt.DeleteSpan("t1", ""); err == nil {
		t.Fatal("DeleteSpan(empty spanID) = nil, want error")
	}
	if err := wt.DeleteTrace(""); err == nil {
		t.Fatal("DeleteTrace(empty traceID) = nil, want error")
	}
}

// Deleting a span or trace that was never inserted is a no-op, not an error,
// and must not manufacture a readable span.
func TestDeleteNonExistentIsNoop(t *testing.T) {
	wt, err := CreateWispTraceWithConfig(testConfig(t))
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	if err := wt.DeleteTrace("never-existed"); err != nil {
		t.Fatalf("DeleteTrace(nonexistent) = %v, want nil", err)
	}
	if err := wt.DeleteSpan("never-existed", "s1"); err != nil {
		t.Fatalf("DeleteSpan(nonexistent) = %v, want nil", err)
	}
	assertNotReadable(t, wt, "never-existed", "s1")

	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	assertNotReadable(t, wt, "never-existed", "s1")
	assertConsistent(t, wt)
}

// A delete issued through the public API must survive a crash before its flush:
// the tombstone is durable in the WAL, so recovery must not resurrect the span.
func TestDeleteSpanDurableAcrossCrash(t *testing.T) {
	cfg := testConfig(t)
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}

	live := testSpan("t1", "s1", 100)
	if err := wt.InsertSpan(live); err != nil {
		t.Fatalf("InsertSpan() error = %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if err := wt.DeleteSpan("t1", "s1"); err != nil {
		t.Fatalf("DeleteSpan() error = %v", err)
	}
	wt.WaitForIngest()

	// Crash before the tombstone's flush: the delete exists only in the WAL.
	crashClose(t, wt)

	wt2, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("reopen CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt2.Close()

	if span, found, err := wt2.GetSpan("t1", "s1"); err != nil {
		t.Fatalf("GetSpan() error = %v", err)
	} else if found {
		t.Fatalf("deleted span was resurrected after crash recovery: %+v", span)
	}
	assertTraceGone(t, wt2, "t1")
}
