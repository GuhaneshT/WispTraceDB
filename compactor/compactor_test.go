package compactor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/GuhaneshT/WispTraceDB/pebble"
	"github.com/GuhaneshT/WispTraceDB/segment"
	"github.com/GuhaneshT/WispTraceDB/wal"
)

func testSpan(traceID, spanID string, ts int64, deleted bool) wal.SpanPayload {
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
		Deleted:   deleted,
	}
}

// setup writes segments (one per spans slice) via the real segment.Writer,
// indexes every span into a fresh Pebble instance and manifest (mirroring
// what cmd.WispTrace's flushSegmentLocked does), and returns everything a
// Compactor needs plus the ids of the segments it wrote.
func setup(t *testing.T, spansPerSegment [][]wal.SpanPayload) (dir string, idx *pebble.DB, manifest *segment.Manifest, segmentIDs []uint64) {
	t.Helper()
	dir = t.TempDir()

	idx, err := pebble.OpenDB(filepath.Join(dir, "lsm"))
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	t.Cleanup(func() { idx.Close() })

	manifest = segment.NewManifest(filepath.Join(dir, "manifest.dat"))
	segDir := filepath.Join(dir, "segments")
	if err := os.MkdirAll(segDir, 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	for i, spans := range spansPerSegment {
		id := uint64(i + 1)
		w := segment.NewWriter()
		for _, s := range spans {
			w.Add(s)
		}
		result, err := w.Flush(segDir, id)
		if err != nil {
			t.Fatalf("Flush(segment %d) error = %v", id, err)
		}

		entries := make(map[string]pebble.SpanLocation, len(result.Offsets))
		for key, offset := range result.Offsets {
			entries[key] = pebble.SpanLocation{SegmentID: id, Offset: offset}
		}
		if err := idx.BatchPutSpans(entries); err != nil {
			t.Fatalf("BatchPutSpans(segment %d) error = %v", id, err)
		}
		if err := manifest.AddSegment(id); err != nil {
			t.Fatalf("AddSegment(%d) error = %v", id, err)
		}
		segmentIDs = append(segmentIDs, id)
	}

	return filepath.Join(dir, "segments"), idx, manifest, segmentIDs
}

func TestCompactMergesSegmentsAndDropsTombstones(t *testing.T) {
	segDir, idx, manifest, ids := setup(t, [][]wal.SpanPayload{
		{testSpan("t1", "s1", 100, false), testSpan("t1", "s2", 110, true)},
		{testSpan("t2", "s1", 200, false)},
	})

	c := New(segDir, manifest, idx)
	result, err := c.Compact(ids, 99) // 99: arbitrary id distinct from inputs
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if result.SpansCarried != 2 {
		t.Fatalf("SpansCarried = %d, want 2", result.SpansCarried)
	}
	if result.SpansDropped != 1 {
		t.Fatalf("SpansDropped = %d, want 1 (the tombstone)", result.SpansDropped)
	}
	if result.NewSegmentID != 99 {
		t.Fatalf("NewSegmentID = %d, want 99", result.NewSegmentID)
	}

	// Old segments must be gone from disk.
	for _, id := range ids {
		if _, err := os.Stat(segment.SegmentPath(segDir, id)); !os.IsNotExist(err) {
			t.Fatalf("segment %d should have been unlinked, stat err = %v", id, err)
		}
	}

	// Manifest reflects only the new segment.
	live, err := manifest.Load()
	if err != nil {
		t.Fatalf("manifest.Load() error = %v", err)
	}
	if len(live) != 1 || live[0] != 99 {
		t.Fatalf("manifest = %v, want [99]", live)
	}

	// Survivors are queryable at the new location; the tombstone is gone
	// entirely (not just moved).
	loc, err := idx.GetSpan([]byte(segment.CompositeKey("t1", "s1")))
	if err != nil {
		t.Fatalf("GetSpan(t1/s1) error = %v", err)
	}
	if loc.SegmentID != 99 {
		t.Fatalf("GetSpan(t1/s1).SegmentID = %d, want 99", loc.SegmentID)
	}
	if _, err := idx.GetSpan([]byte(segment.CompositeKey("t1", "s2"))); err == nil {
		t.Fatal("GetSpan(t1/s2) should not find the tombstoned span")
	}
	loc2, err := idx.GetSpan([]byte(segment.CompositeKey("t2", "s1")))
	if err != nil {
		t.Fatalf("GetSpan(t2/s1) error = %v", err)
	}
	if loc2.SegmentID != 99 {
		t.Fatalf("GetSpan(t2/s1).SegmentID = %d, want 99", loc2.SegmentID)
	}

	// The merged data must actually be readable from the new segment file.
	reader, err := segment.OpenReader(segment.SegmentPath(segDir, 99))
	if err != nil {
		t.Fatalf("OpenReader(99) error = %v", err)
	}
	defer reader.Close()
	got, err := reader.ReadAt(loc.Offset)
	if err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	if got.SpanID != "s1" {
		t.Fatalf("ReadAt() = %+v, want span s1", got)
	}
}

func TestCompactAllTombstonesWritesNoNewSegment(t *testing.T) {
	segDir, idx, manifest, ids := setup(t, [][]wal.SpanPayload{
		{testSpan("t1", "s1", 100, true), testSpan("t1", "s2", 110, true)},
	})

	c := New(segDir, manifest, idx)
	result, err := c.Compact(ids, 42)
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if result.NewSegmentID != 0 {
		t.Fatalf("NewSegmentID = %d, want 0 (no survivors)", result.NewSegmentID)
	}
	if result.SpansCarried != 0 {
		t.Fatalf("SpansCarried = %d, want 0", result.SpansCarried)
	}
	if result.SpansDropped != 2 {
		t.Fatalf("SpansDropped = %d, want 2", result.SpansDropped)
	}

	if _, err := os.Stat(segment.SegmentPath(segDir, 42)); !os.IsNotExist(err) {
		t.Fatalf("segment 42 should never have been created, stat err = %v", err)
	}

	live, err := manifest.Load()
	if err != nil {
		t.Fatalf("manifest.Load() error = %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("manifest = %v, want empty", live)
	}
}

// TestCompactSkipsStaleSpansSupersededElsewhere is the core regression test
// for the staleness check (see package doc). Segment 1 holds the original
// insert for key (t1, s1); segment 2, flushed later, holds a tombstone for
// the SAME key.
//
// Note that setup indexes the tombstone via BatchPutSpans, which is no longer
// what the flush path does — writeSegmentBatch deletes a tombstoned key from
// the index instead of repointing it at the tombstone record. So the layout
// built here is the PRE-FIX one: a segment on disk whose tombstone is still
// indexed. It is worth keeping, because that is exactly what an existing
// database looks like after upgrading, and the second shape of supersession
// (key absent from the index entirely) must not resurrect a span either.
//
// Compacting segment 1 ALONE (never touching segment 2) must not resurrect
// the deleted span: it must recognize its copy of (t1, s1) is stale, skip
// it entirely, and leave Pebble's real (segment 2) entry untouched.
func TestCompactSkipsStaleSpansSupersededElsewhere(t *testing.T) {
	segDir, idx, manifest, ids := setup(t, [][]wal.SpanPayload{
		{testSpan("t1", "s1", 100, false)}, // segment 1: original insert
		{testSpan("t1", "s1", 200, true)},  // segment 2: later tombstone, same key
	})
	segment1ID, segment2ID := ids[0], ids[1]

	// Sanity check on the test's own setup: Pebble must currently point at
	// segment 2 (the tombstone), not segment 1, before compaction runs.
	before, err := idx.GetSpan([]byte(segment.CompositeKey("t1", "s1")))
	if err != nil {
		t.Fatalf("GetSpan() before compaction error = %v", err)
	}
	if before.SegmentID != segment2ID {
		t.Fatalf("test setup invariant broken: index points at segment %d, want %d", before.SegmentID, segment2ID)
	}

	c := New(segDir, manifest, idx)
	result, err := c.Compact([]uint64{segment1ID}, 99)
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if result.SpansStale != 1 {
		t.Fatalf("SpansStale = %d, want 1 (segment 1's copy is superseded)", result.SpansStale)
	}
	if result.SpansCarried != 0 {
		t.Fatalf("SpansCarried = %d, want 0 — a stale span must never be carried forward", result.SpansCarried)
	}
	if result.SpansDropped != 0 {
		t.Fatalf("SpansDropped = %d, want 0 — segment 1's copy isn't the authoritative tombstone, it must not be counted as reclaimed", result.SpansDropped)
	}
	if result.NewSegmentID != 0 {
		t.Fatalf("NewSegmentID = %d, want 0 (nothing survived to carry forward)", result.NewSegmentID)
	}

	// The critical assertion: Pebble's real entry (pointing at segment 2's
	// tombstone) must be untouched by compacting segment 1.
	after, err := idx.GetSpan([]byte(segment.CompositeKey("t1", "s1")))
	if err != nil {
		t.Fatalf("GetSpan() after compaction error = %v — the live tombstone entry must not have been erased", err)
	}
	if after.SegmentID != segment2ID || after.Offset != before.Offset {
		t.Fatalf("GetSpan() after compaction = %+v, want unchanged %+v", after, before)
	}

	// Segment 1 is still safe to unlink: nothing in Pebble points into it.
	if _, err := os.Stat(segment.SegmentPath(segDir, segment1ID)); !os.IsNotExist(err) {
		t.Fatalf("segment 1 should have been unlinked, stat err = %v", err)
	}
	// Segment 2 (holding the actually-referenced tombstone) must survive —
	// it was never part of this compaction.
	if _, err := os.Stat(segment.SegmentPath(segDir, segment2ID)); err != nil {
		t.Fatalf("segment 2 should still exist, stat err = %v", err)
	}
}

// TestCompactDropsLiveRecordWhoseKeyWasTombstoned covers the other shape of
// supersession, and the one production now actually produces: a tombstoned key
// is DELETED from the index rather than repointed at the tombstone record. The
// original live record in an older segment therefore finds no index entry at
// all, which must read as "superseded, drop it" and never as "unindexed, keep
// it" — otherwise compacting the old segment resurrects a deleted span and
// writes it straight back into the index.
func TestCompactDropsLiveRecordWhoseKeyWasTombstoned(t *testing.T) {
	segDir, idx, manifest, ids := setup(t, [][]wal.SpanPayload{
		{testSpan("t1", "s1", 100, false)}, // segment 1: original live insert
		{testSpan("t1", "s1", 200, true)},  // segment 2: tombstone, same key
	})
	segment1ID := ids[0]
	key := segment.CompositeKey("t1", "s1")

	// Model the production flush path: a tombstoned key is removed from the
	// index, so segment 1's live record is now unreachable.
	if err := idx.DeleteSpan([]byte(key)); err != nil {
		t.Fatalf("DeleteSpan() error = %v", err)
	}
	if _, err := idx.GetSpan([]byte(key)); err == nil {
		t.Fatal("tombstone should have removed the index entry")
	}

	c := New(segDir, manifest, idx)
	result, err := c.Compact([]uint64{segment1ID}, 99)
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if result.SpansCarried != 0 {
		t.Fatalf("SpansCarried = %d, want 0 — a span with no index entry is superseded, not carryable", result.SpansCarried)
	}
	if result.SpansStale != 1 {
		t.Fatalf("SpansStale = %d, want 1", result.SpansStale)
	}
	if result.NewSegmentID != 0 {
		t.Fatalf("NewSegmentID = %d, want 0 (nothing survived)", result.NewSegmentID)
	}
	if _, err := os.Stat(segment.SegmentPath(segDir, 99)); !os.IsNotExist(err) {
		t.Fatalf("no merged segment should have been written, stat err = %v", err)
	}

	// The deleted span must stay deleted, and must not have been re-indexed
	// at its old location.
	if _, err := idx.GetSpan([]byte(key)); err == nil {
		t.Fatal("compaction resurrected the tombstoned span into the index")
	}
}

// TestCompactStaleTombstoneDoesNotEraseReinsertedSpan pins the ordering inside
// the scan loop: the staleness check must run BEFORE the tombstone test.
//
// If a tombstone is handled first, its key goes into reclaimKeys and
// BatchDeleteSpans then erases whatever the index currently holds for that key —
// which, after a re-insert of the same span id, is the live record. Compacting
// an old tombstone segment would silently make a live span unreadable. Only the
// tombstone the index actually points at may be reclaimed.
//
// Uses the pre-fix layout (tombstone indexed) deliberately, since that is the
// only case in which a tombstone is ever a reclaim candidate.
func TestCompactStaleTombstoneDoesNotEraseReinsertedSpan(t *testing.T) {
	segDir, idx, manifest, ids := setup(t, [][]wal.SpanPayload{
		{testSpan("t1", "s1", 100, true)},  // segment 1: tombstone
		{testSpan("t1", "s1", 300, false)}, // segment 2: re-inserted, live
	})
	segment1ID, segment2ID := ids[0], ids[1]
	key := segment.CompositeKey("t1", "s1")

	before, err := idx.GetSpan([]byte(key))
	if err != nil {
		t.Fatalf("GetSpan() before compaction error = %v", err)
	}
	if before.SegmentID != segment2ID {
		t.Fatalf("test setup invariant broken: index points at segment %d, want %d", before.SegmentID, segment2ID)
	}

	c := New(segDir, manifest, idx)
	result, err := c.Compact([]uint64{segment1ID}, 99)
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if result.SpansStale != 1 {
		t.Fatalf("SpansStale = %d, want 1 (the tombstone is superseded by the re-insert)", result.SpansStale)
	}
	if result.SpansDropped != 0 {
		t.Fatalf("SpansDropped = %d, want 0 — a superseded tombstone must not be reclaimed", result.SpansDropped)
	}

	// The re-inserted live span must remain indexed and unchanged.
	after, err := idx.GetSpan([]byte(key))
	if err != nil {
		t.Fatalf("GetSpan() after compaction error = %v — the re-inserted span's index entry was erased", err)
	}
	if after.SegmentID != segment2ID || after.Offset != before.Offset {
		t.Fatalf("GetSpan() after compaction = %+v, want unchanged %+v", after, before)
	}
}

// TestExpireSkipsStaleSpansSupersededElsewhere mirrors the Compact staleness
// test, but for retention expiry: an old, expiring segment holds a span's
// original insert; a newer, non-expiring segment holds a later tombstone for
// the same key. Expiring the old segment must not erase the tombstone's
// (correct, current) Pebble entry.
func TestExpireSkipsStaleSpansSupersededElsewhere(t *testing.T) {
	segDir, idx, manifest, ids := setup(t, [][]wal.SpanPayload{
		{testSpan("t1", "s1", 100, false)}, // segment 1: old insert, will expire
		{testSpan("t1", "s1", 900, true)},  // segment 2: recent tombstone, same key
	})
	segment1ID, segment2ID := ids[0], ids[1]

	before, err := idx.GetSpan([]byte(segment.CompositeKey("t1", "s1")))
	if err != nil {
		t.Fatalf("GetSpan() before expire error = %v", err)
	}
	if before.SegmentID != segment2ID {
		t.Fatalf("test setup invariant broken: index points at segment %d, want %d", before.SegmentID, segment2ID)
	}

	c := New(segDir, manifest, idx)
	// Cutoff only old enough to catch segment 1 (MaxTimestamp 100); segment 2
	// (MaxTimestamp 900) must be left alone regardless.
	result, err := c.Expire([]uint64{segment1ID, segment2ID}, 500)
	if err != nil {
		t.Fatalf("Expire() error = %v", err)
	}

	if len(result.DroppedSegmentIDs) != 1 || result.DroppedSegmentIDs[0] != segment1ID {
		t.Fatalf("DroppedSegmentIDs = %v, want [%d]", result.DroppedSegmentIDs, segment1ID)
	}
	if result.SpansStale != 1 {
		t.Fatalf("SpansStale = %d, want 1", result.SpansStale)
	}
	if result.SpansDropped != 0 {
		t.Fatalf("SpansDropped = %d, want 0 — segment 1's copy is stale, not the authoritative record", result.SpansDropped)
	}

	after, err := idx.GetSpan([]byte(segment.CompositeKey("t1", "s1")))
	if err != nil {
		t.Fatalf("GetSpan() after expire error = %v — the live tombstone entry must not have been erased", err)
	}
	if after.SegmentID != segment2ID {
		t.Fatalf("GetSpan() after expire = %+v, want still pointing at segment %d", after, segment2ID)
	}

	if _, err := os.Stat(segment.SegmentPath(segDir, segment1ID)); !os.IsNotExist(err) {
		t.Fatalf("segment 1 should have been unlinked, stat err = %v", err)
	}
	if _, err := os.Stat(segment.SegmentPath(segDir, segment2ID)); err != nil {
		t.Fatalf("segment 2 should still exist, stat err = %v", err)
	}
}

func TestCompactNoIDsErrors(t *testing.T) {
	segDir, idx, manifest, _ := setup(t, nil)
	c := New(segDir, manifest, idx)
	if _, err := c.Compact(nil, 1); err == nil {
		t.Fatal("Compact(nil ids) should error")
	}
}

func TestExpireDropsSegmentsPastCutoff(t *testing.T) {
	segDir, idx, manifest, ids := setup(t, [][]wal.SpanPayload{
		{testSpan("t1", "s1", 100, false)}, // segment 1: old, should expire
		{testSpan("t2", "s1", 900, false)}, // segment 2: recent, should survive
	})

	c := New(segDir, manifest, idx)
	result, err := c.Expire(ids, 500) // cutoff between the two segments' timestamps
	if err != nil {
		t.Fatalf("Expire() error = %v", err)
	}

	if len(result.DroppedSegmentIDs) != 1 || result.DroppedSegmentIDs[0] != 1 {
		t.Fatalf("DroppedSegmentIDs = %v, want [1]", result.DroppedSegmentIDs)
	}
	if result.SpansDropped != 1 {
		t.Fatalf("SpansDropped = %d, want 1", result.SpansDropped)
	}

	if _, err := os.Stat(segment.SegmentPath(segDir, 1)); !os.IsNotExist(err) {
		t.Fatalf("segment 1 should have been unlinked, stat err = %v", err)
	}
	if _, err := os.Stat(segment.SegmentPath(segDir, 2)); err != nil {
		t.Fatalf("segment 2 should still exist, stat err = %v", err)
	}

	if _, err := idx.GetSpan([]byte(segment.CompositeKey("t1", "s1"))); err == nil {
		t.Fatal("GetSpan(t1/s1) should not find the expired span")
	}
	if _, err := idx.GetSpan([]byte(segment.CompositeKey("t2", "s1"))); err != nil {
		t.Fatalf("GetSpan(t2/s1) error = %v, want the surviving span to still be indexed", err)
	}

	live, err := manifest.Load()
	if err != nil {
		t.Fatalf("manifest.Load() error = %v", err)
	}
	if len(live) != 1 || live[0] != 2 {
		t.Fatalf("manifest = %v, want [2]", live)
	}
}

func TestExpireNothingPastCutoffIsNoop(t *testing.T) {
	segDir, idx, manifest, ids := setup(t, [][]wal.SpanPayload{
		{testSpan("t1", "s1", 900, false)},
	})

	c := New(segDir, manifest, idx)
	result, err := c.Expire(ids, 100) // cutoff before the segment's data
	if err != nil {
		t.Fatalf("Expire() error = %v", err)
	}
	if len(result.DroppedSegmentIDs) != 0 {
		t.Fatalf("DroppedSegmentIDs = %v, want none", result.DroppedSegmentIDs)
	}

	if _, err := os.Stat(segment.SegmentPath(segDir, 1)); err != nil {
		t.Fatalf("segment 1 should still exist, stat err = %v", err)
	}
}
