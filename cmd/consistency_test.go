package cmd

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/GuhaneshT/WispTraceDB/pebble"
	"github.com/GuhaneshT/WispTraceDB/segment"
	"github.com/GuhaneshT/WispTraceDB/wal"
)

func insertAndFlush(t *testing.T, wt *WispTrace, spans ...wal.SpanPayload) {
	t.Helper()
	for _, s := range spans {
		if err := wt.InsertSpan(s); err != nil {
			t.Fatalf("InsertSpan() error = %v", err)
		}
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
}

func TestCheckConsistencyCleanStore(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 2
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	insertAndFlush(t, wt, testSpan("t1", "s1", 100), testSpan("t1", "s2", 101))

	rep := wt.CheckConsistency()
	if !rep.OK() {
		t.Fatalf("CheckConsistency() errors = %v", rep.Errors)
	}
	if len(rep.Warnings) != 0 {
		t.Fatalf("CheckConsistency() warnings = %v, want none on a clean store", rep.Warnings)
	}
}

func TestCheckConsistencyDetectsMissingIndexEntry(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 2
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	insertAndFlush(t, wt, testSpan("t1", "s1", 100), testSpan("t1", "s2", 101))

	// Simulate a lost index write: delete s1's entry while its record is
	// still live in the segment.
	if err := wt.index.DeleteSpan([]byte(segment.CompositeKey("t1", "s1"))); err != nil {
		t.Fatalf("DeleteSpan() error = %v", err)
	}

	rep := wt.CheckConsistency()
	if rep.OK() {
		t.Fatal("CheckConsistency() = OK, want an error for the live span whose index entry was deleted")
	}
	found := false
	for _, e := range rep.Errors {
		found = found || contains([]string{e}, "s1", "no index entry")
	}
	if !found {
		t.Fatalf("CheckConsistency() errors = %v, want one for t1||s1 missing index entry", rep.Errors)
	}
}

func TestCheckConsistencyDetectsIndexPointingAtMissingSegment(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 2
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	insertAndFlush(t, wt, testSpan("t1", "s1", 100), testSpan("t1", "s2", 101))

	// A dangling index entry referencing a segment that does not exist.
	phantom := map[string]pebble.SpanLocation{
		segment.CompositeKey("phantom-trace", "phantom-span"): {SegmentID: 9999, Offset: 0},
	}
	if err := wt.index.BatchPutSpans(phantom); err != nil {
		t.Fatalf("BatchPutSpans() error = %v", err)
	}

	rep := wt.CheckConsistency()
	if rep.OK() {
		t.Fatal("CheckConsistency() = OK, want an error for an index entry pointing at a missing segment")
	}
	found := false
	for _, e := range rep.Errors {
		found = found || contains([]string{e}, "9999")
	}
	if !found {
		t.Fatalf("CheckConsistency() errors = %v, want one referencing segment 9999", rep.Errors)
	}
}

func TestCheckConsistencyDetectsMissingSegmentFile(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 2
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	insertAndFlush(t, wt, testSpan("t1", "s1", 100), testSpan("t1", "s2", 101))

	// Lose a whole segment file while the manifest and index still reference it.
	if err := os.Remove(segment.SegmentPath(cfg.SegmentDir, 1)); err != nil {
		t.Fatalf("remove segment 1: %v", err)
	}

	rep := wt.CheckConsistency()
	if rep.OK() {
		t.Fatal("CheckConsistency() = OK, want errors for the manifest-listed-but-missing segment")
	}
	found := false
	for _, e := range rep.Errors {
		found = found || contains([]string{e}, "segment 1")
	}
	if !found {
		t.Fatalf("CheckConsistency() errors = %v, want one referencing segment 1", rep.Errors)
	}
}

// TestCheckConsistencyDetectsCorruptSegmentRecord exercises the checker's
// record-CRC pass. Note that recovery self-heals corruption it can still
// rebuild: spanAlreadyIndexed fails on the corrupted record, so the span is
// re-flushed from the WAL tail and the corrupt segment is retired as an
// orphan. To make the corruption observable the corrupted segment's WAL
// records must be older than the reclaimed tail, so its spans cannot be
// re-flushed and the CRC failure must surface as a consistency error.
func TestCheckConsistencyDetectsCorruptSegmentRecord(t *testing.T) {
	cfg := testConfig(t)
	// Defaults: 3 spans per flush, compaction threshold 10. Nine spans produce
	// three segments and no compaction. The flush after the next reclaims each
	// flush's WAL segment, so by Close() only the last flush's records remain
	// in the tail — segment 1's WAL records are already reclaimed.
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	spans := make([]wal.SpanPayload, 0, 9)
	for i := 0; i < 9; i++ {
		spans = append(spans, testSpan(fmt.Sprintf("t%d", i), fmt.Sprintf("s%d", i), int64(100+i)))
	}
	insertAndFlush(t, wt, spans...)
	if err := wt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Flip a byte deep in the last record of segment 1 (the oldest, holding
	// t0..t2) so the header and bloom section still parse but the record's
	// CRC check fails during ScanAll/ReadAt.
	path := segment.SegmentPath(cfg.SegmentDir, 1)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if len(data) < 64 {
		t.Fatalf("segment file implausibly small: %d bytes", len(data))
	}
	data[len(data)-2] ^= 0xff
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("corrupt segment: %v", err)
	}

	wt2, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer wt2.Close()

	rep := wt2.CheckConsistency()
	if rep.OK() {
		t.Fatal("CheckConsistency() = OK, want an error for a segment whose record CRC fails")
	}
	found := false
	for _, e := range rep.Errors {
		found = found || contains([]string{e}, "segment 1")
	}
	if !found {
		t.Fatalf("CheckConsistency() errors = %v, want one referencing segment 1", rep.Errors)
	}
}

func TestCheckConsistencyReportsUnreferencedSegmentAsWarning(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 2
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	insertAndFlush(t, wt, testSpan("t1", "s1", 100), testSpan("t1", "s2", 101))

	// A valid segment file that no manifest or index entry references: benign
	// crash residue (the id allocator already skips it).
	w := segment.NewWriter()
	w.Add(testSpan("t-orphan", "s1", 200))
	if _, err := w.Flush(cfg.SegmentDir, 99); err != nil {
		t.Fatalf("write orphan segment: %v", err)
	}

	rep := wt.CheckConsistency()
	if !rep.OK() {
		t.Fatalf("CheckConsistency() errors = %v, want warnings only", rep.Errors)
	}
	found := false
	for _, wr := range rep.Warnings {
		found = found || contains([]string{wr}, "99", "not", "manifest")
	}
	if !found {
		t.Fatalf("CheckConsistency() warnings = %v, want one about orphan segment 99", rep.Warnings)
	}
}

func TestCheckConsistencyDetectsCheckpointBeyondHighestSegment(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 2
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}
	defer wt.Close()

	insertAndFlush(t, wt, testSpan("t1", "s1", 100), testSpan("t1", "s2", 101))

	if err := wt.checkpoint.Save(999); err != nil {
		t.Fatalf("checkpoint.Save() error = %v", err)
	}

	rep := wt.CheckConsistency()
	if rep.OK() {
		t.Fatal("CheckConsistency() = OK, want an error for a checkpoint beyond the highest segment id")
	}
	found := false
	for _, e := range rep.Errors {
		found = found || contains([]string{e}, "999")
	}
	if !found {
		t.Fatalf("CheckConsistency() errors = %v, want one referencing checkpoint 999", rep.Errors)
	}
}

// TestRecoverySkipsAlreadyIndexedSpans reproduces the crash window inside
// writeSegmentBatch where spans are fully durable+indexed (segment + index
// batch committed, checkpoint not yet saved) yet still present in the WAL.
// Recovery must not re-flush them, or RangeQuery would return duplicates.
func TestRecoverySkipsAlreadyIndexedSpans(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentFlushThreshold = 3
	wt, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("CreateWispTraceWithConfig() error = %v", err)
	}

	s1 := testSpan("t-dup", "s1", 100)
	s2 := testSpan("t-dup", "s2", 101)
	insertAndFlush(t, wt, s1, s2)

	live, err := wt.LiveSegments()
	if err != nil {
		t.Fatalf("LiveSegments() error = %v", err)
	}
	if fmt.Sprint(live) != "[1]" {
		t.Fatalf("setup: LiveSegments() = %v, want [1]", live)
	}
	if err := wt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Simulate the crash residue: the WAL active segment (empty after the
	// clean flush reclaimed everything below the checkpoint) is appended
	// with the same, already-confirmed spans.
	w, err := wal.CreateWALWithSegmentSize(cfg.WALPath, wal.DefaultMaxSegmentSize)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	if err := w.AppendRecord(wal.WALRecord{Span: s1}); err != nil {
		t.Fatalf("append s1: %v", err)
	}
	if err := w.AppendRecord(wal.WALRecord{Span: s2}); err != nil {
		t.Fatalf("append s2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	wt2, err := CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer wt2.Close()

	// Without the skip, the replayed spans would be re-flushed into a new
	// segment and both copies would surface in RangeQuery.
	live, err = wt2.LiveSegments()
	if err != nil {
		t.Fatalf("LiveSegments() error = %v", err)
	}
	if fmt.Sprint(live) != "[1]" {
		t.Fatalf("after recovery LiveSegments() = %v, want [1] (spans must not be re-flushed)", live)
	}

	all, err := wt2.RangeQuery(RangeFilter{StartTS: 0, EndTS: 1<<63 - 1})
	if err != nil {
		t.Fatalf("RangeQuery() error = %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("RangeQuery() returned %d spans, want exactly 2 (one copy each)", len(all))
	}
	for _, s := range all {
		if s.SpanID != "s1" && s.SpanID != "s2" {
			t.Fatalf("RangeQuery() returned unexpected span %q", s.SpanID)
		}
	}

	got, found, err := wt2.GetSpan("t-dup", "s1")
	if err != nil {
		t.Fatalf("GetSpan() error = %v", err)
	}
	if !found {
		t.Fatal("GetSpan(t-dup, s1) found = false")
	}
	if got.Timestamp != 100 {
		t.Fatalf("GetSpan(t-dup, s1).Timestamp = %d, want 100 (identical data preserved)", got.Timestamp)
	}

	rep := wt2.CheckConsistency()
	if !rep.OK() {
		t.Fatalf("CheckConsistency() errors = %v", rep.Errors)
	}
}

func contains(haystack []string, needles ...string) bool {
	for _, h := range haystack {
		all := true
		for _, n := range needles {
			if !strings.Contains(h, n) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}