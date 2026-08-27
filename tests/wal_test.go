package tests

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/GuhaneshT/WispTraceDB/wal"
)


func recordsEqual(a, b wal.WALRecord) bool {
	return a.Span.TraceID == b.Span.TraceID &&
		a.Span.SpanID == b.Span.SpanID &&
		a.Span.ParentSpanID == b.Span.ParentSpanID &&
		a.Span.Timestamp == b.Span.Timestamp &&
		a.Span.AgentID == b.Span.AgentID &&
		a.Span.Model == b.Span.Model &&
		a.Span.ToolName == b.Span.ToolName &&
		a.Span.Team == b.Span.Team &&
		a.Span.Status == b.Span.Status &&
		a.Span.TokensIn == b.Span.TokensIn &&
		a.Span.TokensOut == b.Span.TokensOut &&
		a.Span.Cost == b.Span.Cost &&
		a.Span.LatencyMs == b.Span.LatencyMs &&
		a.Span.Deleted == b.Span.Deleted &&
		bytes.Equal(a.Span.Payload, b.Span.Payload)
}

func assertReplay(t *testing.T, w *wal.WAL, want []wal.WALRecord) {
	t.Helper()
	got, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("Replay() len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !recordsEqual(got[i], want[i]) {
			t.Fatalf("Replay()[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func makeRecords(n int) []wal.WALRecord {
	records := make([]wal.WALRecord, 0, n)
	for i := 0; i < n; i++ {
		records = append(records, wal.WALRecord{
			Span: wal.SpanPayload{
				TraceID:   fmt.Sprintf("trace-%d", i),
				SpanID:    fmt.Sprintf("span-%d", i),
				Timestamp: int64(i * 10),
				AgentID:   "agent-1",
				Model:     "claude-sonnet-5",
				ToolName:  "bash_tool",
				Team:      "platform",
				Status:    "ok",
				TokensIn:  int32(i),
				TokensOut: int32(i * 2),
				Cost:      float64(i) * 0.001,
				LatencyMs: int64(i * 5),
				Payload:   []byte("vvvvvvvv"),
			},
		})
	}
	return records
}

func TestWALRotatesOnSegmentSizeAndReplaysInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	// ADAPTED: exact per-record byte accounting from the old comment no
	// longer holds (payload layout changed); pick a cap small enough
	// relative to record size to force at least one rotation.
	w, err := wal.CreateWALWithSegmentSize(path, 150)
	if err != nil {
		t.Fatalf("CreateWALWithSegmentSize() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	want := makeRecords(5)
	for _, record := range want {
		if err := w.AppendRecord(record); err != nil {
			t.Fatalf("AppendRecord() error = %v", err)
		}
	}

	segments, err := w.Segments()
	if err != nil {
		t.Fatalf("Segments() error = %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("Segments() = %v, want the log to have rotated at least once", segments)
	}
	if w.CurrentSegment() != segments[len(segments)-1] {
		t.Fatalf("CurrentSegment() = %d, want the highest segment %d", w.CurrentSegment(), segments[len(segments)-1])
	}

	// Rotation must never split or reorder records.
	assertReplay(t, w, want)
}

func TestWALOversizedRecordGetsItsOwnSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := wal.CreateWALWithSegmentSize(path, 32)
	if err != nil {
		t.Fatalf("CreateWALWithSegmentSize() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	want := []wal.WALRecord{
		{Span: wal.SpanPayload{TraceID: "t1", SpanID: "s1", Timestamp: 1, Payload: bytes.Repeat([]byte("x"), 512)}},
		{Span: wal.SpanPayload{TraceID: "t2", SpanID: "s2", Timestamp: 2, Payload: []byte("small")}},
	}
	for _, record := range want {
		if err := w.AppendRecord(record); err != nil {
			t.Fatalf("AppendRecord() error = %v", err)
		}
	}

	assertReplay(t, w, want)
}

func TestWALRotateSealsCurrentSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := wal.CreateWAL(path)
	if err != nil {
		t.Fatalf("CreateWAL() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	first := wal.WALRecord{Span: wal.SpanPayload{TraceID: "t1", SpanID: "s1", Timestamp: 10, Status: "alpha"}}
	if err := w.AppendRecord(first); err != nil {
		t.Fatalf("AppendRecord() error = %v", err)
	}

	sealed, err := w.Rotate()
	if err != nil {
		t.Fatalf("Rotate() error = %v", err)
	}
	if sealed != 1 {
		t.Fatalf("Rotate() sealed = %d, want 1", sealed)
	}
	if w.CurrentSegment() != 2 {
		t.Fatalf("CurrentSegment() = %d, want 2", w.CurrentSegment())
	}

	second := wal.WALRecord{Span: wal.SpanPayload{TraceID: "t2", SpanID: "s2", Timestamp: 20, Status: "beta"}}
	if err := w.AppendRecord(second); err != nil {
		t.Fatalf("AppendRecord() error = %v", err)
	}

	assertReplay(t, w, []wal.WALRecord{first, second})
}

func TestWALRemoveSegmentsUpToKeepsActiveAndLaterSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := wal.CreateWAL(path)
	if err != nil {
		t.Fatalf("CreateWAL() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if err := w.AppendRecord(wal.WALRecord{Span: wal.SpanPayload{TraceID: "t1", Timestamp: 1, Status: "one"}}); err != nil {
		t.Fatalf("AppendRecord() error = %v", err)
	}
	if _, err := w.Rotate(); err != nil {
		t.Fatalf("Rotate() error = %v", err)
	}
	if err := w.AppendRecord(wal.WALRecord{Span: wal.SpanPayload{TraceID: "t2", Timestamp: 2, Status: "two"}}); err != nil {
		t.Fatalf("AppendRecord() error = %v", err)
	}
	sealed, err := w.Rotate()
	if err != nil {
		t.Fatalf("Rotate() error = %v", err)
	}
	survivor := wal.WALRecord{Span: wal.SpanPayload{TraceID: "t3", Timestamp: 3, Status: "three"}}
	if err := w.AppendRecord(survivor); err != nil {
		t.Fatalf("AppendRecord() error = %v", err)
	}

	if err := w.RemoveSegmentsUpTo(sealed); err != nil {
		t.Fatalf("RemoveSegmentsUpTo() error = %v", err)
	}

	segments, err := w.Segments()
	if err != nil {
		t.Fatalf("Segments() error = %v", err)
	}
	if len(segments) != 1 || segments[0] != w.CurrentSegment() {
		t.Fatalf("Segments() = %v, want only the active segment %d", segments, w.CurrentSegment())
	}

	assertReplay(t, w, []wal.WALRecord{survivor})
}

func TestWALRemoveSegmentsUpToNeverDropsActiveSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := wal.CreateWAL(path)
	if err != nil {
		t.Fatalf("CreateWAL() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	unflushed := wal.WALRecord{Span: wal.SpanPayload{TraceID: "t9", Timestamp: 90, Status: "still needed"}}
	if err := w.AppendRecord(unflushed); err != nil {
		t.Fatalf("AppendRecord() error = %v", err)
	}

	// An over-eager caller must not be able to discard records that have not
	// reached an SSTable yet.
	if err := w.RemoveSegmentsUpTo(w.CurrentSegment() + 100); err != nil {
		t.Fatalf("RemoveSegmentsUpTo() error = %v", err)
	}

	assertReplay(t, w, []wal.WALRecord{unflushed})
}

func TestWALReopenAppendsToLatestSegment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := wal.CreateWALWithSegmentSize(path, 150)
	if err != nil {
		t.Fatalf("CreateWALWithSegmentSize() error = %v", err)
	}
	want := makeRecords(5)
	for _, record := range want {
		if err := w.AppendRecord(record); err != nil {
			t.Fatalf("AppendRecord() error = %v", err)
		}
	}
	segmentsBefore, err := w.Segments()
	if err != nil {
		t.Fatalf("Segments() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := wal.CreateWALWithSegmentSize(path, 150)
	if err != nil {
		t.Fatalf("CreateWALWithSegmentSize() reopen error = %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	if reopened.CurrentSegment() != segmentsBefore[len(segmentsBefore)-1] {
		t.Fatalf("CurrentSegment() after reopen = %d, want %d", reopened.CurrentSegment(), segmentsBefore[len(segmentsBefore)-1])
	}

	extra := wal.WALRecord{Span: wal.SpanPayload{TraceID: "t99", Timestamp: 990, Status: "after reopen"}}
	if err := reopened.AppendRecord(extra); err != nil {
		t.Fatalf("AppendRecord() after reopen error = %v", err)
	}

	assertReplay(t, reopened, append(append([]wal.WALRecord{}, want...), extra))
}

// segmentFilePath reconstructs a segment's on-disk path from the WAL's
// documented naming convention (base "wal.log" -> "wal_000001.log").
// ADAPTED: replaces the old direct call to the unexported w.segmentPath(),
// which isn't reachable from an external package.
func segmentFilePath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("wal_%06d.log", id))
}

func TestWALReplayToleratesTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := wal.CreateWAL(path)
	if err != nil {
		t.Fatalf("CreateWAL() error = %v", err)
	}
	want := makeRecords(3)
	for _, record := range want {
		if err := w.AppendRecord(record); err != nil {
			t.Fatalf("AppendRecord() error = %v", err)
		}
	}
	segmentPath := segmentFilePath(dir, w.CurrentSegment())
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Simulate a crash partway through appending the third record.
	info, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if err := os.Truncate(segmentPath, info.Size()-5); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}

	reopened, err := wal.CreateWAL(path)
	if err != nil {
		t.Fatalf("CreateWAL() reopen error = %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	assertReplay(t, reopened, want[:2])
}

func TestWALReplayRejectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	// ADAPTED: recordHeaderSize is unexported; the framing width (version +
	// crc32 + length, 9 bytes) is documented package behavior, mirrored here
	// as a local constant purely to locate a byte inside the first record's
	// payload region to corrupt.
	const recordHeaderSize = 9

	w, err := wal.CreateWAL(path)
	if err != nil {
		t.Fatalf("CreateWAL() error = %v", err)
	}
	for _, record := range makeRecords(3) {
		if err := w.AppendRecord(record); err != nil {
			t.Fatalf("AppendRecord() error = %v", err)
		}
	}
	segmentPath := segmentFilePath(dir, w.CurrentSegment())
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Flip a byte inside the first record's payload; a torn tail is tolerated,
	// silent corruption must not be.
	data, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	data[recordHeaderSize] ^= 0xFF
	if err := os.WriteFile(segmentPath, data, 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	reopened, err := wal.CreateWAL(path)
	if err != nil {
		t.Fatalf("CreateWAL() reopen error = %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	if _, err := reopened.Replay(); err == nil {
		t.Fatal("Replay() error = nil, want a checksum mismatch error")
	}
}

// TestWALTombstoneRoundTripsAsAuthoritativeDelete verifies a tombstone
// (Deleted=true) is an ordinary WAL record — it survives rotation and
// replay with Deleted intact, alongside live records for the same trace.
// PAD1 v1.2 requires tombstones to flow through the same durability path as
// inserts; if Deleted silently dropped during encode/decode, a delete would
// look like a live span after any crash-recovery replay.
func TestWALTombstoneRoundTripsAsAuthoritativeDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := wal.CreateWAL(path)
	if err != nil {
		t.Fatalf("CreateWAL() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	live := wal.WALRecord{Span: wal.SpanPayload{TraceID: "t1", SpanID: "s1", Status: "ok"}}
	tombstone := wal.WALRecord{Span: wal.SpanPayload{TraceID: "t1", SpanID: "s2", Deleted: true}}

	if err := w.AppendRecord(live); err != nil {
		t.Fatalf("AppendRecord(live) error = %v", err)
	}
	if err := w.AppendRecord(tombstone); err != nil {
		t.Fatalf("AppendRecord(tombstone) error = %v", err)
	}

	got, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Replay() len = %d, want 2", len(got))
	}
	if got[0].Span.Deleted {
		t.Fatalf("Replay()[0].Span.Deleted = true, want false (live record)")
	}
	if !got[1].Span.Deleted {
		t.Fatalf("Replay()[1].Span.Deleted = false, want true (tombstone record)")
	}
	if !recordsEqual(got[1], tombstone) {
		t.Fatalf("Replay()[1] = %+v, want %+v", got[1], tombstone)
	}
}

// TestWALAdoptsLegacySingleFileLog verifies that a flat, pre-segmentation
// log file at the base path gets adopted as segment 1.
// ADAPTED: the original test hand-crafted the legacy file's bytes via the
// unexported serializePayload/putRecordHeader. Neither is reachable from
// this package, so instead we produce a real segment file through the
// public API in one WAL instance, then present that same file at the base
// path to a fresh WAL — exercising the identical adoption code path
// without needing internal encoding access.
func TestWALAdoptsLegacySingleFileLog(t *testing.T) {
	sourceDir := t.TempDir()
	sourcePath := filepath.Join(sourceDir, "wal.log")

	seed, err := wal.CreateWAL(sourcePath)
	if err != nil {
		t.Fatalf("CreateWAL() (seed) error = %v", err)
	}
	want := makeRecords(2)
	for _, record := range want {
		if err := seed.AppendRecord(record); err != nil {
			t.Fatalf("AppendRecord() (seed) error = %v", err)
		}
	}
	seedSegment := segmentFilePath(sourceDir, seed.CurrentSegment())
	if err := seed.Close(); err != nil {
		t.Fatalf("Close() (seed) error = %v", err)
	}

	legacyBytes, err := os.ReadFile(seedSegment)
	if err != nil {
		t.Fatalf("ReadFile() (seed segment) error = %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	if err := os.WriteFile(path, legacyBytes, 0644); err != nil {
		t.Fatalf("WriteFile() (legacy) error = %v", err)
	}

	w, err := wal.CreateWAL(path)
	if err != nil {
		t.Fatalf("CreateWAL() error = %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("legacy wal still present at base path, err = %v", err)
	}
	assertReplay(t, w, want)
}