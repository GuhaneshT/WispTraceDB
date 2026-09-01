package segment

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/GuhaneshT/WispTraceDB/wal"
)

func makeSpan(traceID, spanID string, ts int64) wal.SpanPayload {
	return wal.SpanPayload{
		TraceID:   traceID,
		SpanID:    spanID,
		Timestamp: ts,
		AgentID:   "agent-1",
		Model:     "claude-sonnet-5",
		ToolName:  "bash_tool",
		Team:      "platform",
		Status:    "ok",
		TokensIn:  10,
		TokensOut: 20,
		Cost:      0.005,
		LatencyMs: 150,
		Payload:   []byte("hello world"),
	}
}

func TestWriterFlushAndReaderReadAt(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter()

	spans := []wal.SpanPayload{
		makeSpan("trace-1", "span-1", 100),
		makeSpan("trace-1", "span-2", 110),
		makeSpan("trace-2", "span-1", 120),
	}
	for _, s := range spans {
		w.Add(s)
	}

	result, err := w.Flush(dir, 1)
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if result.SpanCount != 3 {
		t.Fatalf("SpanCount = %d, want 3", result.SpanCount)
	}
	if result.MinTimestamp != 100 || result.MaxTimestamp != 120 {
		t.Fatalf("zone map = [%d, %d], want [100, 120]", result.MinTimestamp, result.MaxTimestamp)
	}
	if len(result.Offsets) != 3 {
		t.Fatalf("len(Offsets) = %d, want 3", len(result.Offsets))
	}

	reader, err := OpenReader(result.Path)
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer reader.Close()

	if reader.Header.SegmentID != 1 {
		t.Fatalf("Header.SegmentID = %d, want 1", reader.Header.SegmentID)
	}
	if reader.Header.SpanCount != 3 {
		t.Fatalf("Header.SpanCount = %d, want 3", reader.Header.SpanCount)
	}

	for _, want := range spans {
		key := CompositeKey(want.TraceID, want.SpanID)
		offset, ok := result.Offsets[key]
		if !ok {
			t.Fatalf("Offsets missing key %s", key)
		}
		got, err := reader.ReadAt(offset)
		if err != nil {
			t.Fatalf("ReadAt(%d) error = %v", offset, err)
		}
		if got.TraceID != want.TraceID || got.SpanID != want.SpanID || got.Timestamp != want.Timestamp {
			t.Fatalf("ReadAt(%d) = %+v, want %+v", offset, got, want)
		}
		if got.Cost != want.Cost || got.TokensIn != want.TokensIn {
			t.Fatalf("ReadAt(%d) numeric fields = %+v, want %+v", offset, got, want)
		}
		if string(got.Payload) != string(want.Payload) {
			t.Fatalf("ReadAt(%d) payload = %q, want %q", offset, got.Payload, want.Payload)
		}
	}
}

func TestWriterFlushEmptyErrors(t *testing.T) {
	w := NewWriter()
	if _, err := w.Flush(t.TempDir(), 1); err == nil {
		t.Fatal("Flush() with no spans should error")
	}
}

func TestReaderScanAllReturnsEverySpanInOrder(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter()

	spans := []wal.SpanPayload{
		makeSpan("trace-1", "span-1", 100),
		makeSpan("trace-1", "span-2", 105),
		makeSpan("trace-1", "span-3", 110),
	}
	for _, s := range spans {
		w.Add(s)
	}

	result, err := w.Flush(dir, 7)
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	reader, err := OpenReader(result.Path)
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer reader.Close()

	scanned, err := reader.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll() error = %v", err)
	}
	if len(scanned) != len(spans) {
		t.Fatalf("ScanAll() len = %d, want %d", len(scanned), len(spans))
	}
	for i, want := range spans {
		if scanned[i].Span.SpanID != want.SpanID {
			t.Fatalf("ScanAll()[%d].SpanID = %s, want %s", i, scanned[i].Span.SpanID, want.SpanID)
		}
		// The offset ScanAll reports must be independently usable by ReadAt.
		got, err := reader.ReadAt(scanned[i].Offset)
		if err != nil {
			t.Fatalf("ReadAt(%d) from scanned offset error = %v", scanned[i].Offset, err)
		}
		if got.SpanID != want.SpanID {
			t.Fatalf("ReadAt(scanned offset) SpanID = %s, want %s", got.SpanID, want.SpanID)
		}
	}
}

func TestOpenReaderRejectsBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bogus.seg")
	if err := os.WriteFile(path, []byte("not a segment file, definitely not"), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if _, err := OpenReader(path); err == nil {
		t.Fatal("OpenReader() on a non-segment file should error")
	}
}

func TestOpenReaderRejectsUnsupportedVersion(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter()
	w.Add(makeSpan("trace-1", "span-1", 100))
	result, err := w.Flush(dir, 1)
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Corrupt the version field (bytes 4:6) to an unsupported value.
	data, err := os.ReadFile(result.Path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	data[4] = 0xFF
	data[5] = 0xFF
	if err := os.WriteFile(result.Path, data, 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if _, err := OpenReader(result.Path); err == nil {
		t.Fatal("OpenReader() with unsupported version should error")
	}
}

func TestFlushFailureRemovesPartialFile(t *testing.T) {
	// A directory that doesn't exist forces os.OpenFile to fail immediately,
	// so no partial file is ever created — this test documents that Flush
	// doesn't leave a zero-byte file behind on the earliest possible failure.
	w := NewWriter()
	w.Add(makeSpan("trace-1", "span-1", 100))

	badDir := filepath.Join(t.TempDir(), "does-not-exist")
	if _, err := w.Flush(badDir, 1); err == nil {
		t.Fatal("Flush() into a nonexistent directory should error")
	}
	if _, err := os.Stat(SegmentPath(badDir, 1)); !os.IsNotExist(err) {
		t.Fatalf("segment file should not exist after failed flush, stat err = %v", err)
	}
}

func TestReaderBloomsContainInsertedDimensionValues(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter()

	spans := []wal.SpanPayload{
		makeSpan("t1", "s1", 100), // AgentID=agent-1, Model=claude-sonnet-5, etc.
	}
	other := makeSpan("t1", "s2", 110)
	other.AgentID = "agent-2"
	other.Model = "gpt-4"
	spans = append(spans, other)

	for _, s := range spans {
		w.Add(s)
	}
	result, err := w.Flush(dir, 1)
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	reader, err := OpenReader(result.Path)
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer reader.Close()

	if len(reader.Blooms) != len(BloomDimensions) {
		t.Fatalf("len(Blooms) = %d, want %d", len(reader.Blooms), len(BloomDimensions))
	}

	for _, dim := range BloomDimensions {
		if _, ok := reader.Blooms[dim]; !ok {
			t.Fatalf("Blooms missing dimension %s", dim)
		}
	}

	if !reader.Blooms["agent_id"].MayContain("agent-1") {
		t.Fatal("agent_id bloom should contain agent-1")
	}
	if !reader.Blooms["agent_id"].MayContain("agent-2") {
		t.Fatal("agent_id bloom should contain agent-2")
	}
	if !reader.Blooms["model"].MayContain("gpt-4") {
		t.Fatal("model bloom should contain gpt-4")
	}
	if reader.Blooms["agent_id"].MayContain("agent-that-was-never-inserted") {
		t.Fatal("agent_id bloom should not claim to contain a value never added")
	}
}

func TestScanAllSkipsPastBloomSection(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter()
	spans := []wal.SpanPayload{
		makeSpan("t1", "s1", 100),
		makeSpan("t1", "s2", 110),
	}
	for _, s := range spans {
		w.Add(s)
	}
	result, err := w.Flush(dir, 1)
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	reader, err := OpenReader(result.Path)
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer reader.Close()

	if reader.Header.BloomSectionLength == 0 {
		t.Fatal("BloomSectionLength = 0, want > 0 (5 filters were embedded)")
	}

	scanned, err := reader.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll() error = %v", err)
	}
	if len(scanned) != len(spans) {
		t.Fatalf("ScanAll() len = %d, want %d — bloom section must not leak into record parsing", len(scanned), len(spans))
	}
	for i, want := range spans {
		if scanned[i].Span.SpanID != want.SpanID {
			t.Fatalf("ScanAll()[%d].SpanID = %s, want %s", i, scanned[i].Span.SpanID, want.SpanID)
		}
	}
}

func TestCompositeKeyMatchesTraceAndSpan(t *testing.T) {
	key := CompositeKey("trace-abc", "span-123")
	if key != "trace-abc||span-123" {
		t.Fatalf("CompositeKey() = %q, want %q", key, "trace-abc||span-123")
	}
}
