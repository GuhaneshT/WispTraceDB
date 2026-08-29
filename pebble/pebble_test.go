package pebble

import (
	// "bytes"
	"path/filepath"
	"testing"
)

func TestPutAndGetSpan(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "test_lsm"))
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer db.Close()

	key := []byte("trace-001||span-001")
	location := SpanLocation{SegmentID: 1, Offset: 512}

	if err := db.PutSpan(key, location); err != nil {
		t.Fatalf("PutSpan() error = %v", err)
	}

	got, err := db.GetSpan(key)
	if err != nil {
		t.Fatalf("GetSpan() error = %v", err)
	}
	if got.SegmentID != location.SegmentID || got.Offset != location.Offset {
		t.Fatalf("GetSpan() = %+v, want %+v", got, location)
	}
}

func TestGetSpanNotFound(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "test_lsm"))
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer db.Close()

	_, err = db.GetSpan([]byte("nonexistent"))
	if err == nil {
		t.Fatal("GetSpan() error = nil, want ErrNotFound")
	}
}

func TestPrefixScanReturnsAllSpansInTrace(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "test_lsm"))
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer db.Close()

	traceID := "trace-001"
	prefix := []byte(traceID + "||")

	// Write multiple spans of the same trace.
	locations := []SpanLocation{
		{SegmentID: 1, Offset: 0},
		{SegmentID: 1, Offset: 256},
		{SegmentID: 2, Offset: 512},
	}
	for i, loc := range locations {
		key := append([]byte(nil), prefix...)
		key = append(key, []byte("span-")...)
		key = append(key, byte('0'+i))
		if err := db.PutSpan(key, loc); err != nil {
			t.Fatalf("PutSpan(%d) error = %v", i, err)
		}
	}

	got, err := db.PrefixScan(prefix)
	if err != nil {
		t.Fatalf("PrefixScan() error = %v", err)
	}
	if len(got) != len(locations) {
		t.Fatalf("PrefixScan() len = %d, want %d", len(got), len(locations))
	}
	for i, loc := range got {
		if loc.SegmentID != locations[i].SegmentID || loc.Offset != locations[i].Offset {
			t.Fatalf("PrefixScan()[%d] = %+v, want %+v", i, loc, locations[i])
		}
	}
}

func TestDeleteSpanRemovesFromIndex(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "test_lsm"))
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer db.Close()

	key := []byte("trace-001||span-001")
	location := SpanLocation{SegmentID: 1, Offset: 512}

	if err := db.PutSpan(key, location); err != nil {
		t.Fatalf("PutSpan() error = %v", err)
	}

	if err := db.DeleteSpan(key); err != nil {
		t.Fatalf("DeleteSpan() error = %v", err)
	}

	_, err = db.GetSpan(key)
	if err == nil {
		t.Fatal("GetSpan() after delete should return ErrNotFound")
	}
}

func TestEncodeDecode(t *testing.T) {
	location := SpanLocation{SegmentID: 42, Offset: 8192}
	encoded := encodeSpanLocation(location)

	if len(encoded) != 16 {
		t.Fatalf("encoded length = %d, want 16", len(encoded))
	}

	decoded, err := decodeSpanLocation(encoded)
	if err != nil {
		t.Fatalf("decodeSpanLocation() error = %v", err)
	}

	if decoded.SegmentID != location.SegmentID || decoded.Offset != location.Offset {
		t.Fatalf("decoded = %+v, want %+v", decoded, location)
	}
}

func TestEncodeDecodeInvalidSize(t *testing.T) {
	_, err := decodeSpanLocation([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("decodeSpanLocation(short data) should error")
	}
}

func TestMultipleTracesInIndex(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "test_lsm"))
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer db.Close()

	// Write spans from two different traces.
	trace1Key := []byte("trace-001||span-001")
	trace2Key := []byte("trace-002||span-001")

	loc1 := SpanLocation{SegmentID: 1, Offset: 0}
	loc2 := SpanLocation{SegmentID: 1, Offset: 256}

	if err := db.PutSpan(trace1Key, loc1); err != nil {
		t.Fatalf("PutSpan(trace1) error = %v", err)
	}
	if err := db.PutSpan(trace2Key, loc2); err != nil {
		t.Fatalf("PutSpan(trace2) error = %v", err)
	}

	// Verify they are separate.
	got1, err := db.GetSpan(trace1Key)
	if err != nil {
		t.Fatalf("GetSpan(trace1) error = %v", err)
	}
	if got1.Offset != loc1.Offset {
		t.Fatalf("trace1 offset = %d, want %d", got1.Offset, loc1.Offset)
	}

	got2, err := db.GetSpan(trace2Key)
	if err != nil {
		t.Fatalf("GetSpan(trace2) error = %v", err)
	}
	if got2.Offset != loc2.Offset {
		t.Fatalf("trace2 offset = %d, want %d", got2.Offset, loc2.Offset)
	}

	// Prefix scan trace-001 should not include trace-002.
	trace1Spans, err := db.PrefixScan([]byte("trace-001||"))
	if err != nil {
		t.Fatalf("PrefixScan(trace-001) error = %v", err)
	}
	if len(trace1Spans) != 1 {
		t.Fatalf("PrefixScan(trace-001) len = %d, want 1", len(trace1Spans))
	}
}

func TestBatchPutSpans(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "test_lsm"))
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer db.Close()

	// Batch write multiple spans of the same segment (as would happen during segment flush).
	entries := map[string]SpanLocation{
		"trace-001||span-001": {SegmentID: 1, Offset: 0},
		"trace-001||span-002": {SegmentID: 1, Offset: 256},
		"trace-002||span-001": {SegmentID: 1, Offset: 512},
	}

	if err := db.BatchPutSpans(entries); err != nil {
		t.Fatalf("BatchPutSpans() error = %v", err)
	}

	// Verify all entries were written.
	for keyStr, expectedLoc := range entries {
		got, err := db.GetSpan([]byte(keyStr))
		if err != nil {
			t.Fatalf("GetSpan(%s) error = %v", keyStr, err)
		}
		if got.SegmentID != expectedLoc.SegmentID || got.Offset != expectedLoc.Offset {
			t.Fatalf("GetSpan(%s) = %+v, want %+v", keyStr, got, expectedLoc)
		}
	}
}

func TestBatchDeleteSpans(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "test_lsm"))
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer db.Close()

	// Write some spans.
	keys := []string{"trace-001||span-001", "trace-001||span-002", "trace-002||span-001"}
	for i, key := range keys {
		loc := SpanLocation{SegmentID: 1, Offset: uint64(i * 256)}
		if err := db.PutSpan([]byte(key), loc); err != nil {
			t.Fatalf("PutSpan() error = %v", err)
		}
	}

	// Batch delete the first two.
	if err := db.BatchDeleteSpans(keys[:2]); err != nil {
		t.Fatalf("BatchDeleteSpans() error = %v", err)
	}

	// Verify the first two are gone.
	for _, key := range keys[:2] {
		_, err := db.GetSpan([]byte(key))
		if err == nil {
			t.Fatalf("GetSpan(%s) should error after delete", key)
		}
	}

	// Verify the third still exists.
	got, err := db.GetSpan([]byte(keys[2]))
	if err != nil {
		t.Fatalf("GetSpan(%s) error = %v", keys[2], err)
	}
	if got.Offset != 512 {
		t.Fatalf("GetSpan(%s) offset = %d, want 512", keys[2], got.Offset)
	}
}
