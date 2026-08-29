// Package segment implements Pillar 2 of WispTraceDB (PAD1 §3): turning
// buffered spans into compact, immutable, on-disk segment files.
//
// v1 design decision: this writer uses a simple row-oriented record format
// (framed span-by-span, reusing wal.EncodeSpanPayload for the payload bytes)
// rather than the fully columnar per-field encoding PAD1 §3 describes as the
// long-term target (delta-of-delta timestamps, dict+RLE strings, XOR
// numerics, zstd payload). This is deliberate, not a shortcut taken
// silently: the single riskiest correctness surface in the system is the
// segment/trace-index reconciliation protocol (PAD1 §4), and that must be
// proven correct — including under fault injection — before layering
// compression on top of it. Columnar encoding is a documented follow-up
// once Phase 2 (this writer + reconciliation) and Phase 3 (compaction) are
// both fault-injection tested, per PAD1 §12's build order.
package segment

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/GuhaneshT/WispTraceDB/wal"
)

const (
	// Magic identifies a WispTraceDB segment file ("WTS1" in ASCII-ish hex).
	Magic uint32 = 0x57545331
	// CurrentSegmentVersion is written into every segment header. Bump this
	// whenever the header or record layout changes, and add explicit
	// handling in readHeader for any old version this build must still read
	// (same posture as wal.CurrentWALVersion).
	CurrentSegmentVersion uint16 = 1

	// headerSize is the fixed-width segment header: magic(4) + version(2) +
	// segmentID(8) + spanCount(4) + minTimestamp(8) + maxTimestamp(8).
	headerSize = 4 + 2 + 8 + 4 + 8 + 8

	// recordFramingSize is the per-record framing written before each span's
	// encoded payload: length(4) + crc32(4). Mirrors wal.go's own record
	// framing deliberately — same crash-safety property (a torn write is
	// detected by a short read or bad checksum, never silently misparsed).
	recordFramingSize = 4 + 4
)

// SegmentHeader is the fixed-size header read back from a segment file.
type SegmentHeader struct {
	SegmentID    uint64
	SpanCount    uint32
	MinTimestamp int64
	MaxTimestamp int64
}

// WriteResult is returned once a segment has been durably written (fsynced).
// Offsets maps each span's composite key (see CompositeKey — the same
// trace_id||span_id shape used as the Pebble index key, PAD1 §4) to the byte
// offset of that span's record within the segment file. This is exactly the
// (segment_id, offset) pair PAD1 §4 specifies as the Pebble index value.
type WriteResult struct {
	SegmentID    uint64
	Path         string
	SpanCount    int
	MinTimestamp int64
	MaxTimestamp int64
	Offsets      map[string]uint64
}

// Writer buffers spans in memory and, on Flush, writes them as one immutable
// segment file. A Writer is single-use: create one per segment, Add spans to
// it, Flush it exactly once, then discard it and create a fresh Writer for
// the next segment. Not safe for concurrent use — the caller (WispTrace) must
// serialize Add/Flush calls, the same way the WAL requires serialized append
// access at the engine layer.
type Writer struct {
	spans []wal.SpanPayload
}

// NewWriter returns an empty Writer ready to buffer spans for one segment.
func NewWriter() *Writer {
	return &Writer{spans: make([]wal.SpanPayload, 0, 1024)}
}

// Add buffers a span for the next Flush. Spans are written to the segment in
// the order Add was called — this is not currently re-sorted by timestamp,
// since spans usually arrive close to time order and re-sorting is an
// optimization the design defers (see the package doc's note on columnar
// encoding).
func (w *Writer) Add(span wal.SpanPayload) {
	w.spans = append(w.spans, span)
}

// Len returns the number of buffered, not-yet-flushed spans.
func (w *Writer) Len() int {
	return len(w.spans)
}

// Flush writes every buffered span to a new segment file at
// SegmentPath(dir, segmentID), fsyncs it, and returns the per-span offsets
// needed to index each span into Pebble (PAD1 §4's reconciliation protocol:
// a span is durable-and-queryable only once both this write and the
// corresponding Pebble Put are confirmed — the caller is responsible for
// doing the Pebble side after Flush returns successfully).
//
// If Flush fails partway through, the partial file is removed rather than
// left on disk — a half-written segment must never be mistaken for a valid
// one on next startup. This mirrors PAD1 §3's compaction invariant ("the
// partial new segment is simply orphaned and cleaned up on next startup"),
// applied eagerly here instead of deferred to a startup sweep.
func (w *Writer) Flush(dir string, segmentID uint64) (*WriteResult, error) {
	if len(w.spans) == 0 {
		return nil, fmt.Errorf("segment writer: flush called with no buffered spans")
	}

	path := SegmentPath(dir, segmentID)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("create segment file: %w", err)
	}

	success := false
	defer func() {
		file.Close()
		if !success {
			os.Remove(path)
		}
	}()

	bw := bufio.NewWriter(file)

	minTS, maxTS := w.spans[0].Timestamp, w.spans[0].Timestamp
	for _, s := range w.spans {
		if s.Timestamp < minTS {
			minTS = s.Timestamp
		}
		if s.Timestamp > maxTS {
			maxTS = s.Timestamp
		}
	}

	if err := writeHeader(bw, segmentID, uint32(len(w.spans)), minTS, maxTS); err != nil {
		return nil, fmt.Errorf("write segment header: %w", err)
	}

	offsets := make(map[string]uint64, len(w.spans))
	written := uint64(headerSize)

	for _, span := range w.spans {
		recordOffset := written
		n, err := writeRecord(bw, span)
		if err != nil {
			return nil, fmt.Errorf("write span record: %w", err)
		}
		written += uint64(n)
		offsets[CompositeKey(span.TraceID, span.SpanID)] = recordOffset
	}

	if err := bw.Flush(); err != nil {
		return nil, fmt.Errorf("flush segment buffer: %w", err)
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("fsync segment: %w", err)
	}

	success = true
	return &WriteResult{
		SegmentID:    segmentID,
		Path:         path,
		SpanCount:    len(w.spans),
		MinTimestamp: minTS,
		MaxTimestamp: maxTS,
		Offsets:      offsets,
	}, nil
}

// SegmentPath returns the on-disk path for a segment, following the same
// generation-numbered naming convention as the WAL (wal_NNNNNN.log ->
// segment_NNNNNN.seg).
func SegmentPath(dir string, segmentID uint64) string {
	return filepath.Join(dir, fmt.Sprintf("segment_%06d.seg", segmentID))
}

// CompositeKey builds the trace_id||span_id key used both as the Pebble
// index key (PAD1 §4) and as the map key in WriteResult.Offsets. "||" is
// used as a separator on the (reasonable) assumption that trace/span IDs
// don't themselves contain "||"; this is the same shape decision the
// pebble package's callers already made.
func CompositeKey(traceID, spanID string) string {
	return traceID + "||" + spanID
}

// writeHeader writes the fixed-size segment header.
func writeHeader(w *bufio.Writer, segmentID uint64, spanCount uint32, minTS, maxTS int64) error {
	var buf [headerSize]byte
	binary.LittleEndian.PutUint32(buf[0:4], Magic)
	binary.LittleEndian.PutUint16(buf[4:6], CurrentSegmentVersion)
	binary.LittleEndian.PutUint64(buf[6:14], segmentID)
	binary.LittleEndian.PutUint32(buf[14:18], spanCount)
	binary.LittleEndian.PutUint64(buf[18:26], uint64(minTS))
	binary.LittleEndian.PutUint64(buf[26:34], uint64(maxTS))
	_, err := w.Write(buf[:])
	return err
}

// writeRecord frames one span as length(4) | crc32(4) | encoded payload, and
// returns the total number of bytes written (the framing plus the payload) so
// the caller can track the next record's offset.
func writeRecord(w *bufio.Writer, span wal.SpanPayload) (int, error) {
	payload := wal.EncodeSpanPayload(span)

	var framing [recordFramingSize]byte
	binary.LittleEndian.PutUint32(framing[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(framing[4:8], crc32.ChecksumIEEE(payload))

	if _, err := w.Write(framing[:]); err != nil {
		return 0, err
	}
	if _, err := w.Write(payload); err != nil {
		return 0, err
	}
	return recordFramingSize + len(payload), nil
}
