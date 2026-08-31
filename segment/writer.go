
// v1 design decision: this writer uses a simple row-oriented record format
// (framed span-by-span, reusing wal.EncodeSpanPayload for the payload bytes)
// rather than the fully columnar per-field encoding PAD1 §3 describes as the
// long-term target (delta-of-delta timestamps, dict+RLE strings, XOR
// numerics, zstd payload).

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
	Magic uint32 = 0x57545331
	CurrentSegmentVersion uint16 = 1

	// magic(4) + version(2) + segmentID(8) + spanCount(4) + minTimestamp(8) + maxTimestamp(8).
	headerSize = 4 + 2 + 8 + 4 + 8 + 8
	recordFramingSize = 4 + 4
)

type SegmentHeader struct {
	SegmentID    uint64
	SpanCount    uint32
	MinTimestamp int64
	MaxTimestamp int64
}

type WriteResult struct {
	SegmentID    uint64
	Path         string
	SpanCount    int
	MinTimestamp int64
	MaxTimestamp int64
	Offsets      map[string]uint64
}

type Writer struct {
	spans []wal.SpanPayload
}

func NewWriter() *Writer {
	return &Writer{spans: make([]wal.SpanPayload, 0, 1024)}
}

func (w *Writer) Add(span wal.SpanPayload) {
	w.spans = append(w.spans, span)
}

func (w *Writer) Len() int {
	return len(w.spans)
}


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

func SegmentPath(dir string, segmentID uint64) string {
	return filepath.Join(dir, fmt.Sprintf("segment_%06d.seg", segmentID))
}

.
func CompositeKey(traceID, spanID string) string {
	return traceID + "||" + spanID
}

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
