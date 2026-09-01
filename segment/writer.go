package segment

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/GuhaneshT/WispTraceDB/bloom"
	"github.com/GuhaneshT/WispTraceDB/wal"
)

const (
	Magic uint32 = 0x57545331
	CurrentSegmentVersion uint16 = 2

	// magic(4) + version(2) + segmentID(8) + spanCount(4) + minTimestamp(8)
	// + maxTimestamp(8) + bloomSectionLength(4).
	headerSize        = 4 + 2 + 8 + 4 + 8 + 8 + 4
	recordFramingSize = 4 + 4
	bloomFalsePositiveRate = 0.01
)

var BloomDimensions = []string{"agent_id", "model", "tool_name", "team", "status"}

// dimensionValue returns a span's value for one of BloomDimensions.
func dimensionValue(span wal.SpanPayload, dim string) string {
	switch dim {
	case "agent_id":
		return span.AgentID
	case "model":
		return span.Model
	case "tool_name":
		return span.ToolName
	case "team":
		return span.Team
	case "status":
		return span.Status
	}
	return ""
}

// SegmentHeader is the fixed-size header read back from a segment file.
type SegmentHeader struct {
	SegmentID          uint64
	SpanCount          uint32
	MinTimestamp       int64
	MaxTimestamp       int64
	BloomSectionLength uint32
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

// Flush writes every buffered span to a new segment file
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

	bloomSection := w.buildBloomSection()

	if err := writeHeader(bw, segmentID, uint32(len(w.spans)), minTS, maxTS, uint32(len(bloomSection))); err != nil {
		return nil, fmt.Errorf("write segment header: %w", err)
	}
	if _, err := bw.Write(bloomSection); err != nil {
		return nil, fmt.Errorf("write bloom section: %w", err)
	}

	offsets := make(map[string]uint64, len(w.spans))
	written := uint64(headerSize) + uint64(len(bloomSection))

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

func (w *Writer) buildBloomSection() []byte {
	var section []byte
	for _, dim := range BloomDimensions {
		filter := bloom.New(len(w.spans), bloomFalsePositiveRate)
		for _, span := range w.spans {
			filter.Add(dimensionValue(span, dim))
		}
		section = append(section, filter.Encode()...)
	}
	return section
}

func SegmentPath(dir string, segmentID uint64) string {
	return filepath.Join(dir, fmt.Sprintf("segment_%06d.seg", segmentID))
}

func CompositeKey(traceID, spanID string) string {
	return traceID + "||" + spanID
}

func writeHeader(w *bufio.Writer, segmentID uint64, spanCount uint32, minTS, maxTS int64, bloomSectionLength uint32) error {
	var buf [headerSize]byte
	binary.LittleEndian.PutUint32(buf[0:4], Magic)
	binary.LittleEndian.PutUint16(buf[4:6], CurrentSegmentVersion)
	binary.LittleEndian.PutUint64(buf[6:14], segmentID)
	binary.LittleEndian.PutUint32(buf[14:18], spanCount)
	binary.LittleEndian.PutUint64(buf[18:26], uint64(minTS))
	binary.LittleEndian.PutUint64(buf[26:34], uint64(maxTS))
	binary.LittleEndian.PutUint32(buf[34:38], bloomSectionLength)
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
