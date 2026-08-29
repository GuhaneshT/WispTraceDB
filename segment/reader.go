package segment

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/GuhaneshT/WispTraceDB/wal"
)

// Reader opens a finalized segment file for two access patterns:
//   - ReadAt: a single point read by byte offset, exactly the offset half of
//     the (segment_id, offset) pair PAD1 §4 stores in Pebble. This is the
//     query-path read: point lookup already knows where to seek.
//   - ScanAll: a full sequential read of every span in the segment. Used by
//     crash recovery (rebuilding Pebble entries for the unconfirmed tail past
//     the checkpoint watermark, PAD1 §4) and, later, by compaction (Phase 3)
//     to read every span out of a segment being merged or dropped.
type Reader struct {
	file   *os.File
	Header SegmentHeader
}

// OpenReader opens a segment file and validates its header (magic + version)
// before returning. A version mismatch is rejected explicitly, the same
// posture as wal.replaySegment's version check — a segment written by an
// incompatible future/past layout must never be silently misparsed.
func OpenReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	header, err := readHeader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("read segment header: %w", err)
	}

	return &Reader{file: file, Header: header}, nil
}

// Close releases the underlying file handle.
func (r *Reader) Close() error {
	return r.file.Close()
}

// ReadAt reads and decodes exactly one span record starting at the given
// byte offset. offset is expected to be a value previously returned in a
// WriteResult.Offsets map (equivalently, the offset half of a Pebble index
// entry) — an arbitrary offset is not guaranteed to land on a record
// boundary and will fail its checksum check if it doesn't.
func (r *Reader) ReadAt(offset uint64) (wal.SpanPayload, error) {
	var framing [recordFramingSize]byte
	if _, err := r.file.ReadAt(framing[:], int64(offset)); err != nil {
		return wal.SpanPayload{}, fmt.Errorf("read record framing at offset %d: %w", offset, err)
	}
	length := binary.LittleEndian.Uint32(framing[0:4])
	checksum := binary.LittleEndian.Uint32(framing[4:8])

	payload := make([]byte, length)
	if _, err := r.file.ReadAt(payload, int64(offset)+recordFramingSize); err != nil {
		return wal.SpanPayload{}, fmt.Errorf("read record payload at offset %d: %w", offset, err)
	}
	if crc32.ChecksumIEEE(payload) != checksum {
		return wal.SpanPayload{}, fmt.Errorf("segment record checksum mismatch at offset %d", offset)
	}

	return wal.DecodeSpanPayload(payload)
}

// ScannedSpan pairs a decoded span with the byte offset its record started
// at — the same offset ReadAt expects and the same value a reconciliation
// pass would index into Pebble.
type ScannedSpan struct {
	Offset uint64
	Span   wal.SpanPayload
}

// ScanAll reads every span record in the segment sequentially, from the
// first record after the header through EOF.
func (r *Reader) ScanAll() ([]ScannedSpan, error) {
	if _, err := r.file.Seek(headerSize, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek past segment header: %w", err)
	}
	reader := bufio.NewReader(r.file)

	spans := make([]ScannedSpan, 0, r.Header.SpanCount)
	offset := uint64(headerSize)
	for {
		var framing [recordFramingSize]byte
		if _, err := io.ReadFull(reader, framing[:]); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("scan segment: read framing at offset %d: %w", offset, err)
		}
		length := binary.LittleEndian.Uint32(framing[0:4])
		checksum := binary.LittleEndian.Uint32(framing[4:8])

		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, fmt.Errorf("scan segment: read payload at offset %d: %w", offset, err)
		}
		if crc32.ChecksumIEEE(payload) != checksum {
			return nil, fmt.Errorf("segment record checksum mismatch at offset %d", offset)
		}

		span, err := wal.DecodeSpanPayload(payload)
		if err != nil {
			return nil, fmt.Errorf("decode span at offset %d: %w", offset, err)
		}

		spans = append(spans, ScannedSpan{Offset: offset, Span: span})
		offset += uint64(recordFramingSize) + uint64(length)
	}
	return spans, nil
}

// readHeader parses and validates the fixed-size segment header at the
// current file position (expected to be the start of the file).
func readHeader(file *os.File) (SegmentHeader, error) {
	var buf [headerSize]byte
	if _, err := io.ReadFull(file, buf[:]); err != nil {
		return SegmentHeader{}, err
	}

	magic := binary.LittleEndian.Uint32(buf[0:4])
	if magic != Magic {
		return SegmentHeader{}, fmt.Errorf("bad segment magic: got %#x, want %#x", magic, Magic)
	}
	version := binary.LittleEndian.Uint16(buf[4:6])
	if version != CurrentSegmentVersion {
		return SegmentHeader{}, fmt.Errorf("unsupported segment version %d (this build reads version %d)", version, CurrentSegmentVersion)
	}

	return SegmentHeader{
		SegmentID:    binary.LittleEndian.Uint64(buf[6:14]),
		SpanCount:    binary.LittleEndian.Uint32(buf[14:18]),
		MinTimestamp: int64(binary.LittleEndian.Uint64(buf[18:26])),
		MaxTimestamp: int64(binary.LittleEndian.Uint64(buf[26:34])),
	}, nil
}
