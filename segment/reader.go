package segment

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/GuhaneshT/WispTraceDB/bloom"
	"github.com/GuhaneshT/WispTraceDB/wal"
)

type Reader struct {
	file   *os.File
	Header SegmentHeader
	Blooms map[string]*bloom.Filter
}


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

	blooms, err := readBlooms(file, header.BloomSectionLength)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("read bloom section: %w", err)
	}

	return &Reader{file: file, Header: header, Blooms: blooms}, nil
}

// Close releases the underlying file handle.
func (r *Reader) Close() error {
	return r.file.Close()
}

// ReadAt reads and decodes exactly one span record starting at the given
// byte offset.
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

type ScannedSpan struct {
	Offset uint64
	Span   wal.SpanPayload
}

// ScanAll reads every span record in the segment sequentially, starting
// right after the header and bloom-filter section, through EOF.
func (r *Reader) ScanAll() ([]ScannedSpan, error) {
	start := int64(headerSize) + int64(r.Header.BloomSectionLength)
	if _, err := r.file.Seek(start, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek past segment header/bloom section: %w", err)
	}
	reader := bufio.NewReader(r.file)

	spans := make([]ScannedSpan, 0, r.Header.SpanCount)
	offset := uint64(start)
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
		SegmentID:          binary.LittleEndian.Uint64(buf[6:14]),
		SpanCount:          binary.LittleEndian.Uint32(buf[14:18]),
		MinTimestamp:       int64(binary.LittleEndian.Uint64(buf[18:26])),
		MaxTimestamp:       int64(binary.LittleEndian.Uint64(buf[26:34])),
		BloomSectionLength: binary.LittleEndian.Uint32(buf[34:38]),
	}, nil
}

// readBlooms reads exactly sectionLength bytes (the current file position is
// expected to be right after the header) and decodes BloomDimensions filters
// from it, in that fixed order.
func readBlooms(file *os.File, sectionLength uint32) (map[string]*bloom.Filter, error) {
	data := make([]byte, sectionLength)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, fmt.Errorf("read %d bloom section bytes: %w", sectionLength, err)
	}

	filters := make(map[string]*bloom.Filter, len(BloomDimensions))
	offset := 0
	for _, dim := range BloomDimensions {
		filter, n, err := bloom.Decode(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("decode bloom filter for %s: %w", dim, err)
		}
		filters[dim] = filter
		offset += n
	}
	return filters, nil
}
