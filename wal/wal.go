package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)


type WALMetadata struct {
	Version uint8
}


const CurrentWALVersion uint8 = 1

type SpanPayload struct {
	TraceID      string  // Distributed trace ID
	SpanID       string  // Unique span identifier
	ParentSpanID string  // Parent span identifier; "" means root span — same convention OTel uses, not a separate null encoding on the wire
	Timestamp    int64 // When the span was created
	AgentID      string  // Agent that processed this span
	Model        string  // Model name/identifier
	ToolName     string  // Tool invoked (if any)
	Team         string  // Team identifier
	Status       string  // Span status (success, error, etc.)
	TokensIn     int32   // Input tokens consumed
	TokensOut    int32   // Output tokens generated
	Cost         float64 // Estimated cost of the span
	LatencyMs    int64   // Duration in milliseconds
	Payload      []byte  // Raw span data/metadata
	Deleted      bool    // Whether this span has been marked deleted
}
 
type WALRecord struct {
	Metadata WALMetadata
	Span     SpanPayload
}


const (
	DefaultMaxSegmentSize uint64 = 4 * 1024 * 1024
	recordHeaderSize             = 9
	maxRecordSize         uint64 = 64 << 20
	fixedPayloadHeaderSize = 8 + 4 + 4 + 8 + 8 + 1
	strLenSize  = 2 // uint16 prefix for each string field
	blobLenSize = 4 // uint32 prefix for the Payload blob

)

func stringFields(span *SpanPayload) [8]*string {
	return [8]*string{
		&span.TraceID,
		&span.SpanID,
		&span.ParentSpanID,
		&span.AgentID,
		&span.Model,
		&span.ToolName,
		&span.Team,
		&span.Status,
	}
}

// payloadEncodedSize returns the exact on-disk size of record.
func payloadEncodedSize(record WALRecord) int {
	size := fixedPayloadHeaderSize
	for _, f := range stringFields(&record.Span) {
		size += strLenSize + len(*f)
	}
	size += blobLenSize + len(record.Span.Payload)
	return size
}

//helpers

func putString(dst []byte, s string) int {
	binary.LittleEndian.PutUint16(dst[0:strLenSize], uint16(len(s)))
	copy(dst[strLenSize:], s)
	return strLenSize + len(s)
}

func getString(data []byte) (string, int, error) {
	if len(data) < strLenSize {
		return "", 0, fmt.Errorf("wal payload truncated: want %d bytes for string length, have %d", strLenSize, len(data))
	}
	strLen := int(binary.LittleEndian.Uint16(data[0:strLenSize]))
	end := strLenSize + strLen
	if end > len(data) {
		return "", 0, fmt.Errorf("wal payload truncated: want %d string bytes, have %d", strLen, len(data)-strLenSize)
	}
	s := string(append([]byte(nil), data[strLenSize:end]...)) // copy, don't pin read buffer
	return s, end, nil
}

//encode the wal record payload
func encodePayload(dst []byte, record WALRecord) {
	span := record.Span
	binary.LittleEndian.PutUint64(dst[0:8], uint64(span.Timestamp))
	binary.LittleEndian.PutUint32(dst[8:12], uint32(span.TokensIn))   
	binary.LittleEndian.PutUint32(dst[12:16], uint32(span.TokensOut)) 
	binary.LittleEndian.PutUint64(dst[16:24], math.Float64bits(span.Cost))
	binary.LittleEndian.PutUint64(dst[24:32], uint64(span.LatencyMs))
	if span.Deleted {
		dst[32] = 1
	} else {
		dst[32] = 0
	}

	off := fixedPayloadHeaderSize
	for _, f := range stringFields(&span) { 
		off += putString(dst[off:], *f)
	}

	binary.LittleEndian.PutUint32(dst[off:off+blobLenSize], uint32(len(span.Payload))) 
	off += blobLenSize
	copy(dst[off:], span.Payload) 
}

// serializePayload is a non-hot-path convenience wrapper around encodePayload
// for callers (tests, legacy-format fixtures) that just want an owned
// payload slice. AppendRecord bypasses this and encodes straight into a
// pooled record buffer instead, since it's the path allocation profiling
// flagged.
func serializePayload(record WALRecord) ([]byte, error) {
	buf := make([]byte, payloadEncodedSize(record))
	encodePayload(buf, record)
	return buf, nil
}

// EncodeSpanPayload serializes a span using the same field layout as a WAL record.

func EncodeSpanPayload(span SpanPayload) []byte {
	record := WALRecord{Span: span}
	buf := make([]byte, payloadEncodedSize(record))
	encodePayload(buf, record)
	return buf
}

// DecodeSpanPayload is the inverse of EncodeSpanPayload.
func DecodeSpanPayload(data []byte) (SpanPayload, error) {
	record, err := deserializePayload(data)
	if err != nil {
		return SpanPayload{}, err
	}
	return record.Span, nil
}

// rebuilds a SpanPayload that was encoded.
func deserializePayload(data []byte) (WALRecord, error) {
	if len(data) < fixedPayloadHeaderSize {
		return WALRecord{}, fmt.Errorf("wal payload too short: %d bytes, want at least %d", len(data), fixedPayloadHeaderSize)
	}

	var span SpanPayload
	span.Timestamp = int64(binary.LittleEndian.Uint64(data[0:8]))
	span.TokensIn = int32(binary.LittleEndian.Uint32(data[8:12]))   
	span.TokensOut = int32(binary.LittleEndian.Uint32(data[12:16])) 
	span.Cost = math.Float64frombits(binary.LittleEndian.Uint64(data[16:24])) 
	span.LatencyMs = int64(binary.LittleEndian.Uint64(data[24:32]))           
	span.Deleted = data[32] != 0

	off := fixedPayloadHeaderSize
	for _, f := range stringFields(&span) { 
		s, n, err := getString(data[off:])
		if err != nil {
			return WALRecord{}, err
		}
		*f = s
		off += n
	}

	if len(data)-off < blobLenSize { 
		return WALRecord{}, fmt.Errorf("wal payload truncated: want %d bytes for blob length, have %d", blobLenSize, len(data)-off)
	}
	blobLen := binary.LittleEndian.Uint32(data[off : off+blobLenSize])
	off += blobLenSize
	blobEnd := off + int(blobLen)
	if blobEnd > len(data) {
		return WALRecord{}, fmt.Errorf("wal payload truncated: want %d blob bytes, have %d", blobLen, len(data)-off)
	}
	payload := make([]byte, blobLen)
	copy(payload, data[off:blobEnd])
	span.Payload = payload

	return WALRecord{Span: span}, nil 
}

func putRecordHeader(buf []byte, version uint8, payload []byte) {
	buf[0] = version
	binary.LittleEndian.PutUint32(buf[1:5], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(payload)))
}

// WAL is a segmented write-ahead log. Records are appended to the active
// segment; once a segment exceeds maxSegmentSize, or the engine explicitly
// seals it via Rotate, a new segment is started. Segments are named after the
// base path with a generation suffix: wal.log -> wal_000001.log.
type WAL struct {
	mu             sync.Mutex
	dir            string
	prefix         string
	ext            string
	maxSegmentSize uint64
	segmentID      uint64
	size           uint64
	file           *os.File
}

func CreateWAL(path string) (*WAL, error) {
	return CreateWALWithSegmentSize(path, DefaultMaxSegmentSize)
}

func CreateWALWithSegmentSize(path string, maxSegmentSize uint64) (*WAL, error) {
	if maxSegmentSize == 0 {
		maxSegmentSize = DefaultMaxSegmentSize
	}
	ext := filepath.Ext(path)
	w := &WAL{
		dir:            filepath.Dir(path),
		prefix:         strings.TrimSuffix(filepath.Base(path), ext),
		ext:            ext,
		maxSegmentSize: maxSegmentSize,
	}

	ids, err := w.listSegmentIDs()
	if err != nil {
		return nil, err
	}

	// A pre-segmentation WAL is a single file sitting at the base path. Adopt
	// it as segment 1 rather than silently ignoring the records it holds.
	if len(ids) == 0 {
		if info, statErr := os.Stat(path); statErr == nil && !info.IsDir() {
			if err := os.Rename(path, w.segmentPath(1)); err != nil {
				return nil, fmt.Errorf("migrate legacy wal %s: %w", path, err)
			}
			ids = []uint64{1}
		}
	}

	next := uint64(1)
	if len(ids) > 0 {
		next = ids[len(ids)-1]
	}
	if err := w.openSegment(next); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) segmentPath(id uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%s_%06d%s", w.prefix, id, w.ext))
}

// listSegmentIDs returns the ids of every segment on disk, ascending.
func (w *WAL) listSegmentIDs() ([]uint64, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var ids []uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, w.prefix+"_") || !strings.HasSuffix(name, w.ext) {
			continue
		}
		idPart := strings.TrimSuffix(strings.TrimPrefix(name, w.prefix+"_"), w.ext)
		id, err := strconv.ParseUint(idPart, 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func (w *WAL) openSegment(id uint64) error {
	file, err := os.OpenFile(w.segmentPath(id), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}
	w.file = file
	w.segmentID = id
	w.size = uint64(info.Size())
	return nil
}

// recordBufPool holds reusable record-framing buffers (header + payload) for
// AppendRecord. AppendRecord builds this buffer before acquiring w.mu, so
// concurrent callers can be encoding at the same time; a sync.Pool gives each
// of them its own buffer to reuse without needing a lock of its own (unlike a
// single struct-held buffer, which would either race or require one).
var recordBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

// Append record to the active WAL segment, rotating first if it would overflow.
func (w *WAL) AppendRecord(record WALRecord) error {
	recordSize := recordHeaderSize + payloadEncodedSize(record)

	bufPtr := recordBufPool.Get().(*[]byte)
	defer recordBufPool.Put(bufPtr)
	buf := *bufPtr
	if cap(buf) < recordSize {
		buf = make([]byte, recordSize)
	}
	version := record.Metadata.Version
	if version == 0 {
		version = CurrentWALVersion
	}

	buf = buf[:recordSize]
	encodePayload(buf[recordHeaderSize:], record)
	putRecordHeader(buf, version, buf[recordHeaderSize:])
	*bufPtr = buf

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return fmt.Errorf("wal is closed")
	}

	// size > 0 keeps a single oversized record from rotating forever; it gets
	// a segment to itself instead.
	if w.size > 0 && w.size+uint64(recordSize) > w.maxSegmentSize {
		if _, err := w.rotateLocked(); err != nil {
			return err
		}
	}

	n, err := w.file.Write(buf)
	w.size += uint64(n)
	if err != nil {
		return err
	}
	return w.file.Sync()
}

// Rotate seals the active segment and starts a new one. It returns the id of
// the segment that was sealed, which is the highest segment whose records are
// now immutable and eligible for reclamation once they reach an SSTable.
func (w *WAL) Rotate() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateLocked()
}

func (w *WAL) rotateLocked() (uint64, error) {
	sealed := w.segmentID
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return 0, err
		}
		if err := w.file.Close(); err != nil {
			return 0, err
		}
		w.file = nil
	}
	if err := w.openSegment(sealed + 1); err != nil {
		return 0, err
	}
	return sealed, nil
}

// CurrentSegment returns the id of the segment currently being appended to.
func (w *WAL) CurrentSegment() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segmentID
}

// Segments returns the ids of every segment currently on disk, ascending.
func (w *WAL) Segments() ([]uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.listSegmentIDs()
}

// RemoveSegmentsUpTo deletes every sealed segment with an id <= id. The active
// segment is never removed, so callers cannot accidentally discard the records
// that have not yet been flushed.
func (w *WAL) RemoveSegmentsUpTo(id uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	ids, err := w.listSegmentIDs()
	if err != nil {
		return err
	}
	var firstErr error
	for _, segmentID := range ids {
		if segmentID > id || segmentID == w.segmentID {
			continue
		}
		if err := os.Remove(w.segmentPath(segmentID)); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Replay reads every segment in order after a restart or crash.
func (w *WAL) Replay() ([]WALRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return nil, err
		}
	}

	ids, err := w.listSegmentIDs()
	if err != nil {
		return nil, err
	}

	var records []WALRecord
	for i, id := range ids {
		// Only the final segment can hold a torn tail from a crash mid-append.
		segmentRecords, err := replaySegment(w.segmentPath(id), i == len(ids)-1)
		if err != nil {
			return nil, fmt.Errorf("replay wal segment %d: %w", id, err)
		}
		records = append(records, segmentRecords...)
	}
	return records, nil
}

func replaySegment(path string, tolerateTornTail bool) ([]WALRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var records []WALRecord
	for {
		header := make([]byte, recordHeaderSize)
		if _, err := io.ReadFull(reader, header); err != nil {
			if err == io.EOF {
				break
			}
			// A short header is a partial append; anything else is a real
			// read failure.
			if tolerateTornTail && errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		version := header[0]
		checksum := binary.LittleEndian.Uint32(header[1:5])
		size := binary.LittleEndian.Uint32(header[5:9])
		if uint64(size) > maxRecordSize {
			return nil, fmt.Errorf("WAL record size %d exceeds maximum %d", size, maxRecordSize)
		}
		if version != CurrentWALVersion {
			return nil, fmt.Errorf("WAL record version %d unsupported (this build reads version %d)", version, CurrentWALVersion)
		}

		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if tolerateTornTail && (err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF)) {
				break
			}
			return nil, err
		}
		// A checksum mismatch is always corruption: every append is fsynced, so
		// a crash truncates the tail rather than rewriting a complete record.
		if crc32.ChecksumIEEE(payload) != checksum {
			return nil, fmt.Errorf("WAL checksum mismatch")
		}
		record, err := deserializePayload(payload)
		if err != nil {
			return nil, err
		}
		record.Metadata.Version = version
		records = append(records, record)
	}
	return records, nil
}

// Reset discards every segment and restarts the log from segment 1.
func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
	}

	ids, err := w.listSegmentIDs()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := os.Remove(w.segmentPath(id)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return w.openSegment(1)
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}
