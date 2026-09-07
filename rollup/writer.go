package rollup

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)


type AggregatedMetrics struct {
	BucketKey BucketKey
	Value     Value
}

const (
	RollupMagic uint32 = 0x574C4F52

	CurrentRollupVersion uint16 = 1

	// Header layout (38 bytes total):
	//   Magic           [0:4]    4 bytes  — 0x574C4F52
	//   Version         [4:6]    2 bytes  — uint16 LE
	//   WindowSize      [6:14]   8 bytes  — int64 LE (nanoseconds)
	//   RecordCount     [14:22]  8 bytes  — int64 LE
	//   MinWindowStart  [22:30]  8 bytes  — int64 LE (zone metadata)
	//   MaxWindowStart  [30:38]  8 bytes  — int64 LE (zone metadata)
	rollupHeaderSize = 4 + 2 + 8 + 8 + 8 + 8 
	rollupFramingSize = 4 + 4 
)

type Writer struct {
	aggregatedMetrics []AggregatedMetrics
}

func NewWriter() *Writer {
	return &Writer{aggregatedMetrics: make([]AggregatedMetrics, 0, 1024)}
}

func (w *Writer) Add(metric AggregatedMetrics) {
	w.aggregatedMetrics = append(w.aggregatedMetrics, metric)
}

func (w *Writer) Len() int {
	return len(w.aggregatedMetrics)
}

func RollupPath(dir string, rollupID uint64) string {
	return filepath.Join(dir, fmt.Sprintf("rollup_%06d.rdb", rollupID))
}

func CompositeKey(windowStart int64, model string) string {
	return fmt.Sprintf("%d||%s", windowStart, model)
}

func (w *Writer) Flush(dir string, rollupID uint64, windowSize int64) error {
	if len(w.aggregatedMetrics) == 0 {
		return fmt.Errorf("rollup writer: flush called with no buffered metrics")
	}

	path := RollupPath(dir, rollupID)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create rollup file: %w", err)
	}

	success := false
	defer func() {
		file.Close()
		if !success {
			os.Remove(path)
		}
	}()

	bw := bufio.NewWriter(file)

	
	minWS := w.aggregatedMetrics[0].BucketKey.WindowStart
	maxWS := w.aggregatedMetrics[0].BucketKey.WindowStart
	for _, m := range w.aggregatedMetrics[1:] {
		if m.BucketKey.WindowStart < minWS {
			minWS = m.BucketKey.WindowStart
		}
		if m.BucketKey.WindowStart > maxWS {
			maxWS = m.BucketKey.WindowStart
		}
	}

	if err := writeRollupHeader(bw, windowSize, int64(len(w.aggregatedMetrics)), minWS, maxWS); err != nil {
		return fmt.Errorf("write rollup header: %w", err)
	}
	for _, m := range w.aggregatedMetrics {
		if _, err := writeRollupRecord(bw, m); err != nil {
			return fmt.Errorf("write rollup record: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("flush rollup buffer: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("fsync rollup file: %w", err)
	}

	success = true
	return nil
}

func writeRollupHeader(w *bufio.Writer, windowSize, recordCount, minWindowStart, maxWindowStart int64) error {
	var buf [rollupHeaderSize]byte
	binary.LittleEndian.PutUint32(buf[0:4], RollupMagic)
	binary.LittleEndian.PutUint16(buf[4:6], CurrentRollupVersion)
	binary.LittleEndian.PutUint64(buf[6:14], uint64(windowSize))
	binary.LittleEndian.PutUint64(buf[14:22], uint64(recordCount))
	binary.LittleEndian.PutUint64(buf[22:30], uint64(minWindowStart))
	binary.LittleEndian.PutUint64(buf[30:38], uint64(maxWindowStart))
	_, err := w.Write(buf[:])
	return err
}

func encodeRecord(agg AggregatedMetrics) []byte {
	model := []byte(agg.BucketKey.Model)
	buf := make([]byte, 8+2+len(model)+7*8)
	off := 0

	binary.LittleEndian.PutUint64(buf[off:], uint64(agg.BucketKey.WindowStart))
	off += 8
	binary.LittleEndian.PutUint16(buf[off:], uint16(len(model)))
	off += 2
	copy(buf[off:], model)
	off += len(model)

	binary.LittleEndian.PutUint64(buf[off:], uint64(agg.Value.Count))
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], uint64(agg.Value.SumCost))
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], uint64(agg.Value.SumTokensIn))
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], uint64(agg.Value.SumTokensOut))
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], uint64(agg.Value.SumLatencyMs))
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], uint64(agg.Value.MinLatencyMs))
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], uint64(agg.Value.MaxLatencyMs))

	return buf
}

func writeRollupRecord(w *bufio.Writer, agg AggregatedMetrics) (int, error) {
	payload := encodeRecord(agg)

	var framing [rollupFramingSize]byte
	binary.LittleEndian.PutUint32(framing[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(framing[4:8], crc32.ChecksumIEEE(payload))

	if _, err := w.Write(framing[:]); err != nil {
		return 0, err
	}
	if _, err := w.Write(payload); err != nil {
		return 0, err
	}
	return rollupFramingSize + len(payload), nil
}
