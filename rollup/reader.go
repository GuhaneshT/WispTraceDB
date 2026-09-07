package rollup

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

func ReadSnapshot(path string, windowSize int64) (*Store, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open rollup snapshot %s: %w", path, err)
	}
	defer file.Close()

	
	var headerBuf [rollupHeaderSize]byte
	if _, err := io.ReadFull(file, headerBuf[:]); err != nil {
		return nil, fmt.Errorf("read rollup header: %w", err)
	}

	magic := binary.LittleEndian.Uint32(headerBuf[0:4])
	if magic != RollupMagic {
		return nil, fmt.Errorf("bad rollup magic: got %#x, want %#x", magic, RollupMagic)
	}
	version := binary.LittleEndian.Uint16(headerBuf[4:6])
	if version != CurrentRollupVersion {
		return nil, fmt.Errorf("unsupported rollup version %d (this build reads version %d)",
			version, CurrentRollupVersion)
	}
	storedWindowSize := int64(binary.LittleEndian.Uint64(headerBuf[6:14]))
	if storedWindowSize != windowSize {
		return nil, fmt.Errorf(
			"rollup window size mismatch: file has %d ns, expected %d ns",
			storedWindowSize, windowSize,
		)
	}
	recordCount := int64(binary.LittleEndian.Uint64(headerBuf[14:22]))

	store := NewStore(windowSize)
	reader := bufio.NewReader(file)

	for i := int64(0); i < recordCount; i++ {
		var framing [rollupFramingSize]byte
		if _, err := io.ReadFull(reader, framing[:]); err != nil {
			return nil, fmt.Errorf("read record framing at index %d: %w", i, err)
		}
		length := binary.LittleEndian.Uint32(framing[0:4])
		checksum := binary.LittleEndian.Uint32(framing[4:8])

		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, fmt.Errorf("read record payload at index %d: %w", i, err)
		}
		if crc32.ChecksumIEEE(payload) != checksum {
			return nil, fmt.Errorf("rollup record checksum mismatch at index %d", i)
		}

		agg, err := decodeRecord(payload)
		if err != nil {
			return nil, fmt.Errorf("decode rollup record at index %d: %w", i, err)
		}

		// Write pre-aggregated values directly into the bucket map.
		// This is safe: store is brand-new and not shared yet; no lock needed.
		valueCopy := agg.Value
		store.buckets[agg.BucketKey] = &valueCopy
	}

	return store, nil
}


func decodeRecord(payload []byte) (AggregatedMetrics, error) {
	// Minimum: 8 (WindowStart) + 2 (model len uint16) = 10 bytes before model.
	if len(payload) < 10 {
		return AggregatedMetrics{}, fmt.Errorf("rollup record too short: %d bytes", len(payload))
	}

	off := 0
	windowStart := int64(binary.LittleEndian.Uint64(payload[off:]))
	off += 8
	modelLen := int(binary.LittleEndian.Uint16(payload[off:]))
	off += 2

	if len(payload)-off < modelLen+7*8 {
		return AggregatedMetrics{}, fmt.Errorf(
			"rollup record truncated: need %d more bytes, have %d",
			modelLen+7*8, len(payload)-off,
		)
	}

	model := string(payload[off : off+modelLen])
	off += modelLen

	count := int64(binary.LittleEndian.Uint64(payload[off:]))
	off += 8
	sumCost := int64(binary.LittleEndian.Uint64(payload[off:]))
	off += 8
	sumTokensIn := int64(binary.LittleEndian.Uint64(payload[off:]))
	off += 8
	sumTokensOut := int64(binary.LittleEndian.Uint64(payload[off:]))
	off += 8
	sumLatencyMs := int64(binary.LittleEndian.Uint64(payload[off:]))
	off += 8
	minLatencyMs := int64(binary.LittleEndian.Uint64(payload[off:]))
	off += 8
	maxLatencyMs := int64(binary.LittleEndian.Uint64(payload[off:]))
	_ = off 

	return AggregatedMetrics{
		BucketKey: BucketKey{WindowStart: windowStart, Model: model},
		Value: Value{
			Count:        count,
			SumCost:      sumCost,
			SumTokensIn:  sumTokensIn,
			SumTokensOut: sumTokensOut,
			SumLatencyMs: sumLatencyMs,
			MinLatencyMs: minLatencyMs,
			MaxLatencyMs: maxLatencyMs,
		},
	}, nil
}
