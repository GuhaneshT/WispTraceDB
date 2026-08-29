package cmd

import (
	"fmt"
	"os"
	"sync"

	"github.com/GuhaneshT/WispTraceDB/pebble"
	"github.com/GuhaneshT/WispTraceDB/segment"
	"github.com/GuhaneshT/WispTraceDB/wal"
)

const (
	defaultWALPath          = "wal.log"
	defaultWALMaxSegmentSize = wal.DefaultMaxSegmentSize
	defaultPebblePath       = "wisp_lsm"
	defaultSegmentDir       = "segments"
	defaultCheckpointPath   = "checkpoint.dat"

	// defaultSegmentFlushThreshold is a placeholder pending benchmarking,
	// same posture PAD1 §12 step 1 requires for the WAL's own rotation size:
	// a tuned-for-testing value now, revisited once real ingestion patterns
	// exist. Not a claim that 1000 spans/segment is the right production number.
	defaultSegmentFlushThreshold = 1000
)

type WispTraceConfig struct {
	WALPath                string
	WALMaxSegmentSize      uint64
	PebblePath             string
	SegmentDir             string
	CheckpointPath         string
	SegmentFlushThreshold  int
}

type WispTrace struct {
	mu      sync.RWMutex
	flushMu sync.Mutex

	config WispTraceConfig

	wal        *wal.WAL
	index      *pebble.DB
	checkpoint *Checkpoint

	segmentWriter *segment.Writer
	nextSegmentID uint64
}

func CreateWisp() (*WispTrace, error) {
	return CreateWispTraceWithConfig(DefaultWispTraceConfig())
}

func DefaultWispTraceConfig() WispTraceConfig {
	return WispTraceConfig{
		WALPath:               defaultWALPath,
		WALMaxSegmentSize:     defaultWALMaxSegmentSize,
		PebblePath:            defaultPebblePath,
		SegmentDir:            defaultSegmentDir,
		CheckpointPath:        defaultCheckpointPath,
		SegmentFlushThreshold: defaultSegmentFlushThreshold,
	}
}

func CreateWispTraceWithConfig(config WispTraceConfig) (*WispTrace, error) {
	if config.WALPath == "" {
		config.WALPath = defaultWALPath
	}
	if config.WALMaxSegmentSize == 0 {
		config.WALMaxSegmentSize = defaultWALMaxSegmentSize
	}
	if config.PebblePath == "" {
		config.PebblePath = defaultPebblePath
	}
	if config.SegmentDir == "" {
		config.SegmentDir = defaultSegmentDir
	}
	if config.CheckpointPath == "" {
		config.CheckpointPath = defaultCheckpointPath
	}
	if config.SegmentFlushThreshold == 0 {
		config.SegmentFlushThreshold = defaultSegmentFlushThreshold
	}

	if err := os.MkdirAll(config.SegmentDir, 0755); err != nil {
		return nil, fmt.Errorf("create segment dir: %w", err)
	}

	// Open WAL (Phase 1 — Durable ingestion)
	walInstance, err := wal.CreateWALWithSegmentSize(config.WALPath, config.WALMaxSegmentSize)
	if err != nil {
		return nil, fmt.Errorf("create wal: %w", err)
	}

	// Open Pebble trace index (Phase 2 — Indexing & reconciliation)
	indexInstance, err := pebble.OpenDB(config.PebblePath)
	if err != nil {
		_ = walInstance.Close()
		return nil, fmt.Errorf("open pebble index: %w", err)
	}

	checkpoint := NewCheckpoint(config.CheckpointPath)
	lastConfirmedSegment, err := checkpoint.Load()
	if err != nil {
		_ = walInstance.Close()
		_ = indexInstance.Close()
		return nil, fmt.Errorf("load checkpoint: %w", err)
	}

	wt := &WispTrace{
		config:        config,
		wal:           walInstance,
		index:         indexInstance,
		checkpoint:    checkpoint,
		segmentWriter: segment.NewWriter(),
		// Segment ids are never reused — the next one to write is always
		// one past the last confirmed. On a fresh database (checkpoint = 0)
		// this starts at 1, matching the WAL's own generation-1 convention.
		nextSegmentID: lastConfirmedSegment + 1,
	}

	// NOTE: full crash-recovery replay (rebuilding any WAL records after the
	// checkpoint watermark that never made it into a confirmed segment) is
	// not yet implemented here — see PAD1 §4's reconciliation protocol.
	// Until it lands, a crash between AppendRecord and a successful
	// flushSegmentLocked loses those buffered-but-unflushed spans from the
	// derived (segment+index) state, even though the WAL itself still has
	// them. This is a known, tracked gap, not a silent one.

	return wt, nil
}

// InsertSpan is the entry point for Pillar 1 (PAD1 §3): append the span to
// the WAL (durable immediately, fsynced), then buffer it for the current
// segment. Once the buffer crosses SegmentFlushThreshold, the segment is
// flushed and indexed before InsertSpan returns — so a caller that gets a
// nil error back knows the flush (if one was triggered) has already gone
// through the full reconciliation protocol, not just the WAL append.
func (w *WispTrace) InsertSpan(span wal.SpanPayload) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.wal.AppendRecord(wal.WALRecord{Span: span}); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}

	w.segmentWriter.Add(span)

	if w.segmentWriter.Len() >= w.config.SegmentFlushThreshold {
		if err := w.flushSegmentLocked(); err != nil {
			return fmt.Errorf("flush segment: %w", err)
		}
	}

	return nil
}

// Flush forces the currently buffered spans to be written and indexed as a
// segment, even if SegmentFlushThreshold hasn't been reached. Useful for
// tests and graceful shutdown; a no-op if nothing is buffered.
func (w *WispTrace) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushSegmentLocked()
}

// flushSegmentLocked runs PAD1 §4's reconciliation protocol end to end:
//
//  1. Seal the WAL segment(s) carrying the spans about to be flushed. This
//     is the same pattern WispDB's freezeMutableLocked uses before flushing
//     a memtable (wal.Rotate() to seal, then reclaim once the flush lands) —
//     applied here to a segment flush instead of an SSTable flush.
//  2. Write the segment file (fsynced on return by Writer.Flush).
//  3. Index every span it contains into Pebble in one atomic batch.
//  4. Only once both 2 and 3 are confirmed, advance the checkpoint —
//     this is the line that makes "durable AND queryable" true, per §4.
//  5. Only once the checkpoint has advanced, reclaim the sealed WAL
//     segments — they are now redundant with the segment file + index.
//
// If step 2 or 3 fails, the WAL segment sealed in step 1 is NOT reclaimed,
// so those records remain replayable — the buffered-but-unflushed spans are
// not lost, just not yet visible outside a WAL replay (see the recovery gap
// noted in CreateWispTraceWithConfig).
//
// Must be called with w.mu held (InsertSpan and Flush both do this).
func (w *WispTrace) flushSegmentLocked() error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	if w.segmentWriter.Len() == 0 {
		return nil
	}

	sealedWALSegment, err := w.wal.Rotate()
	if err != nil {
		return fmt.Errorf("rotate wal: %w", err)
	}

	result, err := w.segmentWriter.Flush(w.config.SegmentDir, w.nextSegmentID)
	if err != nil {
		return fmt.Errorf("write segment %d: %w", w.nextSegmentID, err)
	}

	entries := make(map[string]pebble.SpanLocation, len(result.Offsets))
	for key, offset := range result.Offsets {
		entries[key] = pebble.SpanLocation{SegmentID: result.SegmentID, Offset: offset}
	}
	if err := w.index.BatchPutSpans(entries); err != nil {
		return fmt.Errorf("index segment %d: %w", result.SegmentID, err)
	}

	if err := w.checkpoint.Save(result.SegmentID); err != nil {
		return fmt.Errorf("save checkpoint at segment %d: %w", result.SegmentID, err)
	}

	if err := w.wal.RemoveSegmentsUpTo(sealedWALSegment); err != nil {
		return fmt.Errorf("reclaim wal segments up to %d: %w", sealedWALSegment, err)
	}

	w.nextSegmentID++
	w.segmentWriter = segment.NewWriter()
	return nil
}

// Close flushes any buffered spans, then closes the WAL and Pebble index.
// Errors from all three steps are collected rather than short-circuited, so
// a caller sees every resource that failed to close cleanly, not just the
// first one.
func (w *WispTrace) Close() error {
	var errs []error

	w.mu.Lock()
	if err := w.flushSegmentLocked(); err != nil {
		errs = append(errs, fmt.Errorf("final flush: %w", err))
	}
	w.mu.Unlock()

	if w.wal != nil {
		if err := w.wal.Close(); err != nil {
			errs = append(errs, fmt.Errorf("wal close: %w", err))
		}
	}

	if w.index != nil {
		if err := w.index.Close(); err != nil {
			errs = append(errs, fmt.Errorf("index close: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("wisptrace close errors: %v", errs)
	}
	return nil
}
