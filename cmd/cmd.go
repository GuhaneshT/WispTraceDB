package cmd

import (
	"fmt"
	"os"
	"sync"

	"github.com/GuhaneshT/WispTraceDB/compactor"
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
	defaultManifestPath     = "manifest.dat"

	// defaultSegmentFlushThreshold is a placeholder pending benchmarking,
	defaultSegmentFlushThreshold = 1000
)

type WispTraceConfig struct {
	WALPath               string
	WALMaxSegmentSize     uint64
	PebblePath            string
	SegmentDir            string
	CheckpointPath        string
	ManifestPath          string
	SegmentFlushThreshold int
}

type WispTrace struct {
	mu      sync.RWMutex
	flushMu sync.Mutex

	config WispTraceConfig

	wal        *wal.WAL
	index      *pebble.DB
	checkpoint *Checkpoint
	manifest   *segment.Manifest

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
		ManifestPath:          defaultManifestPath,
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
	if config.ManifestPath == "" {
		config.ManifestPath = defaultManifestPath
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
		manifest:      segment.NewManifest(config.ManifestPath),
		segmentWriter: segment.NewWriter(),
		nextSegmentID: lastConfirmedSegment + 1,
	}

	if err := wt.recover(); err != nil {
		_ = walInstance.Close()
		_ = indexInstance.Close()
		return nil, fmt.Errorf("recover: %w", err)
	}

	return wt, nil
}

// recover rebuilds any buffered-but-unflushed state from the WAL after a restart.
func (w *WispTrace) recover() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	records, err := w.wal.Replay()
	if err != nil {
		return fmt.Errorf("replay wal: %w", err)
	}
	if len(records) == 0 {
		return nil
	}

	for _, record := range records {
		w.segmentWriter.Add(record.Span)
	}

	if err := w.flushSegmentLocked(); err != nil {
		return fmt.Errorf("flush recovered spans: %w", err)
	}
	return nil
}

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


func (w *WispTrace) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushSegmentLocked()
}


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

	// Record the new segment as live before advancing the checkpoint
	if err := w.manifest.AddSegment(result.SegmentID); err != nil {
		return fmt.Errorf("record segment %d in manifest: %w", result.SegmentID, err)
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


func (w *WispTrace) Compactor() *compactor.Compactor {
	return compactor.New(w.config.SegmentDir, w.manifest, w.index)
}

// LiveSegments returns the current manifest contents — the segment ids a
// compaction policy or query path should consider live.
func (w *WispTrace) LiveSegments() ([]uint64, error) {
	return w.manifest.Load()
}

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
