package cmd

import (
	"fmt"
	"sync"

	"github.com/GuhaneshT/WispTraceDB/pebble"
	"github.com/GuhaneshT/WispTraceDB/wal"
)

const (
	defaultWALPath          = "wal.log"
	defaultWALMaxSegmentSize = wal.DefaultMaxSegmentSize
	defaultPebblePath       = "wisp_lsm"
)

type WispTraceConfig struct {
	WALPath                string
	WALMaxSegmentSize      uint64
	PebblePath             string
}

type WispTrace struct {
	mu                  sync.RWMutex
	flushMu             sync.Mutex
	config              WispTraceConfig
	wal                 *wal.WAL
	index               *pebble.DB
	immutableWALSegment uint64
}

func CreateWisp() (*WispTrace, error) {
	return CreateWispTraceWithConfig(DefaultWispTraceConfig())
}

func DefaultWispTraceConfig() WispTraceConfig {
	return WispTraceConfig{
		WALPath:                defaultWALPath,
		WALMaxSegmentSize:      defaultWALMaxSegmentSize,
		PebblePath:             defaultPebblePath,
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

	wt := &WispTrace{
		config: config,
		wal:    walInstance,
		index:  indexInstance,
	}

	return wt, nil
}

// Close flushes and closes all resources (WAL, Pebble index).
func (w *WispTrace) Close() error {
	var errs []error

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