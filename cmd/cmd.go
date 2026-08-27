package cmd

import (
	"sync"

	"github.com/GuhaneshT/WispTraceDB/wal"
)


const (
	defaultWALPath          = "wal.log"
	defaultWALMaxSegmentSize = wal.DefaultMaxSegmentSize
)

type WispTraceConfig struct {
	WALPath                string
	WALMaxSegmentSize      uint64
}

type WispTrace struct {
	mu                sync.RWMutex
	flushMu           sync.Mutex
	config            WispTraceConfig
	wal               *wal.WAL
	immutableWALSegment uint64
}

func CreateWisp() (*WispTrace, error) {
	return CreateWispTraceWithConfig(DefaultWispTraceConfig())
}

func DefaultWispTraceConfig() WispTraceConfig {
	return WispTraceConfig{
		WALPath:                defaultWALPath,
		WALMaxSegmentSize:      defaultWALMaxSegmentSize,
	}
}

func CreateWispTraceWithConfig(config WispTraceConfig) (*WispTrace, error) {
	if config.WALPath == "" {
		config.WALPath = defaultWALPath
	}
	// if config.SSTablePath == "" {
	// 	config.SSTablePath = defaultSSTablePath
	// }
	// if config.SSTableBlockSize == 0 {
	// 	config.SSTableBlockSize = defaultSSTableBlockSize
	// }
	// if config.MemTableFlushThreshold == 0 {
	// 	config.MemTableFlushThreshold = defaultMemTableThreshold
	// }
	if config.WALMaxSegmentSize == 0 {
		config.WALMaxSegmentSize = defaultWALMaxSegmentSize
	}
	// if config.SSTableList == nil {
	// 	config.SSTableList = &sstable.SSTableList{}
	// }

	walInstance, err := wal.CreateWALWithSegmentSize(config.WALPath, config.WALMaxSegmentSize)
	if err != nil {
		return nil, err
	}
	// mutableMemTable, err := memtable.CreateMemTableWithThreshold(config.MemTableFlushThreshold)
	// if err != nil {
	// 	_ = walInstance.Close()
	// 	return nil, err
	// }
	WispTrace := &WispTrace{config: config, wal: walInstance}
	// if err := WispTrace.openSSTables(); err != nil {
	// 	_ = WispTrace.Close()
	// 	return nil, err
	// }
	// if err := WispTrace.Recover(); err != nil {
	// 	_ = WispTrace.Close()
	// 	return nil, err
	// }
	return WispTrace, nil
}