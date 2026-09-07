package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/GuhaneshT/WispTraceDB/bloom"
	"github.com/GuhaneshT/WispTraceDB/compactor"
	"github.com/GuhaneshT/WispTraceDB/pebble"
	"github.com/GuhaneshT/WispTraceDB/rollup"
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

	defaultSegmentFlushThreshold      = 1000
	defaultCompactionSegmentThreshold = 10
	defaultRollupSnapshotInterval     = 5 * time.Minute
	defaultCompactionInterval         = 10 * time.Minute
	defaultRetentionPeriod            = 7 * 24 * time.Hour
	defaultSessionTimeout             = 5 * time.Second
)

type WispTraceConfig struct {
	WALPath                     string
	WALMaxSegmentSize           uint64
	PebblePath                  string
	SegmentDir                  string
	CheckpointPath              string
	ManifestPath                string
	SegmentFlushThreshold       int
	CompactionSegmentThreshold  int
	RollupSnapshotInterval      time.Duration
	CompactionInterval          time.Duration
	RetentionPeriod             time.Duration
	SessionTimeout              time.Duration
}

type traceState struct {
	spans    []wal.SpanPayload
	lastSeen int64 
}

type WispTrace struct {
	config WispTraceConfig

	
	wal        *wal.WAL
	index      *pebble.DB
	checkpoint *Checkpoint
	manifest   *segment.Manifest
	rollups    *rollup.RollupManager

	// Background loop lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	bgWg   sync.WaitGroup

	// Hotpath ingestion
	ingestChan chan wal.SpanPayload
	ingestWg   sync.WaitGroup

	// Session buffering 
	sessionMu        sync.Mutex
	sessionBuffer    map[string]*traceState
	sessionSpanCount int

	// segment writer state
	flushMu       sync.Mutex
	nextSegmentID uint64
}

func CreateWisp() (*WispTrace, error) {
	return CreateWispTraceWithConfig(DefaultWispTraceConfig())
}

func DefaultWispTraceConfig() WispTraceConfig {
	return WispTraceConfig{
		WALPath:                    defaultWALPath,
		WALMaxSegmentSize:          defaultWALMaxSegmentSize,
		PebblePath:                 defaultPebblePath,
		SegmentDir:                 defaultSegmentDir,
		CheckpointPath:             defaultCheckpointPath,
		ManifestPath:               defaultManifestPath,
		SegmentFlushThreshold:      defaultSegmentFlushThreshold,
		CompactionSegmentThreshold: defaultCompactionSegmentThreshold,
		RollupSnapshotInterval:     defaultRollupSnapshotInterval,
		CompactionInterval:         defaultCompactionInterval,
		RetentionPeriod:            defaultRetentionPeriod,
		SessionTimeout:             defaultSessionTimeout,
	}
}

func CreateWispTraceWithConfig(config WispTraceConfig) (*WispTrace, error) {
	// Apply defaults for any zero values
	if config.WALPath == "" { config.WALPath = defaultWALPath }
	if config.WALMaxSegmentSize == 0 { config.WALMaxSegmentSize = defaultWALMaxSegmentSize }
	if config.PebblePath == "" { config.PebblePath = defaultPebblePath }
	if config.SegmentDir == "" { config.SegmentDir = defaultSegmentDir }
	if config.CheckpointPath == "" { config.CheckpointPath = defaultCheckpointPath }
	if config.ManifestPath == "" { config.ManifestPath = defaultManifestPath }
	if config.SegmentFlushThreshold == 0 { config.SegmentFlushThreshold = defaultSegmentFlushThreshold }
	if config.CompactionSegmentThreshold == 0 { config.CompactionSegmentThreshold = defaultCompactionSegmentThreshold }
	if config.RollupSnapshotInterval == 0 { config.RollupSnapshotInterval = defaultRollupSnapshotInterval }
	if config.CompactionInterval == 0 { config.CompactionInterval = defaultCompactionInterval }
	if config.RetentionPeriod == 0 { config.RetentionPeriod = defaultRetentionPeriod }
	if config.SessionTimeout == 0 { config.SessionTimeout = defaultSessionTimeout }

	if err := os.MkdirAll(config.SegmentDir, 0755); err != nil {
		return nil, fmt.Errorf("create segment dir: %w", err)
	}

	walInstance, err := wal.CreateWALWithSegmentSize(config.WALPath, config.WALMaxSegmentSize)
	if err != nil {
		return nil, fmt.Errorf("create wal: %w", err)
	}

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

	ctx, cancel := context.WithCancel(context.Background())

	wt := &WispTrace{
		config:        config,
		wal:           walInstance,
		index:         indexInstance,
		checkpoint:    checkpoint,
		manifest:      segment.NewManifest(config.ManifestPath),
		rollups:       rollup.NewRollupManager(),
		ctx:           ctx,
		cancel:        cancel,
		ingestChan:    make(chan wal.SpanPayload, 10000), // Buffer for bursty ingest
		sessionBuffer: make(map[string]*traceState),
		nextSegmentID: lastConfirmedSegment + 1,
	}

	// 1. Load Rollup Snapshots
	if err := wt.loadRollupSnapshots(); err != nil {
		wt.Close()
		return nil, fmt.Errorf("load rollup snapshots: %w", err)
	}

	// 2. Recover un-checkpointed WAL spans (tail)
	if err := wt.recover(); err != nil {
		wt.Close()
		return nil, fmt.Errorf("recover: %w", err)
	}

	// 3. Start background loops
	wt.bgWg.Add(4)
	go wt.ingestLoop()
	go wt.segmentFlushLoop()
	go wt.rollupSnapshotLoop()
	go wt.compactionLoop()

	return wt, nil
}

func (w *WispTrace) loadRollupSnapshots() error {
	windows := []struct {
		name string
		size int64
		dest **rollup.Store
	}{
		{rollup.WindowMinute, rollup.WindowSizeMinute, &w.rollups.Minute},
		{rollup.WindowFiveMin, rollup.WindowSizeFiveMin, &w.rollups.FiveMin},
		{rollup.WindowHour, rollup.WindowSizeHour, &w.rollups.Hour},
		{rollup.WindowDay, rollup.WindowSizeDay, &w.rollups.Day},
	}

	for _, win := range windows {
		path := rollup.RollupPath(w.config.SegmentDir, win.name)
		store, err := rollup.ReadSnapshot(path, win.size)
		if err != nil {
			if os.IsNotExist(err) {
				continue // perfectly fine on first boot
			}
			return fmt.Errorf("failed to load %s rollup: %w", win.name, err)
		}
		*win.dest = store
	}
	return nil
}

func (w *WispTrace) recover() error {
	records, err := w.wal.Replay()
	if err != nil {
		return fmt.Errorf("replay wal: %w", err)
	}
	if len(records) == 0 {
		return nil
	}

	for _, record := range records {
		w.processSpan(record.Span)
	}

	// Flush whatever was recovered immediately before booting
	if err := w.flushAllSessions(); err != nil {
		return fmt.Errorf("flush recovered spans: %w", err)
	}
	return nil
}

func (w *WispTrace) InsertSpan(span wal.SpanPayload) error {
	// 1. Durability: Appends are fsynced by the WAL
	if err := w.wal.AppendRecord(wal.WALRecord{Span: span}); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}

	// 2. Offload to background loops for derived state updates
	w.ingestWg.Add(1)
	select {
	case w.ingestChan <- span:
	case <-w.ctx.Done():
		w.ingestWg.Done()
		return fmt.Errorf("db closed")
	}

	return nil
}

func (w *WispTrace) ingestLoop() {
	defer w.bgWg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case span := <-w.ingestChan:
			w.processSpan(span)
			w.ingestWg.Done()
		}
	}
}

func (w *WispTrace) WaitForIngest() {
	w.ingestWg.Wait()
}

func (w *WispTrace) processSpan(span wal.SpanPayload) {
	// Feed Rollups
	w.rollups.Add(span.Timestamp, span.Model, int64(span.Cost), int64(span.TokensIn), int64(span.TokensOut), span.LatencyMs)

	// Feed Session Buffer
	w.sessionMu.Lock()
	state, ok := w.sessionBuffer[span.TraceID]
	if !ok {
		state = &traceState{spans: make([]wal.SpanPayload, 0, 8)}
		w.sessionBuffer[span.TraceID] = state
	}
	state.spans = append(state.spans, span)
	state.lastSeen = time.Now().UnixNano()
	w.sessionSpanCount++
	
	needsFlush := w.sessionSpanCount >= w.config.SegmentFlushThreshold
	w.sessionMu.Unlock()

	if needsFlush {
		_ = w.flushAllSessions()
	}
}

func (w *WispTrace) segmentFlushLoop() {
	defer w.bgWg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.flushExpiredSessions()
		}
	}
}

func (w *WispTrace) flushExpiredSessions() {
	now := time.Now().UnixNano()
	timeoutNs := w.config.SessionTimeout.Nanoseconds()

	var toFlush []wal.SpanPayload

	w.sessionMu.Lock()
	for traceID, state := range w.sessionBuffer {
		if now-state.lastSeen > timeoutNs {
			toFlush = append(toFlush, state.spans...)
			w.sessionSpanCount -= len(state.spans)
			delete(w.sessionBuffer, traceID)
		}
	}
	w.sessionMu.Unlock()

	if len(toFlush) > 0 {
		_ = w.writeSegmentBatch(toFlush)
	}
}

func (w *WispTrace) flushAllSessions() error {
	var toFlush []wal.SpanPayload
	w.sessionMu.Lock()
	for traceID, state := range w.sessionBuffer {
		toFlush = append(toFlush, state.spans...)
		delete(w.sessionBuffer, traceID)
	}
	w.sessionSpanCount = 0
	w.sessionMu.Unlock()

	if len(toFlush) > 0 {
		return w.writeSegmentBatch(toFlush)
	}
	return nil
}

func (w *WispTrace) writeSegmentBatch(spans []wal.SpanPayload) error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	sealedWALSegment, err := w.wal.Rotate()
	if err != nil {
		return fmt.Errorf("rotate wal: %w", err)
	}

	writer := segment.NewWriter()
	for _, span := range spans {
		writer.Add(span)
	}

	result, err := writer.Flush(w.config.SegmentDir, w.nextSegmentID)
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
	return nil
}

func (w *WispTrace) rollupSnapshotLoop() {
	defer w.bgWg.Done()
	ticker := time.NewTicker(w.config.RollupSnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.flushRollups()
		}
	}
}

func (w *WispTrace) flushRollups() {
	// Simple eviction: drop anything older than RetentionPeriod
	cutoff := time.Now().Add(-w.config.RetentionPeriod).UnixNano()

	w.rollups.Minute.Evict(cutoff)
	w.rollups.FiveMin.Evict(cutoff)
	w.rollups.Hour.Evict(cutoff)
	w.rollups.Day.Evict(cutoff)

	windows := []struct {
		name string
		size int64
		store *rollup.Store
	}{
		{rollup.WindowMinute, rollup.WindowSizeMinute, w.rollups.Minute},
		{rollup.WindowFiveMin, rollup.WindowSizeFiveMin, w.rollups.FiveMin},
		{rollup.WindowHour, rollup.WindowSizeHour, w.rollups.Hour},
		{rollup.WindowDay, rollup.WindowSizeDay, w.rollups.Day},
	}

	for _, win := range windows {
		writer := rollup.NewWriter()
		buckets := win.store.GetAllBuckets()
		for key, val := range buckets {
			writer.Add(rollup.AggregatedMetrics{BucketKey: key, Value: *val})
		}
		if writer.Len() > 0 {
			// Ignoring error in background loop, will retry on next tick
			_ = writer.Flush(w.config.SegmentDir, win.name, win.size)
		}
	}
}

func (w *WispTrace) compactionLoop() {
	defer w.bgWg.Done()
	ticker := time.NewTicker(w.config.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.maybeCompact()
			w.maybeExpire()
		}
	}
}

func (w *WispTrace) maybeCompact() {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	live, err := w.manifest.Load()
	if err != nil || len(live) <= w.config.CompactionSegmentThreshold {
		return
	}

	sort.Slice(live, func(i, j int) bool { return live[i] < live[j] })
	mergeCount := len(live) / 2
	if mergeCount < 2 {
		return
	}
	toMerge := live[:mergeCount]

	newSegmentID := w.nextSegmentID
	w.nextSegmentID++

	_, _ = w.Compactor().Compact(toMerge, newSegmentID)
}

func (w *WispTrace) maybeExpire() {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	live, err := w.manifest.Load()
	if err != nil || len(live) == 0 {
		return
	}
	
	cutoff := time.Now().Add(-w.config.RetentionPeriod).UnixNano()
	_, _ = w.Compactor().Expire(live, cutoff)
}

func (w *WispTrace) Compactor() *compactor.Compactor {
	return compactor.New(w.config.SegmentDir, w.manifest, w.index)
}

// GetSpan looks up one span by trace_id+span_id
func (w *WispTrace) GetSpan(traceID, spanID string) (span wal.SpanPayload, found bool, err error) {
	// First check memory buffer (hot path spans not yet in segment)
	w.sessionMu.Lock()
	if state, ok := w.sessionBuffer[traceID]; ok {
		for _, s := range state.spans {
			if s.SpanID == spanID && !s.Deleted {
				w.sessionMu.Unlock()
				return s, true, nil
			}
		}
	}
	w.sessionMu.Unlock()

	key := []byte(segment.CompositeKey(traceID, spanID))
	location, err := w.index.GetSpan(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return wal.SpanPayload{}, false, nil
		}
		return wal.SpanPayload{}, false, fmt.Errorf("index lookup: %w", err)
	}

	reader, err := segment.OpenReader(segment.SegmentPath(w.config.SegmentDir, location.SegmentID))
	if err != nil {
		return wal.SpanPayload{}, false, fmt.Errorf("open segment %d: %w", location.SegmentID, err)
	}
	defer reader.Close()

	span, err = reader.ReadAt(location.Offset)
	if err != nil {
		return wal.SpanPayload{}, false, fmt.Errorf("read span at segment %d offset %d: %w", location.SegmentID, location.Offset, err)
	}
	return span, true, nil
}

// GetTrace reconstructs a full trace by ID
func (w *WispTrace) GetTrace(traceID string) ([]wal.SpanPayload, bool, error) {
	prefix := []byte(segment.CompositeKey(traceID, ""))
	locations, err := w.index.PrefixScan(prefix)
	if err != nil {
		return nil, false, fmt.Errorf("index prefix scan: %w", err)
	}
	
	spans := make([]wal.SpanPayload, 0, len(locations))
	
	if len(locations) > 0 {
		readers := make(map[uint64]*segment.Reader)
		defer func() {
			for _, r := range readers {
				r.Close()
			}
		}()

		for _, loc := range locations {
			reader, ok := readers[loc.SegmentID]
			if !ok {
				reader, err = segment.OpenReader(segment.SegmentPath(w.config.SegmentDir, loc.SegmentID))
				if err != nil {
					return nil, false, fmt.Errorf("open segment %d: %w", loc.SegmentID, err)
				}
				readers[loc.SegmentID] = reader
			}

			span, err := reader.ReadAt(loc.Offset)
			if err != nil {
				return nil, false, fmt.Errorf("read span at segment %d offset %d: %w", loc.SegmentID, loc.Offset, err)
			}
			spans = append(spans, span)
		}
	}

	// Add any spans still in memory buffer
	w.sessionMu.Lock()
	if state, ok := w.sessionBuffer[traceID]; ok {
		spans = append(spans, state.spans...)
	}
	w.sessionMu.Unlock()

	if len(spans) == 0 {
		return nil, false, nil
	}
	return spans, true, nil
}

// RangeFilter selects spans by time range and, optionally, bounded-cardinality
// dimension values. An empty string on any dimension field means "don't filter".
type RangeFilter struct {
	StartTS, EndTS                        int64
	AgentID, Model, ToolName, Team, Status string
}

func (f RangeFilter) matches(s wal.SpanPayload) bool {
	if s.Timestamp < f.StartTS || s.Timestamp > f.EndTS {
		return false
	}
	if f.AgentID != "" && s.AgentID != f.AgentID {
		return false
	}
	if f.Model != "" && s.Model != f.Model {
		return false
	}
	if f.ToolName != "" && s.ToolName != f.ToolName {
		return false
	}
	if f.Team != "" && s.Team != f.Team {
		return false
	}
	if f.Status != "" && s.Status != f.Status {
		return false
	}
	return true
}

func (f RangeFilter) mayMatchBlooms(blooms map[string]*bloom.Filter) bool {
	checks := []struct {
		dim   string
		value string
	}{
		{"agent_id", f.AgentID},
		{"model", f.Model},
		{"tool_name", f.ToolName},
		{"team", f.Team},
		{"status", f.Status},
	}
	for _, c := range checks {
		if c.value == "" {
			continue
		}
		filter, ok := blooms[c.dim]
		if !ok {
			continue 
		}
		if !filter.MayContain(c.value) {
			return false
		}
	}
	return true
}

// RangeQuery returns every live span matching filter
func (w *WispTrace) RangeQuery(filter RangeFilter) ([]wal.SpanPayload, error) {
	live, err := w.manifest.Load()
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}

	var results []wal.SpanPayload
	for _, id := range live {
		reader, err := segment.OpenReader(segment.SegmentPath(w.config.SegmentDir, id))
		if err != nil {
			return nil, fmt.Errorf("open segment %d: %w", id, err)
		}

		if reader.Header.MaxTimestamp < filter.StartTS || reader.Header.MinTimestamp > filter.EndTS {
			reader.Close()
			continue
		}

		if !filter.mayMatchBlooms(reader.Blooms) {
			reader.Close()
			continue
		}

		spans, err := reader.ScanAll()
		closeErr := reader.Close()
		if err != nil {
			return nil, fmt.Errorf("scan segment %d: %w", id, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close segment %d reader: %w", id, closeErr)
		}

		for _, s := range spans {
			if s.Span.Deleted {
				continue
			}
			if filter.matches(s.Span) {
				results = append(results, s.Span)
			}
		}
	}

	// Add in-memory spans that match
	w.sessionMu.Lock()
	for _, state := range w.sessionBuffer {
		for _, s := range state.spans {
			if !s.Deleted && filter.matches(s) {
				results = append(results, s)
			}
		}
	}
	w.sessionMu.Unlock()

	return results, nil
}

func (w *WispTrace) Flush() error {
	return w.flushAllSessions()
}

// LiveSegments returns the current manifest contents
func (w *WispTrace) LiveSegments() ([]uint64, error) {
	return w.manifest.Load()
}

func (w *WispTrace) Close() error {
	var errs []error

	// 1. Signal background loops to stop and wait for them
	w.cancel()
	w.bgWg.Wait()

	// 2. Flush remaining in-memory spans to a segment
	if err := w.flushAllSessions(); err != nil {
		errs = append(errs, fmt.Errorf("final segment flush: %w", err))
	}

	// 3. Flush final rollup snapshots to disk
	w.flushRollups()

	// 4. Close underlying resources
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
