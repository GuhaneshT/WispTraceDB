package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
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
	defaultWALPath           = "wal.log"
	defaultWALMaxSegmentSize = wal.DefaultMaxSegmentSize
	defaultPebblePath        = "wisp_lsm"
	defaultSegmentDir        = "segments"
	defaultCheckpointPath    = "checkpoint.dat"
	defaultManifestPath      = "manifest.dat"

	defaultSegmentFlushThreshold      = 1000
	defaultCompactionSegmentThreshold = 10
	defaultRollupSnapshotInterval     = 5 * time.Minute
	defaultCompactionInterval         = 10 * time.Minute
	defaultRetentionPeriod            = 7 * 24 * time.Hour
	defaultSessionTimeout             = 5 * time.Second

	// MaxCardinalityPerQuery is the hard limit on distinct values returned by cardinality queries
	// (GetDistinctModels, GetDistinctTeams, GetDistinctAgents) to prevent OOM errors.
	MaxCardinalityPerQuery = 10000
)

type WispTraceConfig struct {
	WALPath                    string
	WALMaxSegmentSize          uint64
	PebblePath                 string
	SegmentDir                 string
	CheckpointPath             string
	ManifestPath               string
	SegmentFlushThreshold      int
	CompactionSegmentThreshold int
	RollupSnapshotInterval     time.Duration
	CompactionInterval         time.Duration
	RetentionPeriod            time.Duration
	SessionTimeout             time.Duration
}

// pendingSpan is an acknowledged span travelling from the WAL to a segment. It
// carries the WAL segment holding its record, which is the only other place
// that copy exists until the flush commits.
//
// walSeg is 0 for a span replayed out of the WAL during recovery: that copy is
// already on disk by definition and was never registered in the pending set.
type pendingSpan struct {
	span   wal.SpanPayload
	walSeg uint64
}

type traceState struct {
	spans    []pendingSpan
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
	ingestChan chan pendingSpan
	ingestWg   sync.WaitGroup
	ingestWgMu sync.Mutex // serializes ingestWg.Add against ingestWg.Wait — see WaitForIngest

	// Session buffering
	sessionMu        sync.Mutex
	sessionBuffer    map[string]*traceState
	sessionSpanCount int

	// segment writer state
	flushMu       sync.Mutex
	nextSegmentID uint64

	// recovering forces processSpan to skip its inline threshold flush (and
	// maybeCompactLocked to skip compaction) while a crash-recovery replay is
	// in flight. Recovery drains the whole WAL tail into a single final
	// segment, so the only flush that can run during recovery already covers
	// every record the WAL still holds; splitting that drain would let one
	// flush reclaim WAL the next one has not rewritten yet.
	recovering bool

	// walPending counts, per WAL segment id, the records that have been
	// fsynced (so acknowledged) but are not yet durable in a segment or
	// applied to the index. A WAL segment may be deleted only once nothing
	// pending references it, which makes "lowest pending id" the exact
	// reclamation bound — no prediction about which flush covered which
	// record, and no assumption that the WAL, the ingest channel and the
	// session buffer advance in lockstep.
	//
	// walPendingMu is held across the WAL append itself (see
	// appendTracked) and across the flush path's reclamation
	// (releaseWALPending), so "append and register" and "compute bound and
	// delete" can never interleave.
	walPendingMu sync.Mutex
	walPending   map[uint64]int
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
	if config.CompactionSegmentThreshold == 0 {
		config.CompactionSegmentThreshold = defaultCompactionSegmentThreshold
	}
	if config.RollupSnapshotInterval == 0 {
		config.RollupSnapshotInterval = defaultRollupSnapshotInterval
	}
	if config.CompactionInterval == 0 {
		config.CompactionInterval = defaultCompactionInterval
	}
	if config.RetentionPeriod == 0 {
		config.RetentionPeriod = defaultRetentionPeriod
	}
	if config.SessionTimeout == 0 {
		config.SessionTimeout = defaultSessionTimeout
	}

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
		ingestChan:    make(chan pendingSpan, 10000), // Buffer for bursty ingest
		sessionBuffer: make(map[string]*traceState),
		walPending:    make(map[uint64]int),
	}

	// Segment IDs are seeded from on-disk truth, not the checkpoint alone:
	// compaction consumes IDs purely in memory (see maybeCompactLocked) and
	// never persists them to the checkpoint, so after restart the checkpoint
	// under-allocates and would collide with a live merged segment. See
	// computeNextSegmentID.
	nextSegmentID, err := computeNextSegmentID(config.SegmentDir, wt.manifest, lastConfirmedSegment)
	if err != nil {
		wt.Close()
		return nil, fmt.Errorf("compute next segment id: %w", err)
	}
	wt.nextSegmentID = nextSegmentID

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

// computeNextSegmentID returns one past the highest segment id ever seen, so
// the next new segment is guaranteed free: max(lastConfirmed, manifest live
// ids, on-disk segment_%06d.seg ids) + 1.
//
// The checkpoint alone under-allocates here: Checkpoint.Save is only called
// by writeSegmentBatch, while maybeCompactLocked carves out merged-segment ids
// from nextSegmentID purely in memory and never persists them. Those ids do
// land in the manifest and as segment files, so after a restart the next flush
// could otherwise O_TRUNC a live merged segment (writer.Flush explicitly
// refuses to overwrite, so this is now a loud error rather than corruption).
//
// A corrupt/unreadable manifest is treated as empty on purpose: the manifest
// is derived state and the directory scan is authoritative for id allocation
// regardless (see segment.Manifest doc).
func computeNextSegmentID(segDir string, manifest *segment.Manifest, lastConfirmed uint64) (uint64, error) {
	maxID := lastConfirmed

	live, err := manifest.Load()
	if err == nil {
		for _, id := range live {
			if id > maxID {
				maxID = id
			}
		}
	}

	entries, err := os.ReadDir(segDir)
	if err != nil {
		return 0, fmt.Errorf("scan segment dir: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "segment_") || !strings.HasSuffix(name, ".seg") {
			continue
		}
		// rollup snapshots and *.tmp files in the same dir never match this
		// strict pattern, so they can't be misread as segment ids.
		digits := strings.TrimSuffix(strings.TrimPrefix(name, "segment_"), ".seg")
		id, perr := strconv.ParseUint(digits, 10, 64)
		if perr != nil {
			continue
		}
		if id > maxID {
			maxID = id
		}
	}

	return maxID + 1, nil
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
			if errors.Is(err, os.ErrNotExist) {
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

	// Recovery is a two-phase drain: replay every tail record into the session
	// buffer, then flush everything once. Inline threshold flushes during the
	// replay are disabled (recovering=true) so no WAL segment is rotated or
	// reclaimed until the whole replay is in memory; an inline flush that
	// reclaimed the rotated tail would lose the remaining replayed spans if
	// this recovery itself crashed partway.
	w.recovering = true
	for _, record := range records {
		// A crash inside writeSegmentBatch can leave spans already durable
		// and indexed (segment + index batch committed) while their WAL
		// records are still present (not reclaimed past the checkpoint).
		// Re-processing them would duplicate the data across two segments, so
		// any span the index already resolves to a readable matching record
		// is skipped as already confirmed.
		if w.spanAlreadyIndexed(record.Span) {
			continue
		}
		w.processSpan(pendingSpan{span: record.Span})
	}
	w.recovering = false

	if len(records) > 0 {
		// Flush whatever was recovered immediately before booting
		if err := w.flushAllSessions(); err != nil {
			return fmt.Errorf("flush recovered spans: %w", err)
		}
	}

	// Manifest is derived state. Rebuild it from the index — the newest
	// committed pointer set — so a crash mid-compaction (index batch applied,
	// manifest swap not yet) resolves to the same single live set the index
	// describes, instead of resurrecting superseded segments.
	if err := w.reconcileManifest(); err != nil {
		return fmt.Errorf("reconcile manifest: %w", err)
	}
	return nil
}

// spanAlreadyIndexed reports whether the index already holds one entry for
// span that resolves, via a readable segment file, back to an exact matching
// record (same write, including Deleted flag, timestamp, and payload). If so
// the span is confirmed: it was checkpointed past and replaying it again would
// duplicate it.
func (w *WispTrace) spanAlreadyIndexed(span wal.SpanPayload) bool {
	key := segment.CompositeKey(span.TraceID, span.SpanID)
	loc, err := w.index.GetSpan([]byte(key))
	if err != nil {
		return false
	}

	reader, err := segment.OpenReader(segment.SegmentPath(w.config.SegmentDir, loc.SegmentID))
	if err != nil {
		return false
	}
	got, err := reader.ReadAt(loc.Offset)
	reader.Close()

	return err == nil && spansEqual(got, span)
}

func spansEqual(a, b wal.SpanPayload) bool {
	return a.TraceID == b.TraceID &&
		a.SpanID == b.SpanID &&
		a.ParentSpanID == b.ParentSpanID &&
		a.Timestamp == b.Timestamp &&
		a.AgentID == b.AgentID &&
		a.Model == b.Model &&
		a.ToolName == b.ToolName &&
		a.Team == b.Team &&
		a.Status == b.Status &&
		a.TokensIn == b.TokensIn &&
		a.TokensOut == b.TokensOut &&
		a.Cost == b.Cost &&
		a.LatencyMs == b.LatencyMs &&
		a.Deleted == b.Deleted &&
		bytes.Equal(a.Payload, b.Payload)
}

// reconcileManifest rewrites the manifest to be exactly the set of segment
// ids referenced by the trace index. Segments are immutable and every record
// in a flush-created segment is indexed at write time, so in the steady state
// the two sets are identical; a crash between a compactor's index batch and
// its manifest swap is the one window that splits them. Dropping a segment
// the index no longer references is safe — its records were either
// superseded by an index update or reclaimed as tombstones.
func (w *WispTrace) reconcileManifest() error {
	indexed, err := w.index.ScanAll()
	if err != nil {
		return fmt.Errorf("scan index: %w", err)
	}

	live := make([]uint64, 0, len(indexed))
	seen := make(map[uint64]bool, len(indexed))
	for _, loc := range indexed {
		if seen[loc.SegmentID] {
			continue
		}
		path := segment.SegmentPath(w.config.SegmentDir, loc.SegmentID)
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("index references segment %d at %s but it is missing: %w", loc.SegmentID, path, err)
		}
		seen[loc.SegmentID] = true
		live = append(live, loc.SegmentID)
	}

	current, err := w.manifest.Load()
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}
	if !segmentSetEqual(current, live) {
		if err := w.manifest.Save(live); err != nil {
			return fmt.Errorf("save manifest: %w", err)
		}
	}
	return nil
}

// segmentSetEqual compares two sets of segment ids without regard to order.
func segmentSetEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[uint64]bool, len(a))
	for _, id := range a {
		seen[id] = true
	}
	for _, id := range b {
		if !seen[id] {
			return false
		}
	}
	return true
}

func (w *WispTrace) InsertSpan(span wal.SpanPayload) error {
	// 1. Durability: the append is fsynced by the WAL, and the record is
	//    registered as not-yet-durable in the same critical section.
	segID, err := w.appendTracked(span)
	if err != nil {
		return fmt.Errorf("wal append: %w", err)
	}

	// 2. Offload to background loops for derived state updates. ingestWgMu
	// guards against the sync.WaitGroup "Add concurrently with Wait" misuse:
	// see WaitForIngest for the other half of this.
	w.ingestWgMu.Lock()
	w.ingestWg.Add(1)
	w.ingestWgMu.Unlock()

	select {
	case w.ingestChan <- pendingSpan{span: span, walSeg: segID}:
	case <-w.ctx.Done():
		w.ingestWg.Done()
		return fmt.Errorf("db closed")
	}

	return nil
}

// appendTracked fsyncs a span's WAL record and marks the segment that received
// it as holding a record which is not yet durable anywhere else.
//
// The lock is deliberately held across the append, not just around the
// bookkeeping that follows it. Registering afterwards leaves a window — append
// returns, record fsynced, span acknowledged to the caller, registration not
// yet done — in which a concurrent flush sees a pending set that omits this
// record, concludes the segment can be deleted, and deletes the only other
// durable copy of an acknowledged span. That is the failure the fault-injection
// harness reports, and it needs no bad luck beyond a flush running in that
// window.
//
// The cost is that record encoding (which wal.AppendRecordWithSeg deliberately
// does outside its own lock) is no longer concurrent. Appends themselves were
// already serialised by the WAL — write plus fsync under one lock — so the
// fsync, which is what the benchmark actually measures, is unaffected. A future
// group-commit append should register its whole batch under a single hold, which
// keeps the same ordering guarantee while amortising the fsync.
func (w *WispTrace) appendTracked(span wal.SpanPayload) (uint64, error) {
	w.walPendingMu.Lock()
	defer w.walPendingMu.Unlock()

	segID, err := w.wal.AppendRecordWithSeg(wal.WALRecord{Span: span})
	if err != nil {
		return 0, err
	}
	w.walPending[segID]++
	return segID, nil
}

func (w *WispTrace) ingestLoop() {
	defer w.bgWg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case ps := <-w.ingestChan:
			w.processSpan(ps)
			w.ingestWg.Done()
		}
	}
}

// WaitForIngest blocks until every span enqueued so far has been processed
// by the background ingest loop, then returns w for chaining (e.g.
// wt.WaitForIngest().GetSpan(...)).
//
// ingestWgMu exists solely to avoid a real sync.WaitGroup misuse: the stdlib
// docs are explicit that "Add" with a positive delta must not race with
// "Wait" when the counter could pass through zero in between — a real risk
// here since InsertSpan can be called concurrently with WaitForIngest.
// Holding the same mutex around both the Add (in InsertSpan) and this Wait
// call serializes them, so Add can never execute while a Wait is in
// progress. Trade-off: a concurrent InsertSpan will block for the duration
// of an in-progress WaitForIngest call — acceptable since every current
// caller is a test wanting a deterministic "drain" point, not a production
// hot path running both concurrently under load.
func (w *WispTrace) WaitForIngest() *WispTrace {
	w.ingestWgMu.Lock()
	w.ingestWg.Wait()
	w.ingestWgMu.Unlock()
	return w
}

func (w *WispTrace) processSpan(ps pendingSpan) {
	span := ps.span

	// Feed Rollups. Cost must go through rollup.ScaleCost — span.Cost is a
	// float64 dollar amount (almost always < $1 for a single LLM span), and
	// a bare int64(span.Cost) truncates it to zero.
	//
	// A tombstone contributes nothing. The rollup stores are purely additive
	// (Count++, SumCost += cost) and have no decrement path anywhere in the
	// package, so counting a deleted span would inflate every aggregate for
	// the lifetime of the window. This closes the case where a span is
	// tombstoned before it is counted; a span that was already counted and is
	// deleted later still keeps its contribution, because reversing it is an
	// exact operation for Count/SumCost/SumTokens but NOT for MinLatencyMs /
	// MaxLatencyMs — un-minimising those needs a per-bucket latency
	// distribution the store does not keep. That residual leak is tracked as
	// an open design question rather than assumed away here.
	if !span.Deleted {
		w.rollups.Add(span.Timestamp, span.Model, rollup.ScaleCost(span.Cost), int64(span.TokensIn), int64(span.TokensOut), span.LatencyMs)
	}

	// Feed Session Buffer. The WAL segment id rides along so the flush that
	// eventually persists this span can release exactly its record.
	w.sessionMu.Lock()
	state, ok := w.sessionBuffer[span.TraceID]
	if !ok {
		state = &traceState{spans: make([]pendingSpan, 0, 8)}
		w.sessionBuffer[span.TraceID] = state
	}
	state.spans = append(state.spans, ps)
	state.lastSeen = time.Now().UnixNano()
	w.sessionSpanCount++

	needsFlush := w.sessionSpanCount >= w.config.SegmentFlushThreshold
	w.sessionMu.Unlock()

	if needsFlush && !w.recovering {
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

	var toFlush []pendingSpan

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
	var toFlush []pendingSpan
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

func (w *WispTrace) writeSegmentBatch(spans []pendingSpan) error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	_, err := w.wal.Rotate()
	if err != nil {
		return fmt.Errorf("rotate wal: %w", err)
	}

	// Collapse duplicate keys within this batch down to their LAST write.
	//
	// A span inserted and then deleted before a flush lands arrives here
	// twice. Writing both records into one segment would leave a superseded
	// live copy sitting beside its own tombstone, and RangeQuery — which walks
	// segments and never consults the index, filtering only on Deleted — would
	// happily return the dead copy. It would also make the segment carry two
	// records for one key, which the index can only point at one of.
	//
	// Order is preserved by first appearance, so the resulting record sequence
	// matches the input's shape; only the content per key is collapsed.
	latestAt := make(map[string]int, len(spans))
	order := make([]string, 0, len(spans))
	for i, span := range spans {
		key := segment.CompositeKey(span.span.TraceID, span.span.SpanID)
		if _, seen := latestAt[key]; !seen {
			order = append(order, key)
		}
		latestAt[key] = i
	}
	deduped := make([]pendingSpan, 0, len(order))
	for _, key := range order {
		deduped = append(deduped, spans[latestAt[key]])
	}

	// Split into records to persist and tombstone keys to remove. A
	// tombstone's only durable effect is the index delete; it never needs a
	// record in a segment.
	liveSpans := make([]wal.SpanPayload, 0, len(deduped))
	var tombstoneKeys []string
	for _, ps := range deduped {
		span := ps.span
		if span.Deleted {
			tombstoneKeys = append(tombstoneKeys, segment.CompositeKey(span.TraceID, span.SpanID))
			continue
		}
		liveSpans = append(liveSpans, span)
	}

	// Tombstone-only batch: write no segment at all.
	//
	// A segment of nothing but tombstones carries no index entries, so startup
	// reconciliation — which derives the manifest from ScanAll — would drop it
	// from the manifest on the next open, leaving a file on disk that no
	// manifest references and compaction therefore never reclaims. Consuming a
	// segment id for it and adding it to the manifest here would just create
	// that divergence deliberately. The index deletes are the whole effect.
	//
	// Releasing the WAL records still has to run here, otherwise a
	// delete-heavy workload would append to the WAL, never write a segment, and
	// so never drop a pending record — the WAL would grow without bound.
	if len(liveSpans) == 0 {
		if err := w.index.BatchApplySpans(nil, tombstoneKeys); err != nil {
			return fmt.Errorf("apply %d tombstone(s) to index: %w", len(tombstoneKeys), err)
		}
		if err := w.flushRollups(); err != nil {
			return fmt.Errorf("snapshot rollups: %w", err)
		}
		return w.releaseWALPending(spans)
	}

	writer := segment.NewWriter()
	for _, span := range liveSpans {
		writer.Add(span)
	}

	result, err := writer.Flush(w.config.SegmentDir, w.nextSegmentID)
	if err != nil {
		return fmt.Errorf("write segment %d: %w", w.nextSegmentID, err)
	}

	// Index the segment's live records and remove this batch's tombstones in a
	// single atomic batch. Committing them separately would open a window in
	// which a tombstoned key still resolved to its pre-delete record. The key
	// was deleted from the index here rather than being repointed at the
	// tombstone, so a deleted span is unreachable from the index — it stays
	// readable only if a reader ignores the index, which is why RangeQuery's
	// segment walk and the GetSpan/GetTrace read paths all filter Deleted as
	// defence in depth.
	entries := make(map[string]pebble.SpanLocation, len(result.Offsets))
	for _, span := range liveSpans {
		key := segment.CompositeKey(span.TraceID, span.SpanID)
		if offset, ok := result.Offsets[key]; ok {
			entries[key] = pebble.SpanLocation{SegmentID: result.SegmentID, Offset: offset}
		}
	}
	if err := w.index.BatchApplySpans(entries, tombstoneKeys); err != nil {
		return fmt.Errorf("index segment %d: %w", result.SegmentID, err)
	}

	if err := w.manifest.AddSegment(result.SegmentID); err != nil {
		return fmt.Errorf("record segment %d in manifest: %w", result.SegmentID, err)
	}

	if err := w.checkpoint.Save(result.SegmentID); err != nil {
		return fmt.Errorf("save checkpoint at segment %d: %w", result.SegmentID, err)
	}

	// Snapshot rollups at the same cadence as the checkpoint (PAD1 v1.1),
	// and — critically — before this batch's WAL records are released. The
	// independent rollupSnapshotLoop ticker is a periodic backstop, not the
	// primary guarantee: it can lag behind how fast segments flush, and WAL
	// reclamation runs at the end of this function, not on that ticker. Without
	// this call, data ingested between two rollup-ticker snapshots but already
	// durable in a segment would be unrecoverable for rollups after a crash —
	// recover() only replays the WAL tail, and by then releaseWALPending would
	// have already deleted it.
	if err := w.flushRollups(); err != nil {
		return fmt.Errorf("snapshot rollups at segment %d: %w", result.SegmentID, err)
	}

	// Only now — segment written, index batch committed, manifest updated,
	// checkpoint saved — are this batch's WAL records redundant. Release them
	// and reclaim whatever the WAL can provably spare.
	if err := w.releaseWALPending(spans); err != nil {
		return err
	}

	w.nextSegmentID++

	// Trigger compaction inline (we already hold flushMu here) rather than
	// waiting for the separate background ticker: keeping live segment count
	// tight keeps range queries fast instead of letting files pile up for up
	// to CompactionSegmentThreshold flushes. Expiry deliberately stays on the
	// background ticker — it is the one maintenance task whose destructive
	// work has no place on the ingest flush path.
	w.maybeCompactLocked()

	return nil
}

// releaseWALPending marks a committed batch's WAL records as redundant and
// deletes the WAL segments that provably hold nothing else.
//
// The rule: a WAL segment is deletable exactly when no pending record lives in
// it, because "pending" means "acknowledged but not yet durable in a segment or
// applied to the index". So the reclamation bound is the lowest pending segment
// id, and everything strictly below it is safe by construction.
//
// The bound this replaces — the WAL segment active during the previous flush's
// drain, minus one — was a prediction rather than a fact. It was only correct if
// every record below that segment had provably been part of that drain's FIFO
// processed prefix, and that is false in three ordinary situations:
//
//   - a span that was fsynced and acknowledged but is still sitting in the
//     ingest channel when the flush snapshots (InsertSpan's append returns
//     before the enqueue, so a producer preempted in that window is acked
//     while its record is already in a sealed segment);
//   - a partial flush, where flushExpiredSessions writes only the expired
//     sessions and leaves the rest buffered while still advancing the bound;
//   - a tombstone-only batch, which advances the bound without writing a
//     segment.
//
// Any of those deletes the only other copy of an acknowledged span, and the
// loss only shows up after a crash — which is why the fault-injection harness,
// not any unit test, is what catches it.
//
// Caller must hold flushMu, so no two batches race to reclaim.
func (w *WispTrace) releaseWALPending(batch []pendingSpan) error {
	w.walPendingMu.Lock()

	for _, ps := range batch {
		if ps.walSeg == 0 {
			// Replayed out of the WAL at startup: on disk already, never
			// registered as pending.
			continue
		}
		if w.walPending[ps.walSeg] <= 1 {
			delete(w.walPending, ps.walSeg)
		} else {
			w.walPending[ps.walSeg]--
		}
	}

	lowest := uint64(0)
	found := false
	for segID := range w.walPending {
		if !found || segID < lowest {
			lowest, found = segID, true
		}
	}

	var bound uint64
	if found {
		// Everything strictly below the oldest un-flushed record is durable.
		bound = lowest - 1
	} else {
		// Nothing acknowledged is un-flushed, so every sealed segment is fully
		// covered. active-1 leaves the segment currently being appended to
		// alone (RemoveSegmentsUpTo would skip it regardless).
		active := w.wal.CurrentSegment()
		if active <= 1 {
			w.walPendingMu.Unlock()
			return nil
		}
		bound = active - 1
	}
	w.walPendingMu.Unlock()

	if err := w.wal.RemoveSegmentsUpTo(bound); err != nil {
		return fmt.Errorf("reclaim wal segments up to %d: %w", bound, err)
	}
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
			// Backstop only — writeSegmentBatch already snapshots rollups at
			// the checkpoint-tied point that actually matters for
			// correctness. Ignore the error here and retry on the next tick.
			_ = w.flushRollups()
		}
	}
}

// flushRollups evicts buckets past RetentionPeriod and snapshots every
// window's remaining buckets to disk. Returns the first write error
// encountered (if any) so callers on the correctness-critical path (see
// writeSegmentBatch) can decide whether to proceed; the periodic background
// caller (rollupSnapshotLoop) discards it and just retries on the next tick.
func (w *WispTrace) flushRollups() error {
	cutoff := time.Now().Add(-w.config.RetentionPeriod).UnixNano()

	w.rollups.Minute.Evict(cutoff)
	w.rollups.FiveMin.Evict(cutoff)
	w.rollups.Hour.Evict(cutoff)
	w.rollups.Day.Evict(cutoff)

	windows := []struct {
		name  string
		size  int64
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
			if err := writer.Flush(w.config.SegmentDir, win.name, win.size); err != nil {
				return fmt.Errorf("flush %s rollup snapshot: %w", win.name, err)
			}
		}
	}
	return nil
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
	w.maybeCompactLocked()
}

// maybeCompactLocked merges the oldest half of the live segments into one
// new segment once the live count exceeds CompactionSegmentThreshold. The
// caller must already hold flushMu (either writeSegmentBatch or the
// compaction loop) so segment ids and the manifest can't be mutated
// concurrently.
func (w *WispTrace) maybeCompactLocked() {
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

	if w.recovering {
		// Recovery holds all replayed spans in memory behind a single final
		// flush, so there is nothing compacted yet to compact.
		return
	}

	_, _ = w.Compactor().Compact(toMerge, newSegmentID)
}

func (w *WispTrace) maybeExpire() {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()
	w.maybeExpireLocked()
}

// maybeExpireLocked drops segments whose zone-map MaxTimestamp is past the
// retention cutoff. The caller must already hold flushMu.
func (w *WispTrace) maybeExpireLocked() {
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

// bufferedSpan reports the authoritative state of spanID in the session buffer.
//
// The third return distinguishes "the buffer has an answer" from "the buffer has
// nothing for this key", which is what lets GetSpan treat a buffered tombstone
// as a final answer instead of falling through to the index and reading the
// pre-delete record out of a segment.
func (w *WispTrace) bufferedSpan(traceID, spanID string) (span wal.SpanPayload, found, resolved bool) {
	w.sessionMu.Lock()
	defer w.sessionMu.Unlock()

	state, ok := w.sessionBuffer[traceID]
	if !ok {
		return wal.SpanPayload{}, false, false
	}
	// Scan backwards: the buffer is append-only, so the last entry for a span
	// id is the most recent write and therefore the authoritative one. Taking
	// the first match instead would resurrect a stale copy when the same span is
	// written twice before a flush.
	for i := len(state.spans) - 1; i >= 0; i-- {
		if state.spans[i].span.SpanID != spanID {
			continue
		}
		if state.spans[i].span.Deleted {
			return wal.SpanPayload{}, false, true
		}
		return state.spans[i].span, true, true
	}
	return wal.SpanPayload{}, false, false
}

// bufferedSpansByKey snapshots the session buffer as composite key → last write,
// plus the key order so callers can produce deterministic results. The buffer is
// append-only, so the last entry for a key is the authoritative one and an
// earlier copy is stale by construction.
//
// Pass an empty traceID to snapshot every buffered trace. The lock is released
// before returning, so callers must not hold it and can do segment I/O on the
// result without blocking ingest.
func (w *WispTrace) bufferedSpansByKey(traceID string) (map[string]wal.SpanPayload, []string) {
	w.sessionMu.Lock()
	defer w.sessionMu.Unlock()

	byKey := make(map[string]wal.SpanPayload)
	order := make([]string, 0)
	for id, state := range w.sessionBuffer {
		if traceID != "" && id != traceID {
			continue
		}
for _, ps := range state.spans {
		key := segment.CompositeKey(ps.span.TraceID, ps.span.SpanID)
		if _, seen := byKey[key]; !seen {
			order = append(order, key)
		}
		byKey[key] = ps.span
	}
	}
	return byKey, order
}

// GetSpan looks up one span by trace_id+span_id. A tombstoned span is
// reported as not found — the same answer RangeQuery gives — so the read APIs
// agree on whether a deleted span exists.
func (w *WispTrace) GetSpan(traceID, spanID string) (span wal.SpanPayload, found bool, err error) {
	// A span still in the session buffer has not reached a segment, and a
	// buffered tombstone has not yet deleted the index entry for the older copy.
	if s, f, resolved := w.bufferedSpan(traceID, spanID); resolved {
		return s, f, nil
	}

	key := []byte(segment.CompositeKey(traceID, spanID))
	location, err := w.index.GetSpan(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return wal.SpanPayload{}, false, nil
		}
		return wal.SpanPayload{}, false, fmt.Errorf("index lookup: %w", err)
	}

	reader, err := segment.OpenReaderLight(segment.SegmentPath(w.config.SegmentDir, location.SegmentID))
	if err != nil {
		return wal.SpanPayload{}, false, fmt.Errorf("open segment %d: %w", location.SegmentID, err)
	}
	defer reader.Close()

	span, err = reader.ReadAt(location.Offset)
	if err != nil {
		return wal.SpanPayload{}, false, fmt.Errorf("read span at segment %d offset %d: %w", location.SegmentID, location.Offset, err)
	}
	// Defence in depth. writeSegmentBatch deletes tombstoned keys from the
	// index rather than pointing them at their record, so a tombstone should
	// be unreachable here. Segments written before that fix can still hold
	// indexed tombstones, and a record may have been replaced at the same
	// offset, so check the flag rather than trusting the index.
	if span.Deleted {
		return wal.SpanPayload{}, false, nil
	}
	return span, true, nil
}

// GetTrace reconstructs a full trace by ID. Tombstoned spans are excluded, so
// a trace consisting only of deleted spans reports not found — consistent with
// GetSpan and RangeQuery.
//
// A buffered write takes precedence over the index. The index holds exactly one
// location per key, so a tombstone that has not been flushed yet has not
// deleted the entry pointing at the pre-delete record; without this the trace
// path would resurrect the span for the whole flush interval even though
// GetSpan correctly reports it gone.
func (w *WispTrace) GetTrace(traceID string) ([]wal.SpanPayload, bool, error) {
	buffered, bufferedOrder := w.bufferedSpansByKey(traceID)

	prefix := []byte(segment.CompositeKey(traceID, ""))
	locations, err := w.index.PrefixScan(prefix)
	if err != nil {
		return nil, false, fmt.Errorf("index prefix scan: %w", err)
	}

	spans := make([]wal.SpanPayload, 0, len(locations)+len(bufferedOrder))

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
			// A buffered write for this key is newer than the record the index
			// points at, so the index copy is superseded — including when the
			// buffered write is the tombstone that will delete it.
			if _, pending := buffered[segment.CompositeKey(span.TraceID, span.SpanID)]; pending {
				continue
			}
			// Defence in depth, same as GetSpan: tombstones are deleted from
			// the index at flush time, so one showing up here means a segment
			// written before that fix.
			if span.Deleted {
				continue
			}
			spans = append(spans, span)
		}
	}

	// Buffered spans for this trace. bufferedSpansByKey already collapsed each
	// key to its last write, so a live copy superseded by a tombstone is simply
	// not present here.
	for _, key := range bufferedOrder {
		s := buffered[key]
		if s.Deleted {
			continue
		}
		spans = append(spans, s)
	}

	if len(spans) == 0 {
		return nil, false, nil
	}
	return spans, true, nil
}

// RangeFilter selects spans by time range and, optionally, bounded-cardinality
// dimension values. An empty string on any dimension field means "don't filter".
type RangeFilter struct {
	StartTS, EndTS                         int64
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

// RangeQuery returns every live span matching filter.
//
// A segment record is a CANDIDATE, not an answer. Segments are immutable, so a
// span deleted after its segment was written is still physically present in that
// segment, and the deletion exists only in the index — a negative fact that a
// segment walk cannot see. Bloom and zone-map pruning narrow the candidate set
// but say nothing about liveness.
//
// So every candidate is re-verified against the index before it is returned:
// the same staleness check the compactor applies, and the same rule as
// invariant 5, "the index entry is the authority on liveness". A record is
// emitted only if the index points at exactly this (segment_id, offset). A
// tombstoned key has no entry at all, and a key rewritten since this segment
// was written points somewhere else; both are superseded.
//
// This costs one index lookup per candidate and is a real regression against
// the previous index-free scan. It is the price of not returning deleted spans
// from the query path, and the obvious optimisation — one snapshot iterator
// instead of N point lookups — is deliberately not taken here.
func (w *WispTrace) RangeQuery(filter RangeFilter) ([]wal.SpanPayload, error) {
	live, err := w.manifest.Load()
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}

	// Snapshot the session buffer up front. Its records are strictly newer than
	// anything in a segment — they have not been flushed yet — so a buffered
	// version of a key supersedes the segment's copy, and a buffered tombstone
	// must suppress it.
	buffered, bufferedOrder := w.bufferedSpansByKey("")

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
			// Defence in depth: a tombstone is no longer written to a segment
			// at all, so this can only fire on a segment predating that change.
			if s.Span.Deleted {
				continue
			}
			key := segment.CompositeKey(s.Span.TraceID, s.Span.SpanID)
			if _, pending := buffered[key]; pending {
				continue
			}
			if !filter.matches(s.Span) {
				continue
			}
			// Staleness check — see the doc comment. Run after the cheap
			// in-memory filter to avoid an index lookup for every scanned
			// record when only a small time window matches.
			loc, err := w.index.GetSpan([]byte(key))
			if err != nil || loc.SegmentID != id || loc.Offset != s.Offset {
				continue
			}
			results = append(results, s.Span)
		}
	}

	// Buffered spans not yet in any segment. Last write per key already won
	// above, so a live copy superseded by a tombstone is simply not there.
	for _, key := range bufferedOrder {
		s := buffered[key]
		if s.Deleted || !filter.matches(s) {
			continue
		}
		results = append(results, s)
	}

	return results, nil
}

// ListTraces returns a list of distinct trace IDs matching filter, capped at limit.
// A limit <= 0 returns all matching trace IDs.
func (w *WispTrace) ListTraces(filter RangeFilter, limit int) ([]string, error) {
	spans, err := w.RangeQuery(filter)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	var traceIDs []string
	for _, s := range spans {
		if !seen[s.TraceID] {
			seen[s.TraceID] = true
			traceIDs = append(traceIDs, s.TraceID)
			if limit > 0 && len(traceIDs) >= limit {
				break
			}
		}
	}
	return traceIDs, nil
}

// QueryWithCursor returns a paginated slice of matching spans starting from cursor, up to pageSize.
// It returns the slice of spans and an opaque nextCursor string. An empty nextCursor means no more results.
func (w *WispTrace) QueryWithCursor(filter RangeFilter, pageSize int, cursor string) ([]wal.SpanPayload, string, error) {
	spans, err := w.RangeQuery(filter)
	if err != nil {
		return nil, "", err
	}

	offset := 0
	if cursor != "" {
		parsed, err := strconv.Atoi(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("invalid cursor: %w", err)
		}
		if parsed > 0 {
			offset = parsed
		}
	}

	if offset >= len(spans) || pageSize <= 0 {
		return []wal.SpanPayload{}, "", nil
	}

	end := offset + pageSize
	nextCursor := ""
	if end < len(spans) {
		nextCursor = strconv.Itoa(end)
	} else {
		end = len(spans)
	}

	return spans[offset:end], nextCursor, nil
}

// QueryByModel returns all live spans matching the given model within [startTS, endTS].
func (w *WispTrace) QueryByModel(model string, startTS, endTS int64) ([]wal.SpanPayload, error) {
	return w.RangeQuery(RangeFilter{Model: model, StartTS: startTS, EndTS: endTS})
}

// QueryByTeam returns all live spans matching the given team within [startTS, endTS].
func (w *WispTrace) QueryByTeam(team string, startTS, endTS int64) ([]wal.SpanPayload, error) {
	return w.RangeQuery(RangeFilter{Team: team, StartTS: startTS, EndTS: endTS})
}

// QueryByStatus returns all live spans matching the given status within [startTS, endTS].
func (w *WispTrace) QueryByStatus(status string, startTS, endTS int64) ([]wal.SpanPayload, error) {
	return w.RangeQuery(RangeFilter{Status: status, StartTS: startTS, EndTS: endTS})
}

// QueryByAgentID returns all live spans matching the given agentID within [startTS, endTS].
func (w *WispTrace) QueryByAgentID(agentID string, startTS, endTS int64) ([]wal.SpanPayload, error) {
	return w.RangeQuery(RangeFilter{AgentID: agentID, StartTS: startTS, EndTS: endTS})
}

// QueryByToolName returns all live spans matching the given toolName within [startTS, endTS].
func (w *WispTrace) QueryByToolName(toolName string, startTS, endTS int64) ([]wal.SpanPayload, error) {
	return w.RangeQuery(RangeFilter{ToolName: toolName, StartTS: startTS, EndTS: endTS})
}

type TokenStats struct {
	TokensIn  int64 `json:"tokens_in"`
	TokensOut int64 `json:"tokens_out"`
	Total     int64 `json:"total"`
}

type TeamCost struct {
	Team string  `json:"team"`
	Cost float64 `json:"cost"`
}

type ModelCount struct {
	Model string `json:"model"`
	Count int64  `json:"count"`
}

// GetCostByModel returns total dollar cost grouped by model for spans within [startTS, endTS].
func (w *WispTrace) GetCostByModel(startTS, endTS int64) (map[string]float64, error) {
	spans, err := w.RangeQuery(RangeFilter{StartTS: startTS, EndTS: endTS})
	if err != nil {
		return nil, err
	}
	res := make(map[string]float64)
	for _, s := range spans {
		if s.Model != "" {
			res[s.Model] += s.Cost
		}
	}
	return res, nil
}

// GetCostByTeam returns total dollar cost grouped by team for spans within [startTS, endTS].
func (w *WispTrace) GetCostByTeam(startTS, endTS int64) (map[string]float64, error) {
	spans, err := w.RangeQuery(RangeFilter{StartTS: startTS, EndTS: endTS})
	if err != nil {
		return nil, err
	}
	res := make(map[string]float64)
	for _, s := range spans {
		if s.Team != "" {
			res[s.Team] += s.Cost
		}
	}
	return res, nil
}

// GetTokensByModel returns input, output, and total token count grouped by model within [startTS, endTS].
func (w *WispTrace) GetTokensByModel(startTS, endTS int64) (map[string]TokenStats, error) {
	spans, err := w.RangeQuery(RangeFilter{StartTS: startTS, EndTS: endTS})
	if err != nil {
		return nil, err
	}
	res := make(map[string]TokenStats)
	for _, s := range spans {
		if s.Model != "" {
			stat := res[s.Model]
			stat.TokensIn += int64(s.TokensIn)
			stat.TokensOut += int64(s.TokensOut)
			stat.Total += int64(s.TokensIn) + int64(s.TokensOut)
			res[s.Model] = stat
		}
	}
	return res, nil
}

// GetTopTeamsByCost returns top teams sorted descending by dollar cost within [startTS, endTS], capped at limit.
func (w *WispTrace) GetTopTeamsByCost(limit int, startTS, endTS int64) ([]TeamCost, error) {
	byTeam, err := w.GetCostByTeam(startTS, endTS)
	if err != nil {
		return nil, err
	}
	teams := make([]TeamCost, 0, len(byTeam))
	for team, cost := range byTeam {
		teams = append(teams, TeamCost{Team: team, Cost: cost})
	}
	sort.Slice(teams, func(i, j int) bool {
		return teams[i].Cost > teams[j].Cost
	})
	if limit > 0 && len(teams) > limit {
		teams = teams[:limit]
	}
	return teams, nil
}

// GetTopModelsBySpanCount returns top models sorted descending by span count within [startTS, endTS], capped at limit.
func (w *WispTrace) GetTopModelsBySpanCount(limit int, startTS, endTS int64) ([]ModelCount, error) {
	spans, err := w.RangeQuery(RangeFilter{StartTS: startTS, EndTS: endTS})
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int64)
	for _, s := range spans {
		if s.Model != "" {
			counts[s.Model]++
		}
	}
	models := make([]ModelCount, 0, len(counts))
	for model, count := range counts {
		models = append(models, ModelCount{Model: model, Count: count})
	}
	sort.Slice(models, func(i, j int) bool {
		return models[i].Count > models[j].Count
	})
	if limit > 0 && len(models) > limit {
		models = models[:limit]
	}
	return models, nil
}

// QueryAggregations returns raw rollup buckets for the given window ("1m", "5m", "1h", "1d") in [startTS, endTS].
func (w *WispTrace) QueryAggregations(window string, startTS, endTS int64) (map[rollup.BucketKey]*rollup.Value, error) {
	return w.rollups.GetBucketInRange(window, startTS, endTS)
}

// GetFailedSpans returns spans with non-success status ("error", etc.) within [startTS, endTS], capped at limit.
func (w *WispTrace) GetFailedSpans(limit int, startTS, endTS int64) ([]wal.SpanPayload, error) {
	spans, err := w.RangeQuery(RangeFilter{StartTS: startTS, EndTS: endTS})
	if err != nil {
		return nil, err
	}
	var failed []wal.SpanPayload
	for _, s := range spans {
		st := strings.ToLower(s.Status)
		if st != "ok" && st != "success" && st != "" {
			failed = append(failed, s)
			if limit > 0 && len(failed) >= limit {
				break
			}
		}
	}
	return failed, nil
}

// GetErrorRate returns the error percentage (0.0 to 100.0) for model (or all models if "") within [startTS, endTS].
func (w *WispTrace) GetErrorRate(model string, startTS, endTS int64) (float64, error) {
	spans, err := w.RangeQuery(RangeFilter{Model: model, StartTS: startTS, EndTS: endTS})
	if err != nil {
		return 0, err
	}
	if len(spans) == 0 {
		return 0, nil
	}
	var errorCount int
	for _, s := range spans {
		st := strings.ToLower(s.Status)
		if st != "ok" && st != "success" && st != "" {
			errorCount++
		}
	}
	return (float64(errorCount) / float64(len(spans))) * 100.0, nil
}

// GetDistinctModels returns a sorted list of unique model names seen within [startTS, endTS].
// limit: maximum number of results to return (0 = no limit, but hard-capped at MaxCardinalityPerQuery for safety).
func (w *WispTrace) GetDistinctModels(startTS, endTS int64, limit int) ([]string, error) {
	spans, err := w.RangeQuery(RangeFilter{StartTS: startTS, EndTS: endTS})
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var models []string
	for _, s := range spans {
		if s.Model != "" && !seen[s.Model] {
			seen[s.Model] = true
			models = append(models, s.Model)
			// Stop early if cardinality safety limit reached
			if len(models) >= MaxCardinalityPerQuery {
				return nil, fmt.Errorf("cardinality limit exceeded: found %d distinct models (limit: %d)", len(models), MaxCardinalityPerQuery)
			}
		}
	}
	sort.Strings(models)
	if limit > 0 && len(models) > limit {
		models = models[:limit]
	}
	return models, nil
}

// GetDistinctTeams returns a sorted list of unique team names seen within [startTS, endTS].
// limit: maximum number of results to return (0 = no limit, but hard-capped at MaxCardinalityPerQuery for safety).
func (w *WispTrace) GetDistinctTeams(startTS, endTS int64, limit int) ([]string, error) {
	spans, err := w.RangeQuery(RangeFilter{StartTS: startTS, EndTS: endTS})
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var teams []string
	for _, s := range spans {
		if s.Team != "" && !seen[s.Team] {
			seen[s.Team] = true
			teams = append(teams, s.Team)
			// Stop early if cardinality safety limit reached
			if len(teams) >= MaxCardinalityPerQuery {
				return nil, fmt.Errorf("cardinality limit exceeded: found %d distinct teams (limit: %d)", len(teams), MaxCardinalityPerQuery)
			}
		}
	}
	sort.Strings(teams)
	if limit > 0 && len(teams) > limit {
		teams = teams[:limit]
	}
	return teams, nil
}

// GetDistinctAgents returns a sorted list of unique agent IDs seen within [startTS, endTS].
// limit: maximum number of results to return (0 = no limit, but hard-capped at MaxCardinalityPerQuery for safety).
func (w *WispTrace) GetDistinctAgents(startTS, endTS int64, limit int) ([]string, error) {
	spans, err := w.RangeQuery(RangeFilter{StartTS: startTS, EndTS: endTS})
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var agents []string
	for _, s := range spans {
		if s.AgentID != "" && !seen[s.AgentID] {
			seen[s.AgentID] = true
			agents = append(agents, s.AgentID)
			// Stop early if cardinality safety limit reached
			if len(agents) >= MaxCardinalityPerQuery {
				return nil, fmt.Errorf("cardinality limit exceeded: found %d distinct agents (limit: %d)", len(agents), MaxCardinalityPerQuery)
			}
		}
	}
	sort.Strings(agents)
	if limit > 0 && len(agents) > limit {
		agents = agents[:limit]
	}
	return agents, nil
}

func (w *WispTrace) Flush() error {
	// Drain the ingest pipeline first: spans acked by InsertSpan are queued
	// to a background goroutine, and flushAllSessions only sees spans already
	// moved into the session buffer. Without this the flush could run while a
	// just-acked span is still in flight and silently miss it.
	w.WaitForIngest()
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
	if err := w.flushRollups(); err != nil {
		errs = append(errs, fmt.Errorf("final rollup snapshot: %w", err))
	}

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

// CloseWithoutFlush stops background loops and closes underlying storage handles
// without flushing in-memory session buffer spans to segments, simulating a crash.
func (w *WispTrace) CloseWithoutFlush() error {
	var errs []error

	w.cancel()
	w.bgWg.Wait()

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
		return fmt.Errorf("wisptrace close without flush errors: %v", errs)
	}
	return nil
}
