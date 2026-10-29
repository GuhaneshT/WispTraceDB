package wisptrace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/GuhaneshT/WispTraceDB/internal/engine"
)

// DB is an open trace store. It is safe for concurrent use.
type DB struct {
	eng       *engine.WispTrace
	ryw       bool
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// Open opens the database stored in dir, creating it if needed. Opening runs
// crash recovery, so it can take longer after an unclean shutdown. A directory
// can be open by one DB at a time.
func Open(dir string, opts ...Option) (*DB, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wisptrace: create %s: %w", dir, err)
	}

	cfg := engine.DefaultWispTraceConfig()
	cfg.WALPath = filepath.Join(dir, "wal.log")
	cfg.SegmentDir = filepath.Join(dir, "segments")
	cfg.PebblePath = filepath.Join(dir, "index")
	cfg.CheckpointPath = filepath.Join(dir, "checkpoint.dat")
	cfg.ManifestPath = filepath.Join(dir, "manifest.dat")
	if o.retention > 0 {
		cfg.RetentionPeriod = o.retention
	}
	if o.flushSpans > 0 {
		cfg.SegmentFlushThreshold = o.flushSpans
	}
	if o.cacheSize != 0 {
		cfg.SegmentCacheSize = o.cacheSize
	}

	eng, err := engine.CreateWispTraceWithConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("wisptrace: open %s: %w", dir, err)
	}
	return &DB{eng: eng, ryw: o.readYourWrite}, nil
}

// Close flushes buffered spans and releases the database. It is idempotent.
func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		db.closed.Store(true)
		db.closeErr = db.eng.Close()
	})
	return db.closeErr
}

// check returns the error to fail a call with before touching the engine.
func (db *DB) check(ctx context.Context) error {
	if db.closed.Load() {
		return ErrClosed
	}
	return ctx.Err()
}

// Ingest validates a span and durably appends it. On a nil return the span is
// fsynced and will survive a crash; it becomes visible to reads shortly after
// (see Sync and WithReadYourWrites).
func (db *DB) Ingest(ctx context.Context, s Span) error {
	if err := db.check(ctx); err != nil {
		return err
	}
	if err := s.Validate(); err != nil {
		return err
	}
	if err := db.eng.InsertSpan(s.toPayload()); err != nil {
		return err
	}
	if db.ryw {
		return db.Sync(ctx)
	}
	return nil
}

// Rejection is a span IngestBatch refused.
type Rejection struct {
	Index int   // position in the input slice
	Err   error // a *ValidationError
}

// BatchResult reports what IngestBatch did.
type BatchResult struct {
	Accepted int
	Rejected []Rejection
}

// IngestBatch ingests spans in order. Invalid spans are reported in the
// result and do not stop the batch. A storage or context error stops it: the
// returned result still counts the spans accepted (and durable) before it.
func (db *DB) IngestBatch(ctx context.Context, spans []Span) (BatchResult, error) {
	var res BatchResult
	for i, s := range spans {
		if err := db.check(ctx); err != nil {
			return res, err
		}
		if err := s.Validate(); err != nil {
			res.Rejected = append(res.Rejected, Rejection{Index: i, Err: err})
			continue
		}
		if err := db.eng.InsertSpan(s.toPayload()); err != nil {
			return res, err
		}
		res.Accepted++
	}
	if db.ryw && res.Accepted > 0 {
		return res, db.Sync(ctx)
	}
	return res, nil
}

// Sync blocks until every span acknowledged so far is visible to reads and
// aggregates, or ctx is done.
func (db *DB) Sync(ctx context.Context) error {
	if err := db.check(ctx); err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		db.eng.WaitForIngest()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Span returns one span, or ErrNotFound.
func (db *DB) Span(ctx context.Context, traceID, spanID string) (Span, error) {
	if err := db.check(ctx); err != nil {
		return Span{}, err
	}
	p, found, err := db.eng.GetSpan(traceID, spanID)
	if err != nil {
		return Span{}, err
	}
	if !found {
		return Span{}, ErrNotFound
	}
	return fromPayload(p), nil
}

// Trace returns every live span of a trace, or ErrNotFound.
func (db *DB) Trace(ctx context.Context, traceID string) ([]Span, error) {
	if err := db.check(ctx); err != nil {
		return nil, err
	}
	ps, found, err := db.eng.GetTrace(traceID)
	if err != nil {
		return nil, err
	}
	if !found || len(ps) == 0 {
		return nil, ErrNotFound
	}
	return fromPayloads(ps), nil
}

// DeleteSpan durably deletes one span. Deleting an absent span is a no-op.
// A span already counted in an aggregate keeps its contribution there.
func (db *DB) DeleteSpan(ctx context.Context, traceID, spanID string) error {
	if err := db.check(ctx); err != nil {
		return err
	}
	if traceID == "" || spanID == "" {
		return &ValidationError{Field: "TraceID/SpanID", Reason: "must not be empty"}
	}
	return db.eng.DeleteSpan(traceID, spanID)
}

// DeleteTrace durably deletes every span of a trace. An absent trace is a no-op.
func (db *DB) DeleteTrace(ctx context.Context, traceID string) error {
	if err := db.check(ctx); err != nil {
		return err
	}
	if traceID == "" {
		return &ValidationError{Field: "TraceID", Reason: "must not be empty"}
	}
	return db.eng.DeleteTrace(traceID)
}

// Report is the outcome of Check.
type Report struct {
	Errors   []string // integrity violations
	Warnings []string
}

// OK reports whether Check found no integrity violations.
func (r Report) OK() bool { return len(r.Errors) == 0 }

// Check cross-checks the index, segments and manifest. It reads the whole
// database, so run it offline or at quiet times.
func (db *DB) Check(ctx context.Context) (Report, error) {
	if err := db.check(ctx); err != nil {
		return Report{}, err
	}
	r := db.eng.CheckConsistency()
	return Report{Errors: r.Errors, Warnings: r.Warnings}, nil
}
