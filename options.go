package wisptrace

import "time"

type options struct {
	retention     time.Duration
	flushSpans    int
	cacheSize     int
	readYourWrite bool
}

// Option configures Open.
type Option func(*options)

// WithRetention drops spans whose start time is older than d. The default is
// 7 days. Retention is judged by the span's own start time, not by when it was
// written, so spans backdated beyond d are removed soon after they are stored.
// Data is dropped a whole storage segment at a time, in a background pass that
// runs about every 10 minutes: a segment is removed only once every span in it
// is older than d, so some older spans can outlive the cutoff.
func WithRetention(d time.Duration) Option { return func(o *options) { o.retention = d } }

// WithFlushThreshold sets how many buffered spans trigger a segment flush.
func WithFlushThreshold(spans int) Option { return func(o *options) { o.flushSpans = spans } }

// WithSegmentCacheSize bounds the open segment readers kept for point reads.
// Zero keeps the default; a negative value disables the cache.
func WithSegmentCacheSize(n int) Option { return func(o *options) { o.cacheSize = n } }

// WithReadYourWrites makes Ingest and IngestBatch wait until the span is
// visible to reads before returning. It serialises concurrent writers, so it
// is off by default; see Sync.
func WithReadYourWrites() Option { return func(o *options) { o.readYourWrite = true } }
