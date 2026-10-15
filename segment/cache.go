// Cache: a bounded, refcounted set of open segment readers keyed by segment
// id, so exact-offset read paths stop reopening the same file on every call.
//
// Scope: only readers that need nothing but the header and ReadAt go through
// the cache — GetSpan, GetTrace, spanAlreadyIndexed. RangeQuery prunes with
// the bloom section and scans with a seek-based ScanAll, neither of which is
// safe or useful on a shared reader, so it keeps its own per-call open.
//
// Three rules govern correctness:
//
//  1. A handed-out reader is valid until its release function runs. Closing
//     is therefore deferred: an entry is closed only when it is out of the
//     map (evicted or purged) AND its refcount has reached zero.
//
//  2. Purge before unlink. On Windows os.Remove fails while any handle to the
//     file is open (see segment's TestDeleteWhileOpenProbe). Compaction and
//     expiry call Purge(ids), which removes the ids from lookup, refuses to
//     ever open them again (the dead set — closes the resurrection race with
//     a query that read the index just before the commit), waits for
//     in-flight readers, and closes the handles. Purge returning nil is the
//     guarantee that unlink will succeed.
//
//  3. No persisted state. A crash simply ends the process and the OS closes
//     every handle; recovery sees an empty cache. The cache cannot corrupt
//     anything — its worst failure is a query error or an orphaned file that
//     CheckConsistency already reports.
package segment

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrPurged is returned by Acquire for a segment id whose readers have
	// been purged: the index no longer references it and its file is being
	// unlinked. Reopening could hand the unlink a fresh handle, so the cache
	// refuses; the caller sees the failure it would already see from opening
	// a vanished file.
	ErrPurged = errors.New("segment is being reclaimed")

	// ErrPurgeTimeout is returned when in-flight readers still hold
	// references after purgeWait. The entries were already detached, so their
	// last release still closes them — but the unlink must not proceed on
	// assumption, so the caller aborts instead (the manifest/index are
	// already committed at that point, leaving an orphan file, which is the
	// designed failure: disk leak, never corruption).
	ErrPurgeTimeout = errors.New("segment readers still in use at purge deadline")

	// ErrCacheClosed is returned by Acquire after Close.
	ErrCacheClosed = errors.New("segment cache is closed")
)

// defaultPurgeWait bounds Purge's wait for in-flight readers. Reads take
// microseconds; waiting this long means a reader leaked its release, which is
// a bug that should fail loudly before unlink rather than after it.
const defaultPurgeWait = 2 * time.Second

type Cache struct {
	segmentDir string
	maxEntries int
	purgeWait  time.Duration

	mu      sync.Mutex
	cond    *sync.Cond // broadcast whenever any entry's refs hits 0, or on purge deadline
	entries map[uint64]*cacheEntry
	lru     *list.List // front = most recently acquired
	dead    map[uint64]struct{}
	closed  bool
}

type cacheEntry struct {
	id      uint64
	reader  *Reader
	refs    int  // in-flight users; entry valid to close at 0
	evicted bool // out of entries/lru; close on last release
	closed  bool
	elem    *list.Element
}

// NewCache returns a cache of at most maxEntries open readers for segmentDir.
// Callers must Close the cache (WispTrace.Close does) so handles are released
// before the process exits cleanly.
func NewCache(segmentDir string, maxEntries int) *Cache {
	c := &Cache{
		segmentDir: segmentDir,
		maxEntries: maxEntries,
		purgeWait:  defaultPurgeWait,
		entries:    make(map[uint64]*cacheEntry),
		lru:        list.New(),
		dead:       make(map[uint64]struct{}),
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// Release is a single-fire release handle for one Acquire. Call Release
// exactly once when the reader is no longer needed; extra calls are no-ops and
// the handle is safe to point at nothing after Release returns. Handles are
// pooled, so caching a hit costs zero allocations per read — the exact-offset
// path's per-call cost used to be the reopen, and a pooled handle keeps it out
// of the per-call allocation budget the way a freshly built closure could not.
type Release struct {
	c      *Cache
	e      *cacheEntry
	reader *Reader // set only for a direct (non-cached) reader, see NewDirectRelease
	fired  atomic.Bool
}

var releasePool = sync.Pool{
	New: func() any { return &Release{} },
}

// Release returns the reader's lease over the entry. It is idempotent: a
// second call is a no-op, and the handle is returned to the pool on the first
// call. After Release returns the caller must not use the handle.
func (r *Release) Release() error {
	if r.fired.Swap(true) {
		return nil
	}
	var err error
	if r.reader != nil {
		err = r.reader.Close()
	} else {
		err = r.c.release(r.e)
		releasePool.Put(r)
	}
	return err
}

// NewDirectRelease wraps a reader so the callers that go through WispTrace's
// acquireSegment abstraction can release it with the same handle type as a
// cached one. Used for the no-cache path, where each read owns its reader.
// The handle is not pooled (it is created per read, the baseline behavior).
func NewDirectRelease(reader *Reader) *Release {
	return &Release{reader: reader}
}

func releaseHandle(c *Cache, e *cacheEntry) *Release {
	r := releasePool.Get().(*Release)
	r.c = c
	r.e = e
	r.fired.Store(false)
	return r
}

// Acquire returns a reader for the segment and a release handle that must be
// released exactly once when the reader is no longer needed (extra Release
// calls are harmless no-ops). The reader may be shared with other goroutines;
// use ReadAt only — ScanAll seeks and is not safe on a shared reader.
//
// The cache miss opens the file while holding the cache lock. That is one
// CreateFile plus a 38-byte read on the light path, and opening outside the
// lock would let a miss race Purge's dead-set check and resurrect a file that
// is about to be unlinked.
func (c *Cache) Acquire(id uint64) (*Reader, *Release, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, fmt.Errorf("segment %d: %w", id, ErrCacheClosed)
	}
	if _, ok := c.dead[id]; ok {
		return nil, nil, fmt.Errorf("segment %d: %w", id, ErrPurged)
	}
	if e, ok := c.entries[id]; ok {
		c.lru.MoveToFront(e.elem)
		e.refs++
		return e.reader, releaseHandle(c, e), nil
	}

	reader, err := OpenReaderLight(SegmentPath(c.segmentDir, id))
	if err != nil {
		return nil, nil, err
	}
	e := &cacheEntry{id: id, reader: reader, refs: 1}
	e.elem = c.lru.PushFront(e)
	c.entries[id] = e
	c.evictOverflowLocked()
	return e.reader, releaseHandle(c, e), nil
}

func (c *Cache) release(e *cacheEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	e.refs--
	if e.refs == 0 {
		c.cond.Broadcast()
		if e.evicted {
			return c.closeEntryLocked(e)
		}
	}
	return nil
}

// Purge removes ids from the cache so no new Acquire can return them, waits
// for every in-flight reader of those ids to release, and closes the handles.
// It returns nil only once zero handles for those ids exist, which is the
// precondition compaction/ expiry rely on before os.Remove. A purged id is
// never opened again for the lifetime of the cache (the dead set).
//
// If readers are still in use after purgeWait, ErrPurgeTimeout is returned
// and the handles are NOT closed — their last release still is — so callers
// must treat the error as "unlink must not proceed".
func (c *Cache) Purge(ids []uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil // Close already closed everything
	}

	pending := make([]*cacheEntry, 0, len(ids))
	for _, id := range ids {
		c.dead[id] = struct{}{}
		if e, ok := c.entries[id]; ok {
			c.detachLocked(e)
			pending = append(pending, e)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	return c.waitAndCloseLocked(pending, false)
}

// Close detaches every entry and closes all handles: in-flight readers are
// waited for (bounded by purgeWait) and then force-closed regardless, because
// Close is terminal and must not hang forever on a leaked release. Readers
// still in flight at that point will fail their ReadAt — acceptable during
// shutdown, where the engine is already stopped.
func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	pending := make([]*cacheEntry, 0, len(c.entries))
	for _, e := range c.entries {
		c.detachLocked(e)
		pending = append(pending, e)
	}
	if len(pending) == 0 {
		return nil
	}
	return c.waitAndCloseLocked(pending, true)
}

// evictOverflowLocked keeps the cache within maxEntries by closing the least
// recently acquired entries that nobody is currently reading. Entries in use
// are skipped rather than yanked out from under their readers, so the bound
// may overshoot by the number of in-flight readers — bounded by concurrency,
// not by data size.
//
// Caller holds c.mu.
func (c *Cache) evictOverflowLocked() {
	for len(c.entries) > c.maxEntries {
		var victim *cacheEntry
		for elem := c.lru.Back(); elem != nil; elem = elem.Prev() {
			if e := elem.Value.(*cacheEntry); e.refs == 0 {
				victim = e
				break
			}
		}
		if victim == nil {
			return // everything is in flight
		}
		c.detachLocked(victim)
		_ = c.closeEntryLocked(victim)
	}
}

// detachLocked removes e from the map and LRU and marks it evicted, so its
// last release closes it. Caller holds c.mu.
func (c *Cache) detachLocked(e *cacheEntry) {
	if cur, ok := c.entries[e.id]; ok && cur == e {
		delete(c.entries, e.id)
	}
	if e.elem != nil {
		c.lru.Remove(e.elem)
		e.elem = nil
	}
	e.evicted = true
}

// closeEntryLocked closes e's reader exactly once. Caller holds c.mu.
func (c *Cache) closeEntryLocked(e *cacheEntry) error {
	if e.closed {
		return nil
	}
	e.closed = true
	return e.reader.Close()
}

// waitAndCloseLocked waits until every entry in pending has refs == 0, then
// closes them; caller holds c.mu. On timeout: with force (shutdown) it closes
// everything anyway; without force (Purge) it closes nothing still in use and
// returns ErrPurgeTimeout. Caller holds c.mu.
func (c *Cache) waitAndCloseLocked(pending []*cacheEntry, force bool) error {
	timedOut := false
	watchdog := time.AfterFunc(c.purgeWait, func() {
		c.mu.Lock()
		timedOut = true
		c.cond.Broadcast()
		c.mu.Unlock()
	})
	defer watchdog.Stop()

	var firstErr error
	for len(pending) > 0 {
		remaining := pending[:0]
		for _, e := range pending {
			if e.refs == 0 {
				if err := c.closeEntryLocked(e); err != nil && firstErr == nil {
					firstErr = err
				}
			} else {
				remaining = append(remaining, e)
			}
		}
		pending = remaining
		if len(pending) == 0 {
			break
		}
		if timedOut {
			if !force {
				return fmt.Errorf("%w after %s (%d reader(s) held)", ErrPurgeTimeout, c.purgeWait, len(pending))
			}
			for _, e := range pending {
				if err := c.closeEntryLocked(e); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			return firstErr
		}
		c.cond.Wait()
	}
	return firstErr
}
