package segment

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/GuhaneshT/WispTraceDB/internal/wal"
)

// writeCacheFixture writes a one-record segment and returns the record's
// offset, so tests can prove reads hit a valid record before and after
// cache operations.
func writeCacheFixture(t *testing.T, dir string, id uint64) uint64 {
	t.Helper()
	w := NewWriter()
	w.Add(wal.SpanPayload{
		TraceID:   "tr",
		SpanID:    "sp",
		Timestamp: 1000,
		Model:     "m",
		Payload:   []byte("cache fixture"),
	})
	res, err := w.Flush(dir, id)
	if err != nil {
		t.Fatalf("write fixture segment %d: %v", id, err)
	}
	return res.Offsets[CompositeKey("tr", "sp")]
}

func mustAcquire(t *testing.T, c *Cache, id uint64) (*Reader, *Release) {
	t.Helper()
	r, rel, err := c.Acquire(id)
	if err != nil {
		t.Fatalf("Acquire(%d): %v", id, err)
	}
	return r, rel
}

func TestCacheAcquireHitReturnsSameReader(t *testing.T) {
	dir := t.TempDir()
	writeCacheFixture(t, dir, 1)
	c := NewCache(dir, 4)
	defer c.Close()

	r1, rel1 := mustAcquire(t, c, 1)
	rel1.Release()
	r2, rel2 := mustAcquire(t, c, 1)
	defer rel2.Release()

	if r1 != r2 {
		t.Fatal("second Acquire returned a different reader; hit path is not reusing the cached handle")
	}
	if _, _, err := c.Acquire(99); err == nil {
		t.Fatal("Acquire(99) on a missing segment: want error")
	}
}

func TestCachePurgedIDIsNeverReopened(t *testing.T) {
	dir := t.TempDir()
	offset := writeCacheFixture(t, dir, 1)
	c := NewCache(dir, 4)
	defer c.Close()

	r, rel := mustAcquire(t, c, 1)
	rel.Release()

	if err := c.Purge([]uint64{1}); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	// The file still exists here — Purge is about handles, not deletion — but
	// the dead set must refuse to open it again: a reopen would hand the
	// upcoming unlink a fresh handle (the resurrection race).
	if _, _, err := c.Acquire(1); !errors.Is(err, ErrPurged) {
		t.Fatalf("Acquire after purge: got %v, want ErrPurged", err)
	}
	// Purging an id never opened is harmless.
	if err := c.Purge([]uint64{42}); err != nil {
		t.Fatalf("Purge(unopened): %v", err)
	}
	// The handle is closed: reads fail even though the file is on disk.
	if _, err := r.ReadAt(offset); err == nil {
		t.Fatal("ReadAt on purged reader: want error, got success")
	}
	// And unlink now succeeds — this is the property compaction depends on.
	if err := os.Remove(SegmentPath(dir, 1)); err != nil {
		t.Fatalf("remove after purge: %v", err)
	}
}

func TestCachePurgeWaitsForInflightReader(t *testing.T) {
	dir := t.TempDir()
	offset := writeCacheFixture(t, dir, 1)
	c := NewCache(dir, 4)
	defer c.Close()

	r, rel := mustAcquire(t, c, 1)

	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		done <- c.Purge([]uint64{1})
	}()
	<-started

	select {
	case err := <-done:
		t.Fatalf("Purge returned while the reader was still held (err=%v)", err)
	case <-time.After(100 * time.Millisecond):
	}

	rel.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Purge after release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Purge did not return after the reader was released")
	}

	if _, err := r.ReadAt(offset); err == nil {
		t.Fatal("ReadAt on purged reader: want error, got success")
	}
}

func TestCachePurgeTimesOutInsteadOfClosingHeldReader(t *testing.T) {
	dir := t.TempDir()
	writeCacheFixture(t, dir, 1)
	c := NewCache(dir, 4)
	defer c.Close()
	c.purgeWait = 50 * time.Millisecond

	_, rel := mustAcquire(t, c, 1)
	start := time.Now()
	err := c.Purge([]uint64{1})
	if !errors.Is(err, ErrPurgeTimeout) {
		t.Fatalf("Purge with held reader: got %v, want ErrPurgeTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Purge timeout took %s, expected ~%s", elapsed, c.purgeWait)
	}
	// The entry was still detached, so the last release closes it.
	if err := rel.Release(); err != nil {
		t.Fatalf("release after timed-out purge: %v", err)
	}
	// And the dead set still refuses reopen even though the purge timed out.
	if _, _, err := c.Acquire(1); !errors.Is(err, ErrPurged) {
		t.Fatalf("Acquire after timed-out purge: got %v, want ErrPurged", err)
	}
}

func TestCacheEvictsLeastRecentlyUsedIdleEntry(t *testing.T) {
	dir := t.TempDir()
	writeCacheFixture(t, dir, 1)
	offset2 := writeCacheFixture(t, dir, 2)
	writeCacheFixture(t, dir, 3)
	c := NewCache(dir, 2)
	defer c.Close()

	r1, rel1 := mustAcquire(t, c, 1)
	rel1.Release()
	r2, rel2 := mustAcquire(t, c, 2)
	rel2.Release()
	// Touch id 1: id 2 is now the least recently used.
	if r, rel, err := c.Acquire(1); err != nil {
		t.Fatalf("Acquire(1): %v", err)
	} else {
		if r != r1 {
			t.Fatal("Acquire(1) before eviction returned a different reader")
		}
		rel.Release()
	}

	// Insert a third entry: overflow evicts id 2 (LRU, idle).
	_, rel3 := mustAcquire(t, c, 3)
	rel3.Release()

	r2b, rel2b := mustAcquire(t, c, 2)
	defer rel2b.Release()
	// The evicted entry's reader was closed, so this must be a fresh open of
	// the same file — a different reader object.
	if r2b == r2 {
		t.Fatal("evicted reader was handed back; eviction did not close and reopen id 2")
	}
	if _, err := r2b.ReadAt(offset2); err != nil {
		t.Fatalf("ReadAt on reopened reader: %v", err)
	}
}

func TestCacheEvictionSkipsInUseEntry(t *testing.T) {
	dir := t.TempDir()
	for id := uint64(1); id <= 2; id++ {
		writeCacheFixture(t, dir, id)
	}
	c := NewCache(dir, 1)
	defer c.Close()

	// Hold both entries across an overflow: neither is idle, so eviction must
	// leave them alone rather than close a reader someone is reading — the
	// size bound overshoots instead.
	r1, rel1 := mustAcquire(t, c, 1)
	r2, rel2 := mustAcquire(t, c, 2)

	r1b, rel1b := mustAcquire(t, c, 1)
	defer rel1b.Release()
	r2b, rel2b := mustAcquire(t, c, 2)
	defer rel2b.Release()
	if r1b != r1 || r2b != r2 {
		t.Fatal("in-use entries were evicted despite maxEntries pressure")
	}
	rel1.Release()
	rel2.Release()
}

func TestCacheDoubleReleaseIsHarmless(t *testing.T) {
	dir := t.TempDir()
	writeCacheFixture(t, dir, 1)
	c := NewCache(dir, 4)
	defer c.Close()

	_, rel := mustAcquire(t, c, 1)
	if err := rel.Release(); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if err := rel.Release(); err != nil {
		t.Fatalf("second release must be a no-op, got %v", err)
	}
	// Refcount is exactly 0, so purge can complete without waiting.
	if err := c.Purge([]uint64{1}); err != nil {
		t.Fatalf("Purge after double release: %v", err)
	}
}

func TestCacheCloseClosesIdleReadersAndRejectsAcquire(t *testing.T) {
	dir := t.TempDir()
	offset := writeCacheFixture(t, dir, 1)
	c := NewCache(dir, 4)

	r, rel := mustAcquire(t, c, 1)
	rel.Release()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := r.ReadAt(offset); err == nil {
		t.Fatal("ReadAt after Close: want error, got success")
	}
	if _, _, err := c.Acquire(1); !errors.Is(err, ErrCacheClosed) {
		t.Fatalf("Acquire after Close: got %v, want ErrCacheClosed", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close must be a no-op, got %v", err)
	}
	// Handles are gone: unlink succeeds.
	if err := os.Remove(SegmentPath(dir, 1)); err != nil {
		t.Fatalf("remove after close: %v", err)
	}
}

func TestCacheCloseForceClosesInflightReader(t *testing.T) {
	dir := t.TempDir()
	writeCacheFixture(t, dir, 1)
	c := NewCache(dir, 4)
	c.purgeWait = 50 * time.Millisecond

	_, rel := mustAcquire(t, c, 1) // leaked on purpose: never released
	start := time.Now()
	if err := c.Close(); err != nil {
		t.Fatalf("Close with in-flight reader: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Close took %s with an in-flight reader; it must not hang", elapsed)
	}
	// Late release after force-close must not panic or double-close.
	if err := rel.Release(); err != nil {
		t.Fatalf("late release: %v", err)
	}
}

func TestCacheConcurrentAcquireRelease(t *testing.T) {
	dir := t.TempDir()
	offsets := make(map[uint64]uint64, 3)
	for id := uint64(1); id <= 3; id++ {
		offsets[id] = writeCacheFixture(t, dir, id)
	}
	c := NewCache(dir, 2) // small bound forces concurrent eviction pressure
	defer c.Close()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id := uint64(1 + (seed+i)%3)
				r, rel, err := c.Acquire(id)
				if err != nil {
					t.Errorf("Acquire(%d): %v", id, err)
					return
				}
				if _, err := r.ReadAt(offsets[id]); err != nil {
					t.Errorf("ReadAt on shared reader: %v", err)
				}
				if err := rel.Release(); err != nil {
					t.Errorf("release: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	// Everything drained: purge completes promptly and handles are closed.
	done := make(chan error, 1)
	go func() { done <- c.Purge([]uint64{1, 2, 3}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Purge after drain: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Purge after drain did not complete")
	}
}
