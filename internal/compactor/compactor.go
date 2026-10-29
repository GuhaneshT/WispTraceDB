// Package compactor merges segments and expires them for retention (PAD1
// §3). Segments are swapped atomically via segment.Manifest, never mutated.
//
// Staleness check: unlike a classic LSM, Pebble here holds exactly one
// current (segment_id, offset) per key — no fallback scan across segments.
// So before touching any scanned span, we check it's still Pebble's current
// entry for that key; if not, it is superseded and must be left untouched.
// This makes tombstone reclamation always safe, with no need to track whether a
// merge is "major" (covers every segment) the way a classic LSM compactor would.
//
// Superseded splits into two shapes here. A key that was overwritten lands on a
// different (segment, offset). A key that was tombstoned has no index entry at
// all, because the flush path deletes tombstone keys rather than repointing
// them. Both fail the check, and both must be dropped rather than carried into
// a merged segment.
//
// Crash safety comes from ordering alone: write the new segment (harmless if
// orphaned) -> commit the Pebble batch (retryable if it fails) -> swap the
// manifest -> purge reader handles -> only then unlink old segment files,
// which by then nothing references. The purge step exists because "nothing
// references" is not enough on Windows: os.Remove also needs no open handle,
// and the reader cache may still hold one (see Purger).
package compactor

import (
	"fmt"
	"os"
	"time"

	"github.com/GuhaneshT/WispTraceDB/internal/pebble"
	"github.com/GuhaneshT/WispTraceDB/internal/segment"
	"github.com/GuhaneshT/WispTraceDB/internal/wal"
)

// Purger is the pre-unlink handle handoff: compaction and expiry call Purge
// with the ids they are about to remove, and Purge returning nil guarantees
// no reader handle for those ids exists anywhere in the process. On Windows
// os.Remove fails while any handle is open (see segment's
// TestDeleteWhileOpenProbe), so this ordering is mandatory, not an
// optimization. Normally the segment reader cache implements this; with no
// cache registered the compactor's own scanners already open-and-close
// before reaching the unlink.
type Purger interface {
	Purge(ids []uint64) error
}

// Compactor executes merges and expiry against one engine's segment
// directory, manifest, and Pebble index.
type Compactor struct {
	segmentDir string
	manifest   *segment.Manifest
	index      *pebble.DB
	purger     Purger // optional; nil means no pre-unlink handle handoff
}

// New returns a Compactor bound to the given segment directory, manifest,
// and index. All three must belong to the same WispTrace instance.
func New(segmentDir string, manifest *segment.Manifest, index *pebble.DB) *Compactor {
	return &Compactor{segmentDir: segmentDir, manifest: manifest, index: index}
}

// WithPurger registers the pre-unlink purger and returns c for chaining. Pass
// a non-nil purger only: a typed-nil interface would pass the nil check in
// purgeAndUnlink and panic when called.
func (c *Compactor) WithPurger(p Purger) *Compactor {
	c.purger = p
	return c
}

// Result reports what a Compact or Expire call did.
type Result struct {
	DroppedSegmentIDs []uint64
	NewSegmentID      uint64 // 0 if no replacement segment was written
	SpansDropped      int    // tombstones reclaimed
	SpansCarried      int    // live spans written into the new segment (Compact only)
	SpansStale        int    // records superseded elsewhere — skipped untouched
}

// Compact merges oldIDs into one new segment at newSegmentID, dropping
// tombstones (see package doc for the staleness check that makes this safe).
func (c *Compactor) Compact(oldIDs []uint64, newSegmentID uint64) (*Result, error) {
	if len(oldIDs) == 0 {
		return nil, fmt.Errorf("compact: no segment ids given")
	}

	var survivors []wal.SpanPayload
	var reclaimKeys []string
	droppedCount := 0
	staleCount := 0

	for _, id := range oldIDs {
		err := scanSegment(c.segmentDir, id, func(offset uint64, s wal.SpanPayload) bool {
			key := segment.CompositeKey(s.TraceID, s.SpanID)

			// Staleness check, and it must run BEFORE the tombstone test. Pebble
			// holds exactly one (segment_id, offset) per key with no
			// cross-segment fallback, so a copy the index no longer points at is
			// a superseded version and must be left completely alone.
			//
			// Including tombstones. A tombstone that is NOT the index's current
			// entry has been superseded — by a re-insert of the same key, say —
			// and acting on it would delete the newer live version's index entry
			// (see reclaimKeys below), making a live span unreadable. Only the
			// tombstone the index actually points at may be reclaimed.
			//
			// Note this now also covers the common case: a post-fix tombstone is
			// deleted from the index at flush time, so the lookup misses and the
			// record is classified stale. That is correct — it is already dead and
			// there is nothing to clean up.
			current, err := c.index.GetSpan([]byte(key))
			if err != nil || current.SegmentID != id || current.Offset != offset {
				staleCount++
				return true
			}

			// The index points at this record and the record is a tombstone: it
			// is the authoritative delete, so reclaim the key and never carry
			// the tombstone into a merged segment.
			if s.Deleted {
				droppedCount++
				reclaimKeys = append(reclaimKeys, key)
				return true
			}
			survivors = append(survivors, s)
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	var writeResult *segment.WriteResult
	if len(survivors) > 0 {
		writer := segment.NewWriter()
		for _, s := range survivors {
			writer.Add(s)
		}
		result, err := writer.Flush(c.segmentDir, newSegmentID)
		if err != nil {
			return nil, fmt.Errorf("write merged segment %d: %w", newSegmentID, err)
		}
		writeResult = result
	}

	if len(reclaimKeys) > 0 {
		if err := c.index.BatchDeleteSpans(reclaimKeys); err != nil {
			removeOrphan(writeResult)
			return nil, fmt.Errorf("reclaim tombstoned index entries: %w", err)
		}
	}
	if writeResult != nil {
		entries := make(map[string]pebble.SpanLocation, len(writeResult.Offsets))
		for key, offset := range writeResult.Offsets {
			entries[key] = pebble.SpanLocation{SegmentID: writeResult.SegmentID, Offset: offset}
		}
		if err := c.index.BatchPutSpans(entries); err != nil {
			removeOrphan(writeResult)
			return nil, fmt.Errorf("index merged segment %d: %w", newSegmentID, err)
		}
	}

	liveIDs, err := c.manifest.Load()
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}
	nextLive := segment.RemoveSegments(liveIDs, oldIDs)
	newID := uint64(0)
	if writeResult != nil {
		newID = writeResult.SegmentID
		nextLive = append(nextLive, newID)
	}
	if err := c.manifest.Save(nextLive); err != nil {
		return nil, fmt.Errorf("save manifest: %w", err)
	}

	result := &Result{
		DroppedSegmentIDs: oldIDs,
		NewSegmentID:      newID,
		SpansDropped:      droppedCount,
		SpansCarried:      len(survivors),
		SpansStale:        staleCount,
	}

	// Safe to unlink: every key from these segments was moved, reclaimed, or
	// was already stale (never pointing here). Purge first so no reader
	// handle is open when os.Remove runs, then retry briefly for handles we
	// do not own (search indexer, antivirus).
	if err := c.purgeAndUnlink(oldIDs); err != nil {
		return result, fmt.Errorf("unlink segments after successful compaction: %w", err)
	}

	return result, nil
}

// Expire drops every segment in candidateIDs whose zone-map MaxTimestamp is
// before cutoff — no merge, just removal, same staleness check as Compact.
func (c *Compactor) Expire(candidateIDs []uint64, cutoff int64) (*Result, error) {
	var expiredIDs []uint64
	var reclaimKeys []string
	droppedCount := 0
	staleCount := 0

	for _, id := range candidateIDs {
		reader, err := segment.OpenReader(segment.SegmentPath(c.segmentDir, id))
		if err != nil {
			return nil, fmt.Errorf("open segment %d: %w", id, err)
		}
		if reader.Header.MaxTimestamp >= cutoff {
			reader.Close()
			continue
		}
		scanErr := reader.ScanAllFunc(func(offset uint64, s wal.SpanPayload) bool {
			key := segment.CompositeKey(s.TraceID, s.SpanID)

			current, err := c.index.GetSpan([]byte(key))
			if err != nil || current.SegmentID != id || current.Offset != offset {
				staleCount++
				return true
			}

			droppedCount++
			reclaimKeys = append(reclaimKeys, key)
			return true
		})
		closeErr := reader.Close()
		if scanErr != nil {
			return nil, fmt.Errorf("scan segment %d: %w", id, scanErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close segment %d reader: %w", id, closeErr)
		}

		expiredIDs = append(expiredIDs, id)
	}

	if len(expiredIDs) == 0 {
		return &Result{}, nil
	}

	if len(reclaimKeys) > 0 {
		if err := c.index.BatchDeleteSpans(reclaimKeys); err != nil {
			return nil, fmt.Errorf("delete expired index entries: %w", err)
		}
	}

	liveIDs, err := c.manifest.Load()
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}
	if err := c.manifest.Save(segment.RemoveSegments(liveIDs, expiredIDs)); err != nil {
		return nil, fmt.Errorf("save manifest: %w", err)
	}

	result := &Result{DroppedSegmentIDs: expiredIDs, SpansDropped: droppedCount, SpansStale: staleCount}

	if err := c.purgeAndUnlink(expiredIDs); err != nil {
		return result, fmt.Errorf("unlink expired segments: %w", err)
	}

	return result, nil
}

// scanSegment opens segment id and streams every record through fn, closing
// the reader before returning. It uses ScanAllFunc rather than ScanAll so a
// merge does not materialise a ScannedSpan slice for the whole segment.
func scanSegment(dir string, id uint64, fn func(offset uint64, span wal.SpanPayload) bool) error {
	reader, err := segment.OpenReader(segment.SegmentPath(dir, id))
	if err != nil {
		return fmt.Errorf("open segment %d: %w", id, err)
	}
	scanErr := reader.ScanAllFunc(fn)
	closeErr := reader.Close()
	if scanErr != nil {
		return fmt.Errorf("scan segment %d: %w", id, scanErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close segment %d reader: %w", id, closeErr)
	}
	return nil
}

// removeOrphan deletes a newly-written segment that a later step failed to
// index. Best-effort: it was never added to the manifest either way.
func removeOrphan(result *segment.WriteResult) {
	if result != nil {
		_ = os.Remove(result.Path)
	}
}

// purgeAndUnlink hands ids to the purger — which must close every reader
// handle for them — and only then removes the files. The manifest swap has
// already happened at this point, so a Purge failure (a reader held past the
// purge deadline) aborts the unlink and leaves orphaned files: CheckConsistency
// reports them and the next manifest reconciliation drops them. Disk leak,
// never corruption, and never a partial unlink racing a live reader.
func (c *Compactor) purgeAndUnlink(ids []uint64) error {
	if c.purger != nil {
		if err := c.purger.Purge(ids); err != nil {
			return fmt.Errorf("purge segment readers: %w", err)
		}
	}
	for _, id := range ids {
		if err := removeWithRetry(segment.SegmentPath(c.segmentDir, id)); err != nil {
			return fmt.Errorf("unlink segment %d: %w", id, err)
		}
	}
	return nil
}

const (
	unlinkAttempts     = 3
	unlinkRetryBackoff = 50 * time.Millisecond
)

// removeWithRetry removes path, retrying briefly: our own handles are already
// closed by the purge, but Windows also denies removal for handles we do not
// own (search indexer, antivirus), which typically release within
// milliseconds. An already-absent file is success — another path may have
// removed it first, which the old unlink loops also treated as success.
func removeWithRetry(path string) error {
	var err error
	for attempt := 1; attempt <= unlinkAttempts; attempt++ {
		if err = os.Remove(path); err == nil || os.IsNotExist(err) {
			return nil
		}
		if attempt < unlinkAttempts {
			time.Sleep(unlinkRetryBackoff)
		}
	}
	return err
}
