// Package compactor merges segments and expires them for retention (PAD1
// §3). Segments are swapped atomically via segment.Manifest, never mutated.
//
// Staleness check: unlike a classic LSM, Pebble here holds exactly one
// current (segment_id, offset) per key — no fallback scan across segments.
// So before touching any scanned span, we check it's still Pebble's current
// entry for that key; if not, it's superseded elsewhere (usually a later
// tombstone) and must be left untouched. This makes tombstone reclamation
// always safe, with no need to track whether a merge is "major" (covers
// every segment) the way a classic LSM compactor would.
//
// Crash safety comes from ordering alone: write the new segment (harmless if
// orphaned) -> commit the Pebble batch (retryable if it fails) -> swap the
// manifest -> only then unlink old segment files, which by then nothing
// references.
package compactor

import (
	"fmt"
	"os"

	"github.com/GuhaneshT/WispTraceDB/pebble"
	"github.com/GuhaneshT/WispTraceDB/segment"
	"github.com/GuhaneshT/WispTraceDB/wal"
)

// Compactor executes merges and expiry against one engine's segment
// directory, manifest, and Pebble index.
type Compactor struct {
	segmentDir string
	manifest   *segment.Manifest
	index      *pebble.DB
}

// New returns a Compactor bound to the given segment directory, manifest,
// and index. All three must belong to the same WispTrace instance.
func New(segmentDir string, manifest *segment.Manifest, index *pebble.DB) *Compactor {
	return &Compactor{segmentDir: segmentDir, manifest: manifest, index: index}
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
		spans, err := scanSegment(c.segmentDir, id)
		if err != nil {
			return nil, err
		}
		for _, s := range spans {
			key := segment.CompositeKey(s.Span.TraceID, s.Span.SpanID)

			current, err := c.index.GetSpan([]byte(key))
			if err != nil || current.SegmentID != id || current.Offset != s.Offset {
				// Reclaimed already, or superseded by a write elsewhere.
				staleCount++
				continue
			}

			if s.Span.Deleted {
				droppedCount++
				reclaimKeys = append(reclaimKeys, key)
				continue
			}
			survivors = append(survivors, s.Span)
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
	// was already stale (never pointing here).
	for _, id := range oldIDs {
		if err := os.Remove(segment.SegmentPath(c.segmentDir, id)); err != nil && !os.IsNotExist(err) {
			return result, fmt.Errorf("unlink segment %d after successful compaction: %w", id, err)
		}
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
		spans, err := reader.ScanAll()
		closeErr := reader.Close()
		if err != nil {
			return nil, fmt.Errorf("scan segment %d: %w", id, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close segment %d reader: %w", id, closeErr)
		}

		expiredIDs = append(expiredIDs, id)
		for _, s := range spans {
			key := segment.CompositeKey(s.Span.TraceID, s.Span.SpanID)

			current, err := c.index.GetSpan([]byte(key))
			if err != nil || current.SegmentID != id || current.Offset != s.Offset {
				staleCount++
				continue
			}

			droppedCount++
			reclaimKeys = append(reclaimKeys, key)
		}
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

	for _, id := range expiredIDs {
		if err := os.Remove(segment.SegmentPath(c.segmentDir, id)); err != nil && !os.IsNotExist(err) {
			return result, fmt.Errorf("unlink expired segment %d: %w", id, err)
		}
	}

	return result, nil
}

func scanSegment(dir string, id uint64) ([]segment.ScannedSpan, error) {
	reader, err := segment.OpenReader(segment.SegmentPath(dir, id))
	if err != nil {
		return nil, fmt.Errorf("open segment %d: %w", id, err)
	}
	spans, err := reader.ScanAll()
	closeErr := reader.Close()
	if err != nil {
		return nil, fmt.Errorf("scan segment %d: %w", id, err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close segment %d reader: %w", id, closeErr)
	}
	return spans, nil
}

// removeOrphan deletes a newly-written segment that a later step failed to
// index. Best-effort: it was never added to the manifest either way.
func removeOrphan(result *segment.WriteResult) {
	if result != nil {
		_ = os.Remove(result.Path)
	}
}
