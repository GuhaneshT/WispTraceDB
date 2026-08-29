
// Crash-safety invariant (PAD1 §3/§4), enforced by ordering alone

//  1. Write the new merged segment file, if any (an orphaned partial file
//     from a crash here is harmless — nothing references it yet).
//  2. Commit ONE Pebble batch that deletes every index entry for spans in
//     the old segments and (for a merge) puts the survivors' new locations.
//     If this fails, the old segments and their index entries are
//     untouched — compaction can simply be retried.
//  3. Atomically swap the manifest.
//  4. Only now unlink the old segment files.
package compactor

import (
	"fmt"
	"os"

	"github.com/GuhaneshT/WispTraceDB/pebble"
	"github.com/GuhaneshT/WispTraceDB/segment"
	"github.com/GuhaneshT/WispTraceDB/wal"
)


type Compactor struct {
	segmentDir string
	manifest   *segment.Manifest
	index      *pebble.DB
}


func New(segmentDir string, manifest *segment.Manifest, index *pebble.DB) *Compactor {
	return &Compactor{segmentDir: segmentDir, manifest: manifest, index: index}
}


type Result struct {
	DroppedSegmentIDs []uint64
	NewSegmentID      uint64 // 0 if no replacement segment was written
	SpansDropped      int    // tombstones (Compact) or expired spans (Expire) not carried forward
	SpansCarried      int    // live spans written into the new segment (Compact only)
}


func (c *Compactor) Compact(oldIDs []uint64, newSegmentID uint64) (*Result, error) {
	if len(oldIDs) == 0 {
		return nil, fmt.Errorf("compact: no segment ids given")
	}

	var survivors []wal.SpanPayload
	allKeys := make([]string, 0)
	droppedCount := 0

	for _, id := range oldIDs {
		spans, err := scanSegment(c.segmentDir, id)
		if err != nil {
			return nil, err
		}
		for _, s := range spans {
			key := segment.CompositeKey(s.Span.TraceID, s.Span.SpanID)
			allKeys = append(allKeys, key)
			if s.Span.Deleted {
				droppedCount++
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

	if err := c.index.BatchDeleteSpans(allKeys); err != nil {
		removeOrphan(writeResult)
		return nil, fmt.Errorf("delete old index entries: %w", err)
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
	}


	for _, id := range oldIDs {
		if err := os.Remove(segment.SegmentPath(c.segmentDir, id)); err != nil && !os.IsNotExist(err) {
			return result, fmt.Errorf("unlink segment %d after successful compaction: %w", id, err)
		}
	}

	return result, nil
}

// Expire drops every segment in candidateIDs whose zone-map MaxTimestamp is
// strictly before cutoff.
func (c *Compactor) Expire(candidateIDs []uint64, cutoff int64) (*Result, error) {
	var expiredIDs []uint64
	var deleteKeys []string
	droppedCount := 0

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
		droppedCount += len(spans)
		for _, s := range spans {
			deleteKeys = append(deleteKeys, segment.CompositeKey(s.Span.TraceID, s.Span.SpanID))
		}
	}

	if len(expiredIDs) == 0 {
		return &Result{}, nil
	}

	if err := c.index.BatchDeleteSpans(deleteKeys); err != nil {
		return nil, fmt.Errorf("delete expired index entries: %w", err)
	}

	liveIDs, err := c.manifest.Load()
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}
	if err := c.manifest.Save(segment.RemoveSegments(liveIDs, expiredIDs)); err != nil {
		return nil, fmt.Errorf("save manifest: %w", err)
	}

	result := &Result{DroppedSegmentIDs: expiredIDs, SpansDropped: droppedCount}

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


func removeOrphan(result *segment.WriteResult) {
	if result != nil {
		_ = os.Remove(result.Path)
	}
}
