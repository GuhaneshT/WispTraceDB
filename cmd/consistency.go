package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/GuhaneshT/WispTraceDB/segment"
)

// ConsistencyReport is the result of a CheckConsistency run. Errors are
// divergences that compromise correctness (data loss, dangling pointers,
// corrupt files); Warnings are harmless-but-noticeable conditions such as
// unreferenced segment files left behind by a crash mid-compaction.
type ConsistencyReport struct {
	Errors   []string
	Warnings []string
}

func (r *ConsistencyReport) OK() bool {
	return len(r.Errors) == 0
}

func (r *ConsistencyReport) addError(format string, args ...interface{}) {
	r.Errors = append(r.Errors, fmt.Sprintf(format, args...))
}

func (r *ConsistencyReport) addWarning(format string, args ...interface{}) {
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, args...))
}

// CheckConsistency cross-checks the on-disk state tree: manifest vs segment
// files, trace index vs segments (both directions), and checkpoint sanity.
//
// Invariants checked:
//   - every manifest-listed segment exists, parses, and matches its header id;
//   - every record in every manifest segment decodes and CRC-checks;
//   - every index entry resolves through a readable segment to a record whose
//     composite key matches the key it is filed under;
//   - every live (non-tombstoned) record in a manifest segment is indexed at
//     its exact (segment_id, offset). Tombstoned records may be present or
//     reclaimed, but never inconsistently pointed at;
//   - checkpoint never exceeds the highest segment id on disk;
//   - segment files not present in the manifest are reported as warnings
//     (benign crash residue — the id allocator and recovery already skip them).
func (w *WispTrace) CheckConsistency() *ConsistencyReport {
	rep := &ConsistencyReport{}

	live, err := w.manifest.Load()
	if err != nil {
		rep.addError("manifest: %v", err)
		return rep
	}

	cp, err := w.checkpoint.Load()
	if err != nil {
		rep.addError("checkpoint: %v", err)
	}

	files, err := listSegmentFiles(w.config.SegmentDir)
	if err != nil {
		rep.addError("scan segment dir: %v", err)
		return rep
	}

	inManifest := make(map[uint64]bool, len(live))
	for _, id := range live {
		inManifest[id] = true
	}

	// 1. Manifest <-> files, plus a decode pass over every listed segment.
	for _, id := range live {
		path := segment.SegmentPath(w.config.SegmentDir, id)
		if _, ok := files[id]; !ok {
			rep.addError("manifest lists segment %d (%s) but no such file exists", id, path)
			continue
		}
		reader, err := segment.OpenReader(path)
		if err != nil {
			rep.addError("segment %d (%s): %v", id, path, err)
			continue
		}
		if reader.Header.SegmentID != id {
			rep.addError("segment file %s header declares id %d, expected %d", path, reader.Header.SegmentID, id)
			reader.Close()
			continue
		}
		spans, scanErr := reader.ScanAll()
		closeErr := reader.Close()
		if scanErr != nil {
			rep.addError("segment %d (%s) failed to scan: %v", id, path, scanErr)
			continue
		}
		if closeErr != nil {
			rep.addError("segment %d (%s) failed to close: %v", id, path, closeErr)
			continue
		}
		if uint32(len(spans)) != reader.Header.SpanCount {
			rep.addError("segment %d header claims %d spans, scanned %d records", id, reader.Header.SpanCount, len(spans))
		}
	}

	// 2. Files that exist but are not in the manifest (crash residue).
	for id := range files {
		if !inManifest[id] {
			rep.addWarning("segment file %s (%d) is not listed in the manifest", files[id], id)
		}
	}

	// 3. Index <-> segments, both directions.
	indexed, err := w.index.ScanAll()
	if err != nil {
		rep.addError("scan index: %v", err)
		return rep
	}

	for key, loc := range indexed {
		path := segment.SegmentPath(w.config.SegmentDir, loc.SegmentID)
		reader, err := segment.OpenReader(path)
		if err != nil {
			rep.addError("index entry %q points at segment %d (%s): %v", key, loc.SegmentID, path, err)
			continue
		}
		span, readErr := reader.ReadAt(loc.Offset)
		reader.Close()
		if readErr != nil {
			rep.addError("index entry %q points at segment %d offset %d which has no readable record: %v", key, loc.SegmentID, loc.Offset, readErr)
			continue
		}
		if segment.CompositeKey(span.TraceID, span.SpanID) != key {
			rep.addError("index entry %q resolves to record %q at segment %d offset %d", key, segment.CompositeKey(span.TraceID, span.SpanID), loc.SegmentID, loc.Offset)
		}
	}

	// Reverse direction: every live record in a manifest segment must be
	// indexed at exactly its own (segment_id, offset). Records filed under a
	// segment the index no longer references are superseded (safe to leave;
	// unreferenced segments are dropped from the manifest by reconciliation).
	for _, id := range live {
		spans, err := w.scanSegmentRecords(id)
		if err != nil {
			rep.addError("segment %d rescan: %v", id, err)
			continue
		}
		for _, sc := range spans {
			key := segment.CompositeKey(sc.Span.TraceID, sc.Span.SpanID)
			loc, ok := indexed[key]
			if sc.Span.Deleted {
				if ok && (loc.SegmentID != id || loc.Offset != sc.Offset) {
					// tombstones may be reclaimed entirely, but if still
					// indexed they must point at this exact record
					rep.addWarning("tombstoned span %q in segment %d is indexed as segment %d offset %d", key, id, loc.SegmentID, loc.Offset)
				}
				continue
			}
			if !ok {
				rep.addError("live span %q in segment %d has no index entry", key, id)
				continue
			}
			if loc.SegmentID != id || loc.Offset != sc.Offset {
				rep.addError("live span %q in segment %d offset %d is indexed as segment %d offset %d", key, id, sc.Offset, loc.SegmentID, loc.Offset)
			}
		}
	}

	// 4. Checkpoint sanity: a watermark, never above the highest segment id.
	highest := uint64(0)
	for id := range files {
		if id > highest {
			highest = id
		}
	}
	if cp > highest {
		rep.addError("checkpoint is %d but the highest segment id on disk is %d", cp, highest)
	}

	return rep
}

// scanSegmentRecords is a small helper to decode every record of one segment.
func (w *WispTrace) scanSegmentRecords(id uint64) ([]segment.ScannedSpan, error) {
	reader, err := segment.OpenReader(segment.SegmentPath(w.config.SegmentDir, id))
	if err != nil {
		return nil, err
	}
	spans, scanErr := reader.ScanAll()
	closeErr := reader.Close()
	if scanErr != nil {
		return nil, scanErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return spans, nil
}

// listSegmentFiles returns the on-disk segment files in dir keyed by the id
// parsed from their strict segment_<id>.seg filename. Rollup snapshots and
// *.tmp files never match this pattern.
func listSegmentFiles(dir string) (map[uint64]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make(map[uint64]string)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "segment_") || !strings.HasSuffix(name, ".seg") {
			continue
		}
		digits := strings.TrimSuffix(strings.TrimPrefix(name, "segment_"), ".seg")
		id, perr := strconv.ParseUint(digits, 10, 64)
		if perr != nil {
			continue
		}
		files[id] = name
	}
	return files, nil
}