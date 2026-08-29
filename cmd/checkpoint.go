package cmd

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Checkpoint persists the id of the last segment for which both the segment
// file and its Pebble index entries are confirmed durable (PAD1 v1.1
// amendment). On startup, reconciliation only needs to consider state after
// this watermark — not the full WAL/segment history — which is what keeps
// boot time bounded as data grows (PDD1 §6 DoD requires startup under 5
// minutes; unbounded replay would eventually violate that).
//
// The checkpoint is itself derived, rebuildable state, not a second source
// of truth (PAD1 §1: "WAL is the single source of truth"). If this file is
// lost or corrupt, the safe fallback is a full WAL replay from genesis next
// startup — slower, never wrong.
type Checkpoint struct {
	path string
}

// NewCheckpoint returns a Checkpoint backed by the given file path. The file
// is not created until the first Save.
func NewCheckpoint(path string) *Checkpoint {
	return &Checkpoint{path: path}
}

// Load returns the last confirmed segment id, or 0 if no checkpoint has ever
// been saved (a fresh database, or an existing one predating the checkpoint
// mechanism — in which case the caller should treat everything as
// unconfirmed and fall back to full WAL replay).
func (c *Checkpoint) Load() (uint64, error) {
	data, err := os.ReadFile(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read checkpoint: %w", err)
	}
	if len(data) != 8 {
		return 0, fmt.Errorf("checkpoint file corrupt: want 8 bytes, got %d", len(data))
	}
	return binary.LittleEndian.Uint64(data), nil
}

// Save durably records segmentID as the new watermark. Writes to a temp file,
// fsyncs it, then renames it over the checkpoint path — the rename is atomic
// on both POSIX and Windows, so a crash mid-save leaves either the old value
// or the new one on disk, never a torn file. This must only be called after
// both the segment write and its Pebble index batch are confirmed (PAD1 §4);
// calling it earlier would let the checkpoint claim more is durable than
// actually is.
func (c *Checkpoint) Save(segmentID uint64) error {
	tmp := c.path + ".tmp"

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], segmentID)

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create temp checkpoint: %w", err)
	}
	if _, err := f.Write(buf[:]); err != nil {
		f.Close()
		return fmt.Errorf("write temp checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp checkpoint: %w", err)
	}
	if err := os.Rename(tmp, c.path); err != nil {
		return fmt.Errorf("rename checkpoint into place: %w", err)
	}
	return nil
}
