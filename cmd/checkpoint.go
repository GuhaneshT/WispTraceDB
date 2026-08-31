package cmd

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Checkpoint persists the id of the last segment for which both the segment
// file and its Pebble index entries are confirmed 
type Checkpoint struct {
	path string
}


func NewCheckpoint(path string) *Checkpoint {
	return &Checkpoint{path: path}
}


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
