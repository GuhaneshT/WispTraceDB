package segment

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

// Manifest is the on-disk record of which segment ids currently make up
// live, queryable state
// The manifest is derived state, same as cmd.Checkpoint: if it were lost,
// the safe (if wasteful) fallback is treating every segment file found on
// disk as live and rebuilding the manifest from that directory listing.
type Manifest struct {
	path string
}

// NewManifest returns a Manifest backed by the given file path
func NewManifest(path string) *Manifest {
	return &Manifest{path: path}
}

// Load returns the current set of live segment ids, ascending. Returns a nil
// slice (not an error) if no manifest has ever been saved.
func (m *Manifest) Load() ([]uint64, error) {
	data, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	if len(data)%8 != 0 {
		return nil, fmt.Errorf("manifest file corrupt: length %d not a multiple of 8", len(data))
	}
	ids := make([]uint64, len(data)/8)
	for i := range ids {
		ids[i] = binary.LittleEndian.Uint64(data[i*8 : i*8+8])
	}
	return ids, nil
}

// Save durably replaces the manifest with the given segment ids (sorted
// ascending on write, for a deterministic on-disk representation)
func (m *Manifest) Save(ids []uint64) error {
	sorted := append([]uint64(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	tmp := m.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create temp manifest: %w", err)
	}

	w := bufio.NewWriter(f)
	var buf [8]byte
	for _, id := range sorted {
		binary.LittleEndian.PutUint64(buf[:], id)
		if _, err := w.Write(buf[:]); err != nil {
			f.Close()
			return fmt.Errorf("write temp manifest: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return fmt.Errorf("flush temp manifest: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp manifest: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp manifest: %w", err)
	}
	if err := os.Rename(tmp, m.path); err != nil {
		return fmt.Errorf("rename manifest into place: %w", err)
	}
	return nil
}

// AddSegment idempotently records id as live
func (m *Manifest) AddSegment(id uint64) error {
	ids, err := m.Load()
	if err != nil {
		return err
	}
	for _, existing := range ids {
		if existing == id {
			return nil
		}
	}
	return m.Save(append(ids, id))
}


func RemoveSegments(ids []uint64, remove []uint64) []uint64 {
	removeSet := make(map[uint64]bool, len(remove))
	for _, id := range remove {
		removeSet[id] = true
	}
	kept := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if !removeSet[id] {
			kept = append(kept, id)
		}
	}
	return kept
}
