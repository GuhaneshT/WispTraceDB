package segment

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestManifestLoadMissingReturnsNil(t *testing.T) {
	m := NewManifest(filepath.Join(t.TempDir(), "manifest.dat"))
	ids, err := m.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("Load() = %v, want empty", ids)
	}
}

func TestManifestSaveAndLoadRoundTrip(t *testing.T) {
	m := NewManifest(filepath.Join(t.TempDir(), "manifest.dat"))

	if err := m.Save([]uint64{3, 1, 2}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	got, err := m.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []uint64{1, 2, 3} // Save sorts ascending
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Load() = %v, want %v", got, want)
	}
}

func TestManifestAddSegmentIsIdempotent(t *testing.T) {
	m := NewManifest(filepath.Join(t.TempDir(), "manifest.dat"))

	if err := m.AddSegment(5); err != nil {
		t.Fatalf("AddSegment(5) error = %v", err)
	}
	if err := m.AddSegment(5); err != nil {
		t.Fatalf("AddSegment(5) again error = %v", err)
	}

	got, err := m.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(got) != 1 || got[0] != 5 {
		t.Fatalf("Load() = %v, want [5] (no duplicate)", got)
	}
}

func TestManifestAddSegmentAppendsDistinctIDs(t *testing.T) {
	m := NewManifest(filepath.Join(t.TempDir(), "manifest.dat"))

	for _, id := range []uint64{1, 2, 3} {
		if err := m.AddSegment(id); err != nil {
			t.Fatalf("AddSegment(%d) error = %v", id, err)
		}
	}

	got, err := m.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []uint64{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Load() = %v, want %v", got, want)
	}
}

func TestRemoveSegmentsFiltersGivenIDs(t *testing.T) {
	all := []uint64{1, 2, 3, 4, 5}
	got := RemoveSegments(all, []uint64{2, 4})
	want := []uint64{1, 3, 5}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RemoveSegments() = %v, want %v", got, want)
	}
}

func TestRemoveSegmentsNoMatchReturnsAll(t *testing.T) {
	all := []uint64{1, 2, 3}
	got := RemoveSegments(all, []uint64{99})
	if !reflect.DeepEqual(got, all) {
		t.Fatalf("RemoveSegments() = %v, want %v", got, all)
	}
}

func TestManifestSaveCorruptFileErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.dat")
	m := NewManifest(path)
	if err := m.Save([]uint64{1}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Truncate to a non-multiple-of-8 length to simulate corruption.
	if err := os.Truncate(path, 3); err != nil {
		t.Fatalf("os.Truncate() error = %v", err)
	}

	if _, err := m.Load(); err == nil {
		t.Fatal("Load() on corrupt manifest should error")
	}
}
