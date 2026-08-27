package pebble

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// SpanLocation is the value stored in Pebble
type SpanLocation struct {
	SegmentID uint64
	Offset    uint64
}

//key = trace_id||span_id, value = (segment_id, offset).
type DB struct {
	db   *pebble.DB
	path string
}

// OpenDB opens or creates a Pebble LSM at the given path.
func OpenDB(path string) (*DB, error) {
	if path == "" {
		path = "wisp_lsm"
	}
	lsmdb, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("pebble open %s: %w", path, err)
	}
	return &DB{db: lsmdb, path: path}, nil
}

// Close flushes pending writes and closes the database.
func (d *DB) Close() error {
	if d.db == nil {
		return nil
	}
	return d.db.Close()
}


func (d *DB) PutSpan(key []byte, location SpanLocation) error {
	value := encodeSpanLocation(location)
	return d.db.Set(key, value, &pebble.WriteOptions{Sync: true})
}

// Delete also returns a not found here
func (d *DB) GetSpan(key []byte) (SpanLocation, error) {
	value, closer, err := d.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return SpanLocation{}, pebble.ErrNotFound
		}
		return SpanLocation{}, fmt.Errorf("pebble get: %w", err)
	}
	defer closer.Close()

	location, err := decodeSpanLocation(value)
	if err != nil {
		return SpanLocation{}, fmt.Errorf("decode span location: %w", err)
	}
	return location, nil
}

// Scan to reconstruct a full trace
func (d *DB) PrefixScan(prefix []byte) ([]SpanLocation, error) {
	iter := d.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	defer iter.Close()

	var locations []SpanLocation
	for iter.First(); iter.Valid(); iter.Next() {
		location, err := decodeSpanLocation(iter.Value())
		if err != nil {
			return nil, fmt.Errorf("decode span location at key %s: %w", string(iter.Key()), err)
		}
		locations = append(locations, location)
	}
	return locations, iter.Close()
}


func (d *DB) DeleteSpan(key []byte) error {
	return d.db.Delete(key, &pebble.WriteOptions{Sync: true})
}


func encodeSpanLocation(loc SpanLocation) []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], loc.SegmentID)
	binary.LittleEndian.PutUint64(buf[8:16], loc.Offset)
	return buf
}


func decodeSpanLocation(data []byte) (SpanLocation, error) {
	if len(data) != 16 {
		return SpanLocation{}, fmt.Errorf("span location must be 16 bytes, got %d", len(data))
	}
	return SpanLocation{
		SegmentID: binary.LittleEndian.Uint64(data[0:8]),
		Offset:    binary.LittleEndian.Uint64(data[8:16]),
	}, nil
}


func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix)+1)
	copy(upper, prefix)
	upper[len(prefix)] = 0x00
	return upper
}