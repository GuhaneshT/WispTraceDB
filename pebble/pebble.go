package pebble

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

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


// spanLocationBufPool holds reusable 16-byte location buffers for PutSpan.
// Encoding happens without holding the Pebble lock, so concurrent callers
// each get their own buffer from the pool, avoiding allocation churn on the
// hot path. The pool is only used for the encode-then-write flow; reads
// decode directly without pooling (decoding is cheap, allocation is the issue).
var spanLocationBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 16)
		return &buf
	},
}

func (d *DB) Close() error {
	if d.db == nil {
		return nil
	}
	return d.db.Close()
}

// PutSpan writes a span location into the trace index. key is trace_id||span_id

func (d *DB) PutSpan(key []byte, location SpanLocation) error {
	if d.db == nil {
		return fmt.Errorf("pebble db is closed")
	}
	bufPtr := spanLocationBufPool.Get().(*[]byte)
	defer spanLocationBufPool.Put(bufPtr)
	buf := *bufPtr
	encodeSpanLocationInto(buf, location)
	return d.db.Set(key, buf, &pebble.WriteOptions{Sync: true})
}

// GetSpan retrieves a span location by its composite key (trace_id||span_id).
// Returns ErrNotFound if the key is not in the index.
func (d *DB) GetSpan(key []byte) (SpanLocation, error) {
	if d.db == nil {
		return SpanLocation{}, fmt.Errorf("pebble db is closed")
	}
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

// PrefixScan returns all span locations with the given prefix (e.g., trace_id||).
// Used to reconstruct a full trace. Pre-allocates result slice expecting ~16 spans.
func (d *DB) PrefixScan(prefix []byte) ([]SpanLocation, error) {
	if d.db == nil {
		return nil, fmt.Errorf("pebble db is closed")
	}
	iter,err := d.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err!=nil{
		return nil,fmt.Errorf("Pebble Iterator error")
	}
	defer iter.Close()

	locations := make([]SpanLocation, 0, 16)
	for iter.First(); iter.Valid(); iter.Next() {
		location, err := decodeSpanLocation(iter.Value())
		if err != nil {
			return nil, fmt.Errorf("decode span location at key %s: %w", string(iter.Key()), err)
		}
		locations = append(locations, location)
	}
	return locations, nil
}



// DeleteSpan removes a span from the trace index (tombstone effect).
// Used during retention-driven expiry when a segment is dropped.
func (d *DB) DeleteSpan(key []byte) error {
	if d.db == nil {
		return fmt.Errorf("pebble db is closed")
	}
	return d.db.Delete(key, &pebble.WriteOptions{Sync: true})
}

// BatchPutSpans writes multiple span locations atomically. Used during segment
// flush to index all spans of a segment in one transaction .
// If the process crashes before Sync, none of the writes are visible; if it
// crashes during Sync, the entire batch either commits or rolls back.
func (d *DB) BatchPutSpans(entries map[string]SpanLocation) error {
	if d.db == nil {
		return fmt.Errorf("pebble db is closed")
	}
	if len(entries) == 0 {
		return nil
	}

	batch := d.db.NewBatch()
	defer batch.Close()

	// Encode all entries into the batch (reuse pooled buffers).
	for keyStr, location := range entries {
		key := []byte(keyStr)
		bufPtr := spanLocationBufPool.Get().(*[]byte)
		buf := *bufPtr
		encodeSpanLocationInto(buf, location)
		if err := batch.Set(key, buf, nil); err != nil {
			spanLocationBufPool.Put(bufPtr)
			return fmt.Errorf("batch.Set: %w", err)
		}
		spanLocationBufPool.Put(bufPtr)
	}

	// Fsync the entire batch at once.
	return batch.Commit(&pebble.WriteOptions{Sync: true})
}

// BatchDeleteSpans removes multiple spans atomically (used at compaction time when
// a segment is dropped and its entries must be removed from the index together).
func (d *DB) BatchDeleteSpans(keys []string) error {
	if d.db == nil {
		return fmt.Errorf("pebble db is closed")
	}
	if len(keys) == 0 {
		return nil
	}

	batch := d.db.NewBatch()
	defer batch.Close()

	for _, keyStr := range keys {
		key := []byte(keyStr)
		if err := batch.Delete(key, nil); err != nil {
			return fmt.Errorf("batch.Delete: %w", err)
		}
	}

	return batch.Commit(&pebble.WriteOptions{Sync: true})
}

func encodeSpanLocationInto(buf []byte, loc SpanLocation) {
	binary.LittleEndian.PutUint64(buf[0:8], loc.SegmentID)
	binary.LittleEndian.PutUint64(buf[8:16], loc.Offset)
}

func encodeSpanLocation(loc SpanLocation) []byte {
	buf := make([]byte, 16)
	encodeSpanLocationInto(buf, loc)
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
    upper := append([]byte(nil), prefix...)

    for i := len(upper) - 1; i >= 0; i-- {
        if upper[i] != 0xff {
            upper[i]++
            return upper[:i+1]
        }
    }

    return nil
}