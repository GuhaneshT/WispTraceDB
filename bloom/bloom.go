// Package bloom implements a fixed-size bloom filter (PAD1 §5): a bit array
// plus a handful of deterministic hash functions, used to prune segments
// during a range/filter query without opening the file.
//
//   - Insert: hash the value through each function, set each resulting bit.
//   - Query: hash the candidate the same way — if any bit is 0, the value is
//     definitely absent; if all bits are 1, it's possibly present.
//
// False positives are possible (a wasted read); false negatives never are.
// Hashing uses two deterministic stdlib FNV variants combined via the
// Kirsch-Mitzenmacher technique, not k independent hash functions — this
// needs to be reproducible across process restarts (a filter is written
// once and read back later), so no randomized seed (e.g. hash/maphash) is
// usable here.
package bloom

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
)

// Filter is a single bloom filter: m bits, k hash functions.
type Filter struct {
	m    uint32
	k    uint32
	bits []byte
}

// New returns an empty filter sized for expectedItems entries at the given
// false-positive rate (e.g. 0.01 for 1%).
func New(expectedItems int, falsePositiveRate float64) *Filter {
	m := optimalM(expectedItems, falsePositiveRate)
	k := optimalK(m, expectedItems)
	return &Filter{m: m, k: k, bits: make([]byte, (m+7)/8)}
}

// optimalM computes the bit-array size using the standard bloom filter
// sizing formula: m = -(n * ln(p)) / (ln(2))^2.
func optimalM(n int, p float64) uint32 {
	if n <= 0 {
		n = 1
	}
	if p <= 0 || p >= 1 {
		p = 0.01
	}
	m := math.Ceil(-1 * float64(n) * math.Log(p) / (math.Ln2 * math.Ln2))
	if m < 8 {
		m = 8
	}
	return uint32(m)
}

// optimalK computes the hash-function count: k = (m/n) * ln(2).
func optimalK(m uint32, n int) uint32 {
	if n <= 0 {
		n = 1
	}
	k := math.Round(float64(m) / float64(n) * math.Ln2)
	if k < 1 {
		k = 1
	}
	return uint32(k)
}

// Add inserts value into the filter.
func (f *Filter) Add(value string) {
	h1, h2 := doubleHash(value)
	for i := uint32(0); i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(f.m)
		f.bits[idx/8] |= 1 << (idx % 8)
	}
}

// MayContain reports whether value might be in the filter. false is a
// definite answer; true means "maybe" (subject to the false-positive rate).
func (f *Filter) MayContain(value string) bool {
	h1, h2 := doubleHash(value)
	for i := uint32(0); i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(f.m)
		if f.bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

// doubleHash returns two independent, deterministic 64-bit hashes of value,
// combined via i*h2 in Add/MayContain to simulate k hash functions from
// just these two (Kirsch-Mitzenmacher).
func doubleHash(value string) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write([]byte(value))
	h2 := fnv.New64()
	h2.Write([]byte(value))
	return h1.Sum64(), h2.Sum64()
}

// Encode serializes the filter as m(4) | k(4) | bits.
func (f *Filter) Encode() []byte {
	buf := make([]byte, 8+len(f.bits))
	binary.LittleEndian.PutUint32(buf[0:4], f.m)
	binary.LittleEndian.PutUint32(buf[4:8], f.k)
	copy(buf[8:], f.bits)
	return buf
}

// EncodedSize returns the exact byte length Encode will produce, without
// allocating — used by callers computing a section length up front.
func (f *Filter) EncodedSize() int {
	return 8 + len(f.bits)
}

// Decode parses a filter from the start of data (as written by Encode) and
// returns it along with the number of bytes consumed, so callers can decode
// several filters back-to-back from one buffer.
func Decode(data []byte) (*Filter, int, error) {
	if len(data) < 8 {
		return nil, 0, fmt.Errorf("bloom filter header truncated: got %d bytes, want at least 8", len(data))
	}
	m := binary.LittleEndian.Uint32(data[0:4])
	k := binary.LittleEndian.Uint32(data[4:8])
	nBytes := int((m + 7) / 8)
	if len(data) < 8+nBytes {
		return nil, 0, fmt.Errorf("bloom filter bits truncated: want %d bytes, have %d", nBytes, len(data)-8)
	}
	bits := append([]byte(nil), data[8:8+nBytes]...)
	return &Filter{m: m, k: k, bits: bits}, 8 + nBytes, nil
}
