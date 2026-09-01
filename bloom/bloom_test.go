package bloom

import "testing"

func TestAddedValuesAlwaysMayContain(t *testing.T) {
	f := New(100, 0.01)
	values := []string{"agent-1", "agent-2", "claude-sonnet-5", "gpt-4", "platform-team"}
	for _, v := range values {
		f.Add(v)
	}
	for _, v := range values {
		if !f.MayContain(v) {
			t.Fatalf("MayContain(%q) = false, want true — no false negatives allowed", v)
		}
	}
}

func TestAbsentValueUsuallyReportsAbsent(t *testing.T) {
	f := New(1000, 0.01)
	for i := 0; i < 1000; i++ {
		f.Add(string(rune('a' + i%26)))
	}
	// A value with a very different shape should come back absent. This is
	// probabilistic in principle but deterministic in practice given fixed
	// hash functions and fixed inputs — if this starts flaking, the hash
	// choice or sizing needs revisiting, not the test.
	if f.MayContain("definitely-not-inserted-xyz-987") {
		t.Fatal("MayContain() = true for a value that was never added — check hash/sizing")
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	f := New(50, 0.01)
	f.Add("agent-1")
	f.Add("model-x")

	encoded := f.Encode()
	if len(encoded) != f.EncodedSize() {
		t.Fatalf("EncodedSize() = %d, want %d (actual Encode() length)", f.EncodedSize(), len(encoded))
	}

	decoded, n, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if n != len(encoded) {
		t.Fatalf("Decode() consumed %d bytes, want %d", n, len(encoded))
	}
	if !decoded.MayContain("agent-1") || !decoded.MayContain("model-x") {
		t.Fatal("decoded filter lost inserted values")
	}
}

func TestDecodeMultipleFiltersBackToBack(t *testing.T) {
	f1 := New(10, 0.01)
	f1.Add("a")
	f2 := New(10, 0.01)
	f2.Add("b")

	buf := append(f1.Encode(), f2.Encode()...)

	d1, n1, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode(f1) error = %v", err)
	}
	d2, n2, err := Decode(buf[n1:])
	if err != nil {
		t.Fatalf("Decode(f2) error = %v", err)
	}
	if n1+n2 != len(buf) {
		t.Fatalf("consumed %d+%d bytes, want %d total", n1, n2, len(buf))
	}
	if !d1.MayContain("a") {
		t.Fatal("d1 lost \"a\"")
	}
	if !d2.MayContain("b") {
		t.Fatal("d2 lost \"b\"")
	}
}

func TestDecodeTruncatedDataErrors(t *testing.T) {
	if _, _, err := Decode([]byte{1, 2, 3}); err == nil {
		t.Fatal("Decode() on truncated header should error")
	}

	f := New(10, 0.01)
	encoded := f.Encode()
	if _, _, err := Decode(encoded[:len(encoded)-1]); err == nil {
		t.Fatal("Decode() on truncated bits should error")
	}
}

func TestEmptyFilterNeverMayContain(t *testing.T) {
	f := New(100, 0.01)
	if f.MayContain("anything") {
		t.Fatal("MayContain() on an empty filter should be false")
	}
}
