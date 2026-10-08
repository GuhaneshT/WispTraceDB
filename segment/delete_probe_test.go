package segment

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// TestDeleteWhileOpenProbe is a contract test for the assumption a segment
// handle cache would be built on: what this OS/toolchain actually does when a
// file is removed while a handle to it is still open.
//
// Today WispTraceDB never does that — every reader closes before the segment
// is touched again, so compactor's os.Remove runs with zero handles open. A
// Phase 2 segment cache would create the first standing handles, so this probe
// answers, per (OS x Go version):
//
//  1. does os.Remove succeed while os.Open holds the file?
//  2. can the open handle still read the data afterwards?
//  3. can a new open/stat of the path see the file while the handle is held?
//     (this is what computeNextSegmentID's os.Stat sweep and reconcile
//     would experience against a delete-pending file on Windows)
//  4. after the handle closes, is the file really gone?
//
// Questions 1 and 3 are platform-dependent and are LOGGED, not asserted — the
// point is to record the answer and re-record it when the Go toolchain
// changes, not to fail CI on a semantic the design does not rely on. The
// design rule the cache must follow is ordering, not luck: Purge handles,
// wait for refs to reach zero, THEN unlink, so unlink races nothing regardless
// of what question 1 answers here.
//
// Questions 2 and 4 are invariants and ARE asserted:
//   - an open handle keeps reading correct data whether or not the unlink
//     succeeded (in-flight reader after purge),
//   - if the remove succeeded, the file is gone once the last handle closes;
//     if it failed, the file is intact.
//
// It prints an unconditional verdict line, same convention as the
// fault-injection harness, so a passing run still reports what was measured:
//
//	go test ./segment -run TestDeleteWhileOpenProbe -v -count=1
func TestDeleteWhileOpenProbe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "probe.bin")
	data := []byte("WispTraceDB delete-while-open probe payload")

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write probe file: %v", err)
	}

	h, err := os.Open(path)
	if err != nil {
		t.Fatalf("open probe file: %v", err)
	}

	removeErr := os.Remove(path)
	removeOK := removeErr == nil

	// Question 2: reads through the still-open handle. Must succeed either
	// way — this is the in-flight reader that was using the segment when the
	// cache purged it.
	buf := make([]byte, len(data))
	if _, err := h.ReadAt(buf, 0); err != nil {
		t.Errorf("read through open handle after os.Remove(%v): %v", removeErr, err)
	} else if string(buf) != string(data) {
		t.Errorf("read through open handle after os.Remove: got %q, want %q", buf, data)
	}

	// Question 3: visibility of the path while the handle is held. Logged
	// only — outcomes differ across platforms and Go versions.
	_, statWhileOpenErr := os.Stat(path)
	reopenWhileOpenErr := probeReopen(path)
	statWhileOpenVerdict := probeVerdict(statWhileOpenErr)
	reopenVerdict := probeVerdict(reopenWhileOpenErr)

	// Question 4, and the assertion half of question 2's contract: once the
	// handle is closed, reality must agree with what remove reported.
	if err := h.Close(); err != nil {
		t.Fatalf("close probe handle: %v", err)
	}

	_, statAfterCloseErr := os.Stat(path)
	goneAfterClose := os.IsNotExist(statAfterCloseErr)

	if removeOK && !goneAfterClose {
		t.Errorf("os.Remove succeeded but the file is still visible after close: stat err = %v", statAfterCloseErr)
	}
	if !removeOK && goneAfterClose {
		t.Errorf("os.Remove failed (%v) but the file vanished after close", removeErr)
	}

	t.Logf("probe: go=%s os=%s/%s delete-while-open=%s read-after-delete=%s stat-while-open=%s reopen-while-open=%s gone-after-close=%t",
		runtime.Version(), runtime.GOOS, runtime.GOARCH,
		probeVerdict(removeErr),
		"OK",
		statWhileOpenVerdict,
		reopenVerdict,
		goneAfterClose)

	if !removeOK {
		t.Logf("probe: os.Remove with open handle was DENIED (%v) — a segment cache must Purge+close handles before unlink (the ordering rule), never rely on delete-while-open.", removeErr)
	}
}

func probeReopen(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return f.Close()
}

func probeVerdict(err error) string {
	switch {
	case err == nil:
		return "OK"
	case os.IsNotExist(err):
		return "NOT-EXIST"
	default:
		return "DENIED(" + err.Error() + ")"
	}
}
