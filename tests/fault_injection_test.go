package tests

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/GuhaneshT/WispTraceDB/cmd"
	"github.com/GuhaneshT/WispTraceDB/segment"
	"github.com/GuhaneshT/WispTraceDB/wal"
)

// Fault-injection harness (PAD1 §3 / PDD1 §7 — "the test that matters").
//
// The worker is a self-execed test process running real re-insert traffic
// under WispTraceDB. The driver terminates it with a forced kill (the Go
// equivalent of kill -9: no defers, no Close, no graceful flush — only what
// was fsynced survives), then reopens the database and checks:
//
//   - every ACKED span survives with byte-identical content (ack durability);
//   - recovered data is consistent (CheckConsistency passes);
//   - no span is duplicated and none appear that were never written.
//
// Every run prints a `verdict:` line carrying all of those counts. A test that
// only reports on failure proves its claims with an exit code, which is not
// evidence a reader can check; the numbers have to be output.
//
// A kill timing is only meaningful if the kill lands on a live process. The
// worker therefore holds open after its own flush when a kill is expected
// (faultHoldEnv), and the driver fails if the worker exited by itself: a
// workload that finishes before the kill would run the graceful-close path a
// second time, the verdict would still be clean (a closed database has nothing
// wrong with it), and the case would quietly stop testing a crash.
//
// The ack oracle is a plain file OUTSIDE the db dir. The worker records seq
// only after InsertSpan returns (i.e. after the WAL fsync). The OS page cache
// survives a process kill, so oracle-listed spans are a sound lower bound for
// "must have been durable": a missing one is a real durability failure.
//
// Kill -9 on Windows maps to TerminateProcess: the worker leaves pebble's
// file handles behind, so the driver retries the reopen briefly.

const (
	faultWorkerEnv = "WISPTRACE_FAULT_WORKER"
	faultDirEnv    = "WISPTRACE_FAULT_DIR"
	faultOracleEnv = "WISPTRACE_FAULT_ORACLE"
	faultMaxEnv    = "WISPTRACE_FAULT_MAX"
	faultHoldEnv   = "WISPTRACE_FAULT_HOLD"

	faultTraces        = 50
	faultTombstoneEach = 17
)

// TestFaultWorker is the crashable ingestion process. It is only active when
// faultWorkerEnv is set (by the driver); otherwise it is a no-op pass so the
// whole suite still runs under `go test ./tests`.
func TestFaultWorker(t *testing.T) {
	if os.Getenv(faultWorkerEnv) != "1" {
		return
	}

	dir := os.Getenv(faultDirEnv)
	if dir == "" {
		t.Fatal("fault worker: WISPTRACE_FAULT_DIR not set")
	}
	oraclePath := os.Getenv(faultOracleEnv)
	if oraclePath == "" {
		t.Fatal("fault worker: WISPTRACE_FAULT_ORACLE not set")
	}
	max, err := strconv.Atoi(os.Getenv(faultMaxEnv))
	if err != nil || max <= 0 {
		t.Fatalf("fault worker: bad WISPTRACE_FAULT_MAX=%q", os.Getenv(faultMaxEnv))
	}

	wt, err := cmd.CreateWispTraceWithConfig(faultTestConfig(dir))
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}

	oracle, err := os.OpenFile(oraclePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open oracle: %v", err)
	}
	defer oracle.Close()

	base := time.Now().UnixNano()
	for seq := 0; seq < max; seq++ {
		if err := wt.InsertSpan(faultSpan(base, seq)); err != nil {
			t.Fatalf("InsertSpan(%d): %v", seq, err)
		}
		// Ack now means durable in the WAL; record it so the driver can
		// demand it back after the kill.
		if _, err := fmt.Fprintf(oracle, "%d\n", seq); err != nil {
			t.Fatalf("oracle write(%d): %v", seq, err)
		}
	}

	if err := wt.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}

	// A worker that exits by itself turns a kill timing into a clean
	// shutdown. Nothing is wrong with a cleanly closed database, so the
	// verdict still passes — and the case silently stops testing a crash,
	// which is exactly how "cool" came to be reported as a steady-state kill
	// when it was running the graceful-close path a second time. When the
	// driver asked for a kill, hold here instead of closing, so that kill
	// always lands on a live process; the driver asserts the worker did not
	// exit on its own.
	if os.Getenv(faultHoldEnv) == "1" {
		<-make(chan struct{})
	}

	if err := wt.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := fmt.Fprintf(oracle, "done\n"); err != nil {
		t.Fatalf("oracle done write: %v", err)
	}
}

// faultTestConfig pins a workload-shaped engine config shared by worker and
// driver. Timestamps are wall-clock based (never epoch-scale), and retention
// is effectively disabled so expiry can never prune data mid-test.
func faultTestConfig(dir string) cmd.WispTraceConfig {
	cfg := cmd.DefaultWispTraceConfig()
	cfg.WALPath = filepath.Join(dir, "wal.log")
	cfg.PebblePath = filepath.Join(dir, "lsm")
	cfg.SegmentDir = filepath.Join(dir, "segments")
	cfg.CheckpointPath = filepath.Join(dir, "checkpoint.dat")
	cfg.ManifestPath = filepath.Join(dir, "manifest.dat")
	cfg.SegmentFlushThreshold = 64
	cfg.CompactionSegmentThreshold = 4
	cfg.RetentionPeriod = 1000 * 24 * time.Hour
	cfg.CompactionInterval = 24 * time.Hour
	cfg.RollupSnapshotInterval = 24 * time.Hour
	// WISPTRACE_FAULT_NOCOMPACT=1 isolates the fault-injection harness from
	// every segment flush and compaction: the only flush that ever happens is
	// the tail replay at reopen. Passes under this env pin the failure to the
	// flush/compaction machinery rather than the WAL replay path.
	if os.Getenv("WISPTRACE_FAULT_NOCOMPACT") == "1" {
		cfg.SegmentFlushThreshold = 1 << 30
		cfg.CompactionSegmentThreshold = 1 << 30
		// Without this the "never flushed" premise dies quietly: the expiry
		// ticker fires every second and sessions older than the 5s default
		// get flushed mid-run on a slow machine, folding everything into a
		// segment long before the kill.
		cfg.SessionTimeout = 24 * time.Hour
	}
	return cfg
}

// faultSpan derives deterministic, recognizable span content from seq. Every
// seq maps to a distinct key; payloads encode seq verbatim for recovery
// verification; every 17th span is a tombstone.
func faultSpan(base int64, seq int) wal.SpanPayload {
	return wal.SpanPayload{
		TraceID:   fmt.Sprintf("t-%d", seq%faultTraces),
		SpanID:    fmt.Sprintf("s-%d", seq),
		Timestamp: base + int64(seq)*int64(time.Microsecond),
		AgentID:   "agent-f",
		Model:     "model-f",
		ToolName:  "tool-f",
		Team:      "team-f",
		Status:    "ok",
		TokensIn:  int32(seq),
		TokensOut: int32(seq + 1),
		Cost:      0.001,
		LatencyMs: int64(seq % 100),
		Payload:   []byte(strconv.Itoa(seq)),
		Deleted:   seq%faultTombstoneEach == 0,
	}
}

func TestFaultInjectionAckedSpansSurviveKills(t *testing.T) {
	specs := []struct {
		name      string
		killAfter time.Duration
		max       int
	}{
		{"graceful-close", 0, 400},
		{"hot", 30 * time.Millisecond, 800},
		// mid's max is far larger than its kill window needs: the worker must
		// still be mid-insert when the kill lands, on any machine. With max=800
		// the workload finished in ~570ms against a 300ms kill — a margin that
		// load can eat, turning the case into a flushed-and-held steady state.
		{"mid", 300 * time.Millisecond, 4000},
		// cool is the opposite: it deliberately lets the workload finish and
		// flush (with the hold guard the kill then lands on the live, flushed
		// process), so max stays small.
		{"cool", 1300 * time.Millisecond, 800},
	}
	if testing.Short() {
		specs = specs[:2] // graceful control + one hot kill
	}
	for _, sp := range specs {
		sp := sp
		t.Run(sp.name, func(t *testing.T) {
			runFaultCycle(t, sp.killAfter, sp.max)
		})
	}
}

// TestFaultInjectionReplayRecoversUnflushedAcks pins the recovery path that the
// four kill timings reach only by accident of timing.
//
// "hot" leaves every acked record in the tail today, but only because the kill
// happens to land before the first flush — and reclamation is now derived from
// records still unflushed, so any run that reaches the worker's own final flush
// (never mind one that closes) leaves an empty tail and makes Replay() a
// legitimate no-op. "Does Replay() rebuild an entire acknowledged workload" was
// therefore pinned nowhere: it depended on machine speed.
//
// This case removes the flush from the picture instead of retuning a sleep:
// thresholds and session expiry are neutralised, so nothing is ever folded into
// a segment, and the whole acknowledged set must come back through Replay()
// alone. The kill is 30ms after the first ack — deliberately inside the ingest
// stream, because a longer window lets the workload finish and reach the
// worker's own flush. requireFullTail then checks the claim at the WAL level,
// before recovery is allowed to run: the frozen tail is non-empty and
// physically contains every acked record.
func TestFaultInjectionReplayRecoversUnflushedAcks(t *testing.T) {
	t.Setenv("WISPTRACE_FAULT_NOCOMPACT", "1")
	runFaultCycleExpecting(t, 30*time.Millisecond, 800, true)
}

func runFaultCycle(t *testing.T, killAfter time.Duration, max int) {
	t.Helper()
	runFaultCycleExpecting(t, killAfter, max, false)
}

func runFaultCycleExpecting(t *testing.T, killAfter time.Duration, max int, requireFullTail bool) {
	t.Helper()
	base := t.TempDir()
	dbDir := filepath.Join(base, "db")
	oraclePath := filepath.Join(base, "oracle.txt")
	cfg := faultTestConfig(dbDir)

	workerCmd := exec.Command(os.Args[0], "-test.run=^TestFaultWorker$", "-test.cpu=1", "-test.count=1")
	workerCmd.Env = append(os.Environ(),
		faultWorkerEnv+"=1",
		faultDirEnv+"="+dbDir,
		faultOracleEnv+"="+oraclePath,
		faultMaxEnv+"="+strconv.Itoa(max),
	)
	// With a kill asked for, the worker holds open after its own flush instead
	// of exiting, so the kill always lands on a live process.
	if killAfter > 0 {
		workerCmd.Env = append(workerCmd.Env, faultHoldEnv+"=1")
	}
	workerCmd.Stdout = io.Discard
	workerCmd.Stderr = io.Discard

	if err := workerCmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(func() {
		if workerCmd.Process != nil && workerCmd.ProcessState == nil {
			_ = workerCmd.Process.Kill()
		}
	})

	if killAfter <= 0 {
		if err := workerCmd.Wait(); err != nil {
			t.Fatalf("graceful worker exit: %v", err)
		}
	} else {
		// Don't kill on an empty database: wait for at least one durable ack.
		deadline := time.Now().Add(45 * time.Second)
		for {
			if countAcks(readOracle(oraclePath)) >= 1 {
				break
			}
			if oracleDone(readOracle(oraclePath)) {
				break
			}
			if time.Now().After(deadline) {
				_ = workerCmd.Process.Kill()
				t.Fatalf("worker never produced a durable ack")
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(killAfter)
		_ = workerCmd.Process.Kill()
		if waitErr := workerCmd.Wait(); waitErr == nil {
			t.Fatalf("worker exited on its own before killAfter=%v: this timing exercised a clean shutdown instead of a crash, so the verdict would pass while testing nothing", killAfter)
		}
	}

	// Freeze the on-disk state exactly as the crash left it, so a later
	// failure can be traced to pre-crash segments vs what recovery produced.
	postcrash := filepath.Join(base, "postcrash")
	diskCopy(t, dbDir, postcrash)

	verifyAfterCrash(t, cfg, readOracle(oraclePath), max, postcrash, requireFullTail)
}

func verifyAfterCrash(t *testing.T, cfg cmd.WispTraceConfig, oracle []string, max int, postcrash string, requireFullTail bool) {
	t.Helper()
	acked := ackSet(oracle)
	if len(acked) == 0 {
		t.Fatal("no acked spans recorded — harness degenerate")
	}
	ackedTombstones := 0
	for seq := range acked {
		if seq%faultTombstoneEach == 0 {
			ackedTombstones++
		}
	}

	// The killed process releases its handles asynchronously on Windows;
	// brief retry lets the reopen compete with handle teardown.
	var wt *cmd.WispTrace
	var err error
	reopenAttempt := 0
	for i := 0; i < 30; i++ {
		reopenAttempt = i + 1
		wt, err = cmd.CreateWispTraceWithConfig(cfg)
		if err == nil {
			t.Logf("reopen succeeded on attempt %d", reopenAttempt)
			break
		}
		t.Logf("reopen attempt %d failed: %v", reopenAttempt, err)
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	defer wt.Close()

	rep := wt.CheckConsistency()
	if !rep.OK() {
		t.Fatalf("consistency errors: %v", rep.Errors)
	}
	for _, w := range rep.Warnings {
		t.Logf("consistency warning: %s", w)
	}

	// 1. Acked spans must survive. A tombstoned span must not be readable at
	// all: found must be false, not "found and flagged Deleted". Asserting
	// found && !span.Deleted would pass for a GetSpan that hands back the
	// tombstone, which is precisely the bug this harness is meant to catch.
	var missing []int
	for seq := range acked {
		traceID, spanID := fmt.Sprintf("t-%d", seq%faultTraces), fmt.Sprintf("s-%d", seq)
		span, found, ge := wt.GetSpan(traceID, spanID)
		if ge != nil {
			t.Fatalf("GetSpan(%s,%s) error = %v", traceID, spanID, ge)
		}
		if seq%faultTombstoneEach == 0 {
			if found {
				t.Fatalf("tombstoned span %d came back readable: Deleted=%v", seq, span.Deleted)
			}
			continue
		}
		if !found {
			missing = append(missing, seq)
			continue
		}
		if span.Deleted {
			t.Fatalf("live span %d came back as a tombstone", seq)
		}
		if string(span.Payload) != strconv.Itoa(seq) {
			t.Fatalf("span %d payload after recovery = %q, want %q", seq, span.Payload, strconv.Itoa(seq))
		}
	}
	t.Logf("--- frozen post-crash WAL tail (what reopen's Replay() sees) ---")
	walTail, tailSeqs := dumpWALSnapshot(t, faultTestConfig(postcrash))

	// 2. Exactly-once visibility and no phantoms. Collected rather than
	// asserted on first contact, so the verdict below reports every count on
	// a passing run instead of reporting nothing.
	all, err := wt.RangeQuery(cmd.RangeFilter{StartTS: 0, EndTS: 1<<63 - 1})
	if err != nil {
		t.Fatalf("RangeQuery() = %v", err)
	}
	live := make(map[int]int)
	var phantoms []int
	for _, s := range all {
		seq, perr := strconv.Atoi(string(s.Payload))
		if perr != nil {
			t.Fatalf("unrecognizable payload %q in range results", s.Payload)
		}
		if seq < 0 || seq >= max {
			phantoms = append(phantoms, seq)
			continue
		}
		live[seq]++
	}
	var duplicates []int
	var notInRange []int
	for seq := range acked {
		if seq%faultTombstoneEach == 0 {
			continue
		}
		switch n := live[seq]; {
		case n > 1:
			duplicates = append(duplicates, seq)
		case n == 0:
			notInRange = append(notInRange, seq)
		}
	}
	var ackedNotInTail []int
	for seq := range acked {
		if !tailSeqs[seq] {
			ackedNotInTail = append(ackedNotInTail, seq)
		}
	}

	// The verdict is the point of the harness: a passing run has to print its
	// evidence, because "it exited 0" is not evidence a reader can check.
	// fmt.Printf rather than t.Logf — a log line only surfaces under -v.
	fmt.Printf("verdict: acked=%d tombstones=%d missing=%d duplicates=%d not-in-range=%d phantoms=%d "+
		"range=%d warnings=%d reopen-attempt=%d wal-tail=%d acked-not-in-tail=%d\n",
		len(acked), ackedTombstones, len(missing), len(duplicates), len(notInRange), len(phantoms),
		len(all), len(rep.Warnings), reopenAttempt, walTail, len(ackedNotInTail))

	if len(missing) > 0 {
		t.Logf("acked=%d first=%d last=%d missing=%d",
			len(acked), missing[0], missing[len(missing)-1], len(missing))
		t.Logf("missing seqs: %v", missing)
		t.Logf("--- post-recovery disk state ---")
		dumpDiskState(t, cfg)
		t.Logf("--- crash-time disk state (pre-recovery) ---")
		dumpDiskState(t, faultTestConfig(postcrash))
		t.Fatalf("%d acked spans lost after crash", len(missing))
	}
	if len(duplicates) > 0 {
		t.Fatalf("live acked spans appeared more than once in RangeQuery: %v", duplicates)
	}
	if len(notInRange) > 0 {
		t.Fatalf("live acked spans absent from RangeQuery: %v", notInRange)
	}
	if len(phantoms) > 0 {
		t.Fatalf("phantom spans outside the written workload: %v", phantoms)
	}
	if requireFullTail {
		if walTail == 0 {
			t.Fatal("no-flush run left an empty WAL tail: Replay() had nothing to do, so this case tested nothing")
		}
		if len(ackedNotInTail) > 0 {
			t.Fatalf("%d acked records are absent from the frozen WAL tail: %v", len(ackedNotInTail), ackedNotInTail)
		}
	}
}

func readOracle(path string) []string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	parts := strings.Split(string(data), "\n")
	lines := make([]string, 0, len(parts))
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			lines = append(lines, strings.TrimSpace(p))
		}
	}
	return lines
}

func ackSet(oracle []string) map[int]bool {
	acked := make(map[int]bool)
	for _, l := range oracle {
		if n, err := strconv.Atoi(l); err == nil {
			acked[n] = true
		}
	}
	return acked
}

func countAcks(oracle []string) int {
	return len(ackSet(oracle))
}

func oracleDone(oracle []string) bool {
	for _, l := range oracle {
		if l == "done" {
			return true
		}
	}
	return false
}

// diskCopy recursively copies src into dst (dst is created). Used to freeze
// the on-disk database state exactly as a crash left it, before recovery has
// a chance to mutate anything.
func diskCopy(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0755); err != nil {
		t.Fatalf("diskCopy mkdir: %v", err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return // nothing to snapshot at crash time is fine
	}
	for _, e := range entries {
		sp := filepath.Join(src, e.Name())
		dp := filepath.Join(dst, e.Name())
		if e.IsDir() {
			diskCopy(t, sp, dp)
			continue
		}
		data, err := os.ReadFile(sp)
		if err != nil {
			continue
		}
		if err := os.WriteFile(dp, data, 0644); err != nil {
			t.Fatalf("diskCopy write %s: %v", dp, err)
		}
	}
}

// dumpWALSnapshot opens cfg's WAL on a frozen copy of the directory (never the
// live db) and replays it, so the harness can report exactly which acked spans
// the crash left durable in the WAL tail. The WAL is opened as its own engine
// would, so torn-tail truncation, if any, happens on the copy only.
//
// It returns the record count and the set of payload sequence numbers found, so
// callers can assert coverage rather than eyeball a log line: the no-flush case
// checks that every acked record is physically present in this tail before
// recovery gets the chance to rebuild it.
func dumpWALSnapshot(t *testing.T, cfg cmd.WispTraceConfig) (int, map[int]bool) {
	t.Helper()
	found := make(map[int]bool)
	maxSeg := cfg.WALMaxSegmentSize
	if maxSeg == 0 {
		maxSeg = wal.DefaultMaxSegmentSize
	}
	w, err := wal.CreateWALWithSegmentSize(cfg.WALPath, maxSeg)
	if err != nil {
		t.Logf("  wal open: %v", err)
		return 0, found
	}
	defer w.Close()
	recs, err := w.Replay()
	if err != nil {
		t.Logf("  wal replay error: %v", err)
		return 0, found
	}
	var seqs []int64
	for _, r := range recs {
		if n, perr := strconv.ParseInt(string(r.Span.Payload), 10, 64); perr == nil {
			found[int(n)] = true
			seqs = append(seqs, n)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	if len(seqs) == 0 {
		t.Logf("  (empty WAL tail)")
		return 0, found
	}
	t.Logf("  %d records, seq range [%d..%d]", len(seqs), seqs[0], seqs[len(seqs)-1])
	t.Logf("  seqs: %v", seqs)
	return len(seqs), found
}

// dumpDiskState prints every sequence found in any segment_*.seg file on
// disk — including orphan files the manifest no longer references — so a lost
// acked span can be traced to one of: never durable (WAL), dropped by
// recovery, or present-but-misindexed.
func dumpDiskState(t *testing.T, cfg cmd.WispTraceConfig) {
	t.Helper()
	for _, p := range []string{cfg.ManifestPath, cfg.CheckpointPath} {
		if data, err := os.ReadFile(p); err == nil {
			t.Logf("  %s: %q", filepath.Base(p), string(data))
		}
	}
	entries, err := os.ReadDir(cfg.SegmentDir)
	if err != nil {
		t.Logf("dumpDiskState: read segment dir: %v", err)
		return
	}
	lsm, lerr := os.ReadDir(cfg.PebblePath)
	if lerr != nil {
		t.Logf("dumpDiskState: read lsm dir: %v", lerr)
	} else {
		var names []string
		for _, e := range lsm {
			if !e.IsDir() {
				names = append(names, e.Name())
			}
		}
		t.Logf("  lsm dir: %v", names)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "segment_") || !strings.HasSuffix(name, ".seg") {
			continue
		}
		reader, err := segment.OpenReader(filepath.Join(cfg.SegmentDir, name))
		if err != nil {
			t.Logf("  %s: unreadable: %v", name, err)
			continue
		}
		spans, serr := reader.ScanAll()
		reader.Close()
		if serr != nil {
			t.Logf("  %s: scan error: %v", name, serr)
			continue
		}
		var seqs []int
		for _, s := range spans {
			if n, perr := strconv.Atoi(string(s.Span.Payload)); perr == nil {
				seqs = append(seqs, n)
			}
		}
		sort.Ints(seqs)
		t.Logf("  %s: %d records -> %v", name, len(seqs), seqs)
	}
}

// TestFaultInjectionInsertFlushDeleteCrash tests crash recovery durability when
// an acknowledged delete for an already-flushed span key is present in the WAL tail.
func TestFaultInjectionInsertFlushDeleteCrash(t *testing.T) {
	dbDir := filepath.Join(t.TempDir(), "db")
	cfg := faultTestConfig(dbDir)

	// Step 1: Open engine, insert span, and flush to segment + index.
	wt, err := cmd.CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}

	span := wal.SpanPayload{
		TraceID:   "t-fault-reuse",
		SpanID:    "s-fault-reuse-1",
		Timestamp: time.Now().UnixNano(),
		AgentID:   "agent-f",
		Model:     "model-f",
		Payload:   []byte("live-payload"),
		Deleted:   false,
	}

	if err := wt.InsertSpan(span); err != nil {
		t.Fatalf("InsertSpan: %v", err)
	}
	wt.WaitForIngest()
	if err := wt.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Step 2: Delete the same span key (append Deleted=true to WAL).
	deleteSpan := span
	deleteSpan.Deleted = true
	if err := wt.InsertSpan(deleteSpan); err != nil {
		t.Fatalf("InsertSpan(delete): %v", err)
	}
	wt.WaitForIngest()

	// Step 3: Crash before the next segment flush (CloseWithoutFlush).
	if err := wt.CloseWithoutFlush(); err != nil {
		t.Fatalf("CloseWithoutFlush: %v", err)
	}

	// Step 4: Reopen engine, triggering crash recovery.
	wt2, err := cmd.CreateWispTraceWithConfig(cfg)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer wt2.Close()

	// Step 5: Assert that post-recovery the span is NOT readable. "found" must
	// be false — accepting a returned tombstone is the bug, not a pass.
	got, found, err := wt2.GetSpan("t-fault-reuse", "s-fault-reuse-1")
	if err != nil {
		t.Fatalf("GetSpan after recovery error = %v", err)
	}
	if found {
		t.Fatalf("span resurrected after crash recovery: got %+v, want not found", got)
	}

	// Step 6: The same must hold for trace reconstruction, which is a separate
	// read path (prefix scan) and historically applied no tombstone filter at
	// all. A tombstone that survives here is invisible to a point lookup but
	// still leaks through the trace API.
	traceSpans, traceFound, err := wt2.GetTrace("t-fault-reuse")
	if err != nil {
		t.Fatalf("GetTrace after recovery error = %v", err)
	}
	if traceFound {
		t.Fatalf("tombstoned span resurfaced through GetTrace: %+v", traceSpans)
	}
}
