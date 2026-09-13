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
// The ack oracle is a plain file OUTSIDE the db dir. The worker records seq
// only after InsertSpan returns (i.e. after the WAL fsync). The OS page cache
// survives a process kill, so oracle-listed spans are a sound lower bound for
// "must have been durable": a missing one is a real durability failure.
//
// Kill -9 on Windows maps to TerminateProcess: the worker leaves pebble's
// file handles behind, so the driver retries the reopen briefly.

const (
	faultWorkerEnv    = "WISPTRACE_FAULT_WORKER"
	faultDirEnv       = "WISPTRACE_FAULT_DIR"
	faultOracleEnv    = "WISPTRACE_FAULT_ORACLE"
	faultMaxEnv       = "WISPTRACE_FAULT_MAX"

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
		{"mid", 300 * time.Millisecond, 800},
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

func runFaultCycle(t *testing.T, killAfter time.Duration, max int) {
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
	workerCmd.Stdout = io.Discard
	workerCmd.Stderr = io.Discard

	if err := workerCmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}

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
		_ = workerCmd.Wait()
	}

	// Freeze the on-disk state exactly as the crash left it, so a later
	// failure can be traced to pre-crash segments vs what recovery produced.
	postcrash := filepath.Join(base, "postcrash")
	diskCopy(t, dbDir, postcrash)

	verifyAfterCrash(t, cfg, readOracle(oraclePath), max, postcrash)
}

func verifyAfterCrash(t *testing.T, cfg cmd.WispTraceConfig, oracle []string, max int, postcrash string) {
	t.Helper()
	acked := ackSet(oracle)
	if len(acked) == 0 {
		t.Fatal("no acked spans recorded — harness degenerate")
	}

	// The killed process releases its handles asynchronously on Windows;
	// brief retry lets the reopen compete with handle teardown.
	var wt *cmd.WispTrace
	var err error
	for i := 0; i < 30; i++ {
		wt, err = cmd.CreateWispTraceWithConfig(cfg)
		if err == nil {
			t.Logf("reopen succeeded on attempt %d", i+1)
			break
		}
		t.Logf("reopen attempt %d failed: %v", i+1, err)
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

	// 1. Acked spans must survive. Tombstones must never come back live.
	var missing []int
	for seq := range acked {
		traceID, spanID := fmt.Sprintf("t-%d", seq%faultTraces), fmt.Sprintf("s-%d", seq)
		span, found, ge := wt.GetSpan(traceID, spanID)
		if ge != nil {
			t.Fatalf("GetSpan(%s,%s) error = %v", traceID, spanID, ge)
		}
		if seq%faultTombstoneEach == 0 {
			if found && !span.Deleted {
				t.Fatalf("tombstoned span %d came back live", seq)
			}
			continue
		}
		if !found {
			missing = append(missing, seq)
			continue
		}
		if string(span.Payload) != strconv.Itoa(seq) {
			t.Fatalf("span %d payload after recovery = %q, want %q", seq, span.Payload, strconv.Itoa(seq))
		}
	}
	t.Logf("--- frozen post-crash WAL tail (what reopen's Replay() sees) ---")
	dumpWALSnapshot(t, faultTestConfig(postcrash))

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

	// 2. Exactly-once visibility and no phantoms.
	all, err := wt.RangeQuery(cmd.RangeFilter{StartTS: 0, EndTS: 1<<63 - 1})
	if err != nil {
		t.Fatalf("RangeQuery() = %v", err)
	}
	live := make(map[int]int)
	for _, s := range all {
		seq, perr := strconv.Atoi(string(s.Payload))
		if perr != nil {
			t.Fatalf("unrecognizable payload %q in range results", s.Payload)
		}
		if seq < 0 || seq >= max {
			t.Fatalf("phantom span %d outside the written workload", seq)
		}
		live[seq]++
	}
	for seq := range acked {
		if seq%faultTombstoneEach == 0 {
			continue
		}
		if n := live[seq]; n != 1 {
			t.Fatalf("live acked span %d appeared %d times in RangeQuery, want exactly 1", seq, n)
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

// dumpWALSnapshot opens cfg's WAL on a frost-copied directory (never the live
// db) and replays it, so the harness can report exactly which acked spans the
// crash left durable in the WAL tail. The WAL is opened as its own engine
// would, so torn-tail truncation, if any, happens on the copy only.
func dumpWALSnapshot(t *testing.T, cfg cmd.WispTraceConfig) {
	t.Helper()
	maxSeg := cfg.WALMaxSegmentSize
	if maxSeg == 0 {
		maxSeg = wal.DefaultMaxSegmentSize
	}
	w, err := wal.CreateWALWithSegmentSize(cfg.WALPath, maxSeg)
	if err != nil {
		t.Logf("  wal open: %v", err)
		return
	}
	defer w.Close()
	recs, err := w.Replay()
	if err != nil {
		t.Logf("  wal replay error: %v", err)
		return
	}
	var seqs []int64
	for _, r := range recs {
		if n, perr := strconv.ParseInt(string(r.Span.Payload), 10, 64); perr == nil {
			seqs = append(seqs, n)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	if len(seqs) == 0 {
		t.Logf("  (empty WAL tail)")
		return
	}
	t.Logf("  %d records, seq range [%d..%d]", len(seqs), seqs[0], seqs[len(seqs)-1])
	t.Logf("  seqs: %v", seqs)
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