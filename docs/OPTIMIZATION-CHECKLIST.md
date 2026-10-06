# WispTraceDB Performance Optimization Checklist

Tracks implementation of the optimizations identified in `benchmark.txt`,
`pers.txt`, and source analysis. Follow the priority order and validate after
each phase.

## Phase 0: Baseline & Measurement
- [x] Record current benchmark baseline (from `benchmark.txt`) into `docs/benchmarks/baseline-20260926.txt`
- [x] Add `BenchmarkRangeQueryWithCompaction` (compaction/staleness path) — `cmd/benchmark_test.go`
- [x] Add `BenchmarkGetSpanWithCompaction` (point lookup post-compaction) — `cmd/benchmark_test.go`
- [x] Run new compaction benchmarks; record ns/op, B/op, allocs/op — `docs/benchmarks/compaction-20261003.txt`
- [x] Run `TestCompactionDiagnostic` — caught a benchmark bug (key collision from `i%500` + fixed span id); fixed `BenchmarkRangeQueryWithCompaction` and the diagnostic to use a unique trace id per span; marked the old RangeQueryWithCompaction numbers invalid
- [x] Confirm `BenchmarkGetSpanWithCompaction` is valid (unique keys) — 4.5x B/op, same allocs; direct evidence for Phase 1 bloom-decode-on-open
- [x] Re-run `BenchmarkRangeQueryWithCompaction` and `TestCompactionDiagnostic` after the key-collision fix; record valid post-compaction scan numbers — `docs/benchmarks/compaction-20261003.txt` Run 2 (RangeQuery 12.59 ms / 3.04 MB / 70,170 allocs; GetSpan 296 µs / 65.7 KB / 36 allocs)
- [x] Attribute the valid compacted RangeQuery latency — a 1000-ts window sits inside a 5000-span merged segment, so ScanAll decodes 5000 records and the staleness check issues 5000 index.GetSpan calls before the cheap in-memory time filter. pprof not needed: the order + counts explain it.
- [x] Reorder RangeQuery to run `filter.matches` BEFORE the per-candidate `index.GetSpan` staleness lookup (correctness-neutral; cuts ~5000 Pebble point reads to ~101 per query at this shape) — `cmd/cmd.go:1186-1203`

## Phase 1: Point-lookup read path (Priority #1) — Reduce GetSpan overhead
- [x] segment: Add `OpenReaderLight(path string)` that reads the header only (no bloom decode). `Blooms` stays nil. — `segment/reader.go`
- [x] `cmd/GetSpan`: use `OpenReaderLight` instead of `OpenReader` — `cmd/cmd.go:981`
- [x] Apply light path in `spanAlreadyIndexed` (recovery path) — uses ReadAt only
- [x] Apply light path in `GetTrace` when reading specific offsets — uses ReadAt only; full reader still used where blooms needed for other paths? GetTrace reads exact offsets via cached readers and never consults reader.Blooms, so light path is fine.
- [ ] Verify no behavior change: existing tests pass (GetSpan returns correct spans, tombstones filtered, GetTrace correctness)
- [x] Benchmark GetSpan vs baseline; record improvement — `docs/benchmarks/post-p1-20261003.txt` (GetSpanWithCompaction B/op -97.5%, ns/op -43%)

## Phase 2: Segment FD/cache (Priority #2) — Avoid reopen churn
- [ ] Design: LRU cache keyed by segment ID (or path). Stores opened reader (header + blooms, or light). Concurrency-safe (RWMutex) with eviction.
- [ ] Implement segment cache (e.g. `segment/cache.go`) with max entries, `Close()` on eviction/`DB.Close()`.
- [ ] Integrate: GetSpan, GetTrace, RangeQuery reuse cached readers where safe. GetTrace already does per-call caching; extend to cross-call.
- [ ] Lifecycle: `DB.Close()` closes all cached readers; cache must not retain stale files on rename/compaction (segments are immutable; compaction creates new IDs).
- [ ] Tests still pass; fault injection unaffected (file handles managed correctly).
- [ ] Benchmark GetSpan/GetTrace/RangeQuery vs post-P1 baseline.

## Phase 3: WAL Group Commit (Priority #3) — Fix parallel insert scaling (0.956x)
- [ ] Design: batch appends with a single fsync. Pending batch (records + waiters/channels) + flusher (timer + batch size). Preserve synchronous ack semantics (InsertSpan returns after durable).
- [ ] wal: extend WAL struct with batch state, mutex, optional background flusher goroutine, per-waiter done channels.
- [ ] `wal.AppendRecord`: enqueue into current batch, wait for commit (or write+sync immediately if batching disabled). External API semantics unchanged.
- [ ] `wal.Close`/`Rotate`: drain (sync) pending batch before closing/rotating.
- [ ] Crash safety: WAL record format unchanged. Durability point remains post-fsync of the committed batch. Never acknowledge before sync.
- [ ] Validate with fault injection: `tests/fault_injection_test.go` must still pass (acked spans survive kills). No behavior regression.
- [ ] Tune: initial batch size/timeout (e.g. 64–256 records or 5–10 ms). Make configurable later if needed.
- [ ] Benchmark `InsertSpan_Parallel` vs baseline; target near-linear scaling vs Sequential.

## Phase 4: Zero-allocation scan (Priority #4) — Reduce RangeQuery GC pressure
- [ ] segment: add iterator/callback API (e.g. `ScanAllFunc(func(offset uint64, span wal.SpanPayload) bool) error`) to avoid materializing a full `[]ScannedSpan`.
- [ ] `cmd/RangeQuery`: use callback-based scan; avoid building large intermediate slices when filtering.
- [ ] wal: review `deserializePayload` copies (blob/string copies are required for safety — focus on container allocations).
- [ ] Consider `sync.Pool` for small temp buffers if profiling shows benefit (after setup cost is gone).
- [ ] Benchmark RangeQuery vs post-P2/P3 baseline; track allocs/op and B/op.

## Phase 5: Compaction/staleness cost analysis (Priority #5) — Measure unmeasured
- [ ] Run `BenchmarkRangeQueryWithCompaction` with `-benchmem`; compare to `BenchmarkRangeQuery`.
- [ ] Instrument/record candidate count vs returned count (optional temporary counters).
- [ ] Evaluate bloom effectiveness vs setup cost at current segment sizes.
- [ ] Decide whether a snapshot iterator over the index (replacing N point `GetSpan` lookups in the staleness check) is worth it while preserving correctness. Correctness > perf.

## Phase 6: Correctness/API gaps (separate from benchmarks)
- [ ] Public delete API (`DeleteSpan`/`DeleteTrace`) — currently only via `InsertSpan{Deleted:true}`.
- [ ] `QueryWithCursor`: avoid re-running full `RangeQuery` per page (design snapshot iterator/cursor over index+buffer).
- [ ] Cardinality cap: return up to `limit` instead of erroring when the cap is reached.
- [ ] Rollup deletion leak: document/track (span counted then deleted later). Reversing Count/Sum is exact; Min/Max latency needs a distribution the store does not keep.
- [ ] `CheckConsistency`: lost-index-write detection gap (documented) — evaluate a second keyspace if needed.

## Validation Gates (must pass after each phase)
- [ ] `go build ./...`
- [ ] `go vet ./...`
- [ ] `go test ./... -v`
- [ ] `go test ./tests -run FaultInjection -v` (durability guard — critical for P3)
- [ ] `go test -bench . -benchmem ./cmd` (compare against baseline)

## Notes
- Columnar segments deferred (not the bottleneck). Setup overhead (opens/bloom decode) dominates measured costs.
- The staleness check (index lookup per candidate) is correctness-required; do not remove it without preserving semantics.
- P1 (light reader) is the smallest surface, lowest risk, clearest win for GetSpan.
- P3 (group commit) changes the WAL write path; treat as the highest durability risk — fault injection must remain green.
