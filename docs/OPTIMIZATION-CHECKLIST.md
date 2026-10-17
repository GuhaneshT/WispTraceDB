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
- [x] Reorder RangeQuery to run `filter.matches` BEFORE the per-candidate `index.GetSpan` staleness lookup (correctness-neutral; cuts ~5000 Pebble point reads to ~101 per query at this shape) — `cmd/cmd.go:1412-1418`

## Phase 1: Point-lookup read path (Priority #1) — Reduce GetSpan overhead
- [x] segment: Add `OpenReaderLight(path string)` that reads the header only (no bloom decode). `Blooms` stays nil. — `segment/reader.go`
- [x] `cmd/GetSpan`: use `OpenReaderLight` instead of `OpenReader` — `cmd/cmd.go:1140`
- [x] Apply light path in `spanAlreadyIndexed` (recovery path) — `cmd/cmd.go:445`; before/after benchmarked — `docs/benchmarks/post-p1b-20261006.txt` (14,591 → 1,615 B/op, 34 → 21 allocs)
- [x] Apply light path in `GetTrace` when reading specific offsets — `cmd/cmd.go:1193`. GetTrace reads exact offsets through cached readers and never consults `reader.Blooms`. Before/after benchmarked — `docs/benchmarks/post-p1b-20261006.txt` (19,836 → 6,818 B/op, 148 → 135 allocs)
- [x] Verify no behavior change: existing tests pass (GetSpan returns correct spans, tombstones filtered, GetTrace correctness) — full suite + 6/6 fault injection green, 6 Oct 2026
- [x] Benchmark GetSpan vs baseline; record improvement — `docs/benchmarks/post-p1-20261003.txt` (GetSpanWithCompaction B/op -97.5%, ns/op -43%)
- [x] Measure the `RangeQuery` filter-before-index reorder — `docs/benchmarks/range-reorder-20261003.txt`

## Phase 2: Segment FD/cache (Priority #2) — Avoid reopen churn
- [x] Design: LRU cache keyed by segment ID. Stores light readers, refcounted so an entry closes only when *evicted AND idle*; `sync.Cond` waiters; permanent dead set for purged ids (blocks the reopen-after-purge resurrection race). — `segment/cache.go`
- [x] Implement segment cache with max entries (`SegmentCacheSize` config: 0→default 64, negative→disabled), `Close()` on eviction and on `DB.Close()`/`CloseWithoutFlush()` (after the final flush, which may itself compact). — `segment/cache.go`, `cmd/cmd.go`
- [x] Integrate GetSpan, GetTrace, `spanAlreadyIndexed` (recovery) via `acquireSegment`. **RangeQuery deliberately NOT cached**: it needs bloom-section reads and `ScanAll` seeks that are unsafe on a shared reader — recorded here as a won't-do, not an omission.
- [x] Lifecycle/compaction: compactor takes a `Purger` (`WithPurger`) and calls `Purge(ids)` *before* unlink — closes handles so `os.Remove` succeeds on Windows (probe: `segment/delete_probe_test.go`); purge failure aborts the unlink → orphan file, reported by `CheckConsistency`, never corruption. Both `Close` paths close the cache; `crashClose` test helper closes it too (a real crash closes handles via the OS).
- [x] Tests pass; fault injection unaffected — full suite green 8 Oct 2026 (`segment/cache_test.go` 10 tests, `cmd/cache_test.go` purge-wiring + disabled-path, fault injection green).
- [~] Benchmark GetSpan/GetTrace (+ SpanAlreadyIndexed) vs post-P1 baseline, with/without `WISPTRACE_BENCH_NO_CACHE=1` A/B escape — recorded 15 Oct 2026 (`docs/benchmarks/post-cache-20261015.txt`): GetSpan 1,581 → **386 B/op, 22 → 14 allocs**; GetTrace 6,818 → **5,711 B/op, 135 → 128**; spanAlreadyIndexed 1,615 → **365 B/op, 21 → 13**. **A/B invalid**: the `WISPTRACE_BENCH_NO_CACHE=1` arm produced byte-identical numbers, so the env var never reached the test binary and the no-cache control is still missing. Re-run in one shell: `cmd /c "set WISPTRACE_BENCH_NO_CACHE=1 && go test ..."`.

## Phase 3: WAL Group Commit (Priority #3) — Fix parallel insert scaling (does not scale; ~0.74x)
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
- [x] Run `BenchmarkRangeQueryWithCompaction` with `-benchmem`; compared to post-reorder — **2,929,206 B/op, 65,272 allocs** (`post-cache-20261015.txt`), down from 3,043,562 B / 70,170 allocs (`compaction-20261003.txt`). `BenchmarkRangeQuery` (uncompacted) did not finish within the suite's default 10m timeout — run separately with `-timeout`.
- [ ] Instrument/record candidate count vs returned count (optional temporary counters).
- [ ] Evaluate bloom effectiveness vs setup cost at current segment sizes.
- [ ] Decide whether a snapshot iterator over the index (replacing N point `GetSpan` lookups in the staleness check) is worth it while preserving correctness. Correctness > perf.

## Phase 6: Correctness/API gaps (separate from benchmarks)
- [x] Public delete API (`DeleteSpan`/`DeleteTrace`) — both write tombstones via `InsertSpan`; `DeleteSpan` idempotent + rejects empty ids, `DeleteTrace` drains ingest then enumerates via `GetTrace`. Pinned by `cmd/tombstone_test.go`.
- [ ] `QueryWithCursor`: avoid re-running full `RangeQuery` per page (design snapshot iterator/cursor over index+buffer).
- [x] Cardinality cap: return up to `limit` instead of erroring when the cap is reached — a positive `limit` now short-circuits the scan (`cardinalityStop`, `cmd/cmd.go`); no-limit/over-cap queries still hard-error. Pinned by `TestCardinalityStopHonoursLimitBeforeCap` + `TestGetDistinctModelsPositiveLimitShortCircuits` (`cmd/cmd_test.go`).
- [ ] Rollup deletion leak: document/track (span counted then deleted later). Reversing Count/Sum is exact; Min/Max latency needs a distribution the store does not keep.
- [ ] `CheckConsistency`: lost-index-write detection gap (documented) — evaluate a second keyspace if needed.

## Validation Gates (must pass after each phase)
- [x] `go build ./...`
- [x] `go vet ./...` (clean, 8 Oct 2026)
- [x] `go test ./... -v` (green, 8 Oct 2026 — includes Phase 2 cache tests)
- [x] `go test ./tests -run FaultInjection -v` (durability guard — 6/6 green, 6 Oct 2026; `-short` re-run green 8 Oct 2026; critical for P3)
- [~] `go test -bench . -benchmem ./cmd` (compare against baseline) — run 15 Oct 2026 (`post-cache-20261015.txt`); `BenchmarkRangeQuery` exceeded the default 10m timeout, so the full suite needs `-timeout 30m` or per-benchmark runs.

## Notes
- Columnar segments deferred (not the bottleneck). Setup overhead (opens/bloom decode) dominates measured costs.
- The staleness check (index lookup per candidate) is correctness-required; do not remove it without preserving semantics.
- P1 (light reader) is the smallest surface, lowest risk, clearest win for GetSpan.
- P3 (group commit) changes the WAL write path; treat as the highest durability risk — fault injection must remain green.
