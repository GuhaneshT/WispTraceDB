# WispTraceDB — Implementation Guidance

This file provides guidance to Claude Code (claude.ai/code) when working with WispTraceDB.

## What this is

WispTraceDB is a single-node, embeddable trace store specialized for LLM agent spans. It is built according to **PAD1.md v1.3.2** (Architecture) and **PDD1.md v1.3** (Design), both frozen in the `docs/` folder. The architecture is **non-negotiable** until an explicit versioned amendment updates it.

**Read `docs/PAD1.md` as the target design and this file as the as-built status.** Where the two disagree, the code and this file win; the gap is recorded in PAD1's amendment log rather than papered over.

Three design pillars drive the entire system:
1. **Pillar 1 — Durable ingestion (WAL):** accept a span, validate it, make it durable, acknowledge it.
2. **Pillar 2 — Immutable storage & compaction (segments):** turn buffered spans into compact, on-disk segments; periodically merge small segments and prune expired data. (Intended end state is columnar — see the deviation section below.)
3. **Pillar 3 — Indexing & aggregation (LSM trace index, bloom/zone-map pruning, rollups):** answer point/range/aggregation queries fast.

Pillars 2 and 3 are derived state — fully rebuildable from Pillar 1 (the WAL). If every segment, index file, and rollup were deleted, the WAL alone would reconstruct correct state.

## v1 release work (in progress — see `docs/V1-API-DESIGN.md`)

- **Step 1 done and verified by the user (2026-10-29):** engine packages moved under `internal/` (`internal/{wal,segment,pebble,bloom,compactor,rollup}`; the old `cmd` package is now `internal/engine`, package `engine`). Build, vet and the full test suite ran green on their machine. Go 1.26.6 lives at `C:\Users\guhanesht\sdk\go\bin` (added to the user PATH); an agent shell here can compile and vet but **cannot execute test binaries** (Windows "Access is denied"), so tests must be run by the user.
- **Step 2 (facade) written, compiles and vets, tests NOT yet run:** root package `wisptrace` (`doc.go errors.go span.go validate.go options.go db.go query.go wisptrace_test.go`). API: `Open(dir, opts...)`, `Ingest/IngestBatch/Sync/Span/Trace/DeleteSpan/DeleteTrace/Query/Costs/TokensByModel/ErrorRate/Check/Close`; options `WithRetention/WithFlushThreshold/WithSegmentCacheSize/WithReadYourWrites`; errors `ErrNotFound/ErrInvalidSpan/ErrClosed/*ValidationError`. On-disk layout inside the dir: `wal.log`, `segments/`, `index/`, `checkpoint.dat`, `manifest.dat`. Not yet done: Top/Distinct wrappers, dedupe-on-retry for rollups, README rewrite.
- Next: `server/` + `cmd/wisptraced`, then LICENSE/CI/docs. Decisions: visibility option A (durable on return, `Sync`, opt-in `WithReadYourWrites`), probe index to avoid rollup double-count on retry (else document at-least-once).
- Path references elsewhere in this file that say `cmd/...` or bare `wal/...` predate the move; the layout/commands sections are updated, historical prose may not be.

## Build order (PAD1 §12)

Implementation follows a strict, sequenced order because durability and crash-consistency depend on earlier phases being proven correct before later ones are designed. **Status below is as-built, not as-planned.**

- **Phase 1 — WAL + crash recovery** ✅ COMPLETE
  - Append-only WAL, `fsync` before ack, segment rotation, CRC-framed records.
  - Span record includes `Deleted bool` tombstone field from day one (v1.2 amendment).
  - Version tagging: every record has a version byte in its header for format-evolution safety.
  - Replay handles torn tails on the last segment, rejects checksum mismatches.
  - Fault-injection test (`kill -9`-in-a-loop): implemented in `tests/fault_injection_test.go`.

- **Phase 2 — Segment write path + reconciliation protocol** ✅ COMPLETE
  - **Segment format is row-oriented, NOT columnar** — see "Deviation: columnar segments" below.
  - Segment header is self-describing: magic, version, segment id, span count, min/max timestamp (zone map), bloom-section length.
  - A bloom section is embedded at write time: one filter per bounded-cardinality dimension (agent_id, model, tool_name, team, status).
  - Records are `length(4) | crc32(4)` framed, payload from `wal.EncodeSpanPayload`.
  - Pebble trace index: key = `trace_id || span_id`, value = `(segment_id, offset)`.
  - **Reconciliation protocol (the riskiest surface):** span is durable-and-queryable only once both segment write and Pebble `Put` are confirmed.
  - Checkpoint/watermark: records the last segment-id for which both sides are confirmed consistent. Startup replay bounded to the checkpoint tail (v1.1 amendment).
  - Tombstone path: **implemented.** WAL `deleted=true` → Pebble `Delete` at the same key, with the tombstone effect landing only in the index and never in the segment. `writeSegmentBatch` collapses each key to its last write, writes live records to a segment, and applies the tombstone deletes in the same atomic `BatchApplySpans` batch. (This was previously broken — tombstones were routed through `BatchPutSpans` and stayed indexed until compaction; see "Deletion semantics" below for the fix and rationale.)
  - Version tagging + documented downgrade path (v1.1 amendment, step 2a).

- **Phase 3 — Compaction with MVCC-style segment visibility** ✅ COMPLETE
  - New-segment-then-atomic-manifest-swap compaction (never in-place mutation).
  - Reader pinning of segment set; deferred cleanup of unreferenced old segments.
  - Segment/index co-deletion: batch Pebble deletes for dropped segment's spans into the same manifest-swap transaction, commit before unlink.
  - Retention-driven expiry: compaction dropping unreferenced segments (reuses co-deletion path), driven off the header `MaxTimestamp` zone map.
  - **Staleness check:** every scanned span is re-verified against the index before being acted on. Pebble holds exactly one `(segment_id, offset)` per key with no cross-segment fallback scan, so acting on a stale copy would resurrect a superseded version. See "The staleness check" below.

- **Phase 4 — Bloom filters + zone maps** ✅ COMPLETE
  - One bloom filter per bounded-cardinality dimension, embedded at segment write time.
  - Kirsch–Mitzenmacher double hashing over two deterministic stdlib FNV-1a/64 variants. Deliberately **not** `hash/maphash`: filters are written once and must still be readable after restart, so the hash must be stable across processes.
  - Zone-map-based time-range pruning.
  - **Range/filter query path.** Scan cost is bounded by bloom + zone-map pruning with **no Pebble in the hot loop for candidates that survive**. It *does* now issue one index lookup per surviving candidate, to re-verify liveness — the "never touches Pebble" claim that stood here previously was **false once tombstones were applied as index deletes**, and is recorded as PAD1 amendment **v1.3.2**. A span deleted after its segment was written is still physically in that segment, and the deletion is a negative fact only the index can see. See "The staleness check" below.

- **Phase 5 — Rollups + aggregation fallback** ⚠️ PARTIAL
  - Continuously-maintained rolling windows: 1m/5m/1h/1d. ✅
  - Rollup snapshot persisted at checkpoint cadence, with magic/version/**window-size** validation on read. ✅
  - Cost stored as fixed-point micro-dollars (`CostScale = 1_000_000`) to avoid float drift and the `int64(cost)` truncation-to-zero bug. ✅
  - **Root-attributed rollups (v1.3 amendment) are NOT implemented.** `rollup.BucketKey` is only `{WindowStart, Model}`. See "Known gaps" below.
  - Columnar-scan fallback for unaligned windows: **NOT implemented** — an unaligned window currently returns nothing from `QueryAggregations`.

- **Phase 6 — Query API + embeddable ergonomics** ⚠️ PARTIAL
  - Point/range/aggregation routing, four query tiers, aggregation + cardinality helpers. ✅
  - Go package API for embedding. ✅
  - HTTP server, Prometheus metrics endpoint, OTLP/JSON ingestion, Docker image, `main` package. ❌ **Not started.**

## Deviation: columnar segments (design target, not as-built)

PAD1 §3 specifies per-column encoding: delta-of-delta timestamps, dictionary+RLE bounded strings, delta/XOR numerics, zstd payload. **None of that is implemented.** `internal/segment/writer.go` writes row-oriented records using `wal.EncodeSpanPayload`, with the only columnar-ish structures being the per-dimension bloom section and the header's min/max zone map.

This was a deliberate deferral, recorded in `docs/checklist.txt`: *"Columnar segments — only after the row-oriented format and query semantics are stable."* Recorded in PAD1 as amendment v1.3.1.

**Consequence for docs and marketing: do not describe the segment format as columnar.** PAD1 describes the target design; this file describes reality. They are not the same thing, and conflating them is a credibility risk.

## Deletion semantics (fixed 30 Sep 2026 — previously two high-severity bugs)

Full write-up in `docs/TECHNICAL-GUIDE.md` §15. Source review had found that deleted spans were both **readable** and **counted**; both are fixed, and the fixes are structural rather than convention-based.

**The rule: a tombstoned key is DELETED from the Pebble index, not repointed at the tombstone record.** The tombstone's only durable effect is the index delete; no tombstone record is ever written to a segment. Putting it in the index instead leaves the record one `ReadAt` from being returned, which converts a storage invariant into a policy that each of the three independent read paths (`GetSpan`, `GetTrace`, `RangeQuery`) must remember separately — that convention is exactly what failed, and the test that should have caught it encoded the same convention rather than the property.

1. **`writeSegmentBatch` (`internal/engine/engine.go`)** collapses the batch to the last write per composite key *before* writing, then splits it: live records go to a segment and into the index; tombstone keys go straight to `index.Delete`. Both directions commit in one atomic `BatchApplySpans` (new in `internal/pebble/pebble.go`), so there is no window where a live record is indexed while a tombstoned key still resolves to its pre-delete record. A tombstone-only batch writes **no segment and consumes no segment id** — a segment of pure tombstones would carry no index entries, so `reconcileManifest` would drop it from the manifest on the next open and the file would sit on disk unreferenced, never reclaimed by compaction. WAL reclamation and the rollup snapshot still run on that path so a delete-heavy workload cannot grow the WAL forever.
2. **Batch dedupe also closes a pre-existing `RangeQuery` leak.** Inserting and deleting a span inside one flush window used to write both records into one segment; `RangeQuery` walks segments and never consults the index, filtering only on `Deleted`, so it returned the superseded live copy. New segments now hold at most one record per key.
3. **`RangeQuery` had a second, deeper leak — found only by the stricter tests.** A span deleted *after* its segment was written is still physically present in that segment: segments are immutable, and the deletion exists only in the index, as a negative fact a segment walk cannot see. Bloom and zone-map pruning narrow the candidate set but say nothing about liveness. So every candidate is now re-verified against the index and emitted only if the index points at exactly that `(segment_id, offset)` — the same staleness check the compactor applies, and invariant 5. This is the price of a segment-walking query not returning deleted spans. **It adds one index lookup per candidate, a real regression on a path already measured at ~132 allocs/span; the obvious fix is one snapshot iterator instead of N point lookups, and it is deliberately not done yet.** Both `GetTrace` and `RangeQuery` also give a buffered (un-flushed) write precedence over the index, so a tombstone suppresses the still-indexed pre-delete record for the whole flush interval instead of only after the next flush.
4. **`GetSpan` (`internal/engine/engine.go`)** resolves through a new `bufferedSpan` helper with a three-valued return, so "a tombstone is buffered" is distinct from "the buffer has nothing for this key". Previously the session loop skipped the tombstone and fell through to the index, resurrecting the pre-delete record. It also scans the append-only buffer backwards, so the last write for a span id wins instead of the first. The segment path re-checks `Deleted` as defence in depth for pre-fix segments.
5. **`GetTrace`** filters `Deleted` on both the index and session paths, and reports not-found for a fully deleted trace.
6. **`processSpan`** skips `rollups.Add` for tombstones, so a span deleted before it is counted never enters any aggregate.
7. **`compactor.Compact`** keeps the staleness check *before* the tombstone test. This ordering is load-bearing and easy to get wrong: a stale tombstone (superseded by a re-insert of the same span id) must not go into `reclaimKeys`, or `BatchDeleteSpans` erases the *newer live* version's index entry. Pinned by `TestCompactStaleTombstoneDoesNotEraseReinsertedSpan`. The two shapes of supersession are both covered: a key overwritten to a different `(segment, offset)`, and a key absent from the index entirely.
8. **`CheckConsistency` — one check had to be downgraded from error to warning, and it is worth understanding why.** It only validates the authoritative (last) record per key per segment, so a pre-fix segment holding a live/tombstone pair no longer reports a false "live span has no index entry" error. An indexed tombstone is now a warning (it is exactly the shape that let `GetSpan` return deleted spans). But a **live** record with **no** index entry also had to become a warning: that is now the normal state of *every* span deleted after its segment was written, and it is structurally indistinguishable from a lost index write. Segments and index alone cannot tell "deliberately deleted" from "write lost", so leaving it an error would mean the checker fires on every deletion in a healthy database. The dangerous direction is untouched and still a hard error: an index entry resolving to nothing readable is a dangling pointer. Net effect: **automated detection of a lost index write is gone.** The clean fix is a second keyspace in the index holding deliberately-deleted keys — write the key there at the same time as the delete, and compaction removes it when the space is reclaimed — which restores the discriminator. Not implemented; see the residual note.
9. **Tests.** `tests/fault_injection_test.go` asserted `if found && !span.Deleted`, which passes for a `GetSpan` that hands back the tombstone — it encoded the convention, not the property. Both sites now assert `found == false`, and the crash test also asserts `GetTrace` finds nothing. The pre-existing `TestGetSpanFindsTombstonedSpanWithDeletedSet` asserted the bug outright (*"a tombstone is still an indexed record, not absent"*) and is now `TestGetSpanDoesNotReturnTombstonedSpan`. New `internal/engine/tombstone_test.go` covers single-window collapse, mixed batches, later-flush deletes, the un-flushed-session supersede window, rollup exclusion (with a control assertion so it cannot pass vacuously), and live-sibling survival.

**Residual, not fixed:** a span counted in a rollup and deleted *later* keeps its contribution. `rollup.Store.Add` is purely additive; reversing `Count`/`SumCost`/`SumTokens` is exact, but un-minimising `MinLatencyMs`/`MaxLatencyMs` needs a per-bucket latency distribution the store does not keep. Tracked as an open design question, not papered over.

**Residual, not fixed:** `CheckConsistency` can no longer automatically detect a **lost index write** (item 8 above). The reason is a direct consequence of the deletion rule, not a separate mistake: expressing a deletion as an index delete is what makes `GetSpan`/`GetTrace`/`RangeQuery` correct, and the same move erases the only place that recorded "this key was deleted on purpose". The fix is a second Pebble keyspace for deleted keys, written atomically with the delete and cleared by compaction when it reclaims the space; read paths do not need it, only the checker does. Deliberately not built here — it adds persisted state whose own retention needs a policy, and shipping it without its own compaction/recovery tests would trade a known gap for an unknown one.

## Known correctness bugs (found by source review 30 Sep 2026 — NOT caught by the test suite)

Full write-up with reasoning in `docs/TECHNICAL-GUIDE.md` §15.

1. ~~Deleted spans are readable via `GetSpan` and `GetTrace`.~~ **Fixed** — see "Deletion semantics" above.
2. ~~Deleted spans are still counted in rollups.~~ **Partially fixed** — tombstones are no longer counted at ingest; a span counted and deleted later still leaks (see residual note above).
3. ~~No public delete API.~~ **Fixed** — `DeleteSpan(traceID, spanID)` and `DeleteTrace(traceID)` now exist on `WispTrace`. Both write tombstones through `InsertSpan` (the same path the tests already exercised), so the deletion fix is now reachable from the public API. `DeleteSpan` is idempotent and rejects empty identifiers; `DeleteTrace` enumerates the trace's live spans via `GetTrace` (so buffered and segment-resident spans are both covered), draining the ingest pipeline first with `WaitForIngest` so a just-acknowledged span cannot slip through. Pinned by `TestDeleteSpanRemovesSpanFromAllReadPaths`, `TestDeleteTraceRemovesOnlyThatTrace`, `TestDeleteEmptyIdentifiersError`, `TestDeleteNonExistentIsNoop`, `TestDeleteSpanDurableAcrossCrash` (`internal/engine/tombstone_test.go`).
4. ~~Cardinality cap fires before the caller's `limit`.~~ **Fixed** — a positive `limit` now short-circuits the scan (`cardinalityStop`, `internal/engine/engine.go`), so `GetDistinctModels(ts, end, 5)` returns 5 names instead of erroring once the scan would reach 10 000 distinct values. No limit (or a limit above the cap) still errors at the cap — the documented hard cap for unbounded queries. Pinned by `TestCardinalityStopHonoursLimitBeforeCap` and `TestGetDistinctModelsPositiveLimitShortCircuits` (`internal/engine/engine_test.go`).
5. **`QueryWithCursor` re-runs the full `RangeQuery` per page** and its numeric-offset cursor is not a stable snapshot; concurrent flush/compaction can make a client skip or duplicate spans across pages.
6. Minor: `maybeCompactLocked` burns a segment ID before its `recovering` guard (`internal/engine/engine.go:1022-1028`); the `recovering` guard is also cleared before the final recovery flush, so its code comment overstates what it protects.

## Known gaps (do not describe as shipped)

- Root-attributed rollups (PAD1 v1.3 amendment) — designed, not built.
- Columnar-scan aggregation fallback for unaligned windows — designed, not built.
- Rollup dimensions are `model` only. PAD1 §6 contemplates a 5-dimension group.
- No OTLP ingestion, no HTTP server, no `main` package. This is a library + engine, not yet a runnable product.
- Tombstones store a full span body in the WAL rather than a compact delete record. Known inefficiency, deferred (`docs/checklist.txt`, "v1 Storage").
- **Benchmarks recorded; the query path is measurably bad, point lookup fixed.** Latest full run: `docs/benchmarks/post-cache-20261015.txt` (15 Oct 2026, same host). `benchmark.txt` has the 26 Sep 2026 origin run and `pers.txt` §5 records root causes verified against source. Two committed SLOs pass with large headroom (point lookup 140 µs vs the 50 ms target). Parallel insert does **not** scale — median ~**0.74x** sequential (was recorded as 0.956x; ranges overlap, so the honest claim is "no scaling, plausibly slight contention"). Root cause unchanged: fsync-per-append under a mutex; fix is group commit, safe here precisely because no cross-writer ordering is promised. The segment reader cache (`internal/segment/cache.go`) plus its pooled `*segment.Release` cut the exact-offset paths again versus the light-open numbers: `GetSpan` **1,581 → 386 B/op, 22 → 14 allocs**; `spanAlreadyIndexed` **1,615 → 365 B/op, 21 → 13 allocs**; `GetTrace` **6,818 → 5,711 B/op, 135 → 128 allocs**. **The cache A/B is unconfirmed**: the intended no-cache run (`WISPTRACE_BENCH_NO_CACHE=1`) produced byte-identical B/op and allocs, so the env var never reached the test binary and the direct-open baseline is still missing. Latency is noise-bound on this laptop and is not claimed in either direction. `RangeQueryWithCompaction` is the remaining hotspot: **~1.20 MB/op, 60,270 allocs** (down from 3.04 MB / 70,170) for a window returning ~101 spans. The filter-before-staleness reorder (it removed the ~4900 per-candidate index lookups) and the streaming `segment.ScanAllFunc` are both applied; the cost is still per *scanned* record (~60k allocs / 5000 scanned records, record decode), and the per-candidate staleness lookup is still N point `Get`s rather than one snapshot iterator — not done. `RangeQuery` itself did not complete inside the suite's default 10m test timeout and must be run separately. Published in `docs/blog/post-1-unconfirmed-is-not-corrupt.md`.

## Current state

### Phase 1 — Complete

**internal/wal/wal.go:**
- `SpanPayload` struct matches PAD1 §2 schema: trace_id, span_id, parent_span_id (nullable as empty string, per OTel convention), timestamp, agent_id, model, tool_name, team, status, tokens_in, tokens_out, cost, latency_ms, payload (opaque blob), deleted (tombstone flag).
- Record header: version(1) | crc32(4) | length(4) = 9 bytes.
- Segment rotation on size threshold + explicit `Rotate()`.
- Torn-tail tolerance only on the last segment.
- Checksum validation.
- WAL record version tagging with `CurrentWALVersion = 1` constant — bumped when wire format changes.
- Replay rejects unsupported versions explicitly.

**tests/wal_test.go:**
- Rotation, reopen, checksum mismatch, torn tail, legacy adoption, tombstone round-trip all covered.
- **Fault-injection harness (`tests/fault_injection_test.go`, PAD1 §3 / PDD1 §7):** a self-execed worker process ingest-loads while the driver force-kills it (`Process.Kill()`, the Go equivalent of kill -9 / TerminateProcess), then reopens and asserts — every acked span survives byte-identical, `CheckConsistency()` passes, nothing is duplicated, nothing phantom appears. Ack durability is verified via an on-disk oracle file outside the db dir. Every case prints an unconditional `verdict:` line (`acked / tombstones / missing / duplicates / not-in-range / phantoms / range / warnings / reopen-attempt / wal-tail / acked-not-in-tail`), so a passing run reports the counts it checked instead of only an exit code.
  - **Verified 6 Oct 2026: 6/6 cases pass** (`TestFaultInjectionAckedSpansSurviveKills` across 4 kill timings + `TestFaultInjectionReplayRecoversUnflushedAcks` + `TestFaultInjectionInsertFlushDeleteCrash`), 4.92s total. Reopen succeeded on attempt 1 in all six cases, so the 30-attempt Windows handle-release retry never fired — defensive, not load-bearing. Both gaps from the 30 Sep run are closed: (1) verdict counts now print on pass; (2) the new replay case puts 58 acked-but-unflushed spans in front of `Replay()` on every run — previously only 3 of the 4 timings did that, because `cool` flushed before the kill.
  - **Hold guard (6 Oct 2026):** whenever `killAfter > 0` the driver sets `WISPTRACE_FAULT_HOLD=1`; the worker blocks after its own flush instead of exiting, and the driver `t.Fatalf`s if `workerCmd.Wait()` returns before the kill. This enforces the harness's own invariant — a *force*-kill test must kill a live process — which was previously false for `cool`: the worker exited cleanly and the "kill" landed after exit, so `cool` silently double-counted the graceful-close shape. `killAfter=0` (graceful-close) skips the hold. The `mid` timing's workload max was raised 800 → 4000 spans so the workload cannot finish before its 300 ms kill.
  - **Remaining harness gaps.** (1) The reopen retry has never fired — the Windows handle-release race is documented, not observed. (2) A kill opens only one of the three WAL-reclamation windows (the ingest-channel window); the expiry partial-flush and tombstone-only-batch windows cannot occur in these runs (sub-2s runs vs the 5 s session timeout; every-17th-tombstone mix never yields a pure-delete flush window) and are covered only by unit tests (`TestPartialFlushDoesNotReclaimStillBufferedRecord`, `TestTombstoneInSingleFlushWindowCollapsesBatch`). Published in `docs/blog/post-1-unconfirmed-is-not-corrupt.md`.

### Phases 2–4 — Complete; Phase 5 partial; Phase 6 partial

**internal/pebble/pebble.go:**
- `SpanLocation` struct: segment_id(8) + offset(8), the value half of every Pebble entry.
- `DB` wraps a Pebble instance with methods:
  - `PutSpan(key, location)`: write a span location to the index (key = trace_id || span_id).
  - `GetSpan(key)`: point lookup, returns `ErrNotFound` if absent or deleted.
  - `PrefixScan(prefix)`: all spans of a trace (prefix = trace_id ||), used for full-trace reconstruction.
  - `ScanAll()`: every (key → location) in the index (diagnostic path, no bounds) — feeds `cmd.CheckConsistency` and startup manifest reconciliation.
  - `DeleteSpan(key)`: remove from index (tombstone effect).
  - `BatchPutSpans` / `BatchDeleteSpans`: atomic multi-key updates (used on flush and compaction).
  - `BatchApplySpans(puts, deleteKeys)`: one atomic batch applying both index puts and tombstone deletes — the flush path, so a tombstoned key never resolves to a pre-delete record.
  - `encodeSpanLocation` / `decodeSpanLocation`: fixed-width (16-byte) value serialization.

**bloom/ (new):** fixed-size bloom filters with Kirsch–Mitzenmacher double hashing over two deterministic stdlib FNV-1a/64 variants; serializes as `m(4)|k(4)|bits`, supports back-to-back decoding.

**segment/ (implemented, row-oriented):** header `Magic(4)=0x57545331 | Version(2)=2 | segmentID(8) | spanCount(4) | minTimestamp(8) | maxTimestamp(8) | bloomSectionLength(4)`; immediately after is a bloom section with 5 filters (agent_id, model, tool_name, team, status) in fixed order; thereafter records are row-oriented `length(4)|crc32(4)` framed payloads from `wal.EncodeSpanPayload`. `Flush` refuses to overwrite an existing segment file and removes partial files on failure. `Manifest` is derived state (can be rebuilt from directory listing), written via tmp+fsync+rename.

**internal/segment/cache.go (reader cache):** a bounded, refcounted set of open segment readers keyed by segment id, sized by `Config.SegmentCacheSize` (default 64, negative disables). The exact-offset read paths (`GetSpan`, `GetTrace`, recovery's `spanAlreadyIndexed`) open through `acquireSegment`, which serves from the cache when configured and otherwise falls back to `OpenReaderLight` (header-only; **no bloom decode** — the fix that took `GetSpan` from 14,547 → 1,581 B/op at `internal/engine/engine.go:422`). `RangeQuery` deliberately does not use the cache or the light open: it needs the bloom section and a seek-based `ScanAll`, neither safe on a shared reader. Handles use a pooled `*segment.Release` lease (idempotent via `fired atomic.Bool`): a reader is closed only when it leaves the map **and** its refcount hits zero. Compaction/expiry call `Purge(ids)` before `os.Remove` because Windows refuses to unlink an open file. Pinned by `internal/engine/cache_test.go`.

**compactor/ (implemented):** `Compact` merges segments and performs segment/index co-deletion (Pebble delete batch committed before manifest swap and unlink). Includes a **staleness check** — every scanned span is re-verified against `index.GetSpan` before acting on it; stale copies do not resurrect superseded versions. `Expire` drops segments whose `MaxTimestamp` is past the cutoff and reuses the same co-deletion path; when all survivors are tombstones it writes no new segment.

**rollup/ (implemented):** four windows (1m/5m/1h/1d). `BucketKey{WindowStart, Model}` only (root-attributed rollups not yet implemented). Cost stored as fixed-point micro-dollars (`CostScale=1_000_000`) to avoid float drift. Rollup snapshots written as `rollup_<window>.rdb` with window-size validation on read.

**internal/engine/consistency.go:**
- `CheckConsistency()` → `ConsistencyReport{Errors, Warnings}`: manifest↔file and index↔segment cross-checks both directions, record-CRC pass, checkpoint-bound sanity, orphan-file reporting.

**Recovery/reconciliation:**
- `computeNextSegmentID()` seeds from `max(checkpoint, manifest, on-disk segment files) + 1` (prevents truncating live merged segments created by compaction).
- `recover()` skips spans the index already resolves to a readable matching record (prevents duplication after a crash inside `writeSegmentBatch`), and `reconcileManifest()` rewrites the manifest to exactly the index-referenced segment set.

**WAL reclamation (fixed 4 Oct 2026 — was the cause of intermittent acknowledged-span loss):** the old bound was *predicted* from which WAL segment was active when a flush drained, which is wrong in three ways: a span is acknowledged when its record is fsynced, **before** it is enqueued for the ingest loop; `flushExpiredSessions` drains only some sessions while still rotating and reclaiming; and a tombstone-only batch advances the bound without writing a segment. In each case a record that was still the only durable copy of an acknowledged span sat below the bound and was deleted, and the loss only became visible after a crash. `TestFaultInjectionAckedSpansSurviveKills` reported 60–90 missing acked spans intermittently, and only on a loaded machine where flushes are slow enough to land in the append/enqueue window.

The bound is now **derived from what is still unflushed, never predicted**. `wal.AppendRecordWithSeg` returns the segment that actually holds the record; `InsertSpan` holds `walPendingMu` across the fsync and the registration, so no reclaim can run in between; `walPending` counts un-durable records per segment; and `releaseWALPending` runs only after the batch is durable in a segment *and* the index, reclaiming strictly below the lowest still-pending segment (or below the active segment when nothing is pending). `flushAllSessions`, `flushExpiredSessions` and `Close` share one drain path, so a partial flush cannot advance the bound past a span it left buffered. Records replayed during recovery carry no pending registration (`walSeg == 0`) and the single post-recovery flush reclaims them. Pinned by `TestFlushedBatchDoesNotReclaimStillUnflushedAckedRecord`, `TestPartialFlushDoesNotReclaimStillBufferedRecord` (both `internal/engine/engine_test.go`) and `TestWALAppendRecordWithSegReportsTheSegmentHoldingTheRecord` (`tests/wal_test.go`).

## Key invariants

All of these are non-negotiable per PAD1:

1. **Single-node only** — no Raft, sharding, replication.
2. **No external services required** — S3, Kafka, etc. are optional, not mandatory.
3. **WAL is the single source of truth** — any component whose state can't be rebuilt by replaying the WAL is a design flaw.
4. **Immutable segments** — compaction never mutates a segment in place; it writes a new one and swaps pointers.
5. **No full SQL in v1** — general-purpose query planning, joins, subqueries are out of scope.
6. **No cgo** — pure Go, single static binary.
7. **Crash-safe durability** — every acknowledged write survives any crash (power loss, OOM kill, panic) with no torn or partial state visible.

## Wire format stability

Once a record format is shipped in v1.0, that format is **expensive to change**. WAL record format decisions (Phase 1) and segment format decisions (Phase 2) are cheap to change now, before any real deployment. After v1.0, a format bump requires:

1. A documented version number change (WAL version byte, segment schema version in the header).
2. Explicit replay logic in the code for any old version this build must still read.
3. A tested downgrade path (can you roll back to the prior version and still read the data?).

**Tombstone field (`deleted bool`) was added to the WAL record format in Phase 1, not Phase 2, specifically because PAD1 v1.2 amendment recognized that retention-driven segment expiry (required in any v1 deployment) requires it. It is cheaper to include it now, unused, than to retrofit it later when the format is already in production.**

## Commands

Go must be on `PATH` for these to run.

```bash
cd WispTraceDB

# Build all packages
go build ./...

# Run all tests
go test ./... -v

# Run a specific test
go test ./internal/wal -run TestWALTombstoneRoundTripsAsAuthoritativeDelete -v

# Run the fault-injection kill -9 harness (use -short for a quick pass)
go test ./tests -run FaultInjection -v

# Run just the consistency checker tests
go test ./internal/engine -run CheckConsistency -v

# Check for issues
go vet ./...

# Format (using goimports if available, otherwise gofmt)
go fmt ./...
```

## Package layout

```
(root)           package wisptrace — the public library. Entry point: db.go (`Open`, `DB`);
                  span.go/validate.go/errors.go/options.go/query.go; doc.go has the guarantees.
internal/wal/
  wal.go          — segmented write-ahead log, span record encoding/decoding

internal/pebble/
  pebble.go       — LSM index (Pebble wrapper), span location storage
  pebble_test.go  — index tests (put/get, prefix scan, delete)
  OPTIMIZATIONS.md — notes on pooled-buffer / allocation work in the index

internal/bloom/
  bloom.go        — fixed-size bloom filter, deterministic double hashing
  bloom_test.go   — no-false-negative property, encode/decode

internal/segment/
  writer.go       — row-oriented segment writer: header, bloom section, records
  reader.go       — segment reader: header/bloom access, ReadAt, ScanAll,
                    streaming ScanAllFunc;
                    OpenReaderLight (header-only, no bloom decode)
  cache.go        — bounded refcounted reader cache; Release lease;
                    Purge-before-unlink for Windows
  manifest.go     — derived segment-set pointer, tmp+fsync+rename swap
  *_test.go       — incl. delete_probe_test.go (Windows unlink-while-open probe)

internal/compactor/
  compactor.go    — segment merge + retention expiry + co-deletion + staleness check
  compactor_test.go

internal/rollup/
  rollup.go       — per-window Store: buckets, Add, range lookup, Evict
  manager.go      — 1m/5m/1h/1d windows, fixed-point cost scaling
  writer.go       — rollup snapshot writer (rollup_<window>.rdb)
  reader.go       — snapshot reader with magic/version/window-size validation
  rollup_test.go

internal/engine/   (package engine; was cmd/)
  engine.go       — WispTrace engine wiring: ingestion, session buffering,
                    segment flush, compaction, query APIs
  consistency.go  — CheckConsistency: index↔segment↔manifest cross-checks
  checkpoint.go   — durable segment watermark
  tombstone_test.go — deletion semantics across all three read paths and both
                    flush timings (single-window, mixed, later-flush, un-flushed)
  cache_test.go   — segment-cache purge wiring + disabled path
  compaction_diagnostic_test.go — TestCompactionDiagnostic (caught a benchmark key-collision bug)
  benchmark_test.go — 8 benchmark harnesses (insert seq/parallel, GetSpan,
                    GetSpanWithCompaction, GetTrace, RangeQuery,
                    RangeQueryWithCompaction, SpanAlreadyIndexed; results in
                    docs/benchmarks/)

tests/
  wal_test.go     — WAL tests (rotation, replay, corruption, tombstones)
  fault_injection_test.go — kill -9 harness: self-execed crashable worker,
                    ack oracle, force-kill driver, post-crash assertions

docs/
  PAD1.md         — frozen v1.3.2 architecture doc (non-negotiable)
  PDD1.md         — frozen v1.3 design doc (non-negotiable)
  checklist.txt   — build/work-split checklist and deferred items
  OPTIMIZATION-CHECKLIST.md — perf-phase tracker (phases 0-6, validation gates)
  TECHNICAL-GUIDE.md — as-built deep dive (§15 = deletion bugs and fixes)
  benchmarks/     — recorded runs (baseline-20260926 ... post-cache-20261015)
  blog/
    post-1-unconfirmed-is-not-corrupt.md — storage/correctness post (lead)
    post-2-cost-is-not-a-metric.md       — domain/rollups post
    linkedin-post.md                    — primary post + 5 A/B hooks
    PUBLISHING-PLAN.md                  — targets, sequencing, competitive read
```

## Updating this doc

Any change to PAD1/PDD1 scope, architecture, or non-functional targets requires a **deliberate, versioned amendment**, not silent drift. Document it in the freeze notes section of those docs. If you add a new package, record its purpose and responsibility here. If you change a decision, record the old and new with a reasoning note.

