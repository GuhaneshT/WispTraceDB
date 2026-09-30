# WispTraceDB — Implementation Guidance

This file provides guidance to Claude Code (claude.ai/code) when working with WispTraceDB.

## What this is

WispTraceDB is a single-node, embeddable trace store specialized for LLM agent spans. It is built according to **PAD1.md v1.3.1** (Architecture) and **PDD1.md v1.3** (Design), both frozen in the `docs/` folder. The architecture is **non-negotiable** until an explicit versioned amendment updates it.

**Read `docs/PAD1.md` as the target design and this file as the as-built status.** Where the two disagree, the code and this file win; the gap is recorded in PAD1's amendment log rather than papered over.

Three design pillars drive the entire system:
1. **Pillar 1 — Durable ingestion (WAL):** accept a span, validate it, make it durable, acknowledge it.
2. **Pillar 2 — Immutable storage & compaction (segments):** turn buffered spans into compact, on-disk segments; periodically merge small segments and prune expired data. (Intended end state is columnar — see the deviation section below.)
3. **Pillar 3 — Indexing & aggregation (LSM trace index, bloom/zone-map pruning, rollups):** answer point/range/aggregation queries fast.

Pillars 2 and 3 are derived state — fully rebuildable from Pillar 1 (the WAL). If every segment, index file, and rollup were deleted, the WAL alone would reconstruct correct state.

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
  - Tombstone path: WAL `deleted=true` → Pebble `Delete` at same key → same reconciliation protocol. The tombstone effect lands only in Pebble, never in the segment.
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
  - Range/filter query path (never touches Pebble).

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

PAD1 §3 specifies per-column encoding: delta-of-delta timestamps, dictionary+RLE bounded strings, delta/XOR numerics, zstd payload. **None of that is implemented.** `segment/writer.go` writes row-oriented records using `wal.EncodeSpanPayload`, with the only columnar-ish structures being the per-dimension bloom section and the header's min/max zone map.

This was a deliberate deferral, recorded in `docs/checklist.txt`: *"Columnar segments — only after the row-oriented format and query semantics are stable."* Recorded in PAD1 as amendment v1.3.1.

**Consequence for docs and marketing: do not describe the segment format as columnar.** PAD1 describes the target design; this file describes reality. They are not the same thing, and conflating them is a credibility risk.

## Known gaps (do not describe as shipped)

- Root-attributed rollups (PAD1 v1.3 amendment) — designed, not built.
- Columnar-scan aggregation fallback for unaligned windows — designed, not built.
- Rollup dimensions are `model` only. PAD1 §6 contemplates a 5-dimension group.
- No OTLP ingestion, no HTTP server, no `main` package. This is a library + engine, not yet a runnable product.
- Tombstones store a full span body in the WAL rather than a compact delete record. Known inefficiency, deferred (`docs/checklist.txt`, "v1 Storage").
- **No recorded benchmarks.** `benchmark.txt` and `pers.txt` are both empty; `cmd/benchmark_test.go` has five harnesses but results were never captured. PAD1 §11 deliberately leaves the ingest-throughput target uncommitted pending measurement, so this gap is consistent with the frozen doc — but do not quote throughput numbers without measuring first.

## Current state

### Phase 1 — Complete

**wal/wal.go:**
- `SpanPayload` struct matches PAD1 §2 schema: trace_id, span_id, parent_span_id (nullable as empty string, per OTel convention), timestamp, agent_id, model, tool_name, team, status, tokens_in, tokens_out, cost, latency_ms, payload (opaque blob), deleted (tombstone flag).
- Record header: version(1) | crc32(4) | length(4) = 9 bytes.
- Segment rotation on size threshold + explicit `Rotate()`.
- Torn-tail tolerance only on the last segment.
- Checksum validation.
- WAL record version tagging with `CurrentWALVersion = 1` constant — bumped when wire format changes.
- Replay rejects unsupported versions explicitly.

**tests/wal_test.go:**
- Rotation, reopen, checksum mismatch, torn tail, legacy adoption, tombstone round-trip all covered.
- **Fault-injection harness (`tests/fault_injection_test.go`, PAD1 §3 / PDD1 §7):** a self-execed worker process ingest-loads while the driver force-kills it (`Process.Kill()`, the Go equivalent of kill -9 / TerminateProcess), then reopens and asserts — every acked span survives byte-identical, `CheckConsistency()` passes, nothing is duplicated, nothing phantom appears. Ack durability is verified via an on-disk oracle file outside the db dir.

### Phases 2–5 — Complete; Phase 6 partial

**pebble/pebble.go:**
- `SpanLocation` struct: segment_id(8) + offset(8), the value half of every Pebble entry.
- `DB` wraps a Pebble instance with methods:
  - `PutSpan(key, location)`: write a span location to the index (key = trace_id || span_id).
  - `GetSpan(key)`: point lookup, returns `ErrNotFound` if absent or deleted.
  - `PrefixScan(prefix)`: all spans of a trace (prefix = trace_id ||), used for full-trace reconstruction.
  - `ScanAll()`: every (key → location) in the index (diagnostic path, no bounds) — feeds `cmd.CheckConsistency` and startup manifest reconciliation.
  - `DeleteSpan(key)`: remove from index (tombstone effect).
  - `BatchPutSpans` / `BatchDeleteSpans`: atomic multi-key updates (used on flush and compaction).
  - `encodeSpanLocation` / `decodeSpanLocation`: fixed-width (16-byte) value serialization.

**bloom/ (new):** fixed-size bloom filters with Kirsch–Mitzenmacher double hashing over two deterministic stdlib FNV-1a/64 variants; serializes as `m(4)|k(4)|bits`, supports back-to-back decoding.

**segment/ (implemented, row-oriented):** header `Magic(4)=0x57545331 | Version(2)=2 | segmentID(8) | spanCount(4) | minTimestamp(8) | maxTimestamp(8) | bloomSectionLength(4)`; immediately after is a bloom section with 5 filters (agent_id, model, tool_name, team, status) in fixed order; thereafter records are row-oriented `length(4)|crc32(4)` framed payloads from `wal.EncodeSpanPayload`. `Flush` refuses to overwrite an existing segment file and removes partial files on failure. `Manifest` is derived state (can be rebuilt from directory listing), written via tmp+fsync+rename.

**compactor/ (implemented):** `Compact` merges segments and performs segment/index co-deletion (Pebble delete batch committed before manifest swap and unlink). Includes a **staleness check** — every scanned span is re-verified against `index.GetSpan` before acting on it; stale copies do not resurrect superseded versions. `Expire` drops segments whose `MaxTimestamp` is past the cutoff and reuses the same co-deletion path; when all survivors are tombstones it writes no new segment.

**rollup/ (implemented):** four windows (1m/5m/1h/1d). `BucketKey{WindowStart, Model}` only (root-attributed rollups not yet implemented). Cost stored as fixed-point micro-dollars (`CostScale=1_000_000`) to avoid float drift. Rollup snapshots written as `rollup_<window>.rdb` with window-size validation on read.

**cmd/consistency.go:**
- `CheckConsistency()` → `ConsistencyReport{Errors, Warnings}`: manifest↔file and index↔segment cross-checks both directions, record-CRC pass, checkpoint-bound sanity, orphan-file reporting.

**Recovery/reconciliation:**
- `computeNextSegmentID()` seeds from `max(checkpoint, manifest, on-disk segment files) + 1` (prevents truncating live merged segments created by compaction).
- `recover()` skips spans the index already resolves to a readable matching record (prevents duplication after a crash inside `writeSegmentBatch`), and `reconcileManifest()` rewrites the manifest to exactly the index-referenced segment set.

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
go test ./wal -run TestWALTombstoneRoundTripsAsAuthoritativeDelete -v

# Run the fault-injection kill -9 harness (use -short for a quick pass)
go test ./tests -run FaultInjection -v

# Run just the consistency checker tests
go test ./cmd -run CheckConsistency -v

# Check for issues
go vet ./...

# Format (using goimports if available, otherwise gofmt)
go fmt ./...
```

## Package layout

```
wal/
  wal.go          — segmented write-ahead log, span record encoding/decoding

pebble/
  pebble.go       — LSM index (Pebble wrapper), span location storage
  pebble_test.go  — index tests (put/get, prefix scan, delete)

bloom/
  bloom.go        — fixed-size bloom filter, deterministic double hashing
  bloom_test.go   — no-false-negative property, encode/decode

segment/
  writer.go       — row-oriented segment writer: header, bloom section, records
  reader.go       — segment reader: header/bloom access, ReadAt, ScanAll
  manifest.go     — derived segment-set pointer, tmp+fsync+rename swap
  *_test.go

compactor/
  compactor.go    — segment merge + retention expiry + co-deletion + staleness check
  compactor_test.go

rollup/
  manager.go      — 1m/5m/1h/1d windows, fixed-point cost scaling
  writer.go       — rollup snapshot writer (rollup_<window>.rdb)
  reader.go       — snapshot reader with magic/version/window-size validation
  rollup_test.go

cmd/
  cmd.go          — WispTrace engine wiring: ingestion, session buffering,
                    segment flush, compaction, query APIs
  consistency.go  — CheckConsistency: index↔segment↔manifest cross-checks
  checkpoint.go   — durable segment watermark
  benchmark_test.go — 5 benchmark harnesses (results not yet recorded)

tests/
  wal_test.go     — WAL tests (rotation, replay, corruption, tombstones)
  fault_injection_test.go — kill -9 harness: self-execed crashable worker,
                    ack oracle, force-kill driver, post-crash assertions

docs/
  PAD1.md         — frozen v1.3.1 architecture doc (non-negotiable)
  PDD1.md         — frozen v1.3 design doc (non-negotiable)
  checklist.txt   — build/work-split checklist and deferred items
  blog/
    post-1-unconfirmed-is-not-corrupt.md — storage/correctness post (lead)
    post-2-cost-is-not-a-metric.md       — domain/rollups post
    linkedin-post.md                    — primary post + 5 A/B hooks
    PUBLISHING-PLAN.md                  — targets, sequencing, competitive read
```

## Updating this doc

Any change to PAD1/PDD1 scope, architecture, or non-functional targets requires a **deliberate, versioned amendment**, not silent drift. Document it in the freeze notes section of those docs. If you add a new package, record its purpose and responsibility here. If you change a decision, record the old and new with a reasoning note.

