# WispTraceDB — Implementation Guidance

This file provides guidance to Claude Code (claude.ai/code) when working with WispTraceDB.

## What this is

WispTraceDB is a single-node, embeddable trace store specialized for LLM agent spans. It is built according to **PAD1.md v1.2** (Architecture) and **PDD1.md v1.2** (Design), both frozen in the `docs/` folder. The architecture is **non-negotiable** until an explicit versioned amendment updates it.

Three design pillars drive the entire system:
1. **Pillar 1 — Durable ingestion (WAL):** accept a span, validate it, make it durable, acknowledge it.
2. **Pillar 2 — Immutable storage & compaction (segments):** turn buffered spans into compact, columnar, on-disk segments; periodically merge small segments and prune expired data.
3. **Pillar 3 — Indexing & aggregation (LSM trace index, bloom/zone-map pruning, rollups):** answer point/range/aggregation queries fast.

Pillars 2 and 3 are derived state — fully rebuildable from Pillar 1 (the WAL). If every segment, index file, and rollup were deleted, the WAL alone would reconstruct correct state.

## Build order (PAD1 §12)

Implementation follows a strict, sequenced order because durability and crash-consistency depend on earlier phases being proven correct before later ones are designed:

- **Phase 1 — WAL + crash recovery** ✅ COMPLETE
  - Append-only WAL, `fsync` before ack, segment rotation, CRC-framed records.
  - Span record includes `Deleted bool` tombstone field from day one (v1.2 amendment).
  - Version tagging: every record has a version byte in its header for format-evolution safety.
  - Replay handles torn tails on the last segment, rejects checksum mismatches.
  - Fault-injection test (`kill -9`-in-a-loop): pending, but code is ready.

- **Phase 2 — Segment write path + reconciliation protocol** 🚧 IN PROGRESS
  - Segment file format: columnar per-column encoding (delta-of-delta timestamps, dict+RLE bounded strings, delta/XOR numerics, zstd payload).
  - Pebble trace index: key = `trace_id || span_id`, value = `(segment_id, offset)`.
  - **Reconciliation protocol (the riskiest surface):** span is durable-and-queryable only once both segment write and Pebble `Put` are confirmed.
  - Checkpoint/watermark: record the last segment-id for which both sides are confirmed consistent. Startup replay bounded to checkpoint tail (v1.1 amendment).
  - Tombstone path: WAL `deleted=true` → Pebble `Delete` at same key → same reconciliation protocol.
  - Version tagging + documented downgrade path (v1.1 amendment, step 2a).

- **Phase 3 — Compaction with MVCC-style segment visibility**
  - New-segment-then-atomic-manifest-swap compaction (never in-place mutation).
  - Reader pinning of segment set; deferred cleanup of unreferenced old segments.
  - Segment/index co-deletion: batch Pebble deletes for dropped segment's spans into the same manifest-swap transaction, commit before unlink.
  - Retention-driven expiry: compaction dropping unreferenced segments (reuses co-deletion path).

- **Phase 4 — Bloom filters + zone maps**
  - One bloom filter per bounded-cardinality dimension, embedded at segment write time.
  - Zone-map-based time-range pruning.
  - Range/filter query path (never touches Pebble).

- **Phase 5 — Rollups + aggregation fallback**
  - Continuously-maintained rolling windows: 1m/5m/1h/1d.
  - Rollup rebuild by WAL replay bounded to checkpoint watermark.
  - Rollup snapshot persisted at checkpoint cadence.
  - Columnar-scan fallback for unaligned windows.

- **Phase 6 — Query API + embeddable ergonomics**
  - Point/range/aggregation routing.
  - Go package API polish for embedding.
  - HTTP server (bearer-token auth, health/readiness, Prometheus metrics).
  - OTLP + simplified JSON ingestion.
  - Docker image with sane defaults.

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
- Fault-injection harness (kill -9 under concurrent load): not yet implemented, but architecture is ready for it.

### Phase 2 — In Progress

**pebble/pebble.go:**
- `SpanLocation` struct: segment_id(8) + offset(8), the value half of every Pebble entry.
- `DB` wraps a Pebble instance with methods:
  - `PutSpan(key, location)`: write a span location to the index (key = trace_id || span_id).
  - `GetSpan(key)`: point lookup, returns `ErrNotFound` if absent or deleted.
  - `PrefixScan(prefix)`: all spans of a trace (prefix = trace_id ||), used for full-trace reconstruction.
  - `DeleteSpan(key)`: remove from index (tombstone effect).
  - `encodeSpanLocation` / `decodeSpanLocation`: fixed-width (16-byte) value serialization.

**pebble/pebble_test.go:**
- Put/get, not found, prefix scan, delete, encode/decode, multiple traces all covered.

**Next for Phase 2:**
- Segment writer (columnar file format with zone map and bloom filters).
- Reconciliation protocol: coordination between WAL→segment→Pebble writes, checkpoint tracking.
- Crash-recovery replay: reconstruct missing index entries from WAL tail (between last checkpoint and current state).

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

# Check for issues
go vet ./...

# Format (using goimports if available, otherwise gofmt)
go fmt ./...
```

## Package layout

```
wal/
  wal.go          — segmented write-ahead log, span record encoding/decoding
  wal_test.go     — WAL tests (rotation, replay, corruption, tombstones)

pebble/
  pebble.go       — LSM index (Pebble wrapper), span location storage
  pebble_test.go  — index tests (put/get, prefix scan, delete)

segment/
  (Phase 2)       — columnar segment file format, zone maps, bloom filters

cmd/
  cmd.go          — placeholder entry point, will hold WispTrace engine wiring

tests/
  wal_test.go     — WAL tests (can also live in wal/ as wal_test.go)

docs/
  PAD1.md         — frozen v1.2 architecture doc (non-negotiable)
  PDD1.md         — frozen v1.2 design doc (non-negotiable)
```

## Updating this doc

Any change to PAD1/PDD1 scope, architecture, or non-functional targets requires a **deliberate, versioned amendment**, not silent drift. Document it in the freeze notes section of those docs. If you add a new package, record its purpose and responsibility here. If you change a decision, record the old and new with a reasoning note.

