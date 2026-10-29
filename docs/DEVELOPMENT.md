# Developing WispTraceDB

Everything here is for contributors and for people evaluating the internals. If you want
to *use* WispTraceDB, start with the [README](../README.md).

Authoritative references: [PAD1.md](PAD1.md) (target architecture, frozen),
[PDD1.md](PDD1.md) (design), [TECHNICAL-GUIDE.md](TECHNICAL-GUIDE.md) (as-built deep dive),
[V1-API-DESIGN.md](V1-API-DESIGN.md) (public API design), [../CLAUDE.md](../CLAUDE.md)
(implementation status and invariants), [OPTIMIZATION-CHECKLIST.md](OPTIMIZATION-CHECKLIST.md).

The public API is the root `wisptrace` package. Everything else lives under `internal/`
and is not importable from other modules.

---

## Current Status

### ✅ What Works
- **Phase 1 (WAL & Crash Recovery)** — Complete. Spans are durable immediately; the WAL survives any crash.
- **Phase 2 (Segment Write & Reconciliation)** — Complete. Segments and index stay in sync; flushed spans are immediately queryable.
- **Phase 3 (Compaction)** — Complete. Segments are merged, old data is pruned, and the index is maintained correctly via the staleness check.
- **Phase 4 (Bloom Filters & Zone Maps)** — Complete. Range queries use bloom filters and timestamp ranges to prune candidates.
- **Phase 5 (Rollups & Aggregation)** — **Partial.** 1m/5m/1h/1d windows group by model; snapshots persist at checkpoint cadence. Root-attributed rollups and the columnar-scan fallback for unaligned windows are designed but not built.
- **Phase 6 (Query API)** — The public `wisptrace` package (library only) includes OTLP/JSON ingestion and manual span creation. There is deliberately no HTTP server or `main` package in v1.

### ⚠️ Known Limitations
- **Library only** — no HTTP server or binary; OTLP/JSON is ingested through `(*DB).IngestOTLP`, and the embedding application supplies the transport. OTLP/protobuf and gRPC are not supported.
- **Rollup dimensions are model-only** — root-attributed rollups are designed but not implemented.
- **Unaligned aggregation windows return nothing** — `QueryAggregations` on a window that is not bucket-aligned silently drops boundary buckets; the columnar-scan fallback is not built.
- **Columnar segments not implemented** — segments are row-oriented; per-column encoding was deferred.
- **`QueryWithCursor` is not a stable snapshot** — it re-runs the full `RangeQuery` per page and uses a numeric-offset cursor; concurrent flush/compaction can skip or duplicate spans across pages.
- **`CheckConsistency` cannot automatically detect a lost index write** — deleted-after-write spans are structurally indistinguishable from a lost index entry, so that check is a warning, not an error.
- **A span counted in a rollup then deleted later keeps its contribution** until the window expires.

### 📈 Performance

Benchmarks are recorded in `docs/benchmarks/` and `benchmark.txt`; root-cause analysis is in `pers.txt` §5. The numbers are laptop-and-Windows-bound (i7-1250U, 12 threads); **the ratios are the findings, not the absolute numbers.**

- **Point lookup**: ~140 µs (well below the 50 ms target). The light read path plus the segment reader cache cut `GetSpan` from 14,547 B / 35 allocs to **386 B / 14 allocs** per lookup; `GetTrace` (5,711 B / 128) and the recovery dedupe check (365 B / 13) came down with it.
- **Parallel insert does not scale** — median **~0.74x** sequential, fsync-per-append under a mutex. WAL group commit is the planned fix.
- **`RangeQuery` on merged segments costs ~1.2 MB and ~60k allocs per query** for a window returning ~101 spans (down from ~3.0 MB / 70k). The cost is per *scanned* record, not per returned span. The per-candidate staleness index lookup now runs after the cheap filter and the scan streams via `ScanAllFunc`; replacing the N point lookups with one index snapshot iterator is not done.
- **`RangeQuery` (uncompacted) did not finish inside the benchmark suite's default 10m timeout** — run it separately with `-timeout`.
- See [docs/benchmarks/post-cache-20261015.txt](docs/benchmarks/post-cache-20261015.txt) and [post-1-unconfirmed-is-not-corrupt.md](docs/blog/post-1-unconfirmed-is-not-corrupt.md) for details and caveats.

---

---

## Building & Testing

### Prerequisites
- Go 1.23 or later

### Commands

```bash
# Build all packages
go build ./...

# Run all tests
go test ./... -v

# Run a specific test
go test ./internal/wal -run TestWALTombstoneRoundTripsAsAuthoritativeDelete -v

# Run the fault-injection harness (kill -9 durability test)
go test ./tests -run FaultInjection -v

# Run the benchmarks (see docs/benchmarks/ for the last recorded runs).
# Use -timeout: the full suite exceeds go test's default 10m because every
# preload insert fsyncs. Run RangeQuery on its own if needed.
go test ./internal/engine -run '^$' -bench . -benchmem -timeout 30m

# Check for issues
go vet ./...

# Format code
go fmt ./...
```

### Fault Injection Test
WispTraceDB includes a fault-injection harness that force-kills the process during writes to verify durability:

```bash
go test ./tests -run FaultInjection -v
```

This test spawns a worker process, force-kills it at various points during ingestion, then verifies that every acknowledged span survives.

---

## Package layout (as built)

```
(root)                     — package wisptrace, the public API: db.go (Open, DB), tracer.go (StartSpan),
                             otlp.go (IngestOTLP), query.go, span.go, validate.go, errors.go, options.go
internal/wal/              — Segmented write-ahead log, span encoding/decoding
internal/pebble/           — LSM index (Pebble wrapper), span location storage
internal/bloom/            — Fixed-size bloom filters for pruning
internal/segment/          — Segment writer/reader, manifest, reader cache, immutable storage
internal/compactor/        — Segment merge, retention expiry, staleness check
internal/rollup/           — Continuous aggregation windows (1m/5m/1h/1d)
internal/engine/           — WispTrace engine: ingestion, queries, checkpoint, consistency, API
tests/                     — Fault injection and WAL tests
docs/                      — Architecture, design, technical guides, benchmarks
```

---

## Roadmap

### Planned (Not Yet Implemented)
- **v0 release hardening** — stabilize `QueryWithCursor`; re-measure and record benchmarks.
- **Group commit** — batch concurrent WAL appends into one fsync to fix the ~1x parallel insert scaling.
- **RangeQuery scan cost** — one Pebble snapshot iterator instead of N point lookups; chunked/callback scan to cut per-span allocations.
- **Compaction measurement** — benchmark the staleness check, currently unmeasured.
- Root-attributed rollups — "total cost of workflows started by Agent X" (designed, not built)
- Columnar-scan aggregation fallback for unaligned windows
- Columnar segment encoding — higher compression and faster scans
- Rollup dedupe on retried spans (probe the index before counting)
- `Top*` / `Distinct*` wrappers on the public `DB` (engine methods exist)
- OTLP/protobuf and gRPC ingestion; an optional ready-made `http.Handler` for `IngestOTLP`
- Read-your-writes without serialising writers (per-span completion signal)

### Not In Scope for v1
- An HTTP server or standalone binary (v1 is library-only; embedders bring their own transport)
- Replication or clustering
- SQL or advanced query language
- Schema evolution beyond the WAL version byte
- Cross-process access (single-node only)

---
