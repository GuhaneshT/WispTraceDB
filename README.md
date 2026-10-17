# WispTraceDB

A single-node, embeddable trace store for LLM agent spans. Purpose-built for durability, fast aggregation queries, and seamless embedding into Go services.

## What is WispTraceDB?

WispTraceDB is a specialized database for storing and querying traces from LLM agent systems. It's designed for:

- **Solo builders and small teams** running LLM agents in production
- **Go services** that want to embed trace storage directly without running a separate database process
- **Applications** that need crash-safe durability, full trace reconstruction, and aggregation queries over cost/latency/token data

It is **not** a general-purpose database. It accepts a stream of spans (units of work in a trace), makes them durable immediately, and answers three classes of queries: point lookups, range scans, and time-windowed aggregations.

---

## Key Features

### 🛡️ Crash-Safe Durability
Every acknowledged span survives any crash — power loss, OOM kill, or panic. The Write-Ahead Log (WAL) is fsynced before each acknowledgment, and all derived state (segments, index, rollups) is rebuildable by replaying the WAL.

### 📊 Full Trace Reconstruction
Retrieve any complete trace by ID. Traces are hierarchical (parent-child span relationships are preserved), and reconstruction is fast even for large traces.

### ⚡ Fast Aggregation Queries
Answer "cost by model" or "token distribution by model" over any time window. Built on continuous rollup windows (1m, 5m, 1h, 1d). Bloom filters and zone maps keep query costs bounded. (Root-attributed "cost of workflows started by Agent X" rollups are designed but not yet implemented.)

### 📦 Embeddable Design
No external services required — no S3, Kafka, or separate database process. Import the Go package into your application and start using it immediately.

### 🔍 LSM-Based Indexing
Trace index powered by Pebble (a production-grade LSM tree). Supports fast point lookups and range scans over traces.

### 📝 Immutable Segments & Compaction
Writes are buffered into row-oriented segments. Compaction merges segments, prunes expired data, and maintains index consistency — all without mutating data in place.

---

## Installation

Requires **Go 1.23 or later**.

```bash
go get github.com/GuhaneshT/WispTraceDB/cmd
```

Or clone the repository and build locally:

```bash
git clone https://github.com/GuhaneshT/WispTraceDB.git
cd WispTraceDB
go build ./...
```

---

## Quick Start

### Embedding WispTraceDB in Your Go Application

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/GuhaneshT/WispTraceDB/cmd"
    "github.com/GuhaneshT/WispTraceDB/rollup"
    "github.com/GuhaneshT/WispTraceDB/wal"
)

func main() {
    // Open or create a database. CreateWisp() uses the default config,
    // which writes relative paths (wal.log, segments/, wisp_lsm/, ...) in
    // the current directory. To choose a location, start from the default
    // config and override paths:
    cfg := cmd.DefaultWispTraceConfig()
    cfg.SegmentDir = "./traces_db/segments"
    cfg.WALPath = "./traces_db/wal.log"
    cfg.PebblePath = "./traces_db/wisp_lsm"
    cfg.CheckpointPath = "./traces_db/checkpoint.dat"
    cfg.ManifestPath = "./traces_db/manifest.dat"

    db, err := cmd.CreateWispTraceWithConfig(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Insert a span. Timestamp is Unix nanoseconds; Cost is a float64 dollar
    // amount; TokensIn/TokensOut are int32.
    span := wal.SpanPayload{
        TraceID:      "trace-123",
        SpanID:       "span-456",
        ParentSpanID: "", // Empty for root span
        Timestamp:    time.Now().UnixNano(),
        AgentID:      "agent-1",
        Model:        "gpt-4",
        ToolName:     "search",
        Team:         "platform",
        Status:       "success",
        TokensIn:     100,
        TokensOut:    50,
        Cost:         0.0015, // in dollars
        LatencyMs:    245,
        Payload:      []byte(`{"result": "found"}`),
        Deleted:      false,
    }

    if err := db.InsertSpan(span); err != nil {
        log.Fatal(err)
    }

    // Spans are buffered and flushed in the background; Flush forces it.
    if err := db.Flush(); err != nil {
        log.Fatal(err)
    }

    // Retrieve a single span.
    retrieved, found, err := db.GetSpan("trace-123", "span-456")
    if err != nil {
        log.Fatal(err)
    }
    if found {
        fmt.Printf("Span: %+v\n", retrieved)
    }

    // Retrieve a full trace.
    trace, found, err := db.GetTrace("trace-123")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Trace has %d spans (found=%v)\n", len(trace), found)

    // Query a time range.
    start := time.Now().Add(-1 * time.Hour).UnixNano()
    end := time.Now().UnixNano()
    spans, err := db.RangeQuery(cmd.RangeFilter{StartTS: start, EndTS: end})
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Found %d spans in the last hour\n", len(spans))

    // Read rollup buckets (grouped by model) for a window.
    aggs, err := db.QueryAggregations(rollup.WindowHour, start, end)
    if err != nil {
        log.Fatal(err)
    }
    for key, agg := range aggs {
        fmt.Printf("Model %s: cost=$%.4f, tokens=%d\n",
            key.Model,
            rollup.UnscaleCost(agg.SumCost),
            agg.SumTokensIn+agg.SumTokensOut)
    }
}
```

---

## Core Concepts

### Spans
A span represents a unit of work in a trace. It includes:
- **Hierarchy**: `TraceID`, `SpanID`, `ParentSpanID` (empty string = root, per OpenTelemetry convention)
- **Execution**: `Timestamp` (Unix nanoseconds), `Status`, `LatencyMs`
- **Resources**: `AgentID`, `Model`, `ToolName`, `Team`
- **Costs**: `TokensIn`, `TokensOut` (`int32`), `Cost` (`float64`, dollars)
- **Payload**: Opaque blob for application-specific data
- **Deleted**: Tombstone flag (see "Deletion semantics" below)

### Traces
A trace is a collection of related spans. Traces are hierarchical — each span can have a parent span, forming a tree or DAG. Root spans have an empty `ParentSpanID`.

### Write-Ahead Log (WAL)
All writes go to the WAL first. The WAL is segmented and fsynced after each append, ensuring durability before acknowledgment. The WAL is also the single source of truth — all other data structures (segments, index, rollups) are derived from it and can be rebuilt if necessary.

### Segments
Buffered spans are periodically flushed into immutable **row-oriented** segments on disk. Each segment includes:
- A 38-byte header with metadata (magic, version, segment ID, span count, min/max timestamp zone map, bloom-section length)
- A bloom section with filters for bounded-cardinality dimensions (agent_id, model, tool_name, team, status) for fast pruning
- Length+CRC framed span records (the same payload encoding the WAL uses)

Segments are **not** columnar and **not** compressed in v0. Columnar per-column encoding is a documented target, deferred until query semantics are stable.

### Index
A Pebble LSM index maps `(trace_id, span_id)` to its location `(segment_id, offset)`. Supports fast point lookups and range scans. The index is the authority on span liveness.

### Compaction
Periodically, small segments are merged into larger ones, and old data is pruned based on retention policies. Compaction never mutates segments in place; it creates new segments and swaps the manifest atomically. Every scanned record is re-verified against the index (a "staleness check") before it is acted on, so tombstone reclamation and partial merges are always safe.

### Rollups
Continuous aggregation windows (1 minute, 5 minutes, 1 hour, 1 day) continuously roll up `cost`, `tokens_in`, `tokens_out`, and `latency_ms` grouped by model. Cost is stored as fixed-point micro-dollars to avoid float drift. Rollups are snapshotted at checkpoint cadence and rebuilt from the WAL after a crash.

### Deletion semantics
Deletes go through `DeleteSpan(traceID, spanID)` and `DeleteTrace(traceID)`. Each writes a tombstone (`SpanPayload{Deleted: true}`) — a WAL record whose only durable effect is removing the span's key from the Pebble index at flush time; no tombstone record is ever written to a segment. All three read paths (`GetSpan`, `GetTrace`, `RangeQuery`) report a deleted span as absent. A span counted in a rollup and deleted *later* still keeps its contribution until its window expires (see "Known limitations").

---

## API Overview

### Lifecycle
- `cmd.CreateWisp() (*WispTrace, error)` — open with the default config
- `cmd.DefaultWispTraceConfig() WispTraceConfig` — defaults to start from
- `cmd.CreateWispTraceWithConfig(cfg WispTraceConfig) (*WispTrace, error)` — open with an explicit config
- `(*WispTrace).Close() error` — flush rollups/segments and close
- `(*WispTrace).CloseWithoutFlush() error` — simulate a crash (tests)

### Writes
- `InsertSpan(span wal.SpanPayload) error` — append + fsync (durable on return); buffered for segmentation
- `DeleteSpan(traceID, spanID string) error` — durably delete one span (idempotent; absent is a no-op)
- `DeleteTrace(traceID string) error` — durably delete every live span of a trace (absent is a no-op)
- `Flush() error` — drain the ingest channel and flush all buffered spans to a segment

### Point Queries
- `GetSpan(traceID, spanID string) (wal.SpanPayload, bool, error)` — one span; `found=false` if deleted/absent
- `GetTrace(traceID string) ([]wal.SpanPayload, bool, error)` — all live spans of a trace

### Range Queries
- `RangeQuery(filter RangeFilter) ([]wal.SpanPayload, error)` — spans in a time range, optionally filtered by `AgentID`/`Model`/`ToolName`/`Team`/`Status`
- `QueryByModel` / `QueryByTeam` / `QueryByStatus` / `QueryByAgentID` / `QueryByToolName` — convenience wrappers
- `ListTraces(filter RangeFilter, limit int) ([]string, error)`
- `QueryWithCursor(filter RangeFilter, pageSize int, cursor string) ([]wal.SpanPayload, string, error)`

### Aggregations
- `QueryAggregations(window string, startTS, endTS int64) (map[rollup.BucketKey]*rollup.Value, error)` — raw rollup buckets (`"1m"`, `"5m"`, `"1h"`, `"1d"`)
- `GetCostByModel` / `GetCostByTeam`, `GetTokensByModel`, `GetTopTeamsByCost`, `GetTopModelsBySpanCount`, `GetFailedSpans`, `GetErrorRate` — derived over `RangeQuery`
- `GetDistinctModels` / `GetDistinctTeams` / `GetDistinctAgents` — cardinality helpers

### Maintenance
- `CheckConsistency() *ConsistencyReport` — validate index↔segment↔manifest consistency (`report.OK()`, `report.Errors`, `report.Warnings`)
- `LiveSegments() ([]uint64, error)` — the current manifest contents

Compaction and retention expiry run automatically on background tickers; there is no public `Compact`/`Expire` method.

---

## Current Status

### ✅ What Works
- **Phase 1 (WAL & Crash Recovery)** — Complete. Spans are durable immediately; the WAL survives any crash.
- **Phase 2 (Segment Write & Reconciliation)** — Complete. Segments and index stay in sync; flushed spans are immediately queryable.
- **Phase 3 (Compaction)** — Complete. Segments are merged, old data is pruned, and the index is maintained correctly via the staleness check.
- **Phase 4 (Bloom Filters & Zone Maps)** — Complete. Range queries use bloom filters and timestamp ranges to prune candidates.
- **Phase 5 (Rollups & Aggregation)** — **Partial.** 1m/5m/1h/1d windows group by model; snapshots persist at checkpoint cadence. Root-attributed rollups and the columnar-scan fallback for unaligned windows are designed but not built.
- **Phase 6 (Query API)** — **Partial.** The Go package API is complete; there is no HTTP server, OTLP ingestion, or `main` package yet.

### ⚠️ Known Limitations
- **No HTTP server / OTLP ingestion / `main` package** — this is a library + engine, not a runnable product.
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
- **`RangeQuery` on merged segments costs ~2.93 MB and ~65k allocs per query** for a window returning ~101 spans — `ScanAll` materialises every record in the crossed segment(s) before filtering. The per-candidate staleness index lookup is reordered to run after the cheap filter; an iterator-based scan is the next fix.
- **`RangeQuery` (uncompacted) did not finish inside the benchmark suite's default 10m timeout** — run it separately with `-timeout`.
- See [docs/benchmarks/post-cache-20261015.txt](docs/benchmarks/post-cache-20261015.txt) and [post-1-unconfirmed-is-not-corrupt.md](docs/blog/post-1-unconfirmed-is-not-corrupt.md) for details and caveats.

---

## Design Philosophy

WispTraceDB follows three immutable design pillars:

1. **Durable Ingestion (WAL)** — Accept a span, validate it, fsync it, acknowledge it. Every acknowledged span survives any crash.
2. **Immutable Storage & Compaction (Segments)** — Buffer spans into immutable segments; periodically merge and prune. Never mutate data in place.
3. **Indexing & Aggregation (LSM + Rollups)** — Answer queries fast via LSM tree and continuous rollups. Bloom filters and zone maps prune candidates.

All derived state (segments, index, rollups) is rebuildable from the WAL. If every segment, index file, and rollup were deleted, the WAL alone would reconstruct correct state.

---

## Architecture & Documentation

**For deep dives**, see:
- [PAD1.md](docs/PAD1.md) — **Frozen v1.3.2 Architecture** (target design, with an amendment log; not a description of shipped behavior)
- [PDD1.md](docs/PDD1.md) — **Frozen v1.3 Design Doc** (objectives, competitive analysis, USP)
- [CLAUDE.md](CLAUDE.md) — as-built status and the authoritative source when it disagrees with PAD1
- [docs/TECHNICAL-GUIDE.md](docs/TECHNICAL-GUIDE.md) — code-accurate walkthrough: deletion semantics, consistency model, recovery details
- [docs/OPTIMIZATION-CHECKLIST.md](docs/OPTIMIZATION-CHECKLIST.md) — performance work tracker
- [docs/checklist.txt](docs/checklist.txt) — v0→v1 build checklist

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
go test ./wal -run TestWALTombstoneRoundTripsAsAuthoritativeDelete -v

# Run the fault-injection harness (kill -9 durability test)
go test ./tests -run FaultInjection -v

# Run the benchmarks (see docs/benchmarks/ for the last recorded runs).
# Use -timeout: the full suite exceeds go test's default 10m because every
# preload insert fsyncs. Run RangeQuery on its own if needed.
go test ./cmd -run '^$' -bench . -benchmem -timeout 30m

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

## Package Layout

```
wal/              — Segmented write-ahead log, span encoding/decoding
pebble/           — LSM index (Pebble wrapper), span location storage
bloom/            — Fixed-size bloom filters for pruning
segment/          — Segment writer/reader, manifest, reader cache, immutable storage
compactor/        — Segment merge, retention expiry, staleness check
rollup/           — Continuous aggregation windows (1m/5m/1h/1d)
cmd/              — WispTrace engine: ingestion, queries, checkpoint, consistency, API
tests/            — Fault injection and WAL tests
docs/             — Architecture, design, technical guides, benchmarks
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
- HTTP server — REST API for remote queries
- OTLP ingestion — native OpenTelemetry Protocol support

### Not In Scope for v1
- Replication or clustering
- SQL or advanced query language
- Schema evolution beyond the WAL version byte
- Cross-process access (single-node only)

---

## License

MIT. See the repository for licensing details.

---

## Contributing

Contributions are welcome. Please ensure:
- All tests pass: `go test ./... -v`
- Code follows Go conventions: `go fmt ./...`
- No new external dependencies without discussion
- New features include tests and documentation

---

## Questions & Issues

For questions or issues, see:
- [CLAUDE.md](CLAUDE.md) for implementation details and known issues
- [docs/](docs/) for architecture and design documents
- GitHub issues for bug reports and feature requests

---

## Summary

WispTraceDB is a purpose-built, durable, embeddable trace store for LLM agents. It's designed for teams who want crash-safe trace storage without running a separate database process. Ship it as a library inside your Go application, write spans to it, and query them fast — all with guaranteed durability and no external dependencies.
