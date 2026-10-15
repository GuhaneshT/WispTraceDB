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
Every acknowledged span survives any crash — power loss, OOM kill, or panic. The Write-Ahead Log (WAL) is fsynced before each acknowledgment, and all queries rebuild from the WAL if needed.

### 📊 Full Trace Reconstruction  
Retrieve any complete trace by ID. Traces are hierarchical (parent-child span relationships are preserved), and reconstruction is fast even for large traces.

### ⚡ Fast Aggregation Queries
Answer "total cost of workflows started by Agent X" or "token distribution by model" over any time window. Built on typed columns and continuous rollup windows (1m, 5m, 1h, 1d). Bloom filters and zone maps keep query costs bounded.

### 📦 Embeddable Design
No external services required — no S3, Kafka, or separate database process. Import the Go package into your application and start using it immediately.

### 🔍 LSM-Based Indexing
Trace index powered by Pebble (a production-grade LSM tree). Supports fast point lookups and range scans over traces.

### 📝 Immutable Segments & Compaction
Writes are buffered into row-oriented segments. Compaction merges segments, prunes expired data, and maintains index consistency — all without mutating data in place.

---

## Installation

```bash
go get github.com/guhanesht/WispTraceDB/cmd
```

Or clone the repository and build locally:

```bash
git clone https://github.com/guhanesht/WispTraceDB.git
cd WispTraceDB
go build ./...
```

---

## Quick Start

### Embedding WispTraceDB in Your Go Application

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/guhanesht/WispTraceDB/cmd"
    "github.com/guhanesht/WispTraceDB/wal"
)

func main() {
    // Open or create a database
    db, err := cmd.Open("./traces_db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Insert a span
    span := wal.SpanPayload{
        TraceID:      "trace-123",
        SpanID:       "span-456",
        ParentSpanID: "", // Empty for root span
        Timestamp:    time.Now(),
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

    err = db.InsertSpan(context.Background(), span)
    if err != nil {
        log.Fatal(err)
    }

    // Retrieve a single span
    retrieved, found, err := db.GetSpan(context.Background(), "trace-123", "span-456")
    if err != nil {
        log.Fatal(err)
    }
    if found {
        fmt.Printf("Span: %+v\n", retrieved)
    }

    // Retrieve a full trace
    trace, err := db.GetTrace(context.Background(), "trace-123")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Trace has %d spans\n", len(trace))

    // Query a time range
    start := time.Now().Add(-1 * time.Hour)
    end := time.Now()
    spans, err := db.RangeQuery(context.Background(), start, end)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Found %d spans in the last hour\n", len(spans))

    // Get aggregated metrics by model
    aggs, err := db.QueryAggregations(context.Background(), 
        cmd.AggregationQuery{
            StartTime:   start,
            EndTime:     end,
            WindowSize:  time.Hour,
            GroupByDims: []string{"model"},
        })
    if err != nil {
        log.Fatal(err)
    }
    for _, agg := range aggs {
        fmt.Printf("Model %s: cost=$%.4f, tokens=%d\n", 
            agg.Model, float64(agg.CostMicroDollars)/1_000_000, agg.TokensIn)
    }
}
```

---

## Core Concepts

### Spans
A span represents a unit of work in a trace. It includes:
- **Hierarchy**: `trace_id`, `span_id`, `parent_span_id` (supports hierarchical traces)
- **Execution**: `timestamp`, `status` (success/error/pending), `latency_ms`
- **Resources**: `agent_id`, `model`, `tool_name`, `team`
- **Costs**: `tokens_in`, `tokens_out`, `cost` (in dollars)
- **Payload**: Opaque blob for application-specific data

### Traces
A trace is a collection of related spans. Traces are hierarchical — each span can have a parent span, forming a tree or DAG. Root spans have empty `parent_span_id`.

### Write-Ahead Log (WAL)
All writes go to the WAL first. The WAL is segmented and fsynced after each append, ensuring durability before acknowledgment. The WAL is also the single source of truth — all other data structures (segments, indexes, rollups) are derived from it and can be rebuilt if necessary.

### Segments
Buffered spans are periodically flushed into immutable row-oriented segments on disk. Each segment includes:
- A header with metadata (segment ID, span count, timestamp range)
- Bloom filters for bounded-cardinality dimensions (agent_id, model, tool_name, team, status) for fast pruning
- Compressed span records

### Index
A Pebble LSM index maps `(trace_id, span_id)` to its location `(segment_id, offset)`. Supports fast point lookups and range scans.

### Compaction
Periodically, small segments are merged into larger ones, and old data is pruned based on retention policies. Compaction never mutates segments in place; it creates new segments and swaps pointers atomically.

### Rollups
Continuous aggregation windows (1 minute, 5 minutes, 1 hour, 1 day) continuously roll up `cost`, `tokens_in`, `tokens_out`, and `latency_ms` grouped by model. Used to answer fast aggregation queries.

---

## API Overview

### Connection
- `cmd.Open(path string) (*DB, error)` — Open or create a database
- `db.Close() error` — Gracefully close the database

### Writes
- `db.InsertSpan(ctx, span) error` — Insert a span (automatically flushed after a timeout or size threshold)
- `db.Flush(ctx) error` — Manually flush buffered spans to disk

### Point Queries
- `db.GetSpan(ctx, traceID, spanID) (span, found, error)` — Retrieve a single span
- `db.GetTrace(ctx, traceID) ([]span, error)` — Retrieve all spans in a trace

### Range Queries
- `db.RangeQuery(ctx, start, end) ([]span, error)` — Retrieve spans within a time range, optionally filtered by dimensions

### Aggregations
- `db.QueryAggregations(ctx, query) ([]aggregation, error)` — Time-windowed aggregations (cost, tokens, latency grouped by model)
- `db.GetDistinctModels(ctx, start, end, limit) ([]string, error)` — Cardinality helper

### Maintenance
- `db.Compact(ctx) error` — Trigger compaction manually
- `db.Expire(ctx, before) error` — Prune spans before a cutoff time
- `db.CheckConsistency(ctx) (report, error)` — Validate index↔segment↔manifest consistency

---

## Current Status

### ✅ What Works
- **Phase 1 (WAL & Crash Recovery)** — Complete. Spans are durable immediately; the WAL survives any crash.
- **Phase 2 (Segment Write & Reconciliation)** — Complete. Segments and index stay in sync; flushed spans are immediately queryable.
- **Phase 3 (Compaction)** — Complete. Segments are merged, old data is pruned, and the index is rebuilt correctly.
- **Phase 4 (Bloom Filters & Zone Maps)** — Complete. Range queries use bloom filters and timestamp ranges to prune candidates.
- **Phase 5 (Rollups & Aggregation)** — Complete. 1m/5m/1h/1d windows continuously aggregate metrics by model.
- **Phase 6 (Query API)** — Partial. Go package API is complete; no HTTP server or OTLP ingestion yet.

### ⚠️ Known Limitations
- **No public delete API** — Tombstones can only be created by passing `Deleted: true` in a span, not by calling a delete function.
- **No HTTP server** — No out-of-the-box HTTP interface; must embed in a Go application.
- **No OTLP ingestion** — No built-in OpenTelemetry Protocol support; you must convert OTLP traces to `SpanPayload` yourself.
- **Rollup dimensions are model-only** — Aggregations group by model; root-attributed rollups are designed but not implemented.
- **Columnar segments not yet implemented** — Segments use row-oriented encoding (not columnar), which was deferred for stability.
- **QueryWithCursor** — Pagination cursor is not stable across concurrent flushes/compaction; use offset-based pagination for now.

### 📈 Performance
- **Point lookup**: ~140 µs (well below the 50 ms target).
- **Parallel insert scales at 0.956x** — fsync-per-append under a mutex. Group commit is planned.
- **GetSpan costs 14.5 KB / 35 allocs** — bloom filters are decoded on every open; a dedicated read path is planned.
- **RangeQuery costs ~132 allocs per returned span** — segment setup overhead exceeds scan cost; coalescing scans is planned.
- See [post-1-unconfirmed-is-not-corrupt.md](docs/blog/post-1-unconfirmed-is-not-corrupt.md) for detailed benchmarks.

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
- [PAD1.md](docs/PAD1.md) — **Frozen v1.3.1 Architecture** (non-negotiable design)
- [PDD1.md](docs/PDD1.md) — **Frozen v1.3 Design Doc** (objectives, competitive analysis, USP)
- [CLAUDE.md](CLAUDE.md) — Implementation status, deviations, known bugs, current state per phase
- [TECHNICAL-GUIDE.md](docs/TECHNICAL-GUIDE.md) — Deletion semantics, consistency model, recovery details

---

## Building & Testing

### Prerequisites
- Go 1.20 or later

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
segment/          — Segment writer/reader, manifest, immutable storage
compactor/        — Segment merge, retention expiry, consistency checking
rollup/           — Continuous aggregation windows (1m/5m/1h/1d)
cmd/              — WispTrace engine: ingestion, queries, API
tests/            — Fault injection and WAL tests
docs/             — Architecture, design, technical guides
```

---

## Roadmap

### Planned (Not Yet Implemented)
- Root-attributed rollups — "total cost of workflows started by Agent X" (designed, not built)
- Columnar segment encoding — higher compression and faster scans
- HTTP server — REST API for remote queries
- OTLP ingestion — native OpenTelemetry Protocol support
- Public delete API — `DeleteSpan` / `DeleteTrace` methods
- Group commit — improve insert throughput
- Dedicated point-lookup read path — reduce allocations for `GetSpan`

### Not In Scope for v1
- Replication or clustering
- SQL or advanced query language
- Schema evolution beyond the WAL version byte
- Cross-process access (single-node only)

---

## License

See LICENSE file.

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
