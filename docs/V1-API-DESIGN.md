# WispTraceDB v1 — Public API & Ingestion Design

Status: **proposed, decisions locked 2026-10-29** (build in progress).
This is a v1 *release* design layered on PAD1 v1.3.2; it does not change the engine's
durability architecture. Anything that does must go through a PAD1 amendment.

## 1. Goals

1. A Go library an application can embed with one import and one `Open` call.
2. OTLP/JSON ingestion built into the library, so any host application can accept
   OpenTelemetry spans through its own transport.
3. One validation path, one type set and one error vocabulary.

## 2. Layout

```
wisptrace/        public facade: Span, Query, errors, Open  (the semver contract)
internal/         wal, segment, pebble, bloom, compactor, rollup, engine (not importable)
```

The engine package currently named `cmd` moves to `internal/engine`. Moving under
`internal/` is only free before v1.0; afterwards every exported symbol is a promise.

## 3. Library API (package `wisptrace`)

```go
db, err := wisptrace.Open("./data", wisptrace.WithRetention(30*24*time.Hour))
defer db.Close()

err = db.Ingest(ctx, wisptrace.Span{ ... })
sp, err  := db.Span(ctx, traceID, spanID)      // errors.Is(err, wisptrace.ErrNotFound)
tr, err  := db.Trace(ctx, traceID)
page, err := db.Query(ctx, wisptrace.Query{Range: wisptrace.Last(time.Hour), Model: "gpt-4"})
costs, err := db.Costs(ctx, wisptrace.ByModel, wisptrace.Last(24*time.Hour))
```

- **Write:** `Ingest`, `IngestBatch` (per-span accepted/rejected), `IngestOTLP`, `DeleteSpan`, `DeleteTrace`.
- **Manual spans:** `StartSpan` -> `*ActiveSpan` (setters, `End`), context-propagated parenting, `NewTraceID`/`NewSpanID`. Built on `Ingest`; no new storage path.
- **Read:** `Span`, `Trace`, `Query` (filter + cursor page), `Traces`, `FailedSpans`, `Costs`, `TokensByModel`, `ErrorRate`, `TopTeamsByCost`, `TopModelsBySpanCount`, `Distinct`; and `Aggregates`, the rollup-backed per-model buckets (1m/5m/1h/1d with count, cost, tokens, latency avg/min/max). Rollups are model-only by design; see the README for bucket-alignment rules.
- **Operate:** `Sync(ctx)`, `Stats()`, `Check(ctx)`, `Close()`.
- **Types:** the package's own `Span` (`time.Time`, `time.Duration`, `json.RawMessage`),
  converted to the engine's `wal.SpanPayload` at the boundary so internals can change freely.
- **Errors:** `ErrNotFound`, `ErrInvalidSpan` (wrapping a field-level `ValidationError`),
  `ErrClosed`, `ErrLocked`, `ErrCorrupt` — all usable with `errors.Is`.
- **Options:** functional options over the existing config knobs. One data directory holds
  `wal.log`, `segments/`, `index/`, `checkpoint.dat`, `manifest.dat`.
- **Context:** honoured on entry and between batch items. The engine cannot abort an fsync
  mid-call; this is documented, not hidden.

### 3.1 Visibility semantics (decided: option A)

`Ingest` returns once the span's WAL record is fsynced: **durable on return, visible shortly.**
Between the ack and the background ingest loop processing the span, reads and aggregates do
not see it; under a flush the gap can reach tens of milliseconds or more.

- `Sync(ctx)` waits until everything enqueued so far has been processed (wraps `WaitForIngest`).
- `WithReadYourWrites()` makes each `Ingest`/`IngestBatch` call `Sync` before returning. It
  serialises concurrent inserts and is opt-in for that reason.
- A per-span completion signal (waiting only for the caller's own span) is a possible
  follow-up and would not change this API.

### 3.2 Validation (new; the engine currently validates nothing)

Enforced in the facade, so the Go API and the HTTP route share it:
non-empty `TraceID`/`SpanID`; timestamp in a sane **nanosecond** range (rejects ms/s);
`Deleted` is never client-settable (deletes only via `DeleteSpan`/`DeleteTrace`);
tokens, cost and latency non-negative, cost finite; the five dimension strings capped at
256 bytes; `Payload` capped at 64 KiB.

### 3.3 Duplicate submissions and rollups (decided: probe, else document)

Storage is idempotent on `(trace_id, span_id)`, but `processSpan` adds to the rollups on every
ingest, so a client retry double-counts cost and tokens. Plan: skip the rollup update when the
key is already indexed/buffered, *if* the insert benchmark shows an acceptable cost; otherwise
document at-least-once counting prominently. Either way the README states which.

## 4. OTLP/JSON ingestion (library only; decided 2026-10-29)

The earlier plan for an HTTP server and a `wisptraced` binary was dropped: v1 ships as an
embeddable library only. OTLP support lives in the library as

```go
func (db *DB) IngestOTLP(ctx context.Context, r io.Reader) (BatchResult, error)
```

- **Input:** an OTLP/JSON `ExportTraceServiceRequest`. int64 fields may be JSON strings or
  numbers; unknown fields are ignored; the request is capped at `MaxOTLPBytes` (16 MiB).
- **Mapping** (documented on the method): hex ids as sent; `startTimeUnixNano` -> `Start`;
  `end - start` -> `Duration`; `status.code` -> `ok|error|unset`; GenAI semantic-convention
  attributes (`gen_ai.request/response.model`, `gen_ai.usage.input/output_tokens`,
  `gen_ai.agent.id/.name`, `gen_ai.tool.name`); `wisp.cost_usd` and `wisp.team` for what the
  conventions lack, with team falling back to resource `service.namespace`; span name, status
  message and unmapped attributes go to `Payload` as JSON.
- **Results:** same guarantees as `Ingest` (durable on return). Per-span rejections carry the
  index in request order across all resource/scope spans, and match `ErrInvalidSpan`. A body
  that is not valid OTLP/JSON returns `ErrInvalidOTLP` and stores nothing; an oversize body
  returns `ErrTooLarge`.
- **Embedding:** the application owns the transport. To accept OpenTelemetry exporters it
  mounts its own handler that calls `IngestOTLP` (a ~20 line `http.Handler`; an optional
  helper in the library is a possible follow-up, not a v1 requirement).
- **Out of scope for v1:** OTLP/protobuf and gRPC, any server, TLS, auth.

Throughput is fsync-bound (one fsync per span); the README states this plainly. WAL group
commit is the planned fix and does not change any API here.

## 5. Build order

1. Move engine packages under `internal/`; all existing tests incl. fault injection stay green.
2. `wisptrace` facade: types, errors, validation, `Open`/`Close`, `Ingest`, `Span`, `Trace`,
   then queries, aggregations, delete, operate.
3. `IngestOTLP` in the library (done).
4. Docs, LICENSE, CI (vet + test + fault injection on Linux and Windows), godoc examples,
   CHANGELOG, release candidate.

## 6. Test plan

OTLP mapping unit tests; one validation test per rule; `IngestOTLP` tests (mapping, rejections, malformed, oversize); facade `Example` tests.
