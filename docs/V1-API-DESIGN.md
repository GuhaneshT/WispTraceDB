# WispTraceDB v1 — Public API & Ingestion Design

Status: **proposed, decisions locked 2026-10-29** (build in progress).
This is a v1 *release* design layered on PAD1 v1.3.2; it does not change the engine's
durability architecture. Anything that does must go through a PAD1 amendment.

## 1. Goals

1. A Go library an application can embed with one import and one `Open` call.
2. A small runnable server (`wisptraced`) that accepts spans over HTTP.
3. One validation path, one type set and one error vocabulary shared by both.

## 2. Layout

```
wisptrace/        public facade: Span, Query, errors, Open  (the semver contract)
server/           HTTP: /v1/spans, /v1/traces (OTLP/JSON), /healthz — calls only wisptrace
cmd/wisptraced/   binary: flags/env -> Open -> server -> graceful Close
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

- **Write:** `Ingest`, `IngestBatch` (per-span accepted/rejected), `DeleteSpan`, `DeleteTrace`.
- **Read:** `Span`, `Trace`, `Query` (filter + cursor page), `Costs`, `Tokens`, `Top`, `Distinct`, `ErrorRate`.
- **Operate:** `Sync(ctx)`, `Stats()`, `Check(ctx)`, `Close()`.
- **Types:** the package's own `Span` (`time.Time`, `time.Duration`, `json.RawMessage`),
  converted to the engine's `wal.SpanPayload` at the boundary so internals can change freely.
- **Errors:** `ErrNotFound`, `ErrInvalidSpan` (wrapping a field-level `ValidationError`),
  `ErrClosed`, `ErrLocked`, `ErrCorrupt` — all usable with `errors.Is`.
- **Options:** functional options over the existing config knobs. One data directory holds
  `wal/`, `segments/`, `index/`, `checkpoint.dat`, `manifest.dat`.
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

## 4. HTTP ingestion (package `server`, built on `wisptrace`)

| Route | Body |
|---|---|
| `POST /v1/spans` | `{"spans":[{trace_id, span_id, parent_span_id, timestamp_ns, agent_id, model, tool_name, team, status, tokens_in, tokens_out, cost_usd, latency_ms, payload}]}` |
| `POST /v1/traces` | OTLP/JSON `ExportTraceServiceRequest` |
| `GET /healthz` | liveness |

- **OTLP mapping** (PAD1 §4a, concretely): hex IDs; `start_time_unix_nano` accepted as JSON
  string or number; `latency_ms = (end-start)/1e6`; `status.code` -> `ok|error|unset`;
  GenAI semantic-convention attributes (`gen_ai.request.model`, `gen_ai.usage.input_tokens`,
  `gen_ai.usage.output_tokens`, `gen_ai.agent.id`, `gen_ai.tool.name`); `wisp.cost_usd` and
  `wisp.team` for what the conventions lack (team falls back to resource `service.namespace`);
  unmapped attributes serialised into `Payload`.
- **Ack:** `200` only after every accepted span is fsynced. Body mirrors OTLP partial success:
  `{accepted, rejected, errors:[{index, reason}]}`. Invalid spans are per-span errors.
- **Limits:** 4 MB body, 1,000 spans/batch, in-flight concurrency cap. `503` + `Retry-After`
  under backpressure; `413`, `401`, `415` as expected; `500` means the WAL append failed
  (never acknowledged).
- **Auth/network:** binds `127.0.0.1` by default; optional bearer token; TLS via reverse proxy.
- **Shutdown:** stop accepting, drain in-flight, then `Close()` (final flush).
- **Out of scope for v1:** OTLP protobuf and gRPC, TLS, query/metrics endpoints.

Throughput is fsync-bound (one fsync per span); the README states this plainly. WAL group
commit is the planned fix and does not change any API here.

## 5. Build order

1. Move engine packages under `internal/`; all existing tests incl. fault injection stay green.
2. `wisptrace` facade: types, errors, validation, `Open`/`Close`, `Ingest`, `Span`, `Trace`,
   then queries, aggregations, delete, operate.
3. `server/` and `cmd/wisptraced`.
4. Docs, LICENSE, CI (vet + test + fault injection on Linux and Windows), godoc examples,
   CHANGELOG, release candidate.

## 6. Test plan

OTLP mapping unit tests; one validation test per rule; `httptest` end-to-end
(POST -> `Sync` -> `Span`/`Trace`/aggregations); kill-after-200 crash test in the
fault-injection style; limit/backpressure tests; facade `Example` tests.
