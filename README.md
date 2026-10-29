# WispTraceDB

**An embeddable trace store for LLM agents, written in pure Go.**

Record every model call, tool call and agent step as a span. Get the full trace back by ID,
filter spans by model, team or status, and answer "what did this cost?" over any time window.
There is no server to run and nothing else to install: it is a Go package that stores its data
in a directory next to your application.

```go
db, _ := wisptrace.Open("./traces")
defer db.Close()

ctx, span := db.StartSpan(ctx, "summarize")
span.SetModel("gpt-4")
span.SetTokens(812, 164)
span.SetCost(0.0213)
span.End(ctx)
```

## Is this for you?

WispTraceDB fits when:

- you run LLM agents in a **Go** service and want their traces stored locally, durably, with no
  separate database or hosted service;
- you care about **cost, tokens, latency and errors** per model, team or agent;
- one machine's disk is enough for your trace volume.

It is not a fit when you need several machines writing to one store, a built-in dashboard or
network API, or full SQL. WispTraceDB is a library: it has no server of its own.

## Install

```
go get github.com/GuhaneshT/WispTraceDB
```

Requires Go 1.23 or newer. No cgo.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	wisptrace "github.com/GuhaneshT/WispTraceDB"
)

func main() {
	ctx := context.Background()

	db, err := wisptrace.Open("./traces")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Record an operation. End stores it durably.
	ctx, span := db.StartSpan(ctx, "answer-question")
	span.SetAgent("support-bot")
	span.SetModel("gpt-4")
	span.SetTeam("platform")
	span.SetTokens(812, 164)
	span.SetCost(0.0213)
	if err := span.End(ctx); err != nil {
		log.Fatal(err)
	}

	// Writes are durable immediately and visible to reads a moment later.
	// Sync waits for that moment.
	if err := db.Sync(ctx); err != nil {
		log.Fatal(err)
	}

	trace, err := db.Trace(ctx, span.TraceID())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("trace %s has %d span(s)\n", span.TraceID(), len(trace))

	costs, err := db.Costs(ctx, wisptrace.ByModel, wisptrace.Last(24*time.Hour))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("cost by model, last 24h:", costs)
}
```

## Recording spans

There are three ways in. All of them validate the span and return only after it is safely on disk.

### 1. Start and end a span (most code should use this)

```go
ctx, agentRun := db.StartSpan(ctx, "agent-run")
defer agentRun.End(context.WithoutCancel(ctx))

ctx, call := db.StartSpan(ctx, "llm-call") // child of agent-run, same trace
call.SetModel("gpt-4")
call.SetTokens(120, 30)
call.SetCost(0.002)
call.SetAttribute("prompt_version", 7)
if err != nil {
	call.SetError(err) // marks the span failed and keeps the message
}
call.End(ctx)
```

- A span started from a context that already holds one becomes its child and joins its trace.
- To join a trace that started in another service, use `wisptrace.WithTraceID(id)` and
  `wisptrace.WithParentSpanID(id)`.
- `End` takes the duration from the clock and marks the span `ok` unless you set a status or an error.
- Pass `End` a context that is still alive. If the operation was cancelled and you pass the same
  context, the span will not be stored. `context.WithoutCancel(ctx)` avoids that.
- `NewTraceID()` and `NewSpanID()` give OpenTelemetry-format IDs if you need to generate your own.

### 2. Hand over finished spans

When you already have complete spans, for example from another system. Mind the start times:
spans older than the retention window (7 days by default) are dropped soon after they are stored.

```go
err := db.Ingest(ctx, wisptrace.Span{
	TraceID: "t1", SpanID: "s1",
	Start:    time.Now(),
	Duration: 245 * time.Millisecond,
	Model:    "gpt-4", Team: "platform", Status: wisptrace.StatusOK,
	TokensIn: 100, TokensOut: 50, CostUSD: 0.0015,
})

res, err := db.IngestBatch(ctx, spans) // invalid spans are reported, not fatal
fmt.Println(res.Accepted, res.Rejected)
```

### 3. OpenTelemetry (OTLP/JSON)

`IngestOTLP` accepts the body an OpenTelemetry exporter sends when configured for `http/json`:

```go
res, err := db.IngestOTLP(ctx, r) // r is an io.Reader over the request body
```

The library has no HTTP server, so you decide how requests reach it. To accept exporters over
HTTP, mount a handler in your own service:

```go
http.HandleFunc("/v1/traces", func(w http.ResponseWriter, r *http.Request) {
	_, err := db.IngestOTLP(r.Context(), r.Body)
	switch {
	case errors.Is(err, wisptrace.ErrInvalidOTLP):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, wisptrace.ErrTooLarge):
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
	case err != nil:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, "{}")
	}
})
```

Point your exporter at it with `OTEL_EXPORTER_OTLP_PROTOCOL=http/json`. Protobuf and gRPC are not
supported. Decompress gzip bodies yourself before calling `IngestOTLP`.

How OTLP fields are stored:

| OTLP | Stored as |
|---|---|
| `traceId`, `spanId`, `parentSpanId` | Trace ID, span ID, parent ID, exactly as sent |
| `startTimeUnixNano`, `endTimeUnixNano` | Start time, and duration (end minus start) |
| `status.code` | `ok`, `error` or `unset` |
| `gen_ai.request.model` / `gen_ai.response.model` | Model |
| `gen_ai.usage.input_tokens` / `output_tokens` | Input and output tokens |
| `gen_ai.agent.id` or `gen_ai.agent.name` | Agent |
| `gen_ai.tool.name` | Tool |
| `wisp.team`, or resource `service.namespace` | Team |
| `wisp.cost_usd` | Cost in US dollars |
| span name, status message, all other attributes | The span's JSON payload |

OpenTelemetry defines no cost or team attribute, so set `wisp.cost_usd` and `wisp.team` yourself
if you want them.

## Reading your data

```go
sp, err := db.Span(ctx, traceID, spanID)  // errors.Is(err, wisptrace.ErrNotFound)
tr, err := db.Trace(ctx, traceID)         // every live span of a trace

// Filter by time and dimensions, 100 per page by default (1000 at most).
q := wisptrace.Query{
	Range:  wisptrace.Last(time.Hour),
	Model:  "gpt-4",
	Status: wisptrace.StatusError,
}
for {
	page, err := db.Query(ctx, q)
	if err != nil { log.Fatal(err) }
	for _, s := range page.Spans { fmt.Println(s.TraceID, s.SpanID) }
	if page.NextCursor == "" { break }
	q.Cursor = page.NextCursor
}

last := wisptrace.Last(24 * time.Hour)

// Totals
costs, err  := db.Costs(ctx, wisptrace.ByTeam, last)        // dollars per team (or ByModel)
tokens, err := db.TokensByModel(ctx, last)                  // in / out / total per model
rate, err   := db.ErrorRate(ctx, "gpt-4", last)             // 0.0 to 1.0; "" means all models

// Rankings and discovery
teams, err  := db.TopTeamsByCost(ctx, 5, last)              // most expensive first
models, err := db.TopModelsBySpanCount(ctx, 5, last)        // most used first
names, err  := db.Distinct(ctx, wisptrace.DimModel, last, 0) // DimTeam, DimAgent too

// Finding things
failed, err := db.FailedSpans(ctx, last, 50)                // spans whose status is a failure
ids, err    := db.Traces(ctx, wisptrace.Query{Range: last, Team: "platform"}) // distinct trace IDs
```

A span counts as failed unless its status is `ok`, `unset` or `success`. OpenTelemetry leaves
successful spans `unset`, so those are not errors.

Results come back oldest first, or newest first with `Query.NewestFirst`; `Trace` returns a trace's
spans in start-time order. Paging evaluates the whole range on every call, so keep the range bounded:
the cost follows the number of spans in the range, not the page size.

Time ranges: `wisptrace.Last(d)`, `wisptrace.Between(from, to)`, or a zero `TimeRange{}` for everything.

### Fast per-model totals (precomputed buckets)

`Costs`, `TokensByModel` and the other totals above read the matching spans, so they slow down as
the window grows. `Aggregates` reads buckets that are updated as spans arrive, so it costs the same
however much data you have. It also gives you latency, which the other totals do not:

```go
buckets, err := db.Aggregates(ctx, wisptrace.WindowHour,
	wisptrace.Between(from.Truncate(time.Hour), to.Truncate(time.Hour)))
for _, b := range buckets {
	fmt.Printf("%s %-8s spans=%d cost=$%.4f in=%d out=%d latency avg=%v min=%v max=%v\n",
		b.Start.Format(time.RFC3339), b.Model, b.Spans, b.CostUSD,
		b.TokensIn, b.TokensOut, b.LatencyAvg, b.LatencyMin, b.LatencyMax)
}
```

Windows are `WindowMinute`, `WindowFiveMinutes`, `WindowHour` and `WindowDay`. Buckets come back
ordered by start time, then model. These are the rules, and they are deliberate:

- **Grouped by model only.** There are no per-team, per-agent or per-tool buckets. Spans recorded
  without a model are grouped under the empty model name `""`. For other dimensions use `Costs`,
  `Query` and the other queries, which read the spans themselves.
- **Whole windows, aligned to the Unix epoch in UTC.** A bucket is returned if its start time is
  inside `[From, To)`. A bucket that begins before `From` is left out even though part of it falls
  inside your range, and one that begins inside the range is returned whole. Truncate your range to
  the window size, as above, to get complete buckets. `wisptrace.Last(24 * time.Hour)` with
  `WindowHour` therefore drops the oldest partial hour and includes the current one.
- **Counted when ingested.** Deleting a span does not remove it from a bucket, and sending the same
  span ID twice counts it twice. The span-reading queries do not have either issue.
- **Cost is kept to a millionth of a dollar per span.** Costs smaller than that round to zero.
- **Latency is in whole milliseconds,** as an average, a minimum and a maximum per bucket.
- **Old buckets are dropped** along with the retention window.
- **Like every read,** a span shows up in buckets once it is visible (see `Sync`).

### Deleting

```go
db.DeleteSpan(ctx, traceID, spanID) // no-op if absent
db.DeleteTrace(ctx, traceID)
```

Deleted spans disappear from every read.

## What you can rely on

**Acknowledged spans survive crashes.** When `Ingest`, `End`, `IngestBatch` or `IngestOTLP` returns
without an error, the span is on disk and will still be there after a power loss, an OOM kill or a
panic. The project's test suite kills a loaded process repeatedly and checks that every acknowledged
span comes back unchanged.

**Visible shortly, not instantly.** A span is durable the moment the call returns, but reads and
aggregates may not see it for a short while afterwards. If you read right after writing, either call
`db.Sync(ctx)` or open the database with `wisptrace.WithReadYourWrites()`.

**Retention defaults to 7 days, judged by the span's start time.** Spans whose start time is older
than the window are removed by a background pass that runs about every 10 minutes, whenever they were
written. That matters if you backfill: spans stamped more than 7 days ago are stored, readable for a
while, then dropped. Removal happens a storage segment at a time, so some older spans can outlive the
cutoff. Change the window with `wisptrace.WithRetention(d)`, and set it above the age of anything you
plan to load.

**Invalid spans are rejected, not stored.** A span needs a non-empty trace ID and span ID. Its start
time must be a real timestamp between the years 2000 and 2100, which catches milliseconds sent where
nanoseconds are expected. Durations, token counts and cost must not be negative, and cost must be a
finite number. ID, model, agent, tool, team and status strings may be at most 256 bytes. A payload
must be valid JSON of at most 64 KiB. The error is a `*ValidationError` naming the field and matches
`wisptrace.ErrInvalidSpan`.

**Sending the same span ID twice replaces the first.** The last write for a `(trace ID, span ID)`
pair wins.

**One process per directory.** A database directory can be open in one process at a time. Close it
before opening it elsewhere.

## Options

```go
db, err := wisptrace.Open("./traces",
	wisptrace.WithRetention(30*24*time.Hour), // default 7 days
	wisptrace.WithFlushThreshold(5000),       // buffered spans before writing a segment; default 1000
	wisptrace.WithSegmentCacheSize(128),      // open segment files kept for lookups; default 64
	wisptrace.WithReadYourWrites(),           // every write waits until it is visible
)
```

## Errors

All are checked with `errors.Is`:

| Error | Meaning |
|---|---|
| `ErrNotFound` | The span or trace does not exist, or was deleted |
| `ErrInvalidSpan` | A span failed validation (details in `*ValidationError`) |
| `ErrInvalidOTLP` | The OTLP request was not valid OTLP/JSON; nothing was stored |
| `ErrTooLarge` | The OTLP request exceeded `MaxOTLPBytes` (16 MiB) |
| `ErrEnded` | `End` was called twice on one span |
| `ErrClosed` | The database was already closed |

Every method takes a `context.Context`. Cancellation is honoured between operations and between
spans in a batch, but a disk sync that has already started cannot be interrupted.

## Checking the database

```go
report, err := db.Check(ctx)
if err == nil && !report.OK() {
	log.Println("integrity problems:", report.Errors)
}
```

`Check` reads everything, so run it when the service is quiet. Close the database with `db.Close()`
so buffered spans are flushed; after an unclean exit, the next `Open` recovers automatically.
`db.Flush(ctx)` writes buffered spans to disk immediately, which is useful before copying the data
directory. It is not needed for durability.

## Know the limits

- **Write speed is bounded by your disk.** Each span waits for a disk sync before it is acknowledged,
  so throughput depends on how fast your storage syncs. Sending a batch does not yet combine those
  syncs, and parallel writers do not speed things up. Faster ingestion is planned.
- **Most aggregates scan the time window you ask for.** `Costs`, `TokensByModel`, `ErrorRate`, the
  `Top*` rankings and `Distinct` read the matching spans, so they get slower as the window grows. They are comfortable for the volumes this
  library targets, not for scanning months of heavy traffic on every request.
- **Paging is positional.** Cursors are positions in a fixed order. New spans arriving after your
  position do not disturb them, but a span inserted, deleted or expired earlier in the order shifts
  later pages by one, so you may see a span twice or miss one. Use a bounded time range and fetch
  the pages promptly.
- **`Aggregates` can over-count.** Its buckets are model-only by design, a deleted span keeps its
  contribution until the bucket expires, and a span ingested twice is counted twice. The queries that
  read spans directly are unaffected.
- **Duration has millisecond precision.** Sub-millisecond durations are truncated.
- **Single machine only.** There is no replication or clustering.
- **Not a server.** The project ships no HTTP server, and OTLP/protobuf and gRPC are not supported.

## How it works, briefly

Spans are appended to a write-ahead log and synced before they are acknowledged. Background workers
batch them into immutable segment files and keep an index so any span or trace is found directly.
Old segments are merged and expired by retention. Everything except the log can be rebuilt from it,
which is why a crash never loses an acknowledged span.

The detailed design, the on-disk format, benchmark results and the list of known issues are in
[docs/](docs/):

- [TECHNICAL-GUIDE.md](docs/TECHNICAL-GUIDE.md): how the engine works, as built
- [PAD1.md](docs/PAD1.md) and [PDD1.md](docs/PDD1.md): architecture and design
- [DEVELOPMENT.md](docs/DEVELOPMENT.md): building, testing, benchmarks, project status and roadmap
- [V1-API-DESIGN.md](docs/V1-API-DESIGN.md): the public API design

## Contributing

Contributions are welcome. Please run `go vet ./...` and `go test ./...` before sending changes,
add tests for new behaviour, and discuss any new dependency first. [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)
explains how to build and test, including the crash-recovery suite.

## License

MIT.
