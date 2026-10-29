# WispTraceDB — Technical Guide

A complete, code-accurate walkthrough of the system as built. Written against the
source, not the design docs — where the two disagree, this file is right and the
disagreement is called out.

Read `docs/PAD1.md` for the target design. Read this for the machine.

---

## 1. What this is

A single-node, embeddable trace store for LLM agent spans. It is a **Go library**,
not a server. There is no `main` package, no HTTP listener, no OTLP receiver, and no
container image. You `import` it into your own binary.

One non-obvious decision drives the whole architecture: **the system keeps its own
segment files on disk *and* uses Pebble as an index over those files.** That is not a
redundant design. It exists because the physical layout of a span record is
something Pebble cannot express, while Pebble is excellent at maintaining a
crash-consistent key→location map.

The cost of that decision is the entire subject of this codebase: when you own both
the index and the data it points at, the index's crash-safety guarantees stop at the
index's own file boundary, and reconciling the two becomes your problem.

---

## 2. The one-paragraph model

> A span arrives. It is appended to a write-ahead log and `fsync`ed; **that fsync is
> the acknowledgement**. Asynchronously the span is folded into a rolling in-memory
> bucket and an in-memory per-trace session buffer. Periodically the session buffer
> is written out as an immutable segment file, and the `(segment_id, byte_offset)` of
> every record in it is committed to Pebble in one atomic batch. A manifest records
> which segment ids are live; a checkpoint records the last segment id for which both
> the file and the index entries are confirmed. Compaction merges old segments into
> new ones and atomically swaps the manifest; retention expiry deletes whole
> segments. On startup, the WAL tail is replayed, anything already indexed is
> skipped, the remainder is flushed, and the manifest is rebuilt from the index.

Everything else is detail.

---

## 3. On-disk layout

```
<db dir>/
  wal.log / wal_000001.log / wal_000002.log ...   append-only, rotated
  wisp_lsm/                                        Pebble LSM (the index)
  segments/
    segment_000001.seg                             immutable data segments
    segment_000002.seg
    rollup_1m.rdb                                 rollup snapshots
    rollup_5m.rdb
    rollup_1h.rdb
    rollup_1d.rdb
  checkpoint.dat                                   8 bytes: uint64 LE
  manifest.dat                                     N × 8 bytes: uint64 LE each
```

Three separate pieces of state track *which segments are live*, and they are
deliberately allowed to disagree:

| State | Written by | Means |
|---|---|---|
| `manifest.dat` | flush **and** compaction | "these segment ids are the queryable set" |
| `checkpoint.dat` | flush **only** | "segment N is confirmed on *both* file and index" |
| directory listing | (derived) | what actually exists on disk |

Keeping these separate is what makes recovery tractable. The manifest can be
rebuilt from the index alone. The directory is authoritative for ID allocation. The
checkpoint is a replay *bound*, not a correctness source of truth.

> Note: rollup snapshots live in `segments/`. This is safe only because every
> directory scan in the codebase matches the strict pattern `segment_<digits>.seg`
> and never a prefix-only match. `computeNextSegmentID` and `listSegmentFiles` both
> re-parse the numeric ID rather than trusting the filename.

---

## 4. The span record

`wal.SpanPayload` — one LLM operation.

| Field | Type | Wire encoding |
|---|---|---|
| `TraceID` | string | uint16 len + bytes |
| `SpanID` | string | uint16 len + bytes |
| `ParentSpanID` | string | uint16 len + bytes; `""` = root |
| `Timestamp` | int64 | 8 bytes, fixed |
| `AgentID` | string | uint16 len + bytes |
| `Model` | string | uint16 len + bytes |
| `ToolName` | string | uint16 len + bytes |
| `Team` | string | uint16 len + bytes |
| `Status` | string | uint16 len + bytes |
| `TokensIn` | int32 | 4 bytes, fixed |
| `TokensOut` | int32 | 4 bytes, fixed |
| `Cost` | float64 | 8 bytes, IEEE754 bits |
| `LatencyMs` | int64 | 8 bytes, fixed |
| `Payload` | []byte | uint32 len + bytes |
| `Deleted` | bool | 1 byte |

Layout: a 33-byte fixed header (timestamp, tokens in/out, cost, latency, deleted) in
that order, then the 8 length-prefixed strings, then the length-prefixed blob.

`ParentSpanID` uses the empty string for a root span rather than a separate null
encoding — this follows the OpenTelemetry convention, so a span can be handed
straight to an OTel-aware consumer without translation.

**`Cost` is `float64` on the wire but must never be summed as a float.** See §9.

The same encoder (`wal.EncodeSpanPayload` / `DecodeSpanPayload`) is used for both
WAL records and segment records. The segment format reuses the WAL payload verbatim
rather than defining a parallel encoding.

### 4a. OTLP mapping (planned)

OTLP (OpenTelemetry Protocol) trace spans map cleanly to `wal.SpanPayload` because
both represent a single operation. A future `otlp` package will translate
`tracepb.Span` into `SpanPayload` without changing the engine's canonical model:

| OTLP field | Maps to | Notes |
|---|---|---|
| `trace_id` (16 bytes) | `TraceID` | Hex/base64 or raw bytes encoded as string in the canonical form used by the engine (16-byte IDs remain valid strings). |
| `span_id` (8 bytes) | `SpanID` | Same as above. |
| `parent_span_id` | `ParentSpanID` | Empty slice → `""` (root), matching OTel convention. |
| `start_time_unix_nano` / `end_time_unix_nano` | `Timestamp`, `LatencyMs` | `Timestamp = start_time_unix_nano` (int64 ns). If both present, `LatencyMs = (end - start)/1_000_000`. If only start is present, latency can come from an attribute. |
| `status.code` / `status.message` | `Status` | Map `STATUS_CODE_OK/ERROR/UNSET` to short strings (`"ok"`, `"error"`, `"unset"` or similar). |
| `attributes` (key-value) | `AgentID`, `Model`, `ToolName`, `Team`, `Status` overrides, `TokensIn/Out`, `Cost`, `LatencyMs` | Use well-known attribute keys (e.g. `llm.agent.id`, `llm.model`, `llm.tool.name`, `team`, `llm.usage.input_tokens`, `llm.usage.output_tokens`, `llm.cost.usd`, `duration_ms`). Unknown attributes may be preserved in `Payload`. |
| `events`, `links`, `resource` | `Payload` | Store a serialized OTLP subset or JSON in `Payload` if needed for fidelity; engine queries don't require them. |
| `flags`, `trace_state` | `Payload` (optional) | Not required by current query dimensions. |

The mapping keeps `Cost` as a float in the wire form but rollups convert to fixed-point micro-dollars; ingestion should pass the numeric cost (if present). Tombstones remain engine-internal (via `Deleted`). This is additive — `InsertSpan` stays the single ingest entry point.

---

## 5. Pillar 1 — Ingest

`WispTrace.InsertSpan(span)` — `cmd/cmd.go:529`. Three steps:

1. **`w.wal.AppendRecord(...)`** — acquires the WAL mutex, rotates if the active
   segment would overflow, appends `version(1) | crc32(4) | length(4) | payload`,
   and calls `file.Sync()`. **This fsync is the durability point.**
2. `ingestWg.Add(1)` under `ingestWgMu` — pairs with `WaitForIngest` (§11).
3. Push the span onto `ingestChan` (buffered, capacity 10 000) and return.

Nothing else is synchronous. Everything downstream is derived state.

> **Why `ingestWgMu` exists.** The Go docs are explicit that a positive `WaitGroup.Add`
> must not race with a `Wait` when the counter could pass through zero in between —
> which is reachable here, because `InsertSpan` can run concurrently with
> `WaitForIngest`. Holding one mutex around both serialises them.
>
> Trade-off, stated in the source: a concurrent `InsertSpan` blocks for the duration
> of an in-progress `WaitForIngest`. Every current caller is a test wanting a
> deterministic drain point, so this is fine — but it would not be fine as a
> production pattern.

`ingestLoop` then calls `processSpan` for each span. `processSpan`
(`cmd/cmd.go:618`) does two things, in this order:

1. **Feeds the rollups** — `w.rollups.Add(ts, model, ScaleCost(cost), tokensIn,
   tokensOut, latency)` — **only when the span is not a tombstone.** This is the
   partial fix for §15 finding 2. A tombstone is skipped here so a span deleted
   before it is counted never enters an aggregate; a span counted and deleted
   *later* still keeps its contribution (the residual gap).
2. **Appends to the session buffer** — an in-memory per-trace `[]pendingSpan` with
   a `lastSeen` timestamp per trace. Each pending span also carries the WAL segment
   id that holds its record (used by WAL reclamation). If the total buffered span
   count crosses `SegmentFlushThreshold`, it triggers a flush inline (suppressed
   during recovery).

### The WAL

`wal/wal.go`. Segmented, rotating at 4 MiB (`DefaultMaxSegmentSize`).

- **Record framing:** `version(1) | crc32(4) | length(4) | payload` = 9-byte header.
  All little-endian.
- **Version byte** is `CurrentWALVersion = 1`. Replay rejects anything else
  explicitly rather than guessing.
- **Torn-tail tolerance applies only to the last segment** (`wal.go:466`). A short
  read or short payload there ends replay cleanly. The same condition on an earlier
  segment is a hard error, because a non-final segment being short means real damage.
- **A CRC mismatch is always fatal** (`wal.go:510`), never tolerated — not even on
  the last segment. The reasoning is in the source: every append is fsynced, so a
  crash truncates the tail rather than rewriting a complete record. A bad checksum is
  therefore corruption, not a torn write.
- **Size cap** of 64 MiB per record, checked before allocating, so a corrupt length
  field cannot cause a huge allocation.
- **Rotation** happens on size, or explicitly via `Rotate()`. Note that
  `writeSegmentBatch` calls `Rotate()` on *every* segment flush, not just on size.
- **Reclamation** is `RemoveSegmentsUpTo(id)`, which refuses to remove the currently
  active segment.
- **Legacy adoption:** a pre-segmentation `wal.log` found with no numbered segments
  is renamed to `wal_000001.log` rather than silently ignored.

Encoding is pooled: `recordBufPool` holds `header+payload` buffers. The buffer is
built *before* taking the mutex, so concurrent callers can encode simultaneously and
the pool gives each its own buffer without a second lock.

---

## 6. Pillar 2 — Flush (buffer → segment)

Two triggers: the 1-second `segmentFlushLoop` ticker flushing traces idle longer than
`SessionTimeout` (default 5 s), or the inline threshold flush from `processSpan`.

Both converge on `writeSegmentBatch(spans []pendingSpan)` — `cmd/cmd.go:711`. **This
is the most important function in the system.** Ordering here is the whole product.

Under `flushMu`:

1. **`w.wal.Rotate()`** — seal the active WAL segment. After this, nothing new
   appends to it.
2. **Collapse the batch to the last write per composite key.** A span inserted and
   then deleted within one flush window arrives as two entries; writing both into
   one segment would leave a superseded live copy beside its own tombstone and give
   the segment two records for one key. Order of first appearance is preserved.
3. **Split into live records and tombstone keys.** A tombstone's only durable
   effect is deleting its key from the index; it never needs a segment record.
4. **Tombstone-only batch:** if there are no live records, write **no segment and
   consume no segment id** — apply the index deletes via `BatchApplySpans(nil,
   tombstoneKeys)`, snapshot rollups, release the WAL records, and return. (A
   segment of pure tombstones would carry no index entries, so `reconcileManifest`
   would drop it on the next open and the file would sit on disk unreferenced.)
5. **Write the segment** — `segment.Writer.Flush`. See §7.
6. **`w.index.BatchApplySpans(entries, tombstoneKeys)`** — the live records'
   `(segment_id, offset)` and the tombstone deletes committed in **one atomic
   Pebble batch**. A span is queryable only once this returns, and a tombstoned key
   never coexists with a live index entry for the same key.
7. **`w.manifest.AddSegment(id)`** — write to `manifest.dat` via tmp + fsync +
   rename.
8. **`w.checkpoint.Save(id)`** — same tmp+fsync+rename discipline.
9. **`w.flushRollups()`** — evict expired buckets and snapshot all four windows.
10. **`w.releaseWALPending(spans)`** — now, and only now, this batch's WAL records
    are redundant. See below.
11. `nextSegmentID++`.
12. `maybeCompactLocked()` — inline, already holding `flushMu`.

### Why rollups are flushed *here* and not only on the background ticker

This is subtle and the source explains it at length. `rollupSnapshotLoop` is a 5-minute
**backstop**, not the guarantee. The real guarantee is that rollups are snapshotted at
the same instant the checkpoint advances. Without that, spans ingested between two
ticker snapshots but already checkpointed into a segment would be **permanently
unrecoverable for rollups**: `recover()` only replays the WAL tail, and by then
`RemoveSegmentsUpTo` has already deleted it.

### WAL reclamation — `walPending` / `releaseWALPending`

WAL reclamation used to be phrased as a *prediction*: "reclaim everything below the
WAL segment that was active when the previous flush drained." That was wrong, and it
caused intermittent loss of acknowledged spans (caught only by the fault-injection
harness). Three ordinary situations break the prediction:

- an acknowledged span still sitting in `ingestChan` when a flush snapshots — the
  record was already fsynced but the flush has no idea it exists;
- a partial flush (`flushExpiredSessions` writes only the expired sessions but still
  advanced the bound);
- a tombstone-only batch, which advances the bound without writing a segment.

The current design stops predicting and starts **counting**. `InsertSpan` calls
`appendTracked` (`cmd/cmd.go:572`), which holds `walPendingMu` across the WAL append
*and* the registration, so no reclaim can run in the window between the fsync and
the count. `walPending[segID]` is the number of acknowledged-but-not-yet-durable
records in each WAL segment. `releaseWALPending` (`cmd/cmd.go:878`) decrements the
counts for the batch just committed, then reclaims everything strictly below the
lowest still-pending segment (or below the active segment when nothing is pending):

```go
lowest := uint64(0)
found := false
for segID := range w.walPending {
    if !found || segID < lowest { lowest, found = segID, true }
}
var bound uint64
if found {
    bound = lowest - 1                 // everything below the oldest unflushed record
} else {
    active := w.wal.CurrentSegment()
    if active <= 1 { /* nothing to reclaim */ }
    bound = active - 1                 // keep the segment being appended to
}
// ... w.wal.RemoveSegmentsUpTo(bound)
```

`flushAllSessions`, `flushExpiredSessions` and `Close` share one drain path, so a
partial flush cannot advance the bound past a span it left buffered. Records replayed
during recovery carry no pending registration (`walSeg == 0`); the single
post-recovery flush reclaims them. A lost count under-reclaims (the WAL grows, which
is boring) rather than over-reclaiming (data loss, silent until the next crash).

---

## 7. The segment format

`segment/writer.go`. **Row-oriented, not columnar.** PAD1 §3 specifies per-column
encoding (delta-of-delta timestamps, dictionary+RLE strings, XOR numerics, zstd);
none of it is implemented, deliberately deferred. Do not describe this format as
columnar.

```
+----------------------------------+
| header (38 bytes)                |
+----------------------------------+
| bloom section (variable)         |   5 filters, fixed order
+----------------------------------+
| record: len(4) | crc32(4) | payload |
| record: len(4) | crc32(4) | payload |
| ...                              |
+----------------------------------+
```

### Header — 38 bytes

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | Magic `0x57545331` ("WTS1") |
| 4 | 2 | Version = 2 |
| 6 | 8 | SegmentID |
| 14 | 4 | SpanCount |
| 18 | 8 | MinTimestamp ← zone map |
| 26 | 8 | MaxTimestamp ← zone map |
| 34 | 4 | BloomSectionLength |

### Bloom section

Five filters, in this fixed order: `agent_id`, `model`, `tool_name`, `team`,
`status`. False-positive rate 0.01. Sizing uses the textbook formulas
`m = -(n·ln p)/(ln 2)²` and `k = (m/n)·ln 2`.

Each filter serialises as `m(4) | k(4) | bits`, so five of them are decodable
back-to-back from one buffer without a length table.

**The hash must be stable across process restarts** — a filter is written once and
read back by a *different process* later. That rules out `hash/maphash`, whose seed
is randomised per process. So `doubleHash` uses two deterministic stdlib FNV
variants (`fnv.New64a` and `fnv.New64`) combined by the Kirsch–Mitzenmacher trick
`h1 + i·h2` to synthesise `k` probes from two hashes.

Bloom properties, as they matter here: false positives are possible (a wasted read),
false negatives are not. A segment is never wrongly *excluded*; it can only be wrongly
*included*.

### Write discipline

`Writer.Flush` does two independent guards:

- **Refuses to overwrite an existing segment file** (`writer.go:90`). Since the open
  is `O_TRUNC`, a stale allocator pointing at a live segment would silently destroy
  it. This converts a data-loss bug into a loud error.
- **Removes its own partial file on failure** via a deferred cleanup keyed on a
  `success` flag.

---

## 8. Pillar 3a — Compaction

`compactor/compactor.go`. `maybeCompactLocked` (`cmd/cmd.go:1009`) fires when the
live segment count exceeds `CompactionSegmentThreshold` (default 10), and merges the
**oldest half** of the live set into one new segment.

### The staleness check — why this is not just an LSM

Pebble holds **exactly one** `(segment_id, offset)` per key. There is no fallback scan
across segments. So before acting on any scanned span, the compactor re-reads the
index and asks: *is this still the current entry for this key?*

```go
current, err := c.index.GetSpan([]byte(key))
if err != nil || current.SegmentID != id || current.Offset != s.Offset {
    staleCount++   // superseded elsewhere (usually by a later tombstone)
    continue
}
```

Both the segment id **and** the offset must match. Without the offset check, a span
that was rewritten into a different position would be treated as live and a
superseded copy could be resurrected.

This one check buys two properties that a classic LSM compactor needs an entire
"major vs minor compaction" classification to get:

- tombstone reclamation is *always* safe, and
- a partial merge (only some segments) is safe, because nothing stale is ever acted
  on.

### Ordering — the whole crash-safety story

1. Write the new segment. If orphaned by a crash, it is inert (not in the manifest).
2. Commit the Pebble batch (reclaim tombstones, re-point survivors). Retryable.
3. Swap the manifest (tmp + fsync + rename).
4. **Only then** unlink the old segment files.

Compaction never mutates a segment in place. Readers pin the manifest set; old files
are unlinked only once nothing references them.

Crash windows, all safe:

| Crash after | Result on restart |
|---|---|
| new segment written | Orphan file, not in manifest → `reconcileManifest` drops it |
| Pebble batch committed | Survivors point at the new segment → reconciliation adopts it |
| manifest swapped | Consistent; old files unlinked next pass |
| some unlinks done | Remaining old files are orphans → dropped by reconciliation |

Note the reclaim batch and the re-point batch are **two separate commits**, not one.
Between them, live spans still point at the *old* segment — which is still on disk,
because unlinking happens later. So reads remain correct throughout.

### `Expire`

Same shape, no merge. `maybeExpireLocked` drops any segment whose header
`MaxTimestamp` is past the retention cutoff (`cmd/cmd.go:1042`), using the zone map
so the decision costs one header read, then reuses the identical staleness +
index-delete + manifest-swap + unlink path. The index entries are deleted *before*
the files are unlinked, which is the only ordering that cannot dangle.

---

## 9. Pillar 3b — Rollups

`rollup/`. Four continuously-maintained windows, all fed in parallel on every span:

| Window | Bucket size |
|---|---|
| `1m` | 60 s in ns |
| `5m` | 300 s in ns |
| `1h` | 3600 s in ns |
| `1d` | 86400 s in ns |

`BucketKey{WindowStart, Model}` — **two dimensions only.** Root-attributed rollups
(PAD1 v1.3) are designed and not built.

`Value` holds `Count`, `SumCost`, `SumTokensIn`, `SumTokensOut`, `SumLatencyMs`,
`MinLatencyMs`, `MaxLatencyMs`.

### The cost-as-fixed-point decision

`span.Cost` is a `float64` dollar amount. A single LLM span is essentially always
well under $1.00. A bare `int64(span.Cost)` therefore **truncates to zero** — a real
bug that was found and fixed.

`CostScale = 1_000_000` (micro-dollars). `ScaleCost` on the way in, `UnscaleCost` on
the way out. Rationale, from the source: millions of incremental additions across a
bucket's lifetime would accumulate floating-point drift, which is the same reason
money is counted in an integer subunit rather than a float.

### Snapshot format

`rollup_<window>.rdb`, in the segment directory. 38-byte header with magic, version,
**window size**, record count, and min/max window start. The reader validates all of
them — including the window size, which prevents loading a `1m` snapshot into a `5m`
store.

Snapshots are **full rewrites**, not deltas: `flushRollups` re-serialises every
bucket each time.

### The unaligned-window gap

`Store.GetBucketInRange` filters `WindowStart >= start && WindowStart < end`
(`rollup/rollup.go:109`). For a query window that is not itself window-aligned, the
boundary buckets fail the comparison and are **silently dropped**. A query for
`[10:00:30, 10:02:30)` returns nothing at all, because the 10:00 bucket starts before
the range. The columnar-scan fallback that would fix this is designed and not built.

---

## 10. Query paths

### `GetSpan(traceID, spanID)` — point

1. `bufferedSpan` checks the in-memory session buffer first. It has a three-valued
   return so "a tombstone is buffered" is distinct from "the buffer has nothing",
   and it scans backwards so the last write for a span id wins.
2. `index.GetSpan(CompositeKey(traceID, spanID))` → `(segment_id, offset)`.
3. `acquireSegment(segment_id)` returns a reader — from the segment reader cache
   when configured, else a direct `OpenReaderLight`.
4. `reader.ReadAt(offset)`, then a `Deleted` re-check as defence in depth for
   pre-fix segments.

`OpenReaderLight` reads the 38-byte header and **does not decode the bloom section**
(a point lookup has an exact offset and consults no filter). Together with the reader
cache, this is what took `GetSpan` from 14 547 B / 35 allocs to **1 581 B / 22
allocs** (`docs/benchmarks/post-p1-20261003.txt`). A tombstoned key is deleted from
the index at flush time, so `GetSpan` reports it as not found — see §15, finding 1.

### `GetTrace(traceID)` — full trace

1. Snapshots the session buffer as `key → last write` (`bufferedSpansByKey`).
2. `index.PrefixScan(traceID + "||")` — a bounded Pebble iteration using
   `LowerBound`/`UpperBound` (the upper bound is computed by incrementing the last
   non-`0xFF` byte).
3. Opens each referenced segment **once** into a per-query reader map, through
   `acquireSegment` (cached / light open).
4. Reads each location, skipping any key a buffered write supersedes and any record
   flagged `Deleted` (defence in depth).
5. Appends the buffered live spans.

A trace consisting only of deleted spans reports not found. See §15, finding 1.

### `RangeQuery(filter)` — scan

It snapshots the session buffer first (`bufferedSpansByKey("")`), then for each
segment in the manifest:

1. `OpenReader` (decodes all five blooms). `RangeQuery` deliberately does **not**
   use the reader cache or the light open — it needs the bloom section and
   seek-based `ScanAll`, neither safe on a shared reader.
2. **Zone-map prune:** skip if `MaxTimestamp < StartTS` or `MinTimestamp > EndTS`.
3. **Bloom prune:** `mayMatchBlooms` — if any specified dimension is definitely
   absent, skip.
4. `ScanAll` — sequential read from just past header+blooms to EOF, CRC-checking
   every record.
5. Per record: skip `Deleted`, skip any key a buffered write supersedes, apply the
   cheap in-memory predicate, then run the **staleness check** — `index.GetSpan(key)`
   must resolve to exactly this `(segment_id, offset)` — before emitting.
6. Close the reader.

Then merge in matching in-memory spans.

Step 5 is the correctness price of a segment-walking query: a span deleted *after*
its segment was written is still physically present in that segment, and the deletion
exists only in the index (§15, finding 2b). Both prunings happen *after* the open, so
the zone map saves the scan but not the bloom decode; at the current scale the setup
cost exceeds the scan.

### The derived queries

Everything else is a thin wrapper over `RangeQuery`: `QueryByModel`, `QueryByTeam`,
`QueryByStatus`, `QueryByAgentID`, `QueryByToolName`, `ListTraces`,
`QueryWithCursor`, `GetCostByModel`, `GetCostByTeam`, `GetTokensByModel`,
`GetTopTeamsByCost`, `GetTopModelsBySpanCount`, `GetFailedSpans`, `GetErrorRate`,
`GetDistinctModels`, `GetDistinctTeams`, `GetDistinctAgents`.

Note that `GetCostByModel` and friends **re-scan and re-decode every matching span**
rather than reading the rollups. They are correct, and they are O(spans in range).
The rollups exist for cheap aggregation; these functions just don't use them.

`RangeQuery` walks the manifest for its candidate records, but it now issues **one
Pebble `GetSpan` per surviving candidate** for the staleness check — so the old
"never touches Pebble" description of this path no longer holds. Bloom and zone-map
pruning narrow the candidate set; they say nothing about liveness.

---

## 11. Recovery

`recover()` — `cmd/cmd.go:369`. Runs during `CreateWispTraceWithConfig`, before the
background loops start.

### Two-phase drain

```go
w.recovering = true
for _, record := range records {
    if w.spanAlreadyIndexed(record.Span) { continue }
    w.processSpan(pendingSpan{span: record.Span})
}
w.recovering = false
if len(records) > 0 { w.flushAllSessions() }
```

**Why one flush at the end instead of flushing as it goes.** Inline threshold flushes
are suppressed while `recovering` is true, so no WAL segment is rotated or reclaimed
until the whole replay is in memory. An inline flush that reclaimed the rotated tail
would lose the remaining replayed spans if this recovery itself crashed partway.

### Idempotence: `spanAlreadyIndexed`

A crash inside `writeSegmentBatch` can leave spans both durable-and-indexed *and*
still present in the WAL (not yet reclaimed past the checkpoint). Re-processing them
would duplicate the data.

`spanAlreadyIndexed` resolves the span's index entry, opens that segment, reads the
record, and compares with `spansEqual` — a full field-by-field comparison including
the `Deleted` flag, timestamp, and payload. Only an exact match counts as confirmed.

This is a read-based idempotence check, which the source notes "feels wrong, isn't
optional" — it costs an index lookup and a segment read per replayed record.

### `reconcileManifest`

The manifest is derived state. It is rebuilt to be **exactly** the set of segment ids
the index references. This resolves the one window where they split: a crash between a
compactor's index batch and its manifest swap. Rebuilding from the index means
recovery cannot resurrect superseded segments.

It also *fails loudly* if the index references a segment file that does not exist
(`cmd/cmd.go:493`) — that is real data loss, not a reconciliation problem.

---

## 12. Consistency checking

`CheckConsistency()` → `ConsistencyReport{Errors, Warnings}`. Errors compromise
correctness; warnings are benign-but-noticeable.

Checks, in both directions:

1. **Manifest → files:** every listed segment exists, parses, and its header
   `SegmentID` matches its filename. Every record decodes and CRC-checks. Scanned
   count matches the header's `SpanCount`.
2. **Files → manifest:** unlisted segment files are reported as **warnings** (crash
   residue — the allocator and recovery already skip them).
3. **Index → segments:** every index entry resolves through a readable segment to a
   record whose composite key matches the key it is filed under.
4. **Segments → index:** a live record's index entry, when present, must point at
   exactly its own `(segment_id, offset)` — otherwise an **error**. A live record with
   *no* index entry is a **warning**, not an error, and the reason is not cosmetic: it is
   the normal state of every span deleted after its segment was written, and it is
   indistinguishable from a lost index write. Tombstoned records may be present or
   reclaimed; if a tombstone is still indexed that is a **warning** (only possible for
   segments predating delete-at-flush, and the shape that let `GetSpan` return deleted
   spans). See Finding 2c in §15 for the full consequence of that downgrade.
5. **Checkpoint sanity:** never above the highest segment id on disk.

---

## 13. Configuration

`DefaultWispTraceConfig()` — `cmd/cmd.go:150`. Every zero value is replaced by its
default, so partial config structs are safe.

| Field | Default | Meaning |
|---|---|---|
| `WALPath` | `wal.log` | base path; segments become `wal_NNNNNN.log` |
| `WALMaxSegmentSize` | 4 MiB | rotation threshold |
| `PebblePath` | `wisp_lsm` | index directory |
| `SegmentDir` | `segments` | segments + rollup snapshots |
| `CheckpointPath` | `checkpoint.dat` | |
| `ManifestPath` | `manifest.dat` | |
| `SegmentFlushThreshold` | 1000 | buffered spans that trigger an inline flush |
| `CompactionSegmentThreshold` | 10 | live segments that trigger a merge of the oldest half |
| `RollupSnapshotInterval` | 5 min | background backstop only |
| `CompactionInterval` | 10 min | background compaction + expiry tick |
| `RetentionPeriod` | 7 days | drives both segment expiry and rollup eviction |
| `SessionTimeout` | 5 s | idle trace age that triggers a flush |
| `SegmentCacheSize` | 64 | max open segment readers kept across reads; `0` → default, negative disables the cache |

`MaxCardinalityPerQuery = 10000` is a package constant, not configurable.

`SegmentCacheSize` backs `segment.Cache` (`segment/cache.go`): a bounded, refcounted
set of open readers keyed by segment id, used by the exact-offset read paths
(`GetSpan`, `GetTrace`, recovery's `spanAlreadyIndexed`). Compaction and expiry call
`Purge(ids)` before unlinking — on Windows `os.Remove` fails while any handle is open
— which is why a purge returning nil is the guarantee that unlink will succeed. The
cache holds no persisted state; a crash simply ends the process and the OS closes the
handles.

---

## 14. Performance: what the numbers actually mean

`benchmark.txt`, one real run, 26 Sep 2026. Windows/amd64, i7-1250U, 12 threads,
`-benchtime 2s -benchmem`.

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `InsertSpan_Sequential` | 373 246 | 2 394 | 6 |
| `InsertSpan_Parallel` | 356 699 | 1 986 | 5 |
| `GetSpan` | 140 070 | 14 547 | 35 |
| `GetTrace` | 233 838 | 19 750 | 147 |
| `RangeQuery` | 2 518 068 | 743 389 | 13 228 |

Both committed SLOs (PAD1 §11) pass with large headroom: trace lookup 0.14 ms point /
0.23 ms trace against a 50 ms p99 target.

The absolute numbers are laptop-and-Windows bound and are not product
characteristics. **The ratios are the findings.**

> **Every row in this table predates the deletion fix and none of them describe the current
> code.** `RangeQuery` and `GetTrace` now consult the session buffer and re-verify
> candidates against the Pebble index (Finding 2b); `GetSpan` does too. The
> `GetTrace`/`RangeQuery` figures in particular are optimistic, and the cost of the new
> per-candidate index lookups is unmeasured. Re-run `cmd/benchmark_test.go` before quoting
> any of these numbers — they are retained below for the shape of each problem, not as
> current evidence.

### Recorded read-path numbers after the P1/P1b/cache fixes

The three exact-offset callers use `OpenReaderLight` and, since Oct 2026, the
`segment.Cache` with its pooled `*segment.Release`, each measured before/after. Bytes
are the reliable signal; latency on this laptop is noise-bound in both directions.

| Path | Before (light open) | After (cache) | Source |
|---|---|---|---|
| `GetSpan` | 1,581 B / 22 allocs | **386 B / 14 allocs** | `post-cache-20261015.txt` |
| `GetSpan`, post-compaction | 1,644 B / 22 allocs | **375 B / 14 allocs** | `post-cache-20261015.txt` |
| `GetTrace` | 6,818 B / 135 allocs | **5,711 B / 128 allocs** | `post-cache-20261015.txt` |
| `spanAlreadyIndexed` | 1,615 B / 21 allocs | **365 B / 13 allocs** | `post-cache-20261015.txt` |
| `RangeQueryWithCompaction` | 3,043,562 B / 70,170 allocs | **1,195,122 B / 60,270 allocs** | `post-cache-20261015.txt` (streaming `ScanAllFunc`) |

> **The cache A/B is unconfirmed.** The intended no-cache control
> (`WISPTRACE_BENCH_NO_CACHE=1`) produced byte-identical B/op and allocs, so the env
> var never reached the test binary and the direct-open baseline for these numbers is
> missing. The deltas above are real (same host, same benchmark) but the attribution to
> the cache specifically is inferred, not measured.

Compaction is now benchmarked (`RangeQueryWithCompaction`, `GetSpanWithCompaction`), but
`RangeQuery` itself still carries the per-candidate staleness lookup and the per-query
open/bloom-decode setup, and did not complete inside the suite's default 10m timeout
— run it separately.

### Parallel insert does not scale (~0.74x)

Latest run: median 1.21 ms/op parallel vs 0.89 ms/op sequential — 12 threads
delivering *less* than one. The earlier 0.956x figure is superseded; the two runs'
ranges overlap, so the honest statement is "no scaling, plausibly slight
contention", not a precise ratio. Either way it is not "modest scaling" — it is
none.

The cause is exactly the designed behaviour: `InsertSpan` fsyncs the WAL before
acknowledging, under a mutex. The ack *is* the fsync, so throughput is capped at disk
sync latency and concurrency cannot move it. This is the durability contract working
correctly.

The fix is **group commit**: batch concurrent appends into one fsync, ack them all
when it returns. This is safe here specifically because the system never promised
cross-writer ordering — appends are already fully serialised by the mutex, so
batching preserves every ordering guarantee that was ever made. The same design
decision that made compaction's partial merge safe makes group commit trivially safe.

### `GetSpan` costs 14 547 B to read a ~200-byte record

`segment.OpenReader` decodes all five bloom filters into a `map[string]*Filter` on
every open (`segment/reader.go:33`). A point lookup has an exact `(segment_id,
offset)` and consults none of them.

**The suggested fix in `pers.txt` was an fd LRU cache. That treats the symptom.** The
file handle is maybe 10% of the cost; the bloom decode is the rest. An fd cache would
happily cache a bloom section that should never have been decoded. The real fix is a
**missing entry point**: open the segment and read one record, without materialising
pruning metadata.

### `RangeQuery` on merged segments — **now measured** (`post-cache-20261015.txt`)

`BenchmarkRangeQueryWithCompaction` (15,000 spans, merged segments, a 1000-ts window
returning ~101 spans) costs **~1.20 MB and ~60,270 allocs per query** after the
filter-before-staleness reorder and the streaming `ScanAllFunc`
(`post-cache-20261015.txt`; it was ~3.04 MB / 70,170 allocs in `compaction-20261003.txt`).
Cost is not per returned span — it is per *scanned* record: the ~60k allocs ÷ ~5,000
scanned records is the decode cost; the ~101 that match are a rounding error.

The older "≈132 allocs per returned span" figure is superseded. Two follow-ups remain:

- **the index re-verification** is per-*candidate*. The filter-before-staleness reorder
  is applied (`cmd.RangeQuery` runs `filter.matches` before `index.GetSpan`), which
  removed roughly one Pebble point lookup per *scanned* record. The deeper fix, one
  Pebble snapshot iterator with `SeekGE` per key instead of N independent `Get`s, is
  still not attempted: new API on the index wrapper.
- **record decode** is now the dominant term. `segment.ScanAllFunc` streams records to a
  callback instead of building a `[]ScannedSpan` of everything (done), but each scanned
  record is still fully decoded before the filter runs; decoding only the fields the
  filter needs is not done.

`RangeQuery` (uncompacted) did not complete inside the benchmark suite's default 10m
timeout — the suite is dominated by fsync-bound preload inserts, so run that benchmark
on its own with `-timeout`.

**The bloom-pruning observation from the original analysis still stands:** to return
~100 spans it opens ~10 files and decodes ~50 bloom filters, and both prunings run
*after* `OpenReader`, so the zone map cannot save the bloom decode. At this scale the
bloom pruning does not obviously earn its keep.

### The instructive bug in this defect

`GetTrace` solves exactly this problem, in the same file, correctly: it builds a
per-segment reader map so it opens each file once per query rather than once per
span. The pattern was written, understood, and tested — one door over from the
function still doing it the naive way.

A shared helper called by two callers with different needs will, given enough time,
do the union of their work for both. The expensive caller keeps paying; the cheap
caller starts paying too, one callee at a time, and nobody notices because the shared
function cannot be obviously wrong.

### Compaction — now measured

`BenchmarkRangeQueryWithCompaction` and `BenchmarkGetSpanWithCompaction` exercise a
post-merge segment set (see `docs/benchmarks/compaction-20261003.txt` and
`post-cache-20261015.txt`). The staleness check's cost was the ~4,900-alloc index-lookup
term removed by the filter-before-staleness reorder; what remains is `ScanAll`
materialisation, above. What is still missing is a *compaction* timing (CPU/I-O/write
amplification of the merge itself), which no benchmark currently records.

---

## 15. Known bugs

Found by reading the source on 30 Sep 2026. Three of them were correctness bugs rather than
performance ones, and **two have since been fixed** — see the resolution notes below. The
third (2c) is a direct consequence of fixing the other two and remains open. Findings 3 and 4
have since been fixed too. Findings 5, 6 and 7 remain open.

### Finding 1 — deleted spans are readable via `GetSpan` and `GetTrace` — **FIXED**

**Severity: high. This was a data-deletion correctness bug.**

> **Resolution.** A tombstoned key is now **deleted from the index** at flush time rather
> than repointed at the tombstone record, and no tombstone record is written to a segment
> at all. `writeSegmentBatch` collapses the batch to the last write per key, then applies
> live puts and tombstone deletes in one atomic `index.BatchApplySpans`. A tombstone-only
> batch writes no segment and consumes no segment id. `GetSpan` now resolves through a
> `bufferedSpan` helper that distinguishes "tombstone buffered" from "nothing buffered" and
> scans the buffer backwards for the authoritative write; both it and `GetTrace` still
> filter `Deleted` on the segment path as defence in depth for pre-fix segments. The two
> fault-injection assertions now require `found == false`.

Why *delete* rather than point the index at the tombstone: both put the record one `ReadAt`
from being returned, but only delete makes it structurally unreachable. Indexing the
tombstone converts a storage invariant into a policy that each of three independent read
paths must remember — which is precisely the convention that failed here, and the reason
`RangeQuery` was correct while `GetSpan` was half-correct and `GetTrace` was not.

A side effect worth recording: a tombstone-only batch must **not** write a segment. A
segment of pure tombstones would carry no index entries, so `reconcileManifest` — which
derives the manifest from `ScanAll` — would drop it on the next open, leaving a file on
disk that no manifest references and compaction therefore never reclaims.

**Original analysis, for the record.**

A tombstone was *not* deleted from the index at flush time. `writeSegmentBatch` sent
**every** span — including `Deleted: true` — through `BatchPutSpans`. There was no
`DeleteSpan` call anywhere in package `cmd`. The index entry was only removed later, by
`compactor.Compact`, which adds tombstones to `reclaimKeys` and calls `BatchDeleteSpans`.

So a tombstoned span remained indexed, and pointing at a real record, until compaction
reclaimed it. During that window:

- `GetSpan` filtered `Deleted` **only on the session-buffer path**. The segment path
  returned whatever `ReadAt` produced with no `Deleted` check → `found = true` for a
  deleted span.
- `GetTrace` had **no `Deleted` filter on either path** — not the `PrefixScan` path, not
  the session-buffer path.
- `RangeQuery` did filter correctly.

So the read APIs disagree with each other about whether a deleted span exists. If a
span is deleted for retention or compliance reasons, `GetSpan` and `GetTrace` still
return its full contents.

**Why the fault-injection test could not see this.** The assertion was:

```go
if found && !span.Deleted {
    t.Fatalf("tombstoned span %d came back live", seq)
}
```

That only fails if a tombstone comes back **live**. Returning the tombstone with
`Deleted: true` satisfies it. The exactly-once check further down uses `RangeQuery`,
which filters correctly, so it passed too. And `CheckConsistency` explicitly treated a
tombstone that *is* indexed as acceptable (warning-only).

**The `if found &&` pattern was the actual bug** — the test was written to tolerate
exactly the behaviour it should have rejected. Both sites now assert `found == false`, and
the crash test additionally asserts `GetTrace` finds nothing.

### Finding 2 — deleted spans are still counted in rollups — **PARTIALLY FIXED**

**Severity: high, same root cause.**

`processSpan` called `w.rollups.Add(...)` unconditionally, before anything looked at
`Deleted`. And `Store.Add` (`rollup/rollup.go:35`) is purely additive — `Count++`,
`SumCost += cost`. There is no decrement path anywhere in the rollup package.

> **Resolution, partial.** `processSpan` now skips `rollups.Add` for tombstones, so a span
> tombstoned before it is counted never enters any aggregate.
>
> **Still open:** a span counted and deleted *later* keeps its contribution. Exact reversal
> of `Count`/`SumCost`/`SumTokens` is mechanically possible — the rollup key
> `(WindowStart, Model)` is derivable from the tombstone record — but
> `MinLatencyMs`/`MaxLatencyMs` cannot be un-minimised without the full per-bucket latency
> distribution, which the store does not keep. Exact reversal needs either a non-invertible
> aggregate or per-span latency retained. This is a design decision, not a typo, and is
> tracked rather than approximated.

**Original analysis, for the record.**

So a deleted span's cost, token counts, and latency remained in every aggregate,
permanently. Combined with finding 1, a deleted span was both *readable* and *counted*.
For a product whose headline feature is cost accounting, that is the worst pair of defects
to ship.

This one is architectural rather than a typo: fully fixing it means a tombstone must
subtract from its rollup bucket, which requires the rollup key to be derivable from the
tombstone record. It is — `(WindowStart, Model)` — so a compensating decrement is
mechanically possible. But `MinLatencyMs`/`MaxLatencyMs` cannot be un-minimised without
the full per-bucket distribution, so exact reversal needs either a non-invertible
aggregate or per-span latency retained. Worth a design decision before an implementation.

### Finding 2b — `RangeQuery` returned deleted spans too — **FIXED**

**Severity: high. Same class as finding 1, and pre-existing.**

`RangeQuery` walks the manifest's segments and filters on `Deleted` per record. It never
consults the index — that is the point of the path ("range/filter query path never touches
Pebble"). But a span deleted *after* its segment was written is still physically present in
that segment, and the deletion lives only in the index as a negative fact. A segment walk
cannot see a negative. Bloom and zone-map pruning narrow the candidate set but say nothing
about liveness, so the deleted span was returned for as long as its segment survived.

There was a second instance in the same function: the session-buffer loop filtered tombstones
but did not collapse duplicate keys, so a live copy immediately followed by a tombstone for
the same span id yielded the live copy.

> **Resolution.** Every candidate is re-verified against the index and emitted only if the
> index points at exactly that `(segment_id, offset)` — the compactor's staleness check, and
> invariant 5. The session buffer is snapshotted first and takes precedence over any segment
> copy of the same key, since a buffered write is strictly newer (un-flushed); that also
> collapses duplicate keys to the last write.
>
> **This is not free.** It adds one index lookup per candidate, on a path already measured at
> ~132 allocs/span. The correct optimisation is a single Pebble snapshot iterator with
> `SeekGE` per key instead of N independent `Get`s, and it is deliberately not attempted
> here — it is new API surface on the index wrapper, and shipping an unmeasured version of it
> would replace a known cost with an unknown one.

This is worth recording as a lesson rather than a footnote: **the deleted-span audit has now
found the same class of bug in all three read paths.** Two were covered by the original tests.
The one the tests missed, `RangeQuery`, is the path whose whole design premise was to avoid
touching the index — which is exactly why it was never re-examined.

### Finding 2c — `CheckConsistency` can no longer detect a lost index write — **NOT FIXED (accepted trade-off)**

**Severity: medium. It is a direct consequence of fixing 2a/2b, not an independent mistake.**

The reverse direction of the consistency check was: *every live record in a manifest segment
is indexed at its exact `(segment_id, offset)`*. A violation meant a lost index write — a span
acknowledged and written to disk but not durably indexed — which is data loss, so it was an
**error**, pinned by `TestCheckConsistencyDetectsMissingIndexEntry`.

That check is now unsound, for a structural reason rather than a coding mistake:

| state | segment | index | interpretation |
|---|---|---|---|
| post-tombstone | live record | **absent** | expected, correct, invisible to all reads |
| lost index write | live record | **absent** | data loss |

The two are identical from the segment layer and the index. There is no discriminator,
because the deletion rule *is* "remove the index entry" — the index was the single place that
recorded "this key was deleted on purpose", and deleting the entry erases that record. This
was found by `TestTombstoneInLaterFlushRemovesIndexEntry`, which failed on
`assertConsistent` rather than on the property it was written to check.

Keeping it an **error** was rejected: with delete-at-flush, **every** deleted span in a healthy
database produces this shape until compaction reclaims the space, so the checker would fire on
every deletion. A diagnostic that cries wolf on correct behaviour is worse than one that misses
a rare fault. It is now a warning, and its message names both possibilities so the output is
not misleading either way. The test that pinned the old behaviour is renamed
`TestCheckConsistencyReportsLiveRecordWithNoIndexEntry` and asserts the downgrade, with the
reasoning in its doc comment so the next reader does not "fix" it back.

The dangerous direction is untouched and still a hard **error**: an index entry that resolves
to nothing readable is a dangling pointer, and that is unambiguously corruption
(`TestCheckConsistencyDetectsIndexPointingAtMissingSegment`).

**Restoring the check** requires a separate durable record of intent. The clean design is a
second keyspace in the index — a *deleted-keys* set — written atomically in the same batch as
the tombstone's index delete, and cleared by compaction when it reclaims the segment:

- absent from index, present in deleted-set → expected (warning: reclaimable)
- absent from both → **lost index write** (error)

Read paths need nothing from it; only the checker does. It was **not** implemented here. It is
new persisted state whose own retention needs a policy, it adds a write to every deletion, and
shipping it without its own compaction and recovery tests would trade a documented,
understood gap for an undocumented one.

> **Design lesson worth keeping.** The cheapest-looking fix — delete the index entry — makes
> all three read paths correct with no new state, and then quietly removes the system's
> ability to detect one specific class of corruption. Correctness and diagnosability are
> separate properties, and a change can be unambiguously right on the first and pay on the
> second. Whenever the repair works by removing a record, ask what was using that record as
> evidence.

### Finding 3 — there is no public delete API — **FIXED**

**Severity: medium — the deletion fix worked but was unreachable.**

`WispTrace` exposed no `DeleteSpan` or `DeleteTrace`. A tombstone could only be created
by a caller constructing `wal.SpanPayload{Deleted: true}` and passing it to
`InsertSpan` — which is exactly what the tests did. The product could not express
"delete this span" through its own API surface.

> **Resolution.** `DeleteSpan(traceID, spanID)` and `DeleteTrace(traceID)` now exist on
> `WispTrace` and write tombstones through `InsertSpan`, the same path the tests already
> exercised. `DeleteSpan` rejects empty identifiers and is idempotent (an absent span is
> a no-op). `DeleteTrace` enumerates the trace's live spans via `GetTrace` — covering
> both buffered and segment-resident spans — after draining the ingest pipeline with
> `WaitForIngest`, so a span acknowledged just before the call cannot slip through as a
> live survivor; a concurrent insert landing after that drain is not covered, by design
> (a delete is a point-in-time operation, not a lock on the trace id). Pinned by
> `TestDeleteSpanRemovesSpanFromAllReadPaths`, `TestDeleteTraceRemovesOnlyThatTrace`,
> `TestDeleteEmptyIdentifiersError`, `TestDeleteNonExistentIsNoop` and
> `TestDeleteSpanDurableAcrossCrash` (`cmd/tombstone_test.go`).

### Finding 4 — cardinality cap fires before the caller's `limit` — **FIXED**

**Severity: medium. A caller's `limit` was ignored.**

`GetDistinctModels(startTS, endTS, limit)` returned an **error** when the scan reached
`MaxCardinalityPerQuery` distinct values — before the caller's `limit` was applied. So
`GetDistinctModels(ts, end, 5)` against a database with 10 000+ distinct models errored
instead of returning 5 names. Same in `GetDistinctTeams` and `GetDistinctAgents`.

> **Resolution.** A positive `limit` now short-circuits the scan. `cardinalityStop`
> (in `cmd/cmd.go`) returns the number of distinct values to collect before stopping and
> whether reaching it is an error: a positive `limit` at or below the cap stops at
> `limit` and is never an error, so the cap can no longer pre-empt it. With no limit
> (or a limit above the cap) the `MaxCardinalityPerQuery` error is preserved — the
> documented hard cap for unbounded queries. Fixed in all three `GetDistinct*`
> functions. Pinned by `TestCardinalityStopHonoursLimitBeforeCap` and
> `TestGetDistinctModelsPositiveLimitShortCircuits` (`cmd/cmd_test.go`); the latter
> would need 10 001 distinct inserts to exercise the cap branch end-to-end, so the
> decision function is pinned directly instead.

### Finding 5 — `QueryWithCursor` re-scans and can skip or duplicate

`QueryWithCursor` (`cmd/cmd.go:1412`) calls the full `RangeQuery` and then slices by
numeric offset. Two consequences:

- **Cost is O(total) per page**, not O(page).
- The cursor is a numeric offset into a freshly re-computed result set whose
  composition changes whenever a segment flush or compaction occurs between pages.
  Segments are appended and merged concurrently with pagination, so a client paging
  through results can skip spans or see them twice. It is not a stable snapshot.

Not a crash-safety issue, but the cursor is not a stable snapshot and should not be
documented as one.

### Finding 6 — `maybeCompactLocked` burns a segment ID before its recovery guard

`cmd/cmd.go:1022-1028` increments `nextSegmentID` and *then* checks `w.recovering` and
returns. Harmless — IDs are never reused, so burning one is safe — but the guard is
in the wrong place.

### Finding 7 — the recovery guard does not cover the final recovery flush

`w.recovering` is set to `false` before `flushAllSessions()` runs
(`cmd/cmd.go:394-398`). The comment states that recovery "must not reclaim any WAL
segment along the way," yet the final flush runs with the guard already down and can
therefore trigger inline compaction. This is safe in practice — the `releaseWALPending`
discipline is independent of the `recovering` flag, and compaction during the final
flush is correct — but the comment and the code disagree about what the flag protects.

---

## 16. The invariants

These are the rules that make the system correct. If you change code, check each one.

1. **The WAL is the single source of truth.** Any state that cannot be rebuilt by
   replaying the WAL is a design flaw. The manifest is rebuildable from the index; the
   index is rebuildable from the WAL; segments are rebuildable from the WAL. The
   checkpoint is a replay bound, not truth.
2. **Segment IDs are never reused.** Enforced by `Writer.Flush` refusing to overwrite,
   and by `computeNextSegmentID` seeding from `max(checkpoint, manifest, on-disk)`.
3. **The ack is the fsync.** Nothing may be acknowledged before `AppendRecord` has
   returned, which is after `file.Sync()`.
4. **Segments are immutable.** Compaction writes new files and swaps pointers. Nothing
   is ever mutated in place.
5. **The index entry is the authority on liveness.** Exactly one `(segment_id, offset)`
   per key. Any code that wants to act on a scanned span must re-verify it against the
   index first — the staleness check.
6. **Index changes are committed before files are unlinked.** Never the reverse, or you
   have dangling pointers.
7. **Manifest and checkpoint are written tmp → fsync → rename.** Both are
   derived-but-durable; neither is written in place.
8. **Torn tails are tolerated only on the final WAL segment.** A CRC mismatch is
   always fatal.
9. **Every record carries a format version**, and readers reject unknown versions
   explicitly rather than guessing.

---

## 17. Invariants that were *violated* and their status

Restating §15 as rules, because the difference is the point:

- **A tombstone must not be visible through any read API.** **Now holds on all three read
  paths** — `GetSpan`, `GetTrace` and `RangeQuery`. The key is removed from the index at
  flush time; the read paths also filter `Deleted` and re-verify candidates against the index.
- **A deleted span must not be counted in any aggregate.** **Holds for tombstones applied
  before ingest; still violated for a span counted and deleted later** (see Finding 2).
- **A cursor must identify a stable snapshot, or be documented not to.** Still neither.

---

## Appendix — file map

```
wal/wal.go              segmented write-ahead log; span payload codec (shared with segments)
pebble/pebble.go        Pebble wrapper; (segment_id, offset) value codec; batched put/delete/apply
bloom/bloom.go          FNV double-hash bloom filter, deterministic across processes
segment/writer.go       segment writer: header, zone map, bloom section, records
segment/reader.go       segment reader: header, blooms, ReadAt, ScanAll; OpenReaderLight
segment/cache.go        bounded refcounted reader cache; Purge-before-unlink; Release
segment/manifest.go     live segment set; tmp+fsync+rename swap
compactor/compactor.go  merge, expiry, staleness check, index/file co-deletion
rollup/rollup.go        bucket store, in-memory, locked
rollup/manager.go       four windows, fixed-point cost scaling
rollup/writer.go        snapshot writer (rollup_<window>.rdb)
rollup/reader.go        snapshot reader with magic/version/window-size validation
cmd/cmd.go              engine wiring: ingest, sessions, flush, compaction, query API
cmd/checkpoint.go       durable segment watermark
cmd/consistency.go      index ↔ segment ↔ manifest cross-checks
cmd/tombstone_test.go   deletion semantics: all three read paths, both flush timings
cmd/cache_test.go       segment-cache purge wiring + disabled path
cmd/benchmark_test.go   8 benchmark harnesses (insert seq/parallel, GetSpan,
                        GetSpanWithCompaction, GetTrace, RangeQuery,
                        RangeQueryWithCompaction, SpanAlreadyIndexed)
tests/wal_test.go       rotation, replay, corruption, tombstones
tests/fault_injection_test.go  self-execed crashable worker, ack oracle, force-kill driver
```
