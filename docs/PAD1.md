# Architecture Doc 1
### Status: **FROZEN v1.3** — baseline for implementation

Defines the *how*. See `PDD1.md` for the *why*, *who*, and *what*.

---

## Amendment v1.3 (additive — strengthens §6's rollup design against the still-open USP risk flagged in Design doc §4/§7)

**The gap this closes:** Design doc §4 flags claim 2 (typed columns + rollups)
as unverified against OpenObserve, with an explicit fallback: "if
OpenObserve already does typed rollups well, the USP shrinks to
embeddability alone." A flat rollup — sum(cost) grouped by (agent_id,
model, team, status) per time bucket — is exactly the kind of aggregate a
schema-on-read system can add without a redesign. §13's reference systems
confirm no named competitor (Tempo, Honeycomb, Jaeger, OpenObserve,
SmithDB) does anything with a trace's own hierarchical structure
(`parent_span_id`) at the aggregation layer — every one of them rolls up
flat span-level dimensions only. Closing this gap strengthens the weaker
USP claim with something structural, not just another metric.

**Decision — root-attributed rollups, additive to the flat rollup tier
already specified in §6:**

- In addition to rolling a span's numeric fields (`cost`, `tokens_in`,
  `tokens_out`, `latency_ms`) into its own flat dimension bucket, also roll
  them into a bucket keyed by the span's **trace root's** dimensions
  (`agent_id`, `model`, `team`) — answering "total cost of workflows
  started by Agent X," not just "cost of spans Agent X directly executed."
  This is the one aggregation shape none of §13's reference systems
  support, because it requires the span/trace hierarchy this system
  already tracks (§2's `parent_span_id`) as an aggregation-time input, not
  just a display-time one.
- **Mechanism reuses an existing concept rather than adding a new one:**
  track each open trace's root-span dimensions in a small cache
  (`trace_id → {agent_id, model, team}`), evicted on the same
  session-window inactivity timeout already specified for segment
  buffering (§3, Pillar 2) — not a second, bespoke expiry mechanism.
- **Degrades gracefully, doesn't hard-fail, matching Pillar 1's existing
  posture (§3):** a child span arriving before its trace's root
  (out-of-order arrival, already an accepted case per §3's "not required:
  ordering across concurrent writers") falls back to attributing under its
  own dimensions if the root isn't cached yet. This is a completeness
  trade-off, not a correctness one — no wrong number is ever produced,
  only a potentially-conservative one until the root is seen.
- **Rebuild story is identical to the flat tier's:** root-attributed
  buckets rebuild the same way — replaying the WAL from the checkpoint
  watermark forward (v1.1 amendment) — because the root cache itself is
  derived from the same span stream, not a separate source of truth.
- **No data-model or WAL-format change.** `parent_span_id` (§2) already
  carries what's needed; this is purely an aggregation-layer addition.
  Nothing here is expensive to retrofit later the way the v1.2 tombstone
  gap was — that's exactly why this is safe to add now as a v1.3
  amendment rather than needing to have been in v1.0.

**Alternatives considered and their disposition** — recorded so this isn't
re-litigated from scratch later:

- **Dimension-lattice rollups** (precompute every combination of the 5
  bounded dimensions — agent_id × model × tool_name × team × status — as
  separate buckets, so any GROUP BY combination hits a precomputed
  number). Rejected for v1: 2⁵ combinations per time bucket is real
  row-count growth against this project's own principle (§7 / Design doc
  §5) that "scope discipline matters more than feature completeness," and
  it doesn't address the OpenObserve risk the way root-attribution does —
  a bigger flat cube is still a flat cube. Not ruled out permanently;
  revisit only if a real query pattern demonstrably needs an unsupported
  combination.
- **Critical-path latency** (trace-level end-to-end latency along the span
  DAG's longest path, distinct from summing individual span latencies —
  meaningful because parallel tool calls shouldn't add). Rejected as a
  *rollup*: unlike root attribution (which only needs the root span,
  arriving early in causal order), a correct critical path needs the
  trace's leaves too, which are more likely to be late-arriving —
  computing it at session-window flush time risks an incomplete,
  silently-wrong answer, a correctness regression this system doesn't
  tolerate elsewhere. This fits better as an on-demand computation over an
  already-assembled trace (a function over the existing §4 point-lookup
  path), not a continuously-maintained aggregate — deferred to the Query
  API work (§12 step 6), not this amendment.

**Scope implication for PDD1:** §4's USP claim 2 gains a concrete,
structural differentiator beyond "we also have rollups" — see PDD1.md
v1.3.

---

## Amendment v1.2 (additive — closes a gap found during v1.1 build planning: tombstones were never specified)

**The gap:** retention-driven segment expiry (§8) is unavoidable in any v1
deployment with a retention policy — it isn't an optional feature the way
a user-facing delete API is. But §2's span record and §4's reconciliation
protocol never defined a tombstone, so there was no specified mechanism to
keep the Pebble trace index consistent with a segment being retired. A
stale index entry pointing at a deleted segment is a correctness bug, not
a degraded mode — this had to be closed before Phase 1 build starts, not
deferred, because the WAL record format decided in Phase 1 is expensive to
change later (same reasoning as the v1.1 checkpoint amendment).

- **Tombstone is a first-class WAL record type, added to the §2 span
  schema now: `Deleted bool` alongside the existing fields** — same shape
  as wisp's own tombstone field. A tombstone is a *write* (Pillar 1's job
  is exactly "accept a write, make it durable, acknowledge it"), so it
  goes through the WAL-first path like any span, not a special case bolted
  onto ingestion later.
- **A tombstone's effect lands only in Pebble, never in the segment.**
  Segments stay immutable (§1). Write the tombstone at the *same key*
  `trace_id∥span_id` in Pebble. A point lookup (§4) that hits a tombstone
  entry stops there and returns deleted — it never falls through to read
  the segment offset. Same semantics as wisp's `Get`: a tombstone is
  authoritative and stops the search.
- **Tombstone writes go through the existing reconciliation protocol
  unchanged (§4)** — the checkpoint only advances once both the WAL
  tombstone record and the Pebble tombstone `Put` are confirmed; crash
  recovery replays pending tombstones from the checkpoint watermark
  forward exactly like pending inserts. No new recovery mechanism needed.
- **Segment/index co-deletion at compaction time (§3) — this is the actual
  answer to "how do both sources get validated."** When a major compaction
  (§ Compactor precedent: wisp's `IsMajor`) drops a segment, the Pebble
  entries for every span that lived in that segment must be deleted in the
  *same* compaction pass, before the old segment file is unlinked.
  Compaction already knows which spans it's dropping, so batch the
  corresponding Pebble deletes into the same manifest-swap transaction
  described in §3's MVCC visibility model. **Invariant:** a segment is only
  safe to physically unlink once no Pebble entry still points into it —
  checkable by construction if the Pebble batch commits before the unlink,
  not after.
- **Scope implication for PDD1:** since the WAL record format must support
  tombstones regardless of whether delete-by-trace ships as a user-facing
  API in v1, add it to PDD1 §5's in-scope list now rather than treat it as
  a v1.5 feature bolted onto an ingestion path that didn't anticipate it.

---

## Amendment v1.1 (supersedes nothing in v1.0 below — additive, resolves open items flagged at freeze review)

- **Embedded LSM is Pebble**, not "Badger/Pebble" as an open choice. Pure Go,
  no cgo, RocksDB-lineage design — chosen because its crash-recovery
  semantics are the cleaner base to wrap for the §4 reconciliation protocol.
  This is a real `go.mod` dependency and a deliberate exception to a
  no-external-deps instinct (unlike wisp's own hand-rolled LSM) — call this
  out explicitly wherever this project is compared to wisp, so the
  difference is never assumed away.
- **Startup reconciliation requires a durable checkpoint/watermark in v1**,
  not unbounded WAL/segment replay. Without it, boot time grows linearly
  with total data ingested, which would eventually violate the "under 5
  minutes" DoD (Design doc §6) — a checkpoint recording the last
  segment-id confirmed both segment-written and LSM-indexed bounds startup
  reconciliation to only the unconfirmed tail. This checkpoint is itself
  WAL-derived state (rebuildable, not a second source of truth) and must be
  updated only after both the segment write and the LSM `Put` are
  confirmed (§4).
- **Rollup crash recovery is bounded by the same checkpoint.** Rollup state
  is rebuildable by replaying the WAL (§6), but "replay the whole WAL" has
  the identical boot-time-growth problem as trace-index reconciliation
  above. Rollup rebuild should replay only from the checkpoint watermark
  forward, not from WAL genesis, once the watermark exists. A snapshot of
  rollup bucket state should be persisted at the same cadence as the
  checkpoint so buckets don't need replay past it either.
- **WAL/index version tagging is added as an explicit Build order step**
  (see §12 below), not left implicit in step 2/3 as originally drafted —
  §9 required this but the original Build order had no slot for it.
- **Concrete WAL rotation / segment-flush size is a placeholder pending
  benchmarking**, same posture as wisp's `MemTableFlushThreshold`: start
  with a small tuned-for-testing value, revisit once real ingestion
  patterns exist. Not blocking for freeze, but Build order step 1 should
  not be called "done" without picking a number to test against, even a
  placeholder.

---

## 1. Non-negotiable constraints

Permanent v1 boundaries, not temporary shortcuts:

| Constraint | Rules out |
|---|---|
| Single-node only | Raft/consensus, sharding, replication, split-brain handling |
| No required external services | S3 as a hard dependency, external brokers, external metadata stores |
| WAL is the single source of truth | Any component whose state can't be rebuilt by replaying the WAL |
| Immutable segments | In-place mutation of on-disk data; complex reader/writer locking |
| No full SQL in v1 | General-purpose query planning, joins, subqueries |
| No cgo | Anything that breaks the single-static-binary property (e.g. RocksDB via cgo) |

**What kind of system this is:** not a classical time-series database
(Prometheus/InfluxDB-style, which assumes a small, slowly-changing set of
labeled series). It's a **columnar, time-partitioned, immutable event
store** — the same family as Grafana Tempo and Honeycomb's Retriever —
with a small classical-TSDB-shaped rollup layer living inside it for the
bounded-cardinality aggregate metrics only.

---

## 2. Data model

### Span (the atomic unit)

| Field | Type | Notes |
|---|---|---|
| `trace_id` | high-cardinality ID | groups spans into a trace |
| `span_id` | high-cardinality ID | unique per span |
| `parent_span_id` | high-cardinality ID, nullable | causal structure within a trace |
| `agent_id`, `model`, `tool_name`, `team`, `status` | bounded-cardinality string | pruned via bloom filter + zone map, not a separate index |
| `tokens_in`, `tokens_out`, `cost`, `latency_ms` | numeric | rollup + aggregation targets — the USP's typed-column claim |
| `timestamp` | int64 | primary time axis; also drives per-segment zone maps |
| `payload` | opaque blob | prompt/response/tool I/O — never scanned by aggregation |
| `deleted` | bool | tombstone flag (v1.2) — see §4's tombstone/co-deletion subsection |

Which OTel GenAI attributes get promoted to typed columns vs. left in the
opaque payload is a deliberate modeling decision to make explicitly per
field, not left implicit — this line is exactly where the "typed columns"
USP claim (Design doc §4) is either earned or lost.

---

## 3. The three pillars

Collapsed from four to three relative to the original draft: rollups are
the aggregation pillar's core mechanism, not a separate concern, and
anomaly detection/alerting have been cut entirely (Design doc §5).

```
Pillar 1 — Durable ingestion (WAL)
        —   must be correct first; everything else is derived
        ↓
Pillar 2 — Immutable storage & compaction (segments)
        ↓
Pillar 3 — Indexing & aggregation (LSM trace index, bloom/zone-map
           pruning, rollups)
```

Pillars 2 and 3 are, in a real sense, **derived state — caches over
Pillar 1.** A useful standing test: *if every segment, index file, and
rollup were deleted right now, could correct state be fully reconstructed
from the WAL alone?* If the answer is ever no, that's a design flaw to fix
before writing more code.

### Pillar 1 — Durable ingestion

**Job:** accept a span, validate it, make it durable, acknowledge it.

**Guarantee:** once acknowledged, a write survives any crash — power loss,
OOM kill, panic — with no torn or partial state visible afterward.

**Not required:** exactly-once semantics (at-least-once with idempotent
trace/span IDs is enough); ordering across concurrent writers.

**Mechanism:** append-only WAL, `fsync` before ack, bounded in-memory
buffer for batching. On buffer overflow, spill to WAL. If the WAL's own
disk is full, reject new writes explicitly rather than drop them silently.

**Tombstones are ordinary writes (v1.2).** A delete is a WAL record with
`deleted=true` (§2), flowing through this same durability path — not a
separate mechanism layered on afterward. See §4 for how a tombstone
propagates to the trace index and §3's compaction subsection for how it
eventually reclaims space.

**Embedded-mode caveat:** as a library inside someone else's process, this
system does not control that process's lifecycle. Crash-recovery testing
must explicitly include "host process dies unexpectedly while embedded" as
its own scenario — it is not automatically covered by testing the
standalone binary's shutdown path.

### Pillar 2 — Immutable storage & compaction

**Job:** turn buffered spans into compact, columnar, on-disk segments;
periodically merge small segments and prune expired data.

**Guarantee:** segments are never partially visible; compaction never
violates Pillar 1's durability contract — WAL entries are only eligible
for truncation once the segment representing them is confirmed durable.

**Per-column encoding:**

| Column type | Encoding | Why |
|---|---|---|
| Timestamps | Delta-of-delta | Near-monotonic within a segment; compresses very well |
| Bounded strings (agent_id, model, tool_name, team, status) | Dictionary + run-length | Small distinct-value set repeated often |
| Numeric metrics (tokens, cost, latency) | Delta/XOR (Gorilla-style) | Well-precedented, but expect lower ratios than classical metrics — LLM cost/latency is spikier than CPU/memory series. Measure, don't assume. |
| Payload | zstd | Good ratio on natural-language text |

**Segment header, embedded at write time (see §5):** min/max timestamp
(zone map) and one bloom filter per bounded-cardinality dimension.
Self-describing — each segment embeds its own schema version, so schema
evolution doesn't require a hard migration across existing segments.

**Compaction and crash safety — MVCC-style segment visibility:**

- Compaction never mutates a segment in place. It writes an entirely new
  segment, then atomically swaps a pointer (e.g. via a manifest file
  rename) from the old segment set to the new one.
- Readers pin whatever segment set was current when their query started;
  old segments aren't deleted until no reader still references them.
- If the process dies before the atomic swap, old segments are untouched
  and correct — the partial new segment is simply orphaned and cleaned up
  on next startup. If it dies after the swap but before old-segment
  cleanup, cleanup just resumes on restart.

**Segment/index co-deletion (v1.2)** — a major compaction that drops a
segment must delete that segment's corresponding Pebble trace-index
entries in the *same* manifest-swap transaction, before the old segment
file is unlinked. A segment is only safe to physically unlink once no
Pebble entry still points into it (§4). Retention-driven expiry uses this
same path — expiry is just compaction dropping a segment nothing else
references, so it needs no separate mechanism.

**Ingestion buffering uses a session-window heuristic** — flush a trace's
spans together once no new span for that trace has arrived for N seconds —
rather than a pure time/size trigger blind to trace boundaries. This
bounds how scattered a trace's spans get across segments, which directly
affects point-lookup read amplification (§4).

**The test that matters:** fault-inject — `kill -9` at random points under
concurrent read/write/compact load, in a loop — and verify after every
recovery that no acknowledged write is missing and no reader ever saw a
torn or duplicated span.

### Pillar 3 — Indexing & aggregation

Covers three distinct mechanisms, each solving a different query shape.
See §4 and §5 for full detail on each.

---

## 4. Trace index — LSM-backed (Pebble), point lookup only

**Why spans can't be assumed contiguous:** a trace's spans arrive
interleaved with spans from many other concurrent traces and flush into
whatever segment happens to be open at the time. Assuming contiguity is an
accident of low concurrency, not a guarantee.

**Correct index shape — composite key, one entry per span:**

```
key   = trace_id || span_id
value = (segment_id, offset)
```

Because the LSM keeps keys sorted, all spans of one trace occupy a
contiguous key range even though the underlying segments don't. A trace
lookup is a **prefix scan**: seek to `trace_id`, iterate while the prefix
matches, collect every `(segment_id, offset)`, then read each location —
potentially from several different segments. Every write is a pure insert,
never a read-modify-write, so concurrent spans of the same trace never
race on a shared index entry.

**Read cost:** N segments touched means N reads to reconstruct a trace.
Session-window buffering (§3, Pillar 2) is the primary mitigation, bounding
scatter at the source rather than compensating for it after the fact.

**LSM library boundary — precisely, this is the whole of it:**

| Handled by the embedded LSM (Pebble) | Handled by your own code |
|---|---|
| Sorted key storage, fast exact-match and prefix-scan lookups | Segment file writes |
| Its own internal memtable → SSTable flush cycle and compaction | WAL fsync before ack |
| Its own crash recovery — *for its own data only* | Segment/index reconciliation (below) |
| | Crash recovery replay across segments + LSM together |
| | Bloom filters, zone maps (not LSM's concern at all) |
| | Checkpoint/watermark tracking (v1.1 amendment above) |

The LSM guarantees its own internal consistency. It has **no visibility
into your segment files** — a `(segment_id, offset)` value is opaque to
it. Keeping the two stores consistent *with each other* is entirely your
responsibility.

**Reconciliation protocol** (the single riskiest correctness surface in
the system — design this on paper before writing flush code):

- A span is only durable-and-queryable once *both* its segment write and
  its LSM `Put` are confirmed.
- A durable checkpoint records the last segment-id for which both sides
  are confirmed consistent (v1.1 amendment). On startup, replay only from
  that checkpoint forward, not from WAL genesis. Any span with a segment
  but no confirmed index entry (or vice versa) in that tail range is
  expected, recoverable, unconfirmed state — re-derive and rewrite it from
  the WAL, never treat it as corruption.

**Tombstones and index/segment co-deletion (v1.2)** — a tombstone (§2, §3
Pillar 1) is written to Pebble at the same key it would otherwise occupy,
`trace_id∥span_id`. A point lookup hitting a tombstone entry stops
immediately and returns deleted, the same way a hit on a live entry
returns a location — it never reads the segment. Tombstone writes go
through the same reconciliation protocol as inserts (below): the
checkpoint only advances once both the WAL record and the Pebble `Put` are
confirmed. Actual space reclamation — removing the tombstone entry itself
and the segment data it shadows — happens at compaction time (§3), not at
delete time; until then the tombstone is the authoritative answer.

**Where the LSM is used — and, just as importantly, where it is not:**
exactly two touchpoints in the whole system.

- **Write time:** one `Put(trace_id∥span_id, location)` call per span, at
  segment flush.
- **Read time:** only `Point lookup` queries call the LSM (`Get`/prefix
  scan). `Range` queries use bloom filters + zone maps (§5) instead.
  `Aggregation` queries use rollups (§6) instead. Neither needs an
  exact-match key lookup, so routing them through the LSM would be
  unnecessary indirection.

---

## 5. Segment pruning — bloom filters + zone maps, no separate tag index

**Decision, superseding an earlier draft's separate LSM-backed tag index:**
segment pruning is handled entirely by structures embedded in each
segment's own header, written once at segment-write time, never requiring
external consistency management. This removes an entire class of the
reconciliation problem described in §4 — a segment's pruning metadata lives
and dies atomically with the segment itself.

**Zone map** — one `(min_ts, max_ts)` pair per segment. Time-range
filtering is the near-universal first predicate on every query, and a
min/max comparison is the cheapest possible way to rule a segment out.

**Bloom filter** — one per bounded-cardinality dimension (`agent_id`,
`model`, `tool_name`, `team`, `status`) per segment. A fixed-size bit array
plus a handful of hash functions:

- **Insert:** hash the value through each function, set each resulting bit.
- **Query:** hash the candidate value the same way — if *any* bit is 0,
  the value is **definitely absent**; if all bits are 1, it's **possibly
  present** (false positives possible, false negatives never).

This asymmetry is exactly what pruning needs: a segment that comes back
"definitely not" is skipped with zero disk reads; a segment that comes
back "maybe" gets scanned for real. Worst case is a wasted read, never a
wrong answer.

**Trade-off, accepted deliberately:** query time scales with segment count
rather than with an index lookup, and false positives mean some reads are
wasted. This is the right trade for a solo build — it eliminates a
crash-consistency problem in exchange for a query-time cost that only
matters at high segment counts, which is a good problem to have because it
means real usage to benchmark against. Revisit only if segment growth
actually makes linear per-query bloom checks too slow.

---

## 6. Aggregation — rollups, and the fallback path

**Job:** answer "cost by agent, last hour" fast, without ever reading the
payload column. This is the query shape the design doc's USP claim (§4,
typed columns) actually rests on — worth treating as the most
scrutinized part of the system, not an afterthought.

**Rollups are not an index.** An index says *where to look*; a rollup *is*
the answer for aligned windows. Rollups never point back at raw spans —
they're a small, separate, continuously-maintained store, fully
rebuildable at any time by replaying the WAL and re-aggregating (bounded
by the checkpoint watermark once one exists — v1.1 amendment). This is the
one place classical TSDB technique (Gorilla-style encoding) legitimately
applies, because rollup data — unlike raw spans — genuinely is a small set
of long-lived, regularly-updated numeric series.

**Mechanism — two-tiered:**

- Continuously-maintained rolling windows (1m / 5m / 1h / 1d), updated as
  spans are ingested, for the common case where a query's window aligns to
  a bucket. **Root-attributed rollups (v1.3):** each span updates its own
  flat dimension bucket *and* a bucket keyed by its trace root's
  `agent_id`/`model`/`team`, via a session-window-evicted `trace_id → root
  dimensions` cache — see Amendment v1.3 for the full mechanism and the
  alternatives (dimension-lattice, critical-path) considered and deferred.
- Fallback: a direct columnar scan of numeric + dimension columns only
  (never payload), for windows that don't align — e.g. "last 47 minutes."

---

## 7. Query API

| Query type | Description | Touches LSM? | Target latency |
|---|---|---|---|
| Point lookup | Full trace by `trace_id` (§4) | Yes — only query type that does | < 50ms p99 |
| Range/filter | Spans by time range + dimension filters (§5) | No — bloom filter + zone map | — |
| Aggregation | sum/avg/count/percentile grouped by dimension (§6) | No — rollups or columnar scan | < 1s p99 over 24h |

Cursor-based pagination for large result sets. A minimal query language or
a fluent Go/HTTP builder — full SQL is explicitly out of scope (Design doc
§5); a real query planner (joins, subqueries) is its own multi-year
problem and scope discipline here matters more than feature completeness.

---

## 8. Payload storage — local by default, S3 optional and non-blocking

Systems like Tempo/Loki/SmithDB offload payload bytes to object storage to
solve **multi-node elasticity** — a stateless node can be replaced without
losing data because the data already lives in S3. This system is
explicitly single-node (§1); that problem doesn't exist here, so importing
the solution would add operational complexity (an AWS account, IAM,
network egress) to solve a problem this design doesn't have — directly
undercutting the "no external services" constraint.

- **v1 default:** payload stays local, zstd-compressed, subject to a
  retention policy enforced in the same pass as compaction (§3). LLM span
  payloads are typically small (KB-scale) and compress well as natural
  language text — this should hold up fine at target-persona scale without
  object storage.
- **v1.5/v2, opt-in only:** an S3-compatible (not AWS-specific — also
  MinIO/R2/B2) cold-storage tier for payloads past a configurable age,
  wired through the existing retention hook (upload-then-drop instead of
  just-drop). Absence of this config is a fully supported, first-class
  state, not a degraded mode.
- **Don't build the S3 path speculatively.** Build the extension point (a
  single hook at payload-expiry time); implement the S3 side only once a
  real user demonstrably hits a local-disk ceiling.

---

## 9. Deployment

- Ships as a single static Go binary (no cgo, per §1) **and** as an
  importable Go package for direct in-process embedding — the primary USP
  claim (Design doc §4). Both modes share the same core engine.
- Docker image with sane defaults — `docker run` to a working instance
  under 5 minutes.
- Config via file + environment variables (12-factor).
- Health/readiness HTTP endpoints; Prometheus-format operational metrics
  (ingest rate, WAL size, compaction lag, query latency).
- Segment directory, WAL, and indexes are self-contained and
  snapshot-able to any object storage for whole-directory backup — separate
  from, and simpler than, the payload-tiering question in §8.

**Upgrade/rollback safety — explicit requirement, not an afterthought.**
Segment self-description (§3) only covers segment format evolution. WAL
format changes and LSM/index-store version changes need their own explicit
version tagging, plus a documented, tested downgrade path. For a database,
"easy to deploy" only matters if it's also "safe to upgrade and roll
back" — this is where user trust is actually won or lost, more than
first-run setup time, and it must be closed before calling v1 done. See
Build order §12 step 2a (v1.1 amendment) — this is no longer implicit.

---

## 10. Security (v1 scope)

- Bearer-token authentication on ingestion and query endpoints.
- TLS termination is a deployment concern (reverse-proxy guidance
  documented), not built into the binary for v1.
- No multi-tenancy in v1 — single-tenant, single-team deployment only.
- Known, acknowledged gap: no token scoping or rate limiting yet. Stated
  explicitly as a limitation, not left silent.

---

## 11. Non-functional targets

| ID | Target |
|---|---|
| Ingest throughput | Set only after benchmarking on a 2 vCPU / 4GB reference instance — don't commit to a number first |
| Trace lookup latency | < 50ms p99 |
| Aggregation query latency | < 1s p99 over a 24h window |
| Durability | No acknowledged write lost on crash (WAL-backed recovery) |
| Resource footprint | Comfortably runs on a $20–40/month VM at early-stage-startup volume |
| Portability | Linux and macOS, amd64 and arm64 |
| OTel compatibility | Pin to a specific GenAI semantic convention version; document mapping; revisit each convention release |

---

## 12. Build order

Given the dependency chain in §3, the correct build order inverts a
typical product roadmap — durability before features:

1. **WAL + crash recovery**, tested with a `kill -9`-in-a-loop fault
   injection harness, until it's trusted completely. Pick a placeholder
   WAL-rotation/segment-flush size to test against now (v1.1 amendment) —
   not blocking, but must exist before step 1 is called done. **Include
   the `deleted` tombstone field in the WAL record format from the start
   (v1.2)** — this is the expensive-to-retrofit decision, not the
   compaction-side handling below.
2. **Segment write path + the segment/trace-index reconciliation
   protocol** (§4), designed on paper first, including the checkpoint/
   watermark mechanism (v1.1 amendment) from the start — retrofitting a
   checkpoint after the reconciliation protocol is built and tested is
   far more expensive than including it in the original design. Tombstone
   writes (v1.2) reuse this same protocol — no separate design needed here.
   - **2a. WAL/index version tagging + downgrade path** (§9) — slot this in
     here, not left implicit, since this is when the on-disk formats
     first exist and are cheapest to version correctly.
3. **Compaction with MVCC-style segment visibility** (§3), also
   fault-injection tested. **Segment/index co-deletion (v1.2)** belongs
   here — batching Pebble entry deletion into the same manifest-swap
   transaction that drops a segment. Fault-inject this specifically:
   crash between the Pebble delete batch and the segment unlink, and
   verify no dangling index entry survives (it shouldn't, if the batch
   commits before the unlink) and no segment is unlinked while entries
   still point into it.
4. **Bloom filters + zone maps** (§5) — low risk, embedded in segment
   writes from step 2 rather than bolted on later.
5. **Rollups + aggregation fallback** (§6) — the USP-critical path;
   validate against real query patterns, not just the demo query. Rollup
   snapshotting at the checkpoint cadence (v1.1 amendment) belongs here.
   **Root-attributed rollups (v1.3)** belong in this same step — the
   root-dimension cache is populated from the same ingestion path already
   being instrumented for the flat tier, not a separate pass.
6. Query API, point/range/aggregation routing, embeddable-package
   ergonomics — lower risk, safe to iterate on quickly once 1–3 are solid.

**Before implementation begins:** run the OpenObserve validation flagged
in Design doc §4 — spin up OpenObserve local mode, feed it representative
LLM-agent-shaped spans, and check whether its LLM-observability feature
already does typed/rollup-native cost-and-token queries. This determines
whether the project's USP is two claims or one.

---

## 13. Reference systems

- **Grafana Tempo** — minimal trace-id indexing over compressed blocks;
  closest public analog to §4's approach.
- **Honeycomb Retriever** — columnar wide-event storage, built because
  classical TSDB/metrics engines couldn't handle high-cardinality event
  data.
- **Jaeger (badger backend)** — proof that embedded-LSM trace storage
  works in production; also the clearest illustration of what NOT to
  do (opaque blob storage, per-tag write amplification, no time-range
  aggregation path) — see Design doc §3.
- **OpenObserve (local mode)** / **Parseable** — proof that single-binary,
  no-dependency, columnar (Parquet) local storage is already a mature,
  shipping pattern — the actual competitive bar for this project, not
  Jaeger. Verify their LLM-observability claims directly (§12) before
  assuming a schema gap exists.
- **SmithDB (LangSmith)** — validates LSM-over-columnar-segments as an
  architecture at a different (distributed, object-storage-backed) scale.
- **Prometheus TSDB storage engine** / **Facebook's Gorilla paper** —
  reference for the rollup layer's numeric encoding (§6), not the raw span
  storage layer.
- **Pebble docs** (v1.1: was "Badger/Pebble", now decided) — the embedded
  LSM this design leans on for the trace index (§4); don't reimplement
  this layer.

---

## 14. Freeze notes

v1.0 was the technical baseline. v1.1 was a deliberate, versioned
amendment resolving four items flagged at the v1.0 freeze review: LSM
library choice, startup-checkpoint requirement, rollup replay bound, and
the missing version-tagging build-order slot. v1.2 closed a gap found
during v1.1 build planning: tombstones were never specified, despite
retention-driven segment expiry making them unavoidable in any v1
deployment. v1.2 added the `deleted` field to the §2 span schema, routed
tombstone writes through the existing §4 reconciliation protocol
unchanged, and specified segment/index co-deletion as part of §3's
compaction. v1.3 (this version) strengthens §6's rollup design with
root-attributed rollups — using the trace/span hierarchy (§2's
`parent_span_id`) as an aggregation-time input, which directly addresses
the still-open USP risk flagged in Design doc §4/§7 (claim 2's flat rollup
alone may not differentiate against a schema-on-read competitor). It also
records two alternatives (dimension-lattice rollups, critical-path
latency) considered and explicitly deferred, not silently dropped. Per the
v1.0 freeze note's own instruction that changes be deliberate amendments,
not silent drift: any future change to the pillar structure, the
trace-index/tag-index design, the payload-storage decision, the LSM
boundary, the checkpoint mechanism, the tombstone/co-deletion protocol, or
the rollup/root-attribution design introduced here should itself be a
further deliberate, versioned amendment against this freeze.
