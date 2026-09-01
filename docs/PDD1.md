# Design Doc 1
### Status: **FROZEN v1.3** — baseline for architecture and implementation

## Amendment v1.3 (additive — see PAD1.md v1.3 for the technical detail this tracks)

- **USP claim 2 (§4) gains a concrete, structural differentiator:
  root-attributed rollups.** Every reference system in §3's competitive
  table rolls up flat span-level dimensions only; none use trace hierarchy
  (parent-child span structure) as an aggregation-time input.
  Root-attributed rollups — "total cost of a workflow started by Agent X,"
  not just "cost of spans Agent X directly executed" — is a rollup shape
  structurally unavailable to a schema-on-read system without it
  independently reinventing trace-structure-aware aggregation. This
  directly addresses the open risk in §4/§7: if the flat-rollup half of
  claim 2 turns out to be something OpenObserve already does, this is the
  part that doesn't reduce to "we also have rollups."
- **Scope (§5)** — the "Aggregation queries" in-scope bullet now includes
  root-attributed rollups as part of the rollup mechanism. No new data
  model or WAL change; see PAD1.md v1.3 for the mechanism (reuses the
  existing session-window eviction concept from Architecture doc §3).
- Two alternative aggregation designs were evaluated and explicitly
  deferred, not silently dropped: dimension-lattice rollups (rejected for
  v1 — doesn't address the USP risk, adds row-count growth against this
  doc's own scope-discipline principle, §7) and critical-path latency
  (reclassified as a query-time trace-analysis feature, not a rollup,
  since a correct answer needs the trace's leaves, which risk being
  late-arriving at rollup-flush time). See PAD1.md v1.3 for the full
  reasoning.

## Amendment v1.2 (additive — see PAD1.md v1.2 for the technical detail this tracks)

- **Tombstone support (delete semantics) moves into v1 in-scope, §5** —
  not because a user-facing delete-by-trace API is being added as a
  feature, but because retention-driven segment expiry (§8 of the
  architecture doc) is unavoidable in any v1 deployment with a retention
  policy, and it requires a WAL-level tombstone record to keep the trace
  index consistent with segments being retired. Since the WAL record
  format is decided in Build order Phase 1 and is expensive to change
  after the fact, this can't be deferred to v1.5 the way the S3 payload
  tier was.
- This does **not** commit to shipping a user-facing "delete this trace"
  API in v1 — that remains a separate, smaller decision. It commits only
  to the WAL record format supporting a `deleted` flag and the
  reconciliation/compaction machinery handling it correctly, which
  retention needs regardless.

## Amendment v1.1 (additive — see PAD1.md v1.1 for the technical amendments this tracks)

- **Project name candidate: "Trellis"** (working name; final call pending).
  Chosen over "wisp v2" deliberately — Architecture doc v1.1 confirms this
  system embeds a third-party LSM (Pebble) and has a fundamentally
  different data model (span/trace vs. `(SeriesID, Timestamp)`), so it is
  a separate module/repo, not an evolution of wisp's dependency-free
  engine. Wisp's design patterns (WAL-first, immutable generation-numbered
  segments, refcounted file lifecycle) carry over as precedent, not as
  imported code.
- No change to objective, scope, or USP claims in this amendment — those
  remain as frozen in v1.0 below. This amendment only records the
  architecture-side resolutions (LSM choice, checkpointing) that Design
  doc §7 (execution risk) and §4 (USP) depend on being resolved before
  build starts.

---

## 1. Objective

Build a **single-node, embeddable trace store**, specialized for LLM agent
spans — hierarchical trace structure plus numeric measures (cost, tokens,
latency) — that:

- guarantees **crash-safe durability** for every acknowledged write,
- reconstructs a **full trace** from a single ID,
- answers **aggregation queries** (cost/tokens/latency by agent/model/team,
  over a time window) fast, via typed columns and continuous rollups,
- ships as **both** a single static Go binary **and** an importable Go
  package — embeddable directly into another process, not just runnable
  beside one,
- requires **no external services** to operate.

---

## 2. Problem and target user

**Target user:** a solo builder or small team running LLM agents in
production, or a developer embedding trace storage directly inside their
own Go service, who wants durable, queryable cost/latency/token data
without operating a separate database process.

**The pain point is real, but genuinely narrow.** Free hosted alternatives
(Langfuse, LangSmith) already serve teams that don't mind sending data to a
third party. The market for people who specifically want data to never
leave their own process — because they're embedding it as a library, not
because they dislike SaaS — is smaller, but it is a real, underserved,
and clearly definable segment (§4).

---

## 3. Competitive landscape

Evaluated directly against the actual field, not a hypothetical one:

| System | No external deps? | Columnar? | Schema | Embeddable as a library? |
|---|---|---|---|---|
| Jaeger (badger backend) | Yes | No — opaque blob + per-tag inverted index | Generic APM | No — standalone binary only |
| OpenObserve (local mode) | Yes | Yes — Parquet | Generic; claims LLM observability | No — standalone binary only |
| Parseable | Mostly (object storage is the intended end state) | Yes — Parquet | Generic, SQL-first | No |
| SmithDB (LangSmith's engine) | No — Postgres + object storage required | Yes — Vortex | Agent-native | No, proprietary |
| Langfuse (self-hosted) | No — ClickHouse required, no swap-out | Yes | Agent-native | No |
| DIY data lake (Parquet + DuckDB) | Yes, if kept local | Yes | Whatever you define | Yes — DuckDB embeds directly |
| **This project** | **No — one dependency, Pebble, deliberate (v1.1)** | **Yes** | **Agent-native, typed cost/token columns** | **Yes — Go package** |

**Key findings that shaped this doc:**

- **"Single binary, no external services" is commoditized, not a USP.**
  Jaeger's badger backend has shipped this for years. OpenObserve's local
  mode does it too, and does it with columnar Parquet storage — closing
  the specific gap (opaque-blob storage, no real aggregation path) that
  made Jaeger a weak comparison. Positioning on deployment simplicity
  alone is not defensible.
- **Jaeger's badger backend has a structural, not incidental, weakness**:
  storing whole spans as opaque KV values with a per-tag inverted index
  causes write amplification per indexed tag, and a documented inability
  to efficiently fetch traces over a time range. This validates the
  columnar-segment design (Architecture doc) as substantively better for
  aggregation-heavy workloads — but it does not, by itself, justify a new
  project once OpenObserve is considered.
- **SmithDB independently validates the core architecture** — it is an
  LSM over immutable, columnar, compacted segments, same shape as this
  design — but targets the opposite deployment mode entirely (distributed,
  object-storage-backed, Postgres metastore, proprietary, Enterprise-only
  self-hosting). It is evidence the pattern works, not a competitor to
  this project's target user.
- **No named competitor offers library-embeddability.** Every serious
  option in this space — Jaeger, OpenObserve, Parseable, SmithDB, Langfuse
  — is a standalone process reached over HTTP. None can be `import`-ed
  directly into a Go binary. This is the one differentiator nothing in the
  field currently contests.

---

## 4. USP — stated precisely, not aspirationally

Two claims, and only two, form the actual case for building this rather
than adopting something that already exists:

1. **Embeddable as a Go library, not just a standalone binary.** A
   developer can compile trace storage directly into their own process —
   no separate service, no network hop, no serialization boundary. This is
   uncontested in the current competitive field.
2. **Typed, first-class GenAI columns with built-in rollups**, rather than
   schema-on-read generality. `cost`, `tokens_in`, `tokens_out`,
   `latency_ms` are indexed numeric columns with continuously-maintained
   rollups, not JSON fields queried ad hoc via SQL each time.

**Explicitly not claimed as a USP:** "single binary," "no dependencies,"
and "fast" in isolation — these are necessary, table-stakes properties
shared by multiple mature competitors, not differentiators on their own.
Note (v1.1): "no dependencies" was never fully true for this project once
Pebble was chosen as the embedded LSM — it was already excluded as a claim
here at v1.0, so this doesn't change the USP, only confirms the doc was
already correctly hedged on it.

**Open risk, not yet validated:** claim 2 assumes OpenObserve's advertised
"LLM observability" support is schema-on-read rather than typed/rollup
native. This has not been directly verified against a running instance.
**This is the single highest-priority validation to run before or during
early implementation** — if OpenObserve already does typed rollups well,
the USP shrinks to embeddability alone, which is a smaller, still-valid,
but differently-scoped project.

**Mitigation added in v1.3:** root-attributed rollups (Architecture doc
v1.3) give claim 2 a structural component — trace-hierarchy-aware
aggregation — that a flat, schema-on-read rollup can't trivially replicate
without independently building the same trace-structure-aware mechanism.
This doesn't remove the need to run the OpenObserve validation above; it
reduces how much rides on that single validation's outcome.

---

## 5. Scope

### In scope for v1

- Durable span ingestion (OTLP + simplified JSON), WAL-backed.
- Immutable, columnar, compacted segment storage.
- Trace-by-ID reconstruction (LSM-backed point lookup).
- Time-range + dimension-filtered span queries (bloom filter + zone map
  pruning, no separate tag index).
- Aggregation queries (sum/avg/count/percentile), backed by continuously
  maintained rollups plus a payload-avoiding columnar scan fallback.
  Rollups include root-attributed buckets (Architecture doc v1.3) that
  aggregate a span's numeric fields under its trace root's dimensions, not
  just its own — see Amendment v1.3 above.
- Single static Go binary AND importable Go package.
- Bearer-token auth, health/readiness endpoints, Prometheus-format
  operational metrics.
- Startup checkpoint/watermark mechanism bounding reconciliation and
  rollup replay cost (Architecture doc v1.1) — added to in-scope list
  since it's now a v1 requirement, not a v1.5 optimization.
- Tombstone support in the WAL record format and reconciliation/compaction
  machinery (Architecture doc v1.2), required by retention-driven segment
  expiry — see Amendment v1.2 above. A user-facing delete-by-trace API is
  not itself committed to v1 by this; only the underlying mechanism is.

### Explicitly out of scope for v1

- **Anomaly detection and alerting** (EWMA/z-score baselines, Slack/webhook
  delivery, cooldown/dedup logic). This is downstream consumer tooling, not
  database functionality — the same way Tempo doesn't alert and SQLite
  doesn't have a dashboard. May exist as a separate, optional companion
  project later; it does not belong in the storage engine.
- Multi-node distribution or replication.
- Full SQL query support.
- Multi-tenancy.
- Mandatory object storage of any kind.
- Built-in eval / LLM-as-judge scoring.
- Semantic/full-text search over prompt content.

---

## 6. Definition of done, v0.1

A developer either (a) runs the Docker image and points an OTel collector
at it, or (b) `go get`s the package and calls it directly from their own
process — and within 5 minutes can: ingest a batch of representative
LLM-agent spans, reconstruct a full trace by ID, and run a "cost by model,
last hour" aggregation query that never touches payload bytes and returns
in well under a second. No other services running, in either deployment
mode. This DoD is why the checkpoint mechanism (§5, Architecture doc v1.1)
had to become a v1 requirement rather than a later optimization — without
it, boot time is unbounded as data grows, silently invalidating "within 5
minutes" for anyone past a small trial dataset.

---

## 7. Risks, stated honestly

- **Trust risk**: a brand-new, solo-built database asks for a level of
  trust disproportionate to almost any other software category. This is
  not solved by good architecture alone — it's earned slowly, through
  demonstrated correctness (public fault-injection test results, not just
  claims).
- **Execution risk**: the architecture is sound; whether the WAL/segment/
  index crash-consistency protocol is actually implemented correctly is
  unproven until built and adversarially tested (see Architecture doc
  §Build order). The checkpoint mechanism (v1.1) adds a second correctness
  surface of the same character — it must be fault-injection tested
  alongside the reconciliation protocol it bounds, not treated as a
  simpler bolt-on.
- **USP-validity risk**: claim 2 in §4 is unverified against OpenObserve as
  of this freeze. If it doesn't hold, the project should re-scope around
  embeddability alone rather than proceed on a false differentiator.
- **Adoption risk**: even with a real technical differentiator, the target
  segment (developers who specifically want library-embeddability) is
  narrower than "small startups broadly." This is an acceptable, explicitly
  chosen trade-off, not an oversight.

---

## 8. Freeze notes

v1.0 was the full scope of design discussion through initial freeze. v1.1
was a deliberate, versioned amendment recording the naming decision and
the architecture-side resolutions from PAD1.md v1.1 that this doc's risk
and scope sections depend on. v1.2 added tombstone support to v1 in-scope
(§5), tracking PAD1.md v1.2's discovery that retention-driven segment
expiry requires WAL-level tombstones regardless of whether a user-facing
delete API ships. v1.3 (this version) adds root-attributed rollups to §4's
USP claim 2 and §5's scope, tracking PAD1.md v1.3's discovery that trace
hierarchy — already tracked via `parent_span_id` and unused by any named
competitor at the aggregation layer — can give the weaker USP claim a
structural component ahead of the still-unresolved OpenObserve validation.
Any future change to objective, target user, USP claims, or v1 scope
should be made as a further deliberate, versioned amendment — not a
silent drift — given how much of the architecture doc depends on the
scope boundaries fixed here.
