// Package wisptrace is an embeddable, single-node trace store for LLM agent
// spans. It is pure Go (no cgo) and keeps all state in one directory.
//
//	db, err := wisptrace.Open("./data")
//	if err != nil { ... }
//	defer db.Close()
//
//	err = db.Ingest(ctx, wisptrace.Span{
//		TraceID: "t1", SpanID: "s1",
//		Start: time.Now(), Duration: 245 * time.Millisecond,
//		Model: "gpt-4", Team: "platform", Status: wisptrace.StatusOK,
//		TokensIn: 100, TokensOut: 50, CostUSD: 0.0015,
//	})
//
// # Recording spans by hand
//
// StartSpan returns a span you fill in as an operation proceeds; End stores it.
// Spans started from a context that already carries one become its children.
//
//	ctx, span := db.StartSpan(ctx, "summarize")
//	defer span.End(ctx)
//	span.SetModel("gpt-4")
//	span.SetTokens(120, 30)
//	span.SetCost(0.002)
//
// Use Ingest or IngestBatch instead when you already have complete Span values.
//
// # OpenTelemetry
//
// IngestOTLP ingests an OTLP/JSON ExportTraceServiceRequest, the body an
// OpenTelemetry exporter set to http/json sends. The package has no server of
// its own: embed it behind whatever transport your application already has.
//
// # Guarantees
//
// Ingest returns only after the span's WAL record has been fsynced: an
// acknowledged span survives any crash. It is durable on return but becomes
// visible to reads and aggregates shortly afterwards, once a background loop
// has processed it. Call Sync to wait for that, or open the database with
// WithReadYourWrites to have every Ingest do so.
//
// Each span costs one fsync, so write throughput is bounded by the disk.
//
// # Limits
//
// Timestamps are Unix nanoseconds internally and latency is stored in whole
// milliseconds, so Duration is truncated to millisecond precision. Context
// cancellation is honoured between operations and between batch items; an
// fsync already in flight cannot be interrupted.
package wisptrace
