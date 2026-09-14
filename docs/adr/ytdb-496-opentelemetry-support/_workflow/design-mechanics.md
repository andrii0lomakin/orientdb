<!-- workflow-sha: 5db61a37462f0b28965113f39a81b6fcb1ed1340 -->
# YTDB-496 OpenTelemetry support — Mechanics

Implementation detail for the four sections listed below. Each section gathers the full pseudo-implementation, mapping tables, and edge cases an implementer needs. The text stands on its own; concepts named here either define themselves locally or appear under the same name in the companion polished view.

## Slow-query threshold gating

The resolver and its rule type reuse the same machinery as `QueryMonitoringModeResolver` — only the `T` parameter on `TagRule<T>` differs (`Long` instead of `QueryMonitoringMode`):

```java
public final class SlowQueryThresholdResolver {
    private final List<TagRule<Long>> rules;           // immutable, compiled once at startup
    private final Map<String, OptionalLong> cache;     // caches the rule-walk outcome (empty = no rule
                                                       // matched), never the caller's fallback, so a per-TX
                                                       // override cannot leak across transactions.
                                                       // bounded LRU: Collections.synchronizedMap over
                                                       // LinkedHashMap(1024, 0.75f, true) with access-order
                                                       // eviction; INFO-rate-limited (once per 60 s window)
                                                       // on eviction so unique-tag-per-request misuse cannot
                                                       // exhaust the heap

    public long resolve(Optional<String> tag, long defaultNanos) {
        if (tag.isEmpty()) return defaultNanos;
        // Cache only the rule-walk outcome; apply the (per-call, possibly per-TX) fallback outside it.
        return cache.computeIfAbsent(tag.get(), this::walkRules).orElse(defaultNanos);
    }
    // walkRules returns OptionalLong.of(value) on first-wins match, OptionalLong.empty() when no rule
    // matches; values stored as nanoseconds. The fallback (per-TX override, else global default) is
    // applied by resolve() AFTER the cache lookup, so a fallback that varies by transaction is never
    // baked into the shared cache.

    public static SlowQueryThresholdResolver global() { ... }
}
```

The gate sits inside `OTelQueryMetricsListener.queryFinished`, not in the YTDB fire site. Three reasons: (1) other listeners registered alongside the OTel one (custom host implementations) may have their own gating policy and must continue to see every event; (2) the gate is OTel-specific configuration and lives in the OTel module rather than `core`; (3) `QueryDetails` already carries the inputs the gate needs — `executionTimeNanos` arrives as a method parameter, error state arrives through `QueryDetails.getErrorType()`, and the query tag arrives through `QueryDetails.getQuerySummary()` (same source the OTel sem-conv mapping already reads).

Pseudo-implementation at the top of `queryFinished`:

```java
@Override
public void queryFinished(QueryDetails details, long startedAtMillis, long executionTimeNanos) {
  boolean isError = details.getErrorType().isPresent();
  if (!isError) {
    long perTxOrGlobal = details.getSlowQueryThresholdOverrideNanos().orElse(defaultThresholdNanos);
    long thresholdNanos = SlowQueryThresholdResolver.global()
        .resolve(Optional.ofNullable(details.getQuerySummary()), perTxOrGlobal);
    if (thresholdNanos > 0 && executionTimeNanos < thresholdNanos) {
      return;  // gate dropped this query
                // SQL path: returns before spanBuilder(), no allocation.
                // Gremlin path: GremlinSpanLifecycleHook already allocated the span
                // at first hasNext(); the early return leaves a skinny span
                // (no sem-conv attributes set); the hook still ends it at close().
    }
  }
  // Gremlin path: read Span.fromContext(Context.current()), set sem-conv attributes.
  // SQL path: spanBuilder(...).setParent(Context.current()).startSpan(); attributes; span.end(...)
  // dispatched via details.getQuerySource() — see §"Context propagation in embedded".
}
```

`QueryDetails.getErrorType(): Optional<String>` is a new default-empty accessor added to the listener contract. Both fire sites populate it from the caught exception when `statement.execute(...)` (SQL) or the traversal close (Gremlin) throws — the same exception that drives the `error.type` attribute on the emitted span. The accessor exists on the listener contract so any consumer (OTel or custom) can read error state without an additional callback signature.

The standalone commit span is not threshold-gated in this design. Commit duration is operationally interesting at every length (fast commits show throughput, slow commits show lock or fsync contention), and the per-TX volume of commit spans is bounded by transaction count rather than query count. If commit-span volume becomes a problem in practice, a follow-up adds `OPENTELEMETRY_COMMIT_SLOW_THRESHOLD_MILLIS` against the same shape — single global value plus optional per-tag rules, error bypass, gate inside the listener.

### Bundled skinny-span filter

The on-the-wire span count for Gremlin without a downstream drop equals the traversal count regardless of how aggressive the slow-query and heartbeat gates are. The asymmetry between SQL and Gremlin is the cause: SQL gate-drops cost zero allocations because the gate runs ahead of `spanBuilder()`, but Gremlin's lifecycle hook has already created the span at first `hasNext()` to give Path B inner SQL (when a step routes through the SQL entry points) a parent during iteration. The early return then leaves a no-sem-conv span that still flows through `Span.end()` → `SpanProcessor.onEnd()` → `BatchSpanProcessor` → exporter.

`SkinnySpanFilterProcessor` closes that gap by short-circuiting the export step. The processor inspects every span at `onEnd` and forwards only those that either carry a `db.system.name` attribute (the gate let them through) or originate from a non-YTDB instrumentation scope (a host span that happens to flow through the same SDK). Implementation shape:

```java
public final class SkinnySpanFilterProcessor implements SpanProcessor {
    private static final String YTDB_SCOPE = "io.youtrackdb";
    private final SpanProcessor delegate;

    public static SpanProcessor wrap(SpanProcessor delegate) {
        return new SkinnySpanFilterProcessor(delegate);
    }

    @Override public void onStart(Context parentContext, ReadWriteSpan span) {
        delegate.onStart(parentContext, span);
    }

    @Override public boolean isStartRequired() { return delegate.isStartRequired(); }
    @Override public boolean isEndRequired()   { return true; }

    @Override public void onEnd(ReadableSpan span) {
        if (isSkinnyYtdbSpan(span)) return;       // drop: do not forward to delegate
        delegate.onEnd(span);
    }

    private boolean isSkinnyYtdbSpan(ReadableSpan span) {
        if (!YTDB_SCOPE.equals(span.getInstrumentationScopeInfo().getName())) return false;
        return span.getAttribute(SemConvAttributes.DB_SYSTEM_NAME) == null;
    }

    @Override public CompletableResultCode forceFlush() { return delegate.forceFlush(); }
    @Override public CompletableResultCode shutdown()   { return delegate.shutdown(); }
}
```

SDK builder wiring. Both YTDB-owned-SDK paths (server-mode `OpenTelemetryServerPlugin.onAfterActivate()` and embedded `YouTrackDBOpenTelemetry.autoConfigure()`) install the filter through `AutoConfiguredOpenTelemetrySdkBuilder.addTracerProviderCustomizer(...)`:

```java
sdkBuilder.addTracerProviderCustomizer((tpb, cfg) -> {
    if (!cfg.getBoolean(OPENTELEMETRY_GREMLIN_DROP_SKINNY_SPANS, true)) return tpb;
    // The configured BatchSpanProcessor is the last processor added to the builder
    // by the autoconfigure step. Replace it with a skinny-filter wrapper.
    SpanProcessor batch = tpb.getActiveSpanProcessor();   // resolved BatchSpanProcessor
    tpb.clearSpanProcessors();
    tpb.addSpanProcessor(SkinnySpanFilterProcessor.wrap(batch));
    return tpb;
});
```

(`getActiveSpanProcessor()` / `clearSpanProcessors()` are illustrative; the actual builder API uses a `Map<String, SpanProcessor>`-shaped registration that the customizer reads and replaces. The intent the design fixes is "filter wraps batch", not the specific builder mechanics, which follow the pinned OTel SDK version.)

Host-owned SDKs. When the host has wired its own `OpenTelemetry` via `setOpenTelemetry(externalSdk, ownedByYtdb=false, ...)` (or via `GlobalOpenTelemetry.set(...)` before YTDB resolves), YTDB does not modify the SDK and the bundled filter is therefore not installed. Hosts that want the same default skinny-span suppression call the public factory:

```java
SpanProcessor batch = BatchSpanProcessor.builder(otlpExporter).build();
SpanProcessor filtered = YouTrackDBOpenTelemetry.skinnySpanFilter(batch);
SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
    .addSpanProcessor(filtered)
    .build();
```

`YouTrackDBOpenTelemetry.skinnySpanFilter(SpanProcessor delegate): SpanProcessor` is the only new public-API method this feature adds.

Why allocate-then-filter, and not something cheaper. The eager Gremlin-span allocation is forced by the Path B parent-child contract when inner SQL spans exist: inner SQL captures the parent `Context` (hence the parent `span_id`) at its construction, mid-iteration, so the outer span must exist by first `hasNext()`. The gate, by contrast, is a *tail* decision: it depends on the traversal's duration, known only at `close()`. OTel's consistent-drop mechanisms are all *head* decisions taken at span start: a `Sampler` (including `ParentBased`) and `SamplingResult.RECORD_ONLY` decide before any duration is observable, so none can drop a span that turned out fast. That leaves two tail-consistent alternatives, both rejected:

- Collector `tail_sampling` keeps traces consistent (it buffers a whole trace and decides at trace end, so no orphans), but it requires shipping *every* span to the Collector before the decision, which is exactly the wire traffic D46 exists to avoid.
- A lazy outer span with a pre-generated `SpanContext` (stamp a `span_id` into `Context` at first `hasNext()`, then materialize the recording span only if the gate passes at `close()`) fails on the SDK surface: the high-level `Tracer` API does not let you inject a chosen `span_id` (the `IdGenerator` owns it), so emitting the real span later would mean fighting the generator or hand-building `SpanData`, more fragile than the filter.

So the filter at `onEnd` is the optimum under the twin constraints *minimize wire traffic* and *use the high-level SDK*: zero extra bytes on the wire, at the cost of a bounded broken-parent edge case.

Broken-parent radius is small. In the dominant case a fast traversal has fast inner SQL, and the inner SQL gates on the *same* tag/threshold the outer inherited (via the OTel `Context` tag-inheritance rule), so the whole Path B subtree drops together and produces no orphan. The orphan (an inner SQL child surviving under a dropped outer parent) arises only in the parent-fast / child-slow corner: the traversal finished under the threshold but one inner statement individually exceeded it. That child still carries its full sem-conv attribute set and the original `trace_id`, so the loss is the outer Gremlin name and duration only (the next paragraph covers how each backend renders it).

Path B downstream effect. When the filter drops the outer Gremlin parent and the inner SQL child survives, the inner SQL span surfaces in the backend as a span whose `parent_span_id` references a span the backend never saw. The mechanism: the inner SQL wrapper captured the outer Gremlin span via `setParent(capturedContext)` at construction (§"Context propagation in embedded" step 2), so the inner span's OTLP record carries the parent's `span_id` on the wire before the filter ever sees the outer span at `onEnd`. By the time the filter decides to drop the outer, the inner has already been handed to `BatchSpanProcessor` with a populated `parent_span_id` field. Most trace backends (Jaeger, Tempo, Datadog) render the inner as a "broken trace" with the SQL span as a synthetic root carrying the original `trace_id`. The diagnostic loss is bounded: operators see the SQL leaf with its full sem-conv attribute set and the original `trace_id` correlates it back to host-side spans; the only signal that disappears is the outer Gremlin name and its duration. Operators who depend on outer-Gremlin visibility for Path B disable the filter via `OPENTELEMETRY_GREMLIN_DROP_SKINNY_SPANS=false`.

### Edge cases / Gotchas

- Default `0` (emit-all) matches the OTel-standard "emit everything, let downstream samplers decide" pattern and gives operators first-run visibility immediately after `OPENTELEMETRY_ENABLED=true`. Operators on read-heavy workloads who want to drop fast successful queries set a positive value (e.g., `100`) and document the choice in their observability runbook; errors always bypass regardless.
- A query whose duration equals the threshold emits, because the comparison is strictly `<` (less-than). A 100 ms query against a 100 ms threshold passes the gate.
- The gate evaluates before any work the listener would otherwise do: no `tracer.spanBuilder(...)`, no attribute reads from `QueryDetails` beyond `getErrorType()` and `getQuerySummary()` (both cheap, lazy in the impl). Gated-out queries pay only the resolver lookup (bounded-LRU lookup once per distinct tag, O(1) on cache hit) and the listener return.
- Tag-rule cache cardinality blow-up: a misuse-pattern host that emits unique tags per request (e.g., a UUID) would otherwise grow each resolver's cache without bound. Both resolvers use a bounded LRU (`Collections.synchronizedMap` over `LinkedHashMap(1024, 0.75f, true)` with access-order eviction, capacity 1024), so the heap footprint is capped at roughly 2 × 200 KB even under sustained unique-tag-per-request abuse. Eviction logs INFO once per 60 s window with the count of evictions in that window; an evicted tag pays the rule walk again on its next access, and rule walks are deterministic so the resolved value stays stable across miss / re-resolve / re-cache. The heartbeat gate has no resolver and contributes no cache.
- A tag that matches no rule resolves to the per-TX override (`withSlowQueryThresholdMillis`) when the transaction set one, else the global default. The cache records the rule-walk outcome — the matched value, or an empty marker when no rule matched — so future identical tags skip the walk; the fallback (per-TX override, else global default) is applied after the cache lookup, at the resolver's fallback slot, so a fallback that varies by transaction is never baked into the shared cache.
- Conflicting rules (two rules match the same tag): first-wins by insertion order. Operators order rules from most-specific to most-general. Duplicate exact rules: the first one wins; subsequent rules log WARN at startup and are dropped.
- The global default is captured at OTel listener construction time and stored in the `defaultThresholdNanos` final field. Mid-process changes to the global default require an SDK rebuild (host calls `YouTrackDBOpenTelemetry.setOpenTelemetry(...)` again, or the server restarts). The tag-rule list is also compiled once at startup; mid-process rule changes are not supported in YTDB-496 (same constraint as for mode rules). The per-TX override is not subject to either constraint: the fire site reads `FrontendTransaction.getSlowQueryThresholdOverrideNanos()` per query and carries it on `QueryDetails`, so a transaction that calls `withSlowQueryThresholdMillis(...)` takes effect immediately without an SDK rebuild.
- Custom (non-OTel) `QueryMetricsListener` implementations are unaffected. The gate lives only in `OTelQueryMetricsListener`; the listener iteration in `iterateAllQueryListeners()` still fires every registered listener for every query. Custom listeners that want their own threshold semantics can call `SlowQueryThresholdResolver.global()` themselves, or implement their own gating logic entirely.
- Threshold-vs-mode coupling. The gate compares against `executionTimeNanos` whose semantic depends on the resolved `QueryMonitoringMode` for the same tag: active-time under `EXACT` (consumer idle excluded), wall-clock under `LIGHTWEIGHT` (consumer idle included). Operators combining a mode-rule and a threshold-rule for the same tag declare a combined intent. For example, `OPENTELEMETRY_QUERY_MODE_TAG_RULES=findHotpath=EXACT` paired with `OPENTELEMETRY_QUERY_SLOW_THRESHOLD_TAG_RULES=findHotpath=100` means "emit a hot-path span when DB-side active work exceeded 100 ms" — not "real-time exceeded 100 ms". The same threshold value `100` paired with `LIGHTWEIGHT` for a different tag means "real-time exceeded 100 ms". A single numeric threshold therefore has different operator-facing meaning depending on its companion mode rule; operators tuning thresholds across tags with mixed modes account for this by reading the mode×threshold table in §"Query tagging and per-tag rule resolution". Cross-tag dashboard aggregation slices spans on `db.youtrackdb.duration_semantics` to keep histograms semantically homogeneous. Inside a single Path B traversal both the outer Gremlin span and the inner SQL spans gate on the same resolved threshold because inner SQL inherits the outer's tag via `YTDBContextKeys.QUERY_TAG` on the OTel `Context` (see §"Query tagging and per-tag rule resolution" → tag inheritance); no parent-child threshold drift within a traversal. The resolved threshold may come from the query-tag rule, the per-TX override, or the global default (Table B in §"Query tagging and per-tag rule resolution"); the mode coupling applies identically whichever threshold tier won.
- Skinny-filter ordering with host-chained processors. The bundled filter wraps the autoconfigured `BatchSpanProcessor` only. A host that appends its own `SpanProcessor` to the same `SdkTracerProvider` after YTDB has installed the filter will receive every span the SDK emits, including skinny ones, because that processor sits as a sibling of the filter (added to the provider's processor list separately) and is not wrapped by it. A host that wants the filter to apply to its processor too instead wraps the host processor with `YouTrackDBOpenTelemetry.skinnySpanFilter(...)` and adds the wrapper, mirroring what the bundled customizer does for the batch processor. The bundled customizer never touches host-added processors; touching them would violate the host-owned-SDK contract and could double-wrap an exporter chain.
- Path B broken-parent rendering across backends. With the filter ON and the gate dropping the outer Gremlin span, the inner SQL child surfaces with a `parent_span_id` the backend never saw. Jaeger renders this as a root span (the SQL leaf) under the original `trace_id`, with the parent slot greyed out; Tempo renders the SQL span as a standalone root; Datadog APM shows the SQL span as the root of the resource trace. None of the three drop or corrupt the trace; the diagnostic loss is the outer Gremlin name and duration only. Operators who depend on outer-Gremlin visibility for Path B disable the filter for the affected workload.
- Cross-instrumentation scope safety. The filter's `InstrumentationScopeInfo.name == "io.youtrackdb"` check is the load-bearing safety against collateral damage. A host that wires the YTDB SDK alongside auto-instrumentation for HTTP, gRPC, or its own application code sees those spans flow through `onEnd` unchanged because their scope name differs. Custom YTDB-adjacent code that creates spans via `GlobalOpenTelemetry.getTracer("io.youtrackdb")` (sharing the name) WOULD be subject to the filter; the workaround is to use a distinct scope name (`io.youtrackdb.host`, the host's own package) when authoring custom YTDB-adjacent instrumentation.
- Filter and `forceFlush` / `shutdown`. The chain wrapper delegates both lifecycle calls unconditionally, so flush semantics during graceful shutdown are identical to the delegate's. Spans dropped by the filter were never added to the batch buffer in the first place, so a flush sees nothing for them.

### References
- D-records: D16 (slow-query threshold inside OTel listener, error bypass, per-tag rules via `TagRule<Long>`), D46 (bundled `SkinnySpanFilterProcessor` wrapping `BatchSpanProcessor` in YTDB-owned SDKs; `InstrumentationScopeInfo.name`-scoped to `"io.youtrackdb"`; public `skinnySpanFilter(SpanProcessor)` factory for host-owned SDKs; gated by `OPENTELEMETRY_GREMLIN_DROP_SKINNY_SPANS=true`)

## Time-based sampling

The heartbeat gate uses one `AtomicLong` and one final-field interval; no resolver, no tag read, no cache. Composition rule: the heartbeat runs first at the top of `OTelQueryMetricsListener.queryFinished`. If it claims the slot, the query emits and the slow-query gate is bypassed. If it does not claim, the slow-query gate evaluates as before. Error queries bypass both gates and always emit; the heartbeat clock is not advanced by error emissions, so a stream of errors does not suppress the heartbeat sample of successful queries.

Pseudo-implementation:

```java
private final AtomicLong lastHeartbeatNanos = new AtomicLong(0);
private final long defaultHeartbeatNanos;  // populated at SDK init from config

@Override
public void queryFinished(QueryDetails details, long startedAtMillis, long executionTimeNanos) {
  boolean isError = details.getErrorType().isPresent();
  if (!isError) {
    if (defaultHeartbeatNanos > 0 && tryClaimHeartbeat(defaultHeartbeatNanos)) {
      // heartbeat slot claimed; fall through to span emission
    } else {
      Optional<String> tag = Optional.ofNullable(details.getQuerySummary());
      long thresholdNanos = SlowQueryThresholdResolver.global()
          .resolve(tag, details.getSlowQueryThresholdOverrideNanos().orElse(defaultThresholdNanos));
      if (thresholdNanos > 0 && executionTimeNanos < thresholdNanos) {
        return;  // both gates dropped this query
      }
    }
  }
  // existing span construction (sem-conv attributes, parent context, span.end)
}

private boolean tryClaimHeartbeat(long heartbeatNanos) {
  long now = System.nanoTime();
  long last = lastHeartbeatNanos.get();
  if (now - last < heartbeatNanos) return false;
  return lastHeartbeatNanos.compareAndSet(last, now);
}
```

The `AtomicLong.compareAndSet` resolves the multi-thread race: under load several queries finish within the same heartbeat window and all see `now - last >= heartbeatNanos`, but only the first CAS succeeds; losers fall through to the slow-query gate. Result: exactly one query per heartbeat window claims the heartbeat slot; the rest either pass the slow-query gate, get dropped, or emit on error.

### Edge cases / Gotchas

- The heartbeat clock is process-global per `OTelQueryMetricsListener` instance (one `AtomicLong`), not per-thread. This matches "one sample every N ms of wall-clock" semantics regardless of QPS or thread count. Per-thread heartbeat would emit one sample per thread per N ms, which scales noise with parallelism.
- No per-tag override exists in v1. The heartbeat samples the workload as a whole; whichever tag arrives first after the interval claims the slot. This biases visibility toward high-QPS tags by construction, which is the intent: heartbeat answers "what is the system spending its time on right now". Operators who need per-tag sampling streams configure downstream OTel pipeline filtering on `db.query.summary` against the global heartbeat output.
- A query that is slow AND wins the heartbeat CAS emits only once, not twice. The composition order (heartbeat first, slow-query second) skips the slow-query gate after a heartbeat claim.
- Error queries always emit, regardless of heartbeat or slow-query state. The heartbeat clock is NOT advanced by error emissions; an error every N ms does not suppress the heartbeat sample of successful queries.
- Heartbeat-emitted spans look identical to slow-query-emitted spans in the trace viewer; no `sample.reason` attribute distinguishes them in YTDB-496. Operators wanting to tell "this was a heartbeat sample" apart from "this was actually slow" compare span duration against the configured slow-query threshold downstream. Adding an explicit attribute is a future-ticket concern (low cost; would land alongside the structured-attributes extension).
- Mid-process changes to the heartbeat interval require an SDK rebuild, same constraint as the slow-query threshold default.
- A heartbeat interval shorter than the CAS cost (single uncontended CAS on a hot AtomicLong, ~10 ns) is wasted: the gate evaluates more often than it can possibly emit. Practical minimum is around 1 ms; sensible production defaults are 100 ms to 10 s.
- Disabling the heartbeat (interval = 0) makes the gate evaluate `if (0 > 0 && …)`, which short-circuits on the first conjunct. Zero overhead for the disabled case.
- Custom (non-OTel) `QueryMetricsListener` implementations are unaffected. The heartbeat gate lives only in `OTelQueryMetricsListener`; the listener iteration in `iterateAllQueryListeners()` still fires every registered listener for every query. Custom listeners wanting their own sampling implement it in their own callback.

### References
- D-records: D18 (time-based heartbeat sampling alongside slow-query gate, AtomicLong CAS for race-free single-emit per window, single global interval with no per-tag override)

## Commit slow-query gating

The gate sits inside `OTelTransactionMetricsListener.writeTransactionCommitted`, not in the YTDB fire site. Same reasoning as the query gate (D16): other transaction listeners registered alongside the OTel one may have their own policy and must continue to see every event; the gate is OTel-specific configuration and lives in the OTel module; `TransactionDetails` already carries the input the gate needs (`executionTimeNanos` arrives as a method parameter; no error state to read because successful commits route through `writeTransactionCommitted` while failed commits route through `writeTransactionFailed` and bypass the gate by construction).

Pseudo-implementation at the top of the callback:

```java
private final long defaultCommitThresholdNanos;  // populated at SDK init from config

@Override
public void writeTransactionCommitted(TransactionDetails details,
                                      long committedAtMillis,
                                      long executionTimeNanos) {
  if (defaultCommitThresholdNanos > 0 && executionTimeNanos < defaultCommitThresholdNanos) {
    return;  // fast successful commit — skip span allocation
  }
  // existing span construction (sem-conv attributes, parent context, span.end)
}

@Override
public void writeTransactionFailed(TransactionDetails details,
                                   long failedAtMillis,
                                   long executionTimeNanos,
                                   Exception cause) {
  // unchanged — failed commits always emit, carrying error.type
}
```

The `defaultCommitThresholdNanos` final field is initialized at OTel listener construction from `OPENTELEMETRY_COMMIT_SLOW_THRESHOLD_MILLIS` multiplied to nanoseconds. Mid-process changes to the global default require an SDK rebuild — same constraint as the query-side threshold default.

### Edge cases / Gotchas

- Default `0` preserves today's always-emit behavior. Every existing test setup that asserts on a commit span continues to pass without configuration change. Hosts that want to drop fast commits configure a positive value explicitly. The query gate's default is also `0` (emit-all) — operators on read-heavy workloads tune the query threshold upward (e.g., to `100` ms) to drop fast successful queries; commits stay at `0` longer because commit volume is bounded by transaction count.
- A commit whose duration equals the threshold emits, because the comparison is strictly `<` (less-than). A 100 ms commit against a 100 ms threshold passes the gate.
- Failed commits (`writeTransactionFailed`) bypass the gate unconditionally. The caught cause populates `error.type` and the span status is set to ERROR. A stream of failing commits emits one span per failure regardless of duration.
- No per-tag override exists in v1. Commits fire at the transaction boundary and have no per-statement query tag context. If operators need per-database or per-host commit thresholds later, the natural extension is a `TagRule<Long>` resolver keyed on `TransactionDetails.getDatabaseName()` and configured via `OPENTELEMETRY_COMMIT_SLOW_THRESHOLD_DATABASE_RULES`; the resolver hierarchy already exists (mirrors the query-side `SlowQueryThresholdResolver`), so adding the per-database axis is one row in the OTel listener's gate. Not in v1 scope because no production demand has surfaced.
- The gate evaluates before any work the listener would otherwise do. No `tracer.spanBuilder(...)`, no attribute reads beyond the method parameter. Gated-out commits pay only the comparison and the listener return. The cost shape matches the query gate's "drop before allocation" property.
- Custom (non-OTel) `TransactionMetricsListener` implementations are unaffected. The gate lives only in `OTelTransactionMetricsListener`; the listener iteration in the merged-snapshot view still fires every registered listener for every commit. Custom listeners that want their own gating semantics implement it inside their callbacks.

### References
- D-records: D38 (commit-side slow-query threshold inside OTel listener, error bypass, single global threshold, no per-tag rules in v1)

## OpenTelemetry logs integration

The chokepoint is `SLF4JLogManager.log(...)` (`core/src/main/java/com/jetbrains/youtrackdb/internal/common/log/SLF4JLogManager.java:38-103`), the single method every YTDB log call crosses. The logs integration adds a `LogAppenderHook` interface alongside `SLF4JLogManager`, a `CopyOnWriteArrayList<LogAppenderHook>` field on `SLF4JLogManager` itself, an `installAppenderHook(LogAppenderHook)` / `removeAppenderHook(LogAppenderHook)` accessor pair, and one new line inside `log(...)` at the existing emit point (right before `logEventBuilder.log()` on line 98) that iterates the hook list. Before each iteration the manager skips records whose `requesterName` starts with `io.opentelemetry.` or equals `io.youtrackdb.otel.appender`; this name-prefix filter blocks the cross-thread recursive cycle that the OTel exporter would otherwise create through a `jul-to-slf4j` bridge (the exporter writes on its own thread pool where the per-thread re-entrance guard does not apply). Operators who need to admit specific OTel-internal loggers override the prefix via `OPENTELEMETRY_LOGS_LOGGER_EXCLUSIONS` (comma-separated full prefixes; defaults to the two values above). The hook signature is binding-agnostic: it takes the resolved requester class name, the resolved database name, the slf4j `Level`, the already-formatted message string, and the optional `Throwable`. Hooks fire after the per-logger `isEnabledForLevel(level)` filter SLF4J already applies, so a hook subscribed at `INFO` for a logger configured at `WARN` sees no records below `WARN`. The OTel module ships `OTelLogAppender` as the only built-in implementation; the hook surface is not internal-only because a host that wants OTel-independent log routing can register its own.

Severity mapping (slf4j `event.Level` → OTel sem-conv `severityNumber`):

| slf4j Level | OTel `severityNumber` | OTel `severityText` |
|---|---|---|
| `TRACE` | 1 | `TRACE` |
| `DEBUG` | 5 | `DEBUG` |
| `INFO` | 9 | `INFO` |
| `WARN` | 13 | `WARN` |
| `ERROR` | 17 | `ERROR` |

The mapping has no `FATAL` row because slf4j `event.Level` carries no `FATAL` constant (it stops at `ERROR`). The `OPENTELEMETRY_LOGS_MIN_SEVERITY` config accepts `FATAL` for forward compatibility but treats it identically to `ERROR` (severity number 17).

Pseudo-implementation:

```java
public final class OTelLogAppender implements LogAppenderHook {
  private final Logger otelLogger;
  private final int minSeverityNumber;
  private final boolean includeMessageBody;            // OPENTELEMETRY_LOGS_INCLUDE_MESSAGE_BODY (default false)
  private final ThreadLocal<Boolean> reentrant = ThreadLocal.withInitial(() -> false);
  private static final java.util.logging.Logger FALLBACK =
      java.util.logging.Logger.getLogger("io.youtrackdb.otel.appender");

  public OTelLogAppender(LoggerProvider provider, Severity minSeverity, boolean includeMessageBody) {
    this.otelLogger = provider.get("io.youtrackdb");
    this.minSeverityNumber = minSeverity.getSeverityNumber();
    this.includeMessageBody = includeMessageBody;
  }

  @Override
  public void onLog(String requesterName, String dbName,
                    org.slf4j.event.Level level,
                    String formatString, String formattedMessage,
                    Throwable thrown, long eventEpochNanos) {
    if (reentrant.get()) return;
    int severityNumber = mapSlf4jLevelToOtel(level);
    if (severityNumber < minSeverityNumber) return;

    reentrant.set(true);
    try {
      // Body-policy default-deny: ship the SLF4J format string unless the host opts in.
      // Keeps parameter values (raw SQL, record content, user identifiers) out of the log body.
      String body = includeMessageBody ? formattedMessage : formatString;
      LogRecordBuilder builder = otelLogger.logRecordBuilder()
          .setSeverity(Severity.fromSeverityNumber(severityNumber))
          .setSeverityText(level.name())
          .setTimestamp(eventEpochNanos, TimeUnit.NANOSECONDS)   // original log-call wall-clock
          .setObservedTimestamp(Instant.now())                    // appender invocation wall-clock
          .setBody(body);

      if (requesterName != null) {
        builder.setAttribute(AttributeKey.stringKey("code.namespace"), requesterName);
      }
      if (dbName != null) {
        builder.setAttribute(AttributeKey.stringKey("db.namespace"), dbName);
      }
      if (thrown != null) {
        builder.setAttribute(AttributeKey.stringKey("exception.type"), thrown.getClass().getName());
        builder.setAttribute(AttributeKey.stringKey("exception.message"),
            thrown.getMessage() == null ? "" : thrown.getMessage());
        builder.setAttribute(AttributeKey.stringKey("exception.stacktrace"),
            stackTraceToString(thrown));
      }
      // hard context: Context.current() picks up the active span automatically.
      builder.emit();
    } catch (RuntimeException | LinkageError | AssertionError t) {
      FALLBACK.log(java.util.logging.Level.WARNING, "OTel log emit failed", t);
    } finally {
      reentrant.set(false);
    }
  }
}
```

The `setTimestamp(eventEpochNanos, NS)` carries the SLF4J `LoggingEvent.getTimeStamp()` value (the wall-clock at the original log call, captured by the SLF4J binding before YTDB's hook fires) lifted from milliseconds to nanoseconds. The OTel sem-conv `Timestamp` field therefore matches the original log-call time even for high-frequency `DEBUG`/`TRACE` records where dispatch latency would otherwise drift `Instant.now()` by tens of microseconds. The `setObservedTimestamp(Instant.now())` follows sem-conv guidance for the "log was observed by the appender" axis. The fallback `Logger.getLogger("io.youtrackdb.otel.appender")` replaces the `Handler.reportError` channel a JUL Handler would have used. The fallback logger is itself part of the SLF4J→JUL or SLF4J→Logback dispatch chain (via slf4j-jdk14 in server mode), so its output reaches the operator's normal log sink without re-entering the OTel hook (the appender's own logger name is filtered out at install time to keep the cycle impossible).

**Body-policy default-deny.** The `body` slot is set from `formatString` when `OPENTELEMETRY_LOGS_INCLUDE_MESSAGE_BODY=false` (the default) and from `formattedMessage` when `true`. The default-false matches the trace-pillar discipline of `OPENTELEMETRY_QUERY_INCLUDE_PARAMETERS=false`: log lines whose format string is `"Query failed: {} with args {}"` ship that string as the body, rather than `"Query failed: SELECT FROM User WHERE id = 42 with args [42]"`. The `exception.stacktrace` attribute (set from `Throwable.getStackTrace()`) and `exception.message` (from `Throwable.getMessage()`) are NOT gated by the same flag because the throwable identity itself is the load-bearing diagnostic signal for failure investigation; hosts that need to redact those install a host-side OTel `LogRecordProcessor` at the collector boundary. The flag is a default-deny knob for the body slot only.

The `SLF4JLogManager` hook iteration runs after the existing `isEnabledForLevel(level)` filter and before the marker/format work the existing path does. Hooks see the same formatted message SLF4J emits, not the raw format string and varargs; that keeps each hook from re-running `String.format(...)` and gives every hook a consistent view.

### Hard-context correlation across threads

Every span the OTel listeners create runs inside a `try (Scope s = span.makeCurrent())` block. While that scope is open on the current thread, `Context.current()` returns a context carrying the span; any log call from that thread between `makeCurrent()` and `s.close()` reaches `OTelLogAppender.onLog(...)` through the hook iteration, and the `LogRecordBuilder.emit()` call attaches the same span context to the log record. Trace viewers that support log-to-trace correlation (Grafana with the OTel collector, Jaeger with the unified UI) render the log inside the span's timeline.

The correlation is automatic only when the log call originates on the thread that owns the span scope. Logs emitted from a background thread spawned inside the span scope (a thread-pool task, a `CompletableFuture` continuation) lose the correlation unless the caller propagates the context. Same caveat as for child-span creation across threads, which OTel covers through `Context.taskWrapping(...)` / `Context.makeCurrent()` on the receiving thread.

### Edge cases / Gotchas

- A host that binds `slf4j-nop` (no-op SLF4J provider): SLF4J's per-logger `isEnabledForLevel(...)` returns `false` for every level under the NoOp provider, so `SLF4JLogManager.log(...)` short-circuits before the hook iteration and `OTelLogAppender.onLog(...)` is never invoked. This is the documented behavior — a host that explicitly disables logging gets no OTel logs either. Operators who want OTel logs without SLF4J output bind any non-NoOp provider with its level set high enough to suppress local output (e.g., `slf4j-jdk14` with `INFO` floor).
- High-frequency `DEBUG` / `TRACE` records (a tight loop logging every page read at `DEBUG`): SLF4J's per-logger level filter drops them before the hook iteration when the bound provider's level is set higher. When the bound level admits them, the severity floor drops them at the top of `onLog(...)` before any `LogRecordBuilder` allocation. Operators tune `OPENTELEMETRY_LOGS_MIN_SEVERITY` to control OTel-side volume independently from the bound provider's level config.
- An exception thrown from `OTelLogAppender.onLog(...)`: the `SLF4JLogManager` hook iteration wraps each hook call in `try { hook.onLog(...); } catch (Exception | LinkageError | AssertionError t) { /* fallback */ }` (same union as the listener wrappers per the exception-isolation contract). A throwing hook is logged via the same fallback `Logger.getLogger("io.youtrackdb.otel.appender")` channel and the next hook still fires; YTDB's own log call returns normally. `VirtualMachineError` and `ThreadDeath` propagate.
- Concurrent `setOpenTelemetry` calls during active logging: when the facade swaps SDKs, the previously-registered `OTelLogAppender` is removed via `SLF4JLogManager.removeAppenderHook(oldHook)` before the new one is installed, so each log record reaches exactly one OTel pipeline. The window between remove and install is small but non-zero; log records in that window are dropped on the OTel side but still reach the bound SLF4J provider (the existing path is untouched). Multiple `LogAppenderHook` implementations coexist by being separate list entries.
- Bootstrap-time logs (records emitted before `OPENTELEMETRY_LOGS_ENABLED` is read): these reach only the bound SLF4J provider, which is the desired behavior. Operators who need OTel coverage of bootstrap logs configure `OPENTELEMETRY_LOGS_ENABLED=true` via a JVM property (`-Dyoutrackdb.opentelemetry.logs.enabled=true`) so the flag is set before the first log call.
- Recursive logging from inside the OTel exporter: if the OTel exporter emits a log via `LogManager`, the hook would see its own log and feed it back into OTel, creating an unbounded loop. Two complementary guards cover the cases. The thread-local `reentrant` guard above breaks the same-thread cycle by short-circuiting `onLog(...)` when an `onLog(...)` is already on the stack for this thread. The cross-thread cycle (OTel exporter writes a log on its own thread pool via `jul-to-slf4j` bridge, which routes JUL into SLF4J and back into `LogManager`) is caught by the `requesterName`-prefix filter in `SLF4JLogManager.log(...)`: records whose requester logger name starts with `io.opentelemetry.` (the OTel SDK's own logger namespace) or equals `io.youtrackdb.otel.appender` (our fallback channel) skip the hook iteration entirely. The `OPENTELEMETRY_LOGS_LOGGER_EXCLUSIONS` config entry lets operators tune the prefix list when a deployment needs to admit specific OTel-internal logger names.
- Hosts already using the OTel `opentelemetry-instrumentation-logback-appender` (or its log4j equivalent) on their SLF4J binding: that path catches every log record SLF4J emits, including YTDB's. Running both side-by-side double-emits each YTDB log record (once via the host's appender, once via YTDB's hook). Operators run one or the other, not both. The hook is the recommended path because it carries the `Context.current()` read at the YTDB invocation site, not at the host's appender callback site (potentially on a different thread).

### References
- D-records: D34 (single-chokepoint `LogAppenderHook` invoked inside `SLF4JLogManager.log`, severity-floor pre-filter, hard-context inheritance from `Context.current()`), D35 (thread-local re-entrance guard against recursive logging from inside the OTel exporter)
- Invariants: Listener exception isolation (the hook follows the same isolation contract; a throw from `onLog` is caught at the iteration site and reported via the fallback `Logger.getLogger("io.youtrackdb.otel.appender")` channel)

## Metrics integration

The bridge surfaces two layers: sem-conv DB metrics with names defined by the OpenTelemetry spec, and YTDB-specific gauges under the `youtrackdb.*` namespace. The CoreMetrics inventory carries some of these today; this design adds the rest alongside the `MetricGroup` enum that drives the included-groups filter. Each row is annotated `(existing)` when its profiler source already lives in `core/.../profiler/metrics/CoreMetrics.java`, or `(new)` when this design adds the underlying `MetricDefinition` plus the writer site that populates it.

**Sem-conv v1.33.0 DB metrics (curated subset):**

| OTel metric name | Stability | Instrument | YTDB source |
|---|---|---|---|
| `db.client.connection.count` | stable | `ObservableLongUpDownCounter` | active session count read from `DatabaseSessionRegistry` (new) |
| `db.client.operation.duration` | stable | `ObservableDoubleHistogram` | query and commit `executionTimeNanos` aggregated across the last collection period — sourced from the SQL and Gremlin listener fire sites, not from `MetricsRegistry`, so no new profiler-side metric is needed |
| `db.client.response.returned_rows` | experimental | `ObservableDoubleHistogram` | row-count distribution from `QueryDetails.getResultCount(): OptionalLong` (the listener contract adds the accessor; the SQL fire site populates it from the `InstrumentedSqlResultSet` wrapper's per-`next()` row counter, which is correct for both `LocalResultSet` and `CachedResultSetView` inner result-sets per YTDB-820 coordination; the Gremlin path populates it from `YTDBQueryMetricsStep`'s row counter, incremented after each successful `super.next()` on the terminal step, uniformly across Path A and Path B because the step is appended at the pipeline tail by `YTDBQueryMetricsStrategy.apply`) |

**YTDB-specific (`youtrackdb.*` namespace):**

| OTel metric name | Group | Instrument | Profiler source |
|---|---|---|---|
| `youtrackdb.cache.hit_ratio` | `cache` | `ObservableDoubleGauge` | `CoreMetrics.CACHE_HIT_RATIO` (existing — `Ratio`, cache hits over reads, 60s window) |
| `youtrackdb.disk.read_rate_bps` | `cache` | `ObservableDoubleGauge` | `CoreMetrics.DISK_READ_RATE` (existing — `TimeRate`, bytes per second, 60s window) |
| `youtrackdb.disk.write_rate_bps` | `cache` | `ObservableDoubleGauge` | `CoreMetrics.DISK_WRITE_RATE` (existing — `TimeRate`, bytes per second, 60s window) |
| `youtrackdb.cache.page_reads_total` | `cache` | `ObservableLongCounter` | `CoreMetrics.PAGE_READ_COUNT` (new — `Counter`, monotonic page-read count populated by the disk-cache page-fault path) |
| `youtrackdb.cache.evictions_total` | `cache` | `ObservableLongCounter` | `CoreMetrics.FILE_EVICTION_RATE` (existing — converted from rate to monotonic counter at OTel-bridge translation time) |
| `youtrackdb.wal.pending_bytes` | `wal` | `ObservableLongGauge` | `CoreMetrics.WAL_PENDING_BYTES` (new — `Gauge<Long>`, in-flight WAL backlog read at the WAL writer's `flushPending()` site) |
| `youtrackdb.wal.flush_rate_bps` | `wal` | `ObservableDoubleGauge` | `CoreMetrics.WAL_FLUSH_RATE` (new — `TimeRate`, WAL flush bytes per second, 60s window) |
| `youtrackdb.lock.contention_count` | `locks` | `ObservableLongCounter` | `CoreMetrics.LOCK_WAIT_COUNT` (new — `Counter`, monotonic wait-event count populated by `ReadWriteLock` wrappers in the storage layer) |
| `youtrackdb.storage.size_bytes` | `storage` | `ObservableLongGauge` (per `database` attribute) | `CoreMetrics.DATABASE_SIZE_BYTES` (new — per-`MetricScope.Database` `Gauge<Long>` read from the storage layer's `getSizeOnDisk()`) |
| `youtrackdb.transaction.commit_rate` | `transactions` | `ObservableDoubleGauge` | `CoreMetrics.TRANSACTION_WRITE_RATE` (existing — `TimeRate`, write commits per second, 60s window) |
| `youtrackdb.transaction.rate` | `transactions` | `ObservableDoubleGauge` | `CoreMetrics.TRANSACTION_RATE` (existing — `TimeRate`, all transactions per second, 60s window) |
| `youtrackdb.transaction.rollback_rate` | `transactions` | `ObservableDoubleGauge` | `CoreMetrics.TRANSACTION_WRITE_ROLLBACK_RATE` (existing — `TimeRate`, write rollbacks per second, 60s window) |
| `youtrackdb.transaction.active_count` | `transactions` | `ObservableLongGauge` | `CoreMetrics.ACTIVE_TX_COUNT` (existing — per-database `Gauge<Integer>`) |
| `youtrackdb.transaction.oldest_age_seconds` | `transactions` | `ObservableLongGauge` | `CoreMetrics.OLDEST_TX_AGE` (existing — per-database `Gauge<Long>`) |

Sem-conv stability matters for dashboard authors: `stable` metrics will keep their names and semantics in future spec revisions; `experimental` ones may rename or change attribute shapes between sem-conv versions. The bridge surfaces both, but a downstream dashboard that depends on experimental metrics needs to track the sem-conv changelog between v1.33.0 and whatever version YTDB pins next.

### MetricsRegistry enumeration and lazy-registration API

`OTelMetricsBridge` cannot rely on call-site knowledge of every `MetricDefinition` — this design adds enough new ones that a hand-maintained mirror list would drift. Two new public-API methods on `MetricsRegistry` close the gap:

```java
public interface MetricVisitor {
  void visit(String fullyQualifiedName, MetricDefinition<?, ?> def, Metric<?> metric);
}

public void forEachMetric(MetricVisitor visitor);  // walks GLOBAL + every DatabaseMetrics group, calls visitor exactly once per metric instance currently registered

public record MetricRegistrationEvent(
    String fullyQualifiedName,
    MetricDefinition<?, ?> def,
    Metric<?> metric,
    @Nullable String databaseName  // null for GLOBAL scope metrics
) {}

public void addRegistrationListener(Consumer<MetricRegistrationEvent> listener);
public void removeRegistrationListener(Consumer<MetricRegistrationEvent> listener);
```

Implementation: `MetricsRegistry` holds a `CopyOnWriteArrayList<Consumer<MetricRegistrationEvent>>` and fires every listener inside `MetricsGroup.init(...)` (the existing `computeIfAbsent` site) when a new metric is created. The fire is synchronous on the registering thread; listeners that need to do heavy work (registering an OTel `ObservableInstrument` is non-trivial) must schedule it onto their own executor. `OTelMetricsBridge` posts to its scheduled executor so the registration thread is not blocked. Each listener fire runs inside `try { listener.accept(event); } catch (Exception | LinkageError | AssertionError t) { LogManager.instance().warn(this, "metric-registration listener threw", t); }`, so a misconfigured listener logs at WARN and the database-open path completes — the API enforces exception isolation rather than relying on listener-side discipline.

`forEachMetric(...)` walks `globalMetrics.mGroup.metrics`, then iterates `perDatabaseMetrics.values()` walking each `DatabaseMetrics.mGroup.metrics`. The walk is a consistent snapshot of `ConcurrentHashMap.entrySet()` — concurrent registrations during the walk may or may not be visible, but every metric registered before `forEachMetric` was called is visited exactly once. The registration listener picks up anything added during or after the walk, so the combination of `forEachMetric` at `start()` plus the listener subscription is a complete enumeration with no race window.

### Async instrument lifecycle

`OTelMetricsBridge.start()` does three things at SDK init:

1. Call `Profiler.getMetricsRegistry().forEachMetric(...)` to enumerate every currently-registered metric, build a `Map<String, ObservableInstrument>` keyed by the OTel metric name, and register each `ObservableInstrument` against the `SdkMeterProvider`'s `Meter` (`provider.get("io.youtrackdb")`). The callback closure captures the source `Metric` reference, not the value, so each collection cycle re-reads through the registry.
2. Subscribe a `Consumer<MetricRegistrationEvent>` via `registry.addRegistrationListener(...)`. Each event posts a task onto the bridge's `ScheduledExecutorService` that does the same name-build + `ObservableInstrument` register dance as step 1 for the newly-added metric. This covers the per-database metrics created lazily when `Profiler.getMetricsRegistry().databaseMetric(def, dbName)` is first invoked for a database opened after `start()` ran.
3. Schedule a periodic task at `OPENTELEMETRY_METRICS_PERIOD_MILLIS`. The SDK's `PeriodicMetricReader` drives the actual export; the bridge's task re-reads YTDB-side time-windowed metrics (`TimeRate`, `Ratio`) via `getRate()`/`getRatio()` independently of the OTel reader. `Meter` flushes those windows internally every `flushRateTicks`; the task keeps the value the exporter sees fresh between polls.

`OTelMetricsBridge.stop()` calls `registry.removeRegistrationListener(...)`, cancels the scheduled task, calls `unregister()` on each `ObservableInstrument` (drops the SDK-side callback registration), and clears the map. Idempotent: stopping a stopped bridge is a no-op.

`OTelMetricsBridge.refresh()` is a test seam: it forces a synchronous read through each registered callback, so unit tests can assert on the exporter side without waiting for the periodic reader's interval.

### Counter group filter

Operators with cardinality constraints opt into subsets via `OPENTELEMETRY_METRICS_INCLUDED_GROUPS`. The bridge classifies each profiler metric into one of six groups at registration time, reading a `group()` annotation on `MetricDefinition` (an enum with values `QUERIES`, `CACHE`, `STORAGE`, `WAL`, `LOCKS`, `TRANSACTIONS`, set per `Metric` at construction time — this design adds the enum and the annotation). When the included-groups set is non-empty, the bridge skips `register()` for metrics whose group is excluded; the corresponding OTel metric simply does not appear at the exporter.

Group classification table (drives both `MetricDefinition.group()` returns and the filter parsing):

| Group | Examples (profiler-side) |
|---|---|
| `queries` | query duration histogram, query throughput, result-row distribution |
| `cache` | page-cache hit ratio, page reads, eviction count |
| `storage` | per-database size, page count, allocated bytes |
| `wal` | pending bytes, flush rate, segment count |
| `locks` | contention count, wait time histogram, deadlock count |
| `transactions` | active count, commit rate, rollback count |

Operators wanting metrics on storage health but not query throughput configure `OPENTELEMETRY_METRICS_INCLUDED_GROUPS=storage,wal,locks`. The default empty value enables every group, matching "metrics ON = everything" semantics; a host that wants nothing at all leaves `OPENTELEMETRY_METRICS_ENABLED=false` rather than emptying the included list.

### Edge cases / Gotchas

- High-cardinality attributes: a `youtrackdb.storage.size_bytes` per-database gauge fans out one OTel data point per `database` attribute value at every collection cycle. Hosts with 10k+ databases see 10k+ data points per period; the exporter side has to absorb that. The bridge does NOT add per-class or per-RID attributes — `MetricScope.Class` profiler entries collapse to one OTel data point per (metric, database) pair. If per-class breakdown is needed downstream, the host configures a dedicated OTel exporter that views the raw profiler dump.
- Bridge thread vs profiler-thread contention: the profiler's own collection threads (the `ScheduledExecutorService` passed to `Profiler` constructor) may be updating a `TimeRate` or `Ratio` while the bridge's callback reads through it. Both sides use lock-free reads on the metric primitives (`Gauge.value()` is a volatile read; `TimeRate.getRate()` snapshots an `AtomicReference`), so the contention surface is the `AtomicReference` CAS in `Meter`'s internal flush. Worst case: the bridge reads a sample one window behind reality. Acceptable for 10-second collection cadence.
- Exporter back-pressure: OTel `PeriodicMetricReader` blocks the SDK's internal export thread when the configured exporter (OTLP, Prometheus) backs up. The bridge's scheduled task is independent of the reader — it advances time-windowed YTDB metrics regardless of exporter state — so back-pressure on the OTel side does not stall YTDB's in-JVM metric collection.
- SDK swap during active metrics: when `setOpenTelemetry` swaps SDKs, the old `OTelMetricsBridge.stop()` runs before the new one's `start()`, so each `ObservableInstrument` is registered against exactly one `SdkMeterProvider` at a time. The window between stop and start is small; any metric reader poll in that window sees zero data points (the SDK-side callbacks are unregistered).
- Disabled metrics with a host-wired SDK: when `OPENTELEMETRY_ENABLED=true` and `OPENTELEMETRY_METRICS_ENABLED=false`, the bridge is not started even if the host's OTel SDK carries a real `MeterProvider`. The host's other instrumentations continue to emit; only YTDB's bridge stays silent. This is intentional — a host that wants YTDB metrics specifically must opt them in independently of the master switch.
- Profiler not initialized: if the bridge is started before `Profiler.onStartup()` completes (race during very early bootstrap), the initial `forEachMetric(...)` walk visits zero metrics. The `addRegistrationListener(...)` subscription still fires for every metric `Profiler.onStartup()` subsequently creates, so the bridge ends up fully wired without a separate retry path. No WARN is emitted in this case — the late-arriving registration events are the expected wiring sequence under early bootstrap.
- Late-DB-open registration event: when `databaseMetric(def, dbName)` first runs for a new database, the registration listener fires synchronously on the registering thread. The bridge handler posts the OTel `ObservableInstrument` registration to its own `ScheduledExecutorService` and returns immediately, so the database-open path is not blocked by OTel-side allocation. The first metric reader poll after the registration sees the new instrument; polls before the registration land see no data point for that database (consistent with the database simply not having been registered yet).
- Concurrent enumeration race: a database opened concurrently with `start()`'s `forEachMetric(...)` walk may or may not be visited by the walk — `ConcurrentHashMap.entrySet()` is weakly consistent. Either outcome is correct: the `addRegistrationListener(...)` subscription registered before the walk catches anything the walk missed, so every metric registers exactly once across the combination of walk + listener fires (the listener's idempotence check on `registered.containsKey(name)` handles the rare case where both paths see the same registration event).

### References
- D-records: D36 (OTelMetricsBridge surfaces `Profiler.getMetricsRegistry()` via OTel async instruments at a configurable period, with a scheduled task re-reading YTDB-side `TimeRate`/`Ratio` rates, flushed internally by `Meter`, independently of the OTel reader), D37 (group-based opt-in via `OPENTELEMETRY_METRICS_INCLUDED_GROUPS` with six-group taxonomy `queries`/`cache`/`storage`/`wal`/`locks`/`transactions`)
- Invariants: Listener exception isolation (callback exceptions inside `ObservableInstrument` callbacks are caught and reported via OTel's own error handler, never propagating to the profiler), Counter source untouched (the bridge reads; the profiler keeps writing on its own threads independent of OTel state)

## Quick-start observability stack

The quick-start docker-compose example assembles five containerized services into one Collector pipeline plus three viewer backends, with Grafana provisioning the operator-facing UI surface. The deep mechanism here covers what the upstream tools do, how the Collector pipeline routes the three signals, and what the smoke script actually verifies — material that does not belong in design.md because it is upstream-tool-specific operational detail, not YTDB design.

### Collector pipeline shape

The Collector config (`otel-collector-config.yaml`) defines three pipelines on one process, each consuming the same OTLP receiver pair and routing through shared processors to a signal-specific exporter:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 512
    spike_limit_mib: 128
  batch:
    timeout: 1s
    send_batch_size: 1024
  resource:
    attributes:
      - key: deployment.environment
        value: local
        action: upsert

exporters:
  otlp/jaeger:
    endpoint: jaeger:4317
    tls:
      insecure: true
  otlphttp/loki:
    endpoint: http://loki:3100/otlp
  prometheus:
    endpoint: 0.0.0.0:8889
    namespace: youtrackdb
    const_labels:
      deployment_environment: local

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch, resource]
      exporters: [otlp/jaeger]
    logs:
      receivers: [otlp]
      processors: [memory_limiter, batch, resource]
      exporters: [otlphttp/loki]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch, resource]
      exporters: [prometheus]
```

Three details matter:

1. **`memory_limiter` runs first** in every pipeline. Order matters — the limiter must see batches before the `batch` processor accumulates them, otherwise a runaway emit overruns the limit before the limiter rejects it.
2. **`batch` runs before `resource`** so the `deployment.environment=local` attribute lands on the batched envelope, not on every individual span / log / metric. The attribute is identical across all data points in a batch, so per-data-point application wastes CPU.
3. **Prometheus is pull-not-push**. The Collector's `prometheus` exporter exposes a `/metrics` HTTP endpoint on `:8889`; Prometheus scrapes it at the 15 s interval configured in `prometheus.yml`. The other two exporters push (`otlp/jaeger` to Jaeger's OTLP receiver on `:4317`; `otlphttp/loki` to Loki's OTLP HTTP path on `:3100/otlp`).

### Grafana datasource provisioning and correlator wiring

Grafana provisions the three datasources at startup via `grafana/provisioning/datasources/datasources.yml`. The load-bearing entries are the trace-to-logs and trace-to-metrics correlators on the Jaeger datasource:

```yaml
apiVersion: 1
datasources:
  - name: Jaeger
    type: jaeger
    uid: jaeger
    url: http://jaeger:16686
    jsonData:
      tracesToLogsV2:
        datasourceUid: loki
        spanStartTimeShift: -2m
        spanEndTimeShift: 2m
        tags: [{ key: service.name, value: service_name }]
        filterByTraceID: true
        filterBySpanID: false
      tracesToMetrics:
        datasourceUid: prometheus
        spanStartTimeShift: -2m
        spanEndTimeShift: 2m
        tags: [{ key: service.name, value: service_name }]
  - name: Loki
    type: loki
    uid: loki
    url: http://loki:3100
  - name: Prometheus
    type: prometheus
    uid: prometheus
    url: http://prometheus:9090
```

The `tracesToLogsV2` correlator wires Jaeger's span-detail panel "Logs for this span" button to a Loki query filtered by `trace_id` resolved from the span's trace context. The 2-minute window on either side of the span absorbs clock skew between the YTDB host and the Loki ingester; tightening the window risks missing logs emitted near the span boundary. The `filterByTraceID: true` setting tells Grafana to add `|= "<traceId>"` to the Loki LogQL, scoping the result to one trace.

The `tracesToMetrics` correlator points at Prometheus and joins on `service.name`. Operators clicking the "Metrics for this service" link in Jaeger land in a Prometheus panel filtered to `youtrackdb` metrics within the same 2-minute window.

### Dashboard placeholder strategy

Grafana dashboards reference datasources by UID. The committed JSON files under `grafana/dashboards/` use placeholder strings (`"datasource": "${DS_JAEGER}"`, `"datasource": "${DS_LOKI}"`, `"datasource": "${DS_PROMETHEUS}"`) that Grafana resolves at dashboard load time against the provisioned datasource UIDs. Without placeholders, exported dashboards bake in the UID of whatever Grafana instance authored them and fail to load on a fresh stack with re-provisioned UIDs.

The author workflow for any dashboard edit: bring the stack up, open Grafana, edit the dashboard in the UI, export via "Share → Export → Save to file", run `scripts/normalize-dashboard.sh <file>` to replace the captured UIDs with the placeholder strings, commit. The normalize script is a thin `jq` wrapper that operates on the exported JSON deterministically.

### Smoke-script verification semantics

`scripts/smoke.sh` exits non-zero when any pillar fails to land within 30 seconds. The sequence:

1. **Bring up the stack** if not already healthy (`docker compose up -d --wait`).
2. **Run a minimal embedded YTDB query** via a one-shot `java -jar` invocation against the Maven-built artifact, with `OPENTELEMETRY_ENABLED=true` and the local Collector endpoint. The query is a fixed `SELECT FROM OUser LIMIT 1` against an in-memory database — small, deterministic, exercises the SQL pillar through `db.query(...)`.
3. **Poll Jaeger** at `http://localhost:16686/api/services` until `youtrackdb` appears, then `http://localhost:16686/api/traces?service=youtrackdb&limit=1` until at least one trace lands. 30 s timeout per pillar.
4. **Poll Loki** at `http://localhost:3100/loki/api/v1/labels` until `service_name` appears, then `http://localhost:3100/loki/api/v1/query?query={service_name="youtrackdb"}` until at least one log record returns.
5. **Poll Prometheus** at `http://localhost:9090/api/v1/label/service_name/values` until `youtrackdb` appears, then `http://localhost:9090/api/v1/query?query=db_client_operation_duration_seconds_count{service_name="youtrackdb"}` until at least one sample returns.
6. **Exit 0** when all three polls succeed. **Exit non-zero with a diagnostic message naming the missing pillar** when any poll times out at 30 s.

The 30 s timeout is the load-bearing constant. It must be longer than the Collector's `batch` timeout (1 s) plus the worst-case exporter push interval plus the Prometheus scrape interval (15 s), with margin for cold-start container readiness. Shorter timeouts cause false negatives on under-resourced CI runners; longer timeouts mask actual failures by retrying until the optional CI job's outer timeout kicks in.

### Edge cases / Gotchas

- **Loki OTLP HTTP path mismatch**. Loki 3.x accepts OTLP on `/otlp/v1/logs`, not the bare `/otlp` path Loki 2.x advertised. The Collector config writes the full path explicitly (`http://loki:3100/otlp`) and the Collector appends `/v1/logs` per OTLP spec. A common operator surprise on a stale Loki version: logs land in Loki but the OTLP receiver returns 404 because the path resolver fell through. The README troubleshooting section names this case and points at the matching image tag.
- **Prometheus scrape lag on a fresh stack**. The first scrape happens 15 s after Prometheus starts. The smoke script's 30 s timeout absorbs this; an operator running queries immediately after bring-up may see "no data" in Grafana for up to 15 s. Documented behavior, not a bug.
- **Collector resource attribute vs span attribute precedence**. The `resource` processor sets `deployment.environment=local` as a resource attribute, which Grafana surfaces alongside span attributes in the trace-detail panel. A host-side `OPENTELEMETRY_SERVICE_NAME` override propagates into the same resource attribute set via the autoconfigure builder; the Collector's `resource` processor uses `action: upsert` so it does not overwrite an already-set `service.name`, only fills it in when absent.
- **Jaeger Elasticsearch backend swap (out-of-scope)**. The example uses Jaeger's all-in-one binary with in-memory storage, which loses traces on container restart. Operators who want persistence beyond a workstation demo migrate to Jaeger's Elasticsearch backend; the swap is a Jaeger config change and the YTDB side does not move. Documented in the README's "production tightening" section as a pointer.
- **Cardinality blow-up via per-tenant span names**. Default span-name format `commit <dbName>` produces one span name per database. Multi-tenant hosts with thousands of databases hit Jaeger's span-name aggregation limits; the design's `OPENTELEMETRY_COMMIT_SPAN_NAME_INCLUDES_DBNAME=false` switch drops dbName from the span name, moving identity to the `db.namespace` attribute where Jaeger indexes it cheaply. The example does not flip the switch (single-database demo), but the README points at the operator-facing tuning.

### References
- D-records: D41 (Quick-start observability stack lands inside YTDB-496 PR as example-files-only deliverable; rationale and alternatives in implementation-plan.md)
- Invariants: example-files-only (no source-code edits in `core`, `server`, or the OTel module), pinned image versions only (no `latest` tags; periodic refresh tracked under follow-up ticket `YTDB-OTel-EXAMPLE-VERSIONS`), Maven `<resources>` exclusion (example never ships inside the built JAR)
