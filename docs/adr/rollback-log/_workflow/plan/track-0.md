# Track 0: Load-test harness + expected MT scalability declarations

## Purpose / Big Picture
Builds the L1/L2/L3 load-test harness with falsifiable per-scenario MT scalability predictions.

<!-- Reserved for Move 2 — ADDED/MODIFIED/REMOVED triad. Empty until Move 2 lands. -->

Builds the L1/L2/L3 load-test harness (JMH primitive + `ConcurrentTestHelper` + integration-test scaffolding) and **declares expected scalability factors per scenario** as falsifiable architectural predictions (D37). Phase 4's same-node A/B comparison (Hetzner CCX33) flags any divergence > 2× as bottleneck-detected. No baselines committed in this track — only the harness and predictions.
**Scope:** ~3 steps. Smoke-runs against legacy code locally to verify plumbing.

## Progress
- [ ] Review + decomposition
- [ ] Step implementation
- [ ] Track-level code review
- [ ] Track completion

## Surprises & Discoveries
<!-- Continuous-log. Promoted by the orchestrator from per-step "What was discovered" when the finding affects future steps or other tracks. Empty at Phase 1. -->

## Decision Log
<!-- Continuous-log. Execution-time decisions: inline-replan choices, scope-downs, dependency reveals, gate-override reasons. -->

<!-- Reserved for Move 1 — per-track inlined Decision Records. -->

## Outcomes & Retrospective
<!-- Continuous-log. Review iteration outcomes and the track-completion summary at Phase C. -->

## Context and Orientation

- **L1 JMH microbenchmark scaffolding** under
  `tests/src/test/java/.../benchmarks/rollbacklog/` (align with
  existing `tests/.../benchmarks/` JMH layout — reuse the project's
  already-wired JMH version + Maven configuration). One subdirectory
  per primitive (`tryconvertwrite/`, `claimtable/`, `dpb/`,
  `fsmcursor/`, etc.). Each gets a `*Benchmark.java` class with JMH
  annotations (`@State`, `@Threads`, `@BenchmarkMode(Mode.Throughput)`,
  `@OutputTimeUnit(TimeUnit.SECONDS)`), parameterized by thread count
  via `@Param` or `@Threads`.
- **L2 component-level concurrent test scaffolding** as extensions to
  `test-commons`'s `ConcurrentTestHelper` and `TestBuilder` /
  `TestFactory` patterns. New `LoadTestHelper` wraps N-thread
  workload execution with deterministic ramp-up barriers, captures
  throughput / latency / fallback-rate, and reports results in a
  JSON shape compatible with L1 JMH output for unified
  comparison-report generation in Phase 4.
- **L3 end-to-end composition harness** — integration-test-flavor
  harness running `db.save(vertex)` and similar workloads under N
  concurrent writers against an embedded YouTrackDB instance. Reuses
  `EmbeddedTestSuite` patterns where appropriate; new
  `LoadTestBase` base class for N-writer scenarios with **disk
  storage** (`-Dyoutrackdb.test.env=ci`) — L3 must exercise the
  STEAL-enabled cache path; in-memory storage skips the page
  eviction interaction. Per-PID temp directories per the project's
  parallel-test guidance.
- **`LoadTestExpectations` declarations file**: a Java constants
  file (or parallel YAML / properties depending on the JMH ingestion
  path) declaring one
  `ExpectedScalability(scenarioName, expectedFactor,
  architecturalArgument, sourceDecisionRecord)` entry per scenario.
  The `scenarioName` matches the JMH benchmark identifier or the
  L2/L3 test method name; `expectedFactor` is the predicted
  scalability on 16 cores (e.g., 14.0 for "near-linear", 2.5 for
  "history-tree right-edge contention", 1.0 for "serialized — same
  leaf"); `architecturalArgument` is a one-line citation; and
  `sourceDecisionRecord` references D18 / D36 / D28 / etc. The
  file is the **single source of truth** in code; design.md's
  "Expected MT Scalability" section is the human-readable
  counterpart and the two must stay in sync (Phase 1 review
  checks this).
- **Smoke runs against legacy code locally** — no measurement
  captured, just verification that the harness compiles, scenarios
  run end-to-end, and JMH / L2 / L3 outputs serialize correctly.
  The actual A/B measurement happens in Phase 4 on Hetzner CCX33.
- **Phase 4 runner script** — shell or Python (place per existing
  `tools/` or `scripts/` convention) that drives the same-node
  A/B comparison: provisions a CCX33 via the
  `run-jmh-benchmarks-hetzner` skill; checks out `develop`, builds,
  runs all scenarios, captures `legacy-results.json`; checks out
  `rollback-log` HEAD, builds on the **same node**, runs all
  scenarios, captures `branch-results.json`; emits
  `perf-validation-report.md` with per-scenario throughput delta,
  scalability factor (`actual ÷ expected`), fallback rate, latency
  tail (p50/p95/p99), and **gap-analysis flags** for scenarios
  where `|actual − expected| > 2 × expected`.

## Plan of Work

- **No new test framework dependencies.** JMH is already wired
  (`jmh-ldbc/`, `tests/.../benchmarks/`). `ConcurrentTestHelper`
  already exists in `test-commons`. Extend, don't replace.
- **Expected scalability declarations are derived, not measured.**
  For each scenario, the architectural argument predicts the
  factor (e.g., "L&Y BTree disjoint-leaf concurrent put: ~14-16×
  on 16 cores because D18 page-level latches don't contend on
  disjoint pages and D36 removes the tree lock"). Predictions are
  coarse point estimates with implicit ±20%; the 2× tolerance in
  Phase 4's gap analysis absorbs reasonable error.
- **Output JSON shape** unified across L1/L2/L3 so the Phase 4
  report generator consumes one schema. Suggested fields per
  scenario per run:
  `{ scenarioName, layer (L1|L2|L3), threadCount, throughputOps,
  p50Ms, p95Ms, p99Ms, fallbackRate (nullable), notes }`.
- **L3 must use disk storage** — in-memory has no persistent WAL
  and doesn't evict, so STEAL is unobservable.
- **No baselines committed.** Subsequent tracks add load tests;
  numbers come from Phase 4 only.
- **Recommended step order**:
  - 0.1: L1 + L2 + L3 harness scaffolding + `LoadTestExpectations`
    skeleton + Maven wiring.
  - 0.2: Smoke runs locally against legacy code; verify JSON output
    shape; verify the harness ingestion plumbing.
  - 0.3: Phase 4 runner script (Hetzner provisioning + checkout +
    build + run + report-generator).

## Concrete Steps
<!-- Phase A placeholder — decomposition writes a thin numbered roster here: one entry per step with description, `risk:` tag, and a `[ ]` status checkbox. Per-step episodes do NOT live here; they live in `## Episodes` below. The roster is immutable after Phase A except for the status checkbox flip and the optional `commit:` annotation Phase B appends. -->

## Episodes
<!-- Continuous-log. Phase B sub-step 7 appends one block per completed step, identified by step number + commit SHA. Empty at Phase 1; Phase A does not populate. -->

## Validation and Acceptance
<Track-level behavioral acceptance criteria.>

<!-- Phase A placeholder for per-step EARS/Gherkin lines. -->

<!-- Reserved for Move 3 — EARS or Gherkin acceptance lines used verbatim as test method names. Empty until Move 3 lands. -->

## Idempotence and Recovery
<!-- Phase A placeholder — names per-step idempotence and recovery paths once steps are decomposed. -->

## Artifacts and Notes
<!-- Continuous-log (rare). Cross-step artifact references that don't belong to one specific step. Per-step episode content lives in `## Episodes` above. Often empty. -->

## Interfaces and Dependencies

**In scope:**
- JMH scaffolding files, `ConcurrentTestHelper`
  extensions, L3 integration-harness base classes,
  `LoadTestExpectations` declarations skeleton, and the Phase 4
  runner script.

**Out of scope:**
- Per-scenario benchmark / load-test
  *implementations* — those land in Tracks A, L, C, V, H, D, T2.
  Track 0 provides the harness; subsequent tracks consume it.
- Any baseline-number capture. Phase 4 is the only
  place legacy-vs-branch numbers are produced.
- YCSB integration. YCSB lives outside this PR per
  D37.

Smoke runs must pass on legacy code — the harness shape must be
compatible with both legacy and post-cutover code (no
API references to types or methods that don't yet exist;
subsequent tracks add their scenarios incrementally as the
relevant APIs land).

**Inter-track dependencies:**
- **Tracks A, L, C, V, H, D, T2** consume Track 0's harness for
  their per-track load tests. Each consuming track adds its
  scenarios to `LoadTestExpectations` and registers its
  benchmark / test classes under the harness conventions.
- **Phase 4** consumes Track 0's runner script and harness to
  produce the comparison report.
- **Track F** (observability metrics) is independent. F's metrics
  are for production observability, not load-test instrumentation;
  some metrics (`fsm_target_hint_hit_rate`,
  `fsm_failure_correction_rate`, fallback rate per commit) are
  *also* useful as load-test sanity-check signals, but Track 0
  does not depend on F.
