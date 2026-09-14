# Track F: Observability and metrics

## Purpose / Big Picture
Wires production-observability counters and gauges for every new subsystem introduced by the rollback-log redesign.

<!-- Reserved for Move 2 — ADDED/MODIFIED/REMOVED triad. Empty until Move 2 lands. -->

Per-tree obsolete-version counts, history-store fall-through rate, purge rate, fallback-path frequency, page-steal rate, rollback-logical-ops/tx, claim-table size + contention; rebuild-window metrics for D27/D28/D29/D30; D32 recovery-window metrics (`recovery_logical_rollback_duration_seconds`, `recovery_in_flight_tx_count`); D33 DPB metrics (`dpb_post_crash_walk_in_progress_count`, `dpb_set_bits_count`). Integrates with existing metric export path. Lowest-priority track — lands last.
**Scope:** ~2 steps.
**Depends on:** Track L, Track V, Track H, Track D, Track E

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

- New counters / gauges:
  - History store: per-`indexId` total entries, fall-through rate
    (% of UNIQUE reads that hit history vs in-tree only),
    fall-through chain depth (avg history walks per read).
  - History purge: leaves-scanned/cycle, entries-reclaimed/cycle,
    no-op cycles count, cycle-wraps count.
  - Non-UNIQUE inline: per-tree obsolete-tombstone count, average
    `(K, RID)` chain length (sampled).
  - Fallback-path frequency: rate of stamp-validation failures
    leading to short-term component exclusive lock.
  - Page-steal rate: rate of dirty-page evictions.
  - Rollback-logical-ops count per tx, total per minute.
  - UNIQUE claim-table size per index, contention rate.
  - L&Y right-link traversals per descent (avg).
  - Record-CRUD rollback: stale-chunks-from-rolled-back-txs counter
    (separate from commit-side stale-chunks production rate, so
    operators can distinguish rollback-heavy from
    long-running-tx-stuck-LWM workloads — same disk-space symptom,
    different remediation; see D23).
  - Histogram volatility + crash rebuild (D27, supersedes D24's
    `histogram_aborted_delta_applied_count`):
    - `histogram_rebuild_in_progress_count` — gauge of indexes
      currently rebuilding (decremented to 0 once all rebuilds
      complete after a crash open).
    - `histogram_rebuild_completion_lag_seconds` — per-index
      histogram, p50/p95 of "time from storage open to rebuild
      publish" measured per crash event.
    - `histogram_pages_per_second_during_rebuild` — sustained
      rebuild scan throughput; surfaces slow rebuilds for capacity
      planning.
    - `histogram_query_during_empty_count` — counts query plans
      made against empty (post-crash, pre-rebuild) histograms;
      persistently high values suggest query workloads landing
      during the rebuild window.
  - FSM redesign + crash rebuild (D28, mirrors histogram pattern):
    - `fsm_rebuild_in_progress_count` — gauge of collections
      currently rebuilding their FSM after a crash open.
    - `fsm_rebuild_completion_lag_seconds` — per-collection
      histogram, p50/p95 of "time from storage open to FSM
      rebuild publish" measured per crash event.
    - `fsm_pages_per_second_during_rebuild` — sustained scan
      throughput of the data-file walk that populates FSM after
      crash; surfaces slow rebuilds for large collections.
    - `fsm_target_hint_hit_rate` — fraction of chunk writes that
      found a usable target via `targetPageIndex` without
      consulting the FSM. Steady-state value is the headline
      indicator that P2 is working: should approach 1 in
      insert-heavy workloads.
    - `fsm_failure_correction_rate` — fraction of chunk writes
      that hit verify-then-correct (candidate found insufficient,
      FSM pushed corrected actual value, retried). Bounded by
      `(chunk_size / page_size) × concurrency`; spikes indicate
      FSM staleness or thrashing.
    - `fsm_extension_rate` — rate of `addPage` extensions on the
      data file. Cross-references with target-hint hit rate to
      distinguish "cache is stale" from "no free pages exist."
  - Approximate-records-count rebuild (D29, mirrors histogram
    and FSM rebuild patterns):
    - `approx_count_rebuild_in_progress_count` — gauge of
      collections currently in `StatsStatus == REBUILDING`.
      Decremented to 0 once all rebuilds complete after a
      crash open.
    - `approx_count_rebuild_completion_lag_seconds` — per-
      collection histogram, p50/p95 of "time from storage open
      to `publishRebuildResult` call" measured per crash event.
    - `approx_count_rebuild_pages_per_second` — sustained
      throughput of the `.cpm` scan that populates the volatile
      counter after crash; surfaces slow rebuilds for very
      large collections.
    - `approx_count_drift_post_publish` — sampled gauge of the
      per-collection drift between `volatile.approximateRecordsCount`
      and an exact count taken at the moment of publish
      (instrumented via an optional verifier triggered in tests
      and on a low duty-cycle in production). Surfaces whether
      the drift-tolerant merge is producing unexpectedly large
      errors on real workloads — guides whether to switch to
      the per-bucket exact merge alternative documented in D29.
    - `approx_count_ddl_blocked_during_rebuild_count` —
      counter of DDL `DROP CLASS` / `TRUNCATE CLASS` rejections
      attributed to `StatsStatus == REBUILDING`. Cross-references
      with rebuild lag to distinguish "too many crashes" from
      "users hitting the rebuild window."
  - Index entry-count rebuild (D30, mirrors histogram / FSM /
    records-count rebuild patterns minus the DDL/StatsStatus
    dimension):
    - `index_count_rebuild_in_progress_count` — gauge of B-Tree
      index engines currently rebuilding their entry-count
      (decremented to 0 once all rebuilds complete after a
      crash open). Per-storage gauge; cross-references with
      `histogram_rebuild_in_progress_count` for crash-recovery
      progress dashboards.
    - `index_count_rebuild_completion_lag_seconds` — per-
      index histogram, p50/p95 of "time from storage open to
      `publishRebuildResult` call" measured per crash event.
    - `index_count_rebuild_pages_per_second` — sustained
      throughput of the leaf scan that populates the volatile
      counter after crash; surfaces slow rebuilds for very
      large indexes.
    - `index_count_drift_post_publish` — sampled gauge of
      per-engine drift between `approximateIndexEntriesCount`
      and an exact count taken at the moment of publish
      (instrumented via an optional verifier triggered in
      tests and on a low duty-cycle in production). Surfaces
      whether the drift-tolerant merge produces unexpectedly
      large errors on real workloads.
    **No `index_count_ddl_blocked_*` metric** — D30 has no
    DDL guard analog to D29's; the audit confirmed no DDL
    guards use index counts as soft-fences.
  - Recovery in-flight tx reconstruction (D32, recovery-window
    observability):
    - `recovery_logical_rollback_duration_seconds` — per-storage
      histogram, p50/p95 of total time spent in recovery's
      logical-rollback phase per crash event. Measured from the
      moment the recovery-window `TsMinHolder` is installed
      (after analysis pass) until it is removed (after the last
      in-flight tx has transitioned to ROLLED_BACK). High values
      indicate either many in-flight txs or long descriptor
      chains; cross-references with `recovery_in_flight_tx_count`.
    - `recovery_in_flight_tx_count` — gauge per crash event of
      the number of in-flight txs identified during analysis
      (i.e., transactions with `ATOMIC_UNIT_START` and no
      matching `ATOMIC_UNIT_END`). Captures workload-side
      pre-crash concurrency / tx-size discipline. Persistently
      high values are a workload smell (long-running txs
      interrupted by crash); spikes after planned restarts may
      indicate an upstream service generating many short txs at
      shutdown time.
  - DPB heap-resident refactor (D33, GC-driven convergence
    observability):
    - `dpb_post_crash_walk_in_progress_count` — gauge per
      collection set to 1 when the heap bitset is initialized
      to the conservative "all bits set" state on crash open;
      cleared to 0 once GC's first full cycle for that
      collection completes (i.e., GC has visited every page
      and cleared bits for clean ones). Operators can monitor
      per-collection convergence after crash and detect "GC is
      stuck" patterns where the gauge persists indefinitely.
    - `dpb_set_bits_count` — sampled gauge of heap bitset
      population per collection (count of bits currently set).
      Capacity-planning input — large values indicate either
      sustained stale-chunk production or GC backlog.
      Cross-references with the existing
      `stale-chunks-from-rolled-back-txs` and
      `stale-chunks-from-commits` counters to distinguish
      arrival rate from drain rate.
- Integration: register metrics via existing metric/export path
  (likely `YouTrackDBProfiler`-adjacent). No new export pipeline.

## Plan of Work

- Use existing metric infrastructure.
- Counters: `LongAdder` per event, no allocation.
- Sampled metrics use periodic estimation (every N reads).
- Tests verify metric values increment under expected operations.

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

**In scope:** metric definition, registration, hot-path
instrumentation in `BTreeSingleValueIndexEngine`,
`BTreeMultiValueIndexEngine`, `SharedLinkBagBTree`,
`HistoryBTree`, `HistoryBTreePurge`, eviction path,
rollback path, fallback path.

**Out of scope:** new dashboards, new alerting rules, cross-cluster
aggregation.

Must not add contention or hot-path allocation. `LongAdder` for
counters; no `AtomicLong` on hottest paths.

**Inter-track dependencies:**
- Depends on Track L (L&Y traversal metrics).
- Depends on Track V (non-UNIQUE chain metrics).
- Depends on Track H (history-store metrics).
- Depends on Track D (fallback/steal/rollback metrics).
- Depends on Track E (purge metrics).
- Lowest-priority track; can land last.
