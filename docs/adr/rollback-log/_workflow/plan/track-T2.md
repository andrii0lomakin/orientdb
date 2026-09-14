# Track T2: Crash-recovery and durability integration tests

## Purpose / Big Picture
JVM-kill-and-restart matrix that validates REDO, portion-UNDO, logical rollback, and D32's S23 post-condition across every crash point.

<!-- Reserved for Move 2 — ADDED/MODIFIED/REMOVED triad. Empty until Move 2 lands. -->

JVM-kill-and-restart harness + crash-point matrix (mid-component-op, post-end-marker pre-WAL-durability, post-WAL-durability pre-page-durability, mid-logical-rollback, mid-recovery-driven rollback, cross-tree partial flush, L&Y cascading-split mid-cascade). Validates S6/S7/S8/S9/S10/S11/S12 + D32's S23 post-condition. Plus jetCheck crash-recovery property test.
**Scope:** ~6 steps including L3 `RebuildVsCrudLoadTest` (D37/S25).
**Depends on:** Track D

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

- **JVM-kill-and-restart test harness**. Parent process launches a
  child JVM running a scripted workload; crash injected at a
  designated logical point; parent reopens storage, replays
  recovery, asserts post-recovery state.
- **Crash-point injection hook**: test-only thread-local counter
  triggers `System.exit` at a configured label. Labels:
  `"after-pageop-N-of-component-op"`,
  `"after-component-op-end-record"`,
  `"after-logical-operation-descriptor"`,
  `"after-atomic-unit-end-record"`,
  `"after-wal-flush"`,
  `"after-page-flush"`,
  `"mid-logical-rollback-after-inverse-N"`,
  `"mid-recovery-rollback-after-inverse-N"`,
  `"mid-cascading-split-after-stage-N"`.
- **Recovery matrix by crash point**:
  - **Mid-component-op**: portion-level UNDO restores pre-op state.
  - **Post-component-op, pre-ATOMIC_UNIT_END**: logical rollback
    emits CLRs for completed inverses.
  - **Post-ATOMIC_UNIT_END, pre-WAL-durability**: tx treated as
    in-flight → logical rollback.
  - **Post-WAL-durability, pre-page-durability**: REDO from WAL
    restores post-commit state.
  - **Mid-logical-rollback**: REDO replays completed inverses,
    re-runs `rollbackLogically` for remainder.
  - **Mid-recovery-rollback**: second recovery REDO-replays first
    recovery's CLRs.
- **Cross-tree partial flush** (the edge-case G we traced):
  construct a state where the durable in-tree page is flushed but
  the non-durable history page isn't (or vice versa). Crash.
  Assert: (a) Step 0 wipes the history file (it does not exist on
  disk immediately before WAL replay); (b) REDO brings the in-tree
  to consistent post-op state from WAL records; (c) logical
  rollback (if needed) succeeds using descriptor's `prev_value`
  without ever attempting a history read.
- **Non-durable wipe verification**: explicit test asserting that
  after a clean shutdown AND after a crash shutdown, on next open
  the history file content is empty (verified before any tx runs).
  Catches regressions in the always-create-on-open discipline (S13).
- **L&Y cascading-split mid-cascade matrix**: crash at each stage
  boundary of a cascading split (after leaf-split commit, after
  parent insert, mid-cascade). Assert recovery yields a valid
  L&Y state (right-link descender finds all keys; structural
  splits remain even after logical rollback).
- **S9 (dual-gate durability) tests**:
  - Construct state where ATOMIC_UNIT_END is WAL-durable but at
    least one mutated page remains dirty. Force truncation
    attempt. Assert segments containing tx's ATOMIC_UNIT_START are
    retained.
  - Symmetric for rollback path: CLR records must not be truncated
    before page durability.
- **Page-stealing recovery test**: long tx whose page writes
  exceed cache capacity; assert pages are evicted to disk mid-tx;
  crash after commit; restart; assert REDO correctly re-materializes.
- **D32 — In-flight tx reconstruction tests**:
  - **Re-registration assertion**: crash with N in-flight txs of
    known `unitId`s; on restart, instrument
    `AtomicOperationsTable` to expose its IN_PROGRESS contents;
    assert exactly the N expected `unitId`s are present after
    WAL analysis but before logical rollback runs.
  - **`tsOffset` floor**: crash with the lowest in-flight
    `unitId` substantially below `idGen.getLastId() + 1`; assert
    the post-construction `tsOffset` covers it (no
    `IllegalStateException` from
    `AtomicOperationsTable.startOperation` at line 504).
  - **Recovery-window `TsMinHolder`**: instrument `tsMins` to
    capture additions/removals during recovery; assert one
    synthetic holder is present from analysis-pass completion
    until the last logical rollback completes; assert
    `tsMin == min(in-flight unitId)` for that holder.
  - **LWM bound during rollback**: instrument
    `computeGlobalLowWaterMark` invocations during recovery;
    assert returned LWM ≤ `min(in-flight unitId)` throughout
    the logical-rollback phase.
  - **Prev-chunk preservation**: construct a workload where each
    in-flight tx_W has known prev chunks (specific page indices
    /slots); crash, recover, assert the chunks are still
    physically present at the moment each inverse op reads
    `descriptor.prev_position_entry`. (Test hook on the GC path
    to register page reclamation events keyed by `(pageIndex,
    slot)` — assert no such event fires for prev-chunk pages
    during recovery.)
  - **GC-vs-rollback ordering**: assert `STATUS.OPEN` is NOT
    set until after every in-flight tx has transitioned to
    ROLLED_BACK; assert GC's `PeriodicRecordsGc.run` short-
    circuits at every invocation made before `STATUS.OPEN`.
  - **S23 post-condition**: after `recoverIfNeeded` returns,
    assert `AtomicOperationsTable` contains zero IN_PROGRESS
    entries. Equivalently, the `OperationInformation` array
    has no entry with status == IN_PROGRESS.
  - **Mid-recovery-rollback crash + restart idempotence** (also
    listed in the recovery matrix above): kill recovery
    mid-rollback after the K-th inverse has emitted CLRs but
    before the K+1-th has been issued; restart; assert the
    second recovery re-registers the same in-flight tx_W,
    replays the K CLRs via REDO (no double-application
    observable from end state), and resumes the reverse walk
    to completion. End state matches an uninterrupted recovery.
- **jetCheck crash-recovery property test**
  (`CrashRecoveryPropertyTest`):
  - Generator: random op sequences + random crash point within an
    op sequence.
  - Apply ops to oracle and (in-process) child JVM. At crash
    point, kill child, restart, run recovery.
  - Assertion: post-recovery state matches oracle's "all ops
    before crash that fully committed; all in-flight ops rolled
    back."
  - Plus tree invariants from L&Y property test (no orphan pages,
    sibling-pointer consistency, etc.).
- **L3 `RebuildVsCrudLoadTest`** (per D37/S25; consumes Track 0's
  harness; expected scalability declared in design.md §"Expected
  MT Scalability"). Validates that D27 (histogram) / D28 (FSM) /
  D29 (records count) / D30 (index counters) / D33 (DPB) crash
  rebuilds do not starve concurrent CRUDs, and that post-publish
  drift stays within the formula bounds. Scenarios:
  - **`RebuildVsCrud.RecordsCount`** — kill-restart with a
    populated collection; immediately on `STATUS.OPEN`, run N
    concurrent CRUDs while the per-collection
    `maybeScheduleApproxCountRebuild` background task scans the
    `.cpm` file. Asserts: (a) CRUDs complete within bounded
    latency tail (rebuild does not block the volatile counter
    mutation hot path); (b) post-`publishRebuildResult` drift
    `|approximateRecordsCount − exact_count|` ≤ formula bound
    (`error = C_before − D_before` per D29) plus 5% slack;
    (c) DDL `DROP CLASS` without `UNSAFE` rejected with the
    expected message while `StatsStatus == REBUILDING`.
  - **`RebuildVsCrud.IndexCounters`** — same shape, applied to
    the per-index `BTree.scanLiveEntryCount` rebuild (D30).
    Asserts: (a) CRUDs scale; (b) post-publish drift bounded;
    (c) `IndexAbstract.getSize()` reads return the volatile
    counter as-is (no gating) and converge to the exact count
    after publish.
  - **`RebuildVsCrud.Histograms`** — same shape for histogram
    rebuild (D27). Asserts: (a) CRUDs scale; (b) post-rebuild
    CHM matches expected bucket frequencies + HLL registers;
    (c) query plans during the rebuild window use empty-histogram
    fallback heuristics (verified by querying with a
    test-instrumented planner).
  - **`RebuildVsCrud.FsmRebuild`** — kill-restart, then drive
    N concurrent chunk writes while the FSM rebuild scans the
    data file (D28). Asserts: (a) chunk writes succeed via
    extension fallback during the rebuild window (bounded
    under-utilization, no failure); (b) post-rebuild FSM matches
    a clean-shutdown FSM for the same workload (within
    bucket-granularity tolerance); (c) the
    `fsm_rebuild_in_progress_count` metric clears to 0 after
    rebuild publish.
  - **`RebuildVsCrud.DpbConvergence`** — kill-restart, then drive
    N concurrent UPDATEs/DELETEs while GC's first cycle walks
    the conservative all-bits-set DPB and clears bits for clean
    pages (D33). Asserts: (a) CRUDs scale; (b) GC converges (no
    persistent extra walk cost after the first full cycle for
    each collection completes); (c) the
    `dpb_post_crash_walk_in_progress_count` metric clears to 0.
  Adds `LoadTestExpectations` entries for all five scenarios.
  Expected scalability for the CRUDs: bounded by the regular
  non-rebuild equivalent (within 10% — rebuild scan competes
  for some cache bandwidth but not for write locks on the same
  pages).

## Plan of Work

- The existing `LocalPaginatedStorageRestoreFromWALIT` test family
  already does restart-and-verify. Extend / duplicate its pattern
  so crash-point hooks can be registered programmatically.
- For S9 orphan-record tests: deterministic control over
  `WOWCache.dirtyPages` to construct exact LSN layout. Options:
  test-only `WOWCache.holdPageDirty(pageId, lsn)`, or workload
  primitive with deterministic timing barriers.
- Disk storage required (`-Dyoutrackdb.test.env=ci`); in-memory
  has no persistent WAL.
- Flake budget is zero.
- Recommended step decomposition during Phase A: (1) harness +
  crash-point hook; (2) committed-tx + page-stealing recovery; (3)
  in-flight-tx logical rollback + cross-tree partial flush; (4)
  L&Y cascading-split mid-cascade matrix + dual-gate S9; (5)
  jetCheck crash-recovery property test.

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

**In scope:** restart harness, crash-point injection hook,
recovery-matrix tests, S9 tests, page-stealing recovery test,
property test.

**Out of scope:** randomized fuzz beyond jetCheck; performance/
throughput benchmarks during recovery; cross-node replication.

Tests must run in CI without elevated OS privileges; use
`System.exit` or subprocess pattern. Must not leak child processes on test failure — use
`Process.destroyForcibly` in `@After`. Each test runs in a unique per-PID temp directory.

**Inter-track dependencies:**
- **Track D** is a hard dependency.
- **Track A** dependency — `PageOperation.undo` correctness verified
  end-to-end.
- **Track L** dependency — L&Y cascade scenarios exercised.
- **Track V** dependency — non-UNIQUE recovery scenarios.
- **Track H** dependency — history-tree recovery scenarios.
- **Parallelizable with T1, E, F**.
