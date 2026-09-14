# Track T1: Transaction rollback integration tests

## Purpose / Big Picture
Validates end-to-end rollback correctness across op-type matrix, multi-op intra-tx, and concurrent rollback scenarios.

<!-- Reserved for Move 2 — ADDED/MODIFIED/REMOVED triad. Empty until Move 2 lands. -->

End-to-end rollback coverage across tx size, op mix, concurrency; per-op-type matrix for UNIQUE (history-store path) and non-UNIQUE (inline tombstone); intra-tx put-remove-put; S5 invariant; UNIQUE claim release. Plus jetCheck round-trip property tests.
**Scope:** ~4 steps.
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

- Per-op-type rollback matrix (one test class per op type, JUnit 4
  for core module per repo convention):
  - `record.create` → rollback: assert no record at the RID, no
    cluster-position allocation leaked.
  - `record.delete` → rollback: assert record restored.
  - `record.update` → rollback: assert record restored.
  - `index.put(K, RID)` (UNIQUE) → rollback: assert K returns
    RID_prev (from history); history entry at `(indexId, K, T_W)`
    gone.
  - `index.remove(K)` (UNIQUE) → rollback: assert K restored to
    pre-remove value via history-store inverse.
  - `index.put(K, RID)` (non-UNIQUE) → rollback: assert tx's
    `(K, RID, T_W)` entry gone from in-tree.
  - `index.remove(K, RID)` (non-UNIQUE) → rollback: assert tx's
    tombstone entry at `(K, RID, T_W)` gone.
  - `linkbag.add(fromRid, toRid)` → rollback: assert edge absent.
  - `linkbag.remove(fromRid, toRid)` → rollback: assert edge
    restored.
  - UNIQUE-index claim release: subsequent put of same key by a
    different tx succeeds without `RecordDuplicatedException`.
- **Multi-op rollback tests**: a single tx mixing record create +
  index put (UNIQUE + non-UNIQUE) + link-bag add + UNIQUE put,
  rolled back atomically; assert all inverse ops apply and S5
  holds across every subsystem.
- **Multi-step intra-tx rollback (the put-remove-put case)**: tx
  does index.put(K, RID_1), index.remove(K), index.put(K, RID_2),
  then rolls back. Assert in-tree restored to pre-tx state, history
  entry gone, claim released.
- **Large-tx rollback tests**: a tx whose touched-page count
  exceeds half the default cache capacity (forces page stealing
  mid-tx). On rollback, assert completion without OOM and final
  tree state matches pre-tx.
- **Concurrent-rollback scenarios**:
  - Two disjoint txs both rolling back — no interference.
  - Two txs touching the same UNIQUE index key (history-store):
    one commits, one rolls back; observer at various snapshot
    times sees the correct version.
  - Rollback racing with a concurrent split-time GC cycle — no
    lost cleanup, no double-deletion.
  - Observer tx started mid-rollback; assert visibility predicate
    correctly treats rolling-back tx as IN_PROGRESS until
    ROLLED_BACK.
- **S5 invariant assertion tests**: helper that enumerates every
  entry in every tree (in-tree + history) and link-bag and asserts
  none match the rolled-back tx's `commitTs` after rollback completes.
- **jetCheck round-trip property test**
  (`LogicalRollbackRoundTripPropertyTest`):
  - Generator: random tx body of N ops (mixed put/remove on
    UNIQUE/non-UNIQUE/link-bag).
  - Snapshot tree state before tx start. Run the tx, then roll it
    back.
  - Assert: post-rollback state is logically equivalent to
    pre-tx snapshot (every key/value, every entry in every tree,
    every claim table empty for the tx).
  - **D6/S14 coverage**: the property test specifically targets
    descriptor `prev_value` capture correctness. Forces UNIQUE
    replacement chains within a tx (put K=A, put K=B, put K=C, then
    rollback) and asserts post-rollback K=A (the committed pre-tx
    value, not B or C). This exercises the "first-write captures
    committed prev, subsequent writes reuse our prev" rule from
    Track H.

## Plan of Work

- Extend existing `test-commons` base classes
  (`TestBuilder` / `TestFactory` / `ConcurrentTestHelper`).
- Use embedded storage (in-memory) for most tests; subset on disk
  via `-Dyoutrackdb.test.env=ci` to exercise the STEAL-enabled
  cache path.
- Concurrent scenarios use explicit `CyclicBarrier` /
  `CountDownLatch`; tests must be deterministic with zero flake
  budget.
- Direct-tree scan helper for S5: package-private iterator on
  `BTree` and `HistoryBTree` and `SharedLinkBagBTree` gated to
  test scope.

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

**In scope:** new test classes under `core/src/test/java/`.

**Out of scope:** JVM-kill / restart scenarios (T2); randomized fuzz
beyond jetCheck.

Tests must be deterministic; no `Thread.sleep`-based timing. Do not rely on internal implementation details (specific page
indices, latch stamp values).

**Inter-track dependencies:**
- **Track D** is a hard dependency.
- **Track L** dependency — L&Y semantics exercised.
- **Track V** dependency — non-UNIQUE rollback inverses.
- **Track H** dependency — UNIQUE history-backed inverses.
- **Track C** dependency — claim-release tests.
- **Parallelizable with T2, E, F**.
