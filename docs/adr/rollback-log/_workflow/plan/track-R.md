# Track R: StampedLock-based optimistic reads on all page access paths

## Purpose / Big Picture
Converts remaining direct shared-mode page-read sites to the existing `executeOptimisticStorageRead` helper.

<!-- Reserved for Move 2 — ADDED/MODIFIED/REMOVED triad. Empty until Move 2 lands. -->

Converts remaining direct shared-mode page-read sites (`IndexAbstract`, `IndexMultiValues`, `IndexOneValue`, `IndexManagerEmbedded`, minor `PaginatedCollectionV2` / `DiskStorage`) to `executeOptimisticStorageRead`. Pure refactor; commit semantics unchanged.
**Scope:** ~5 steps including concurrent-reader integration tests.

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

- Convert the ~33 remaining direct `acquireSharedLock` read sites in
  the index subsystem and minor paginated-collection paths to route
  through the existing
  `StorageComponent.executeOptimisticStorageRead` helper. The helper
  already orchestrates "optimistic-first with pinned-shared-lock
  fallback" and records stamps into `OptimisticReadScope`.
- Files and approximate call-site counts (exact offsets resolved at
  Phase A):
  - `core/.../internal/core/index/IndexAbstract.java` — ~12 sites.
  - `core/.../internal/core/index/IndexMultiValues.java` — ~8 sites.
  - `core/.../internal/core/index/IndexOneValue.java` — ~8 sites.
  - `core/.../internal/core/index/IndexManagerEmbedded.java` —
    ~4 sites.
  - `core/.../internal/core/storage/collection/v2/PaginatedCollectionV2.java`
    — 1 site.
  - `core/.../internal/core/storage/disk/DiskStorage.java` — 1 site.
- Concurrent-reader integration tests — one per subsystem: read
  under concurrent writer's `put`. Asserts readers do not block the
  writer's exclusive-latch acquisition at component-op commit.

## Plan of Work

- Each converted method follows the pattern already established by
  `BTree.get()` (sbtree v3) and `SharedLinkBagBTree.findCurrentEntry()`:
  ```java
  return executeOptimisticStorageRead(
      atomicOp,
      () -> /* optimistic: loadPageOptimistic + read + maybe
               validateLastOrThrow on indirect-pointer reads */,
      () -> /* pinned fallback: existing shared-lock code, unchanged */);
  ```
- Recommended step order:
  - R1 — `IndexAbstract`: ~12 sites.
  - R2 — `IndexMultiValues`: ~8 sites.
  - R3 — `IndexOneValue`: ~8 sites.
  - R4 — `IndexManagerEmbedded` + minor: ~4 sites in
    `IndexManagerEmbedded` plus single sites in
    `PaginatedCollectionV2` and `DiskStorage`.
  - R5 — Concurrent-reader integration tests.
- The optimistic lambdas must:
  - Use `StorageComponent.loadPageOptimistic` on every page load.
  - Call `scope.validateLastOrThrow()` after following indirect
    pointers.
  - Not catch `OptimisticReadFailedException` internally.

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

**In scope:** the six files listed above.

**Out of scope:**
- `acquireSharedLock` sites inside
  `StorageComponent.executeOptimisticStorageRead`'s pinned-fallback
  path.
- Cache-internal `acquireSharedLock` implementations.
- Existing converted reads in `BTree`, `SharedLinkBagBTree`,
  `FreeSpaceMap`.

No new exception type, no runtime assertion, no package-visibility
changes (D14). Commit semantics are unchanged; this track is a pure refactor.

**Inter-track dependencies:**
- **Track D** is downstream — D's stamp-validation-at-commit
  produces "short-term latches only" only when readers are in
  optimistic mode.
- **Independent of all other tracks** — parallelizable with A, L, V,
  C, H.
