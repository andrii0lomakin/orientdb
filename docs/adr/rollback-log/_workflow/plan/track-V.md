# Track V: Non-UNIQUE multi-version composite key with ts

## Purpose / Big Picture
Extends non-UNIQUE indexes to a multi-version composite key `(K, RID, ts)` with snapshot-isolated read and split-time GC.

<!-- Reserved for Move 2 — ADDED/MODIFIED/REMOVED triad. Empty until Move 2 lands. -->

Extends `BTreeMultiValueIndexEngine` composite key from `(K, RID)` to `(K, RID, ts)`; read walks version chain at `(K, RID, *)` picking highest-ts visible (insert or tombstone); Tier-1 split-time GC reclaims pre-LWM obsolete entries. Bloat bounded by mutation-rate × LWM lag.
**Scope:** ~5 steps including jetCheck SI property tests + L2 concurrent-appender load test.
**Depends on:** Track L

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

- Extend the composite key for non-UNIQUE indexes from `(K, RID)`
  to `(K, RID, ts)`. The `BTreeMultiValueIndexEngine` uses
  `BTree<CompositeKey>` today; the `CompositeKey` serializer gains
  the ts field. Existing entries are migrated... actually no
  migration: per project constraint, on-disk compatibility is not
  preserved. Fresh storage starts with the new layout.
- **Read path** (`BTreeMultiValueIndexEngine.iterateEntriesBetween`,
  `BTreeMultiValueIndexEngine.get`, etc.): walk the entries at
  `(K, RID, *)` in reverse-ts order (highest ts first), pick the
  first visible at the reader's snapshot.
  - Visibility: an entry at ts T_e is visible at snapshot T_R iff
    `T_e ≤ T_R AND T_e ∉ inProgressTxs(T_R)` (using
    `AtomicOperationsTable.isEntryVisible`).
  - If the highest-visible entry is a tombstone, treat (K, RID) as
    absent at T_R.
  - If no entry visible, treat (K, RID) as absent.
- **Write path** (`BTreeMultiValueIndexEngine.put`,
  `BTreeMultiValueIndexEngine.remove`):
  - `put(K, RID)`: append entry at `(K, RID, T_W)` with empty
    payload.
  - `remove(K, RID)`: append entry at `(K, RID, T_W)` with
    tombstone payload.
  - No replacement, no in-place mutation. Pure append.
  - Same-tx idempotency: subsequent put/remove of `(K, RID)` within
    the same tx writes to the same composite key
    `(K, RID, T_W)` — natural B-Tree overwrite. Cleaner than today's
    code which special-cases this.
- **Tier-1 split-time GC retention rule extension** in
  `BTree.filterAndRebuildBucket` (and analogous helpers in
  `SharedLinkBagBTree` for symmetry — link-bag uses the same
  composite-key-with-ts pattern). Rule: for each `(K, RID)` cluster
  in the bucket, retain the highest-ts entry with `ts ≤ LWM` (the
  baseline) plus all entries with `ts > LWM`. Drop everything else.
  - If the highest-ts-≤-LWM entry is a tombstone AND no entries with
    `ts > LWM` exist for this `(K, RID)`, drop the tombstone too
    (no readers will see it).
  - Same retention rule used for link-bag (D7's cost analysis
    applies symmetrically).
- **jetCheck property tests** (`NonUniqueIndexSIPropertyTest`):
  - Generator: random sequences of tx-bracketed
    (put, remove, get, range-scan) ops on `(K, RID)` pairs.
  - Oracle: a model of `Map<(K, RID), List<Event>>` where each
    Event is `(op_type, ts, tx_id)`. Visibility resolution per
    `(K, RID, T_R)` follows the same predicate as the B-Tree.
  - Assertions: every `(K, RID, T_R)` probe agrees between B-Tree
    and oracle.
- **L2 concurrent-appender load test** for non-UNIQUE multi-version
  writes (per D37/S25; consumes Track 0's harness; expected
  scalability declared in design.md §"Expected MT Scalability").
  Lives under `tests/.../benchmarks/rollbacklog/multivalue/`. Three
  scenarios:
  - **`MultiValueConcurrentPut.DisjointKeys`** — N writers each
    append `put(K_i, RID_i)` on distinct K_i. No version-chain
    contention; expected scalability: ~14-16× on 16 cores
    (page-level locking on disjoint leaves under D18 + D36 + L&Y
    dominates).
  - **`MultiValueConcurrentPut.HotPair`** — N writers each
    append `put(K, RID_i)` with shared K but distinct RID. Each
    write lands at a distinct composite key `(K, RID_i, T_W)`,
    but all writes target the same leaf-region (entries with
    prefix K). Expected scalability: bounded by leaf-extension
    rate at the K-prefix region; ~3-5× on 16 cores.
  - **`MultiValueConcurrentPut.SameKAndRid`** — N writers each
    append entries at the same `(K, RID)` pair (each with a
    different `T_W` since each is its own tx). Pure ascending-
    ts insertion at the right edge of the `(K, RID)` group;
    expected scalability: bounded similarly to the hot-pair
    case but with tighter contention because all writes land in
    the same composite-key prefix-cluster. Expected ~2-4×.
  Adds `LoadTestExpectations` entries for all three with
  architectural-argument citations referencing D7 / D9 / D18.

## Plan of Work

- **Composite key serializer change** is isolated to one or two
  serializer classes used by `BTreeMultiValueIndexEngine`. Adding
  the `ts` field is mechanical.
- **Read-path visibility filter** is the most subtle piece. The
  current `BTreeMultiValueIndexEngine.iterateEntriesBetween` walks
  leaves and yields entries; the new code yields only entries
  whose visibility predicate at the reader's snapshot resolves to
  "present." Reverse iteration over the version chain at each
  `(K, RID)` cluster picks the highest visible.
- **Tombstone semantics**: a tombstone entry has a dedicated marker
  value (e.g., `TombstoneRID` or a flag). Existing `TombstoneRID`
  class can be reused.
- **Idempotency short-circuit** in `put` / `remove`: if the tx's
  own `(K, RID, T_W)` entry already exists in the leaf, don't
  re-append. Cheap check.
- **Tier-1 GC** runs at split time only (D9). The retention rule
  never drops the "baseline" (highest-ts entry with `ts ≤ LWM`)
  for any `(K, RID)`, ensuring readers with snapshot ≥ LWM see a
  correct view.
- This track is a **cohesive refactor** of non-UNIQUE / link-bag
  read+write semantics. Read and write changes land together per
  file: each file's read + write changes in the same step.
- Same-tx idempotency simplifies (delete the legacy "find existing
  first" branches). Pure append model.

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

**In scope:** `BTreeMultiValueIndexEngine.java`, the composite-key
serializer used by it (likely `IndexMultiValuKeySerializer.java`
or similar), `SharedLinkBagBTree.java` (for symmetry — also
composite-key-with-ts), `BTree.filterAndRebuildBucket` retention
rule, related tests.

**Out of scope:** the commit flow in `AbstractStorage`, the flush
boundary in `AtomicOperationBinaryTracking`, the rollback path —
all in Track D. This track retains tx-end buffered commit; the
tree is simply multi-version at commit-apply time.

Existing tests pinned to single-version semantics (e.g.,
assertions on B-Tree entry count per (K, RID)) must be updated
in this track. `IndexCountDelta` behavior is unchanged — continue accumulating
per put/remove. Accept the existing ±1 drift under concurrent
write-skew as non-goal.

**Inter-track dependencies:**
- **Track L** is a hard dependency — read-path correctness under
  concurrent splits requires the right-link descender. Without it,
  a reader could miss an entry that's mid-relocation between two
  split-time component ops.
- **Track H** is independent — H affects UNIQUE indexes only.
- **Track D** depends on this for non-UNIQUE rollback semantics.
