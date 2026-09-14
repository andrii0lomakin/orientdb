# Track C: UNIQUE-index claim table

## Purpose / Big Picture
Adds an in-memory `UniquenessClaimTable` per UNIQUE index to detect cross-tx uniqueness violations without scans.

<!-- Reserved for Move 2 — ADDED/MODIFIED/REMOVED triad. Empty until Move 2 lands. -->

Adds `UniquenessClaimTable` — concurrent per-UNIQUE-index map of `(userKey) → txId` held tx-long. Replaces scan-based uniqueness in `validatedPut`. Required even under buffered commit because the new in-place UNIQUE write model would otherwise allow cross-tx write-skew.
**Scope:** ~4 steps including `UniquenessClaimTableMTTest` (VMLens, D22) and L1 JMH for hot/cold/bimodal contention (D37/S25).

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

- New class `UniquenessClaimTable` per UNIQUE index. Holds a
  `ConcurrentHashMap<CompositeKey, Long>` mapping `userKey → txId`.
  Single global instance per UNIQUE index, created at index open.
- Integration point: `BTreeSingleValueIndexEngine.validatedPut`.
  Before persisting the new entry, `compute` a claim on
  `(userKey) → currentTxId`. If another in-flight tx holds the
  claim → throw `RecordDuplicatedException` / equivalent
  uniqueness-violation exception.
- Tx-lifecycle hooks: `AtomicOperationsManager.endAtomicOperation`
  (commit and rollback both) releases the tx's claims from all
  claim tables. Integrate with the existing operation-end cleanup
  that processes `lockedComponents`.
- `validatedPut` legacy scan removed (the pre-insert range scan for
  uniqueness is now obsolete).
- Tests:
  - Two concurrent UNIQUE puts on same key → one succeeds, one
    throws.
  - Put, commit, second put of same key → first-committer stays
    visible.
  - Put, rollback, put of same key by second tx → claim released,
    second succeeds.
  - Claim leaks on tx abandonment → caught by timeout-eviction hook
    (tie into existing stale-tx handling if present; otherwise
    document as accepted leak).
  - **VMLens MT test** (`UniquenessClaimTableMTTest`, per D22) —
    exhaustively explores thread interleavings using
    `AllInterleavingsBuilder` (pattern from
    `AtomicOperationsTableMTTest`):
    - Two concurrent `tryClaim(K)` on the same key in different
      txs: every interleaving must produce exactly one winner
      (the other throws / returns failure); no schedule leaves
      two claims live.
    - Concurrent `tryClaim` + `release` in different txs on
      different keys must not interfere — every interleaving leaves
      both maps in a self-consistent state.
    - `tryClaim → release` round-trip on a single key under a
      second tx's concurrent `tryClaim` for the same key: every
      interleaving where `release` happens-before the second
      `tryClaim` must let the second `tryClaim` succeed; every
      interleaving where the second `tryClaim` runs first must
      have it observe the live claim and fail.
    - 2-thread, single-op-per-thread, `MAX_ITERATIONS = 100` to
      stay within the VMLens-internal-limit envelope.
- **L1 JMH microbenchmark for `UniquenessClaimTable`** (per D37/S25;
  consumes Track 0's harness; expected scalability declared in
  design.md §"Expected MT Scalability"). Lives under
  `tests/.../benchmarks/rollbacklog/claimtable/`. Three scenarios:
  - **`ClaimColdKeys`** — N writers each acquire claims on
    distinct (random) keys. CHM is fully striped; expected
    scalability: ~14-16× on 16 cores (CHM stripe count default
    is 16+).
  - **`ClaimHotKey`** — N writers all attempt to claim the same
    key. Exactly one wins; the others throw
    `RecordDuplicatedException`. Expected scalability: 1× (forced
    serialization on the hot key, but the cost per failed claim
    should be trivially small — measured here).
  - **`ClaimBimodal`** — 80% writers claim cold keys, 20%
    contend on a small hot-key set. Expected scalability:
    intermediate; the hot-key contention should not degrade the
    cold-key path's throughput materially. Validates that
    stripe-level contention stays bounded.
  Adds `LoadTestExpectations` entries for all three.

## Plan of Work

- The claim table is in-memory only. Crash recovery doesn't need to
  reconstruct claims — the tree state (committed entries) is
  authoritative post-crash.
- Use `ConcurrentHashMap.compute` for race-free claim acquisition:
  if existing holder is our txId → no-op; else if null → set to our
  txId; else → throw.
- Claim release on tx end: iterate the tx's set of claimed
  `(indexId, userKey)` entries (tracked per-tx on the
  `AtomicOperation` object) and `remove` each matching `(userKey,
  txId)` — use atomic remove-if-equals to avoid clobbering another
  tx's later claim.
- Hot UNIQUE indexes may benefit from stripe-hashed claim tables to
  avoid a single-map hot-bucket; design with stripe-ability in mind
  even if the initial implementation uses a single map.

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

**In scope:** new `UniquenessClaimTable` class,
`BTreeSingleValueIndexEngine.validatedPut`,
`BTreeSingleValueIndexEngine.put` call sites, tx end hook in
`AtomicOperationsManager`.

**Out of scope:** NOT_UNIQUE / ALLOW_MULTIVALUE indexes, link-bag
writes, record-level locks.

The claim is on `userKey` only (not `(userKey, rid)`) — UNIQUE
index semantics mandate one rid per userKey. Do not attempt cross-process / distributed coordination — single
process only. Do not persist claims across crashes.

**Inter-track dependencies:**
- **Track H** (history B-Tree) integrates with the same
  `BTreeSingleValueIndexEngine` and produces the read fall-through
  path; tx-lifecycle integration aligns with H's commit/rollback
  flow.
- **Track D** is downstream — tx-lifecycle hooks in
  `AtomicOperationsManager.endAtomicOperation` must be coordinated
  with the new commit/rollback semantics, but the integration point
  is the same method regardless of in-place vs. buffered.
- **Independent of Tracks A, L, V, R, E**.
