# Track E: History-store purge (non-durable)

## Purpose / Big Picture
Adds a periodic range-delete task that reclaims history-tree entries older than the global LWM.

<!-- Reserved for Move 2 — ADDED/MODIFIED/REMOVED triad. Empty until Move 2 lands. -->

New periodic task that range-deletes history-tree entries with `replaced_at_ts < LWM`. Mirrors `PeriodicRecordsGc` scheduling + `stateLock.readLock()` outer guard; diverges on discovery (range scan by ts), budgeting (per-cycle leaf cap), and WAL (pure non-durable, S7-relaxed). Crash safety trivial — cursor in-memory, history file wiped before REDO.
**Scope:** ~2 steps.
**Depends on:** Track H, Track D

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

- New `HistoryBTreePurge` runnable (thin wrapper mirroring
  `PeriodicRecordsGc` in shape) delegating to
  `AbstractStorage.periodicHistoryPurge()`.
- New method on `HistoryBTree`:
  `purgeRange(atomicOp, fromKey, budget, lwm) → PurgeResult`. Walks
  forward from `fromKey`, visiting up to `budget` leaves; for each
  leaf with entries `replaced_at_ts < lwm`, issues a component op
  that removes those entries. **Each component op is pure-non-durable
  (S7-relaxed) — emits zero WAL records.**
- In-memory cursor `(indexId, lastPurgedKey)` + `lastLwm` for "no-
  advancement → no-work" fast skip. Lost on crash; recovery wipes
  history file anyway, so a fresh empty cursor + fresh empty file
  are coherent.
- Scheduling: `DiskStorage.open()` registers `HistoryBTreePurge` on
  the shared `fuzzyCheckpointExecutor` via `scheduleWithFixedDelay`.
  Cancellation at `DiskStorage.close()`.
- New `GlobalConfiguration` entries:
  - `STORAGE_HISTORY_PURGE_INTERVAL_SECS` (default 600)
  - `STORAGE_HISTORY_PURGE_BUDGET_PAGES_PER_CYCLE` (default 1024)
- Integration tests:
  - LWM-stuck behavior: hold a long-running tx; verify purge does
    no-op cycles without allocating or logging.
  - Concurrent-write-and-purge: writer doing rapid puts while
    purge sweeps; verify cursor doesn't get stuck and no data
    races.
  - Kill-and-restart-resume: kill JVM mid-purge, restart, verify
    post-recovery the history file is empty (Step 0 wipe), purge
    resumes from a fresh cursor and operates correctly on new
    entries.

## Plan of Work

- **Reuse the records-GC pattern for scheduling and the deadlock
  guard** (see D15). `PeriodicRecordsGc` +
  `AbstractStorage.periodicRecordsGc` (`AbstractStorage.java:6201`)
  provides:
  - Shared `fuzzyCheckpointExecutor.scheduleWithFixedDelay`.
  - `stateLock.readLock().tryLock()` outer guard against the 3-way
    deadlock with file-delete + flush.
  - Per-component-op execution through
    `executeInsideAtomicOperation(op -> executeInsideComponentOperation(op, ...))`.
  - Per-tree exception isolation.
- **Simplifications relative to records GC** (because the tree is
  non-durable):
  - **No WAL discipline.** Each purge component op modifies only
    non-durable history pages, so the entire op is "pure non-durable"
    — `AtomicOperationBinaryTracking.commitChanges` short-circuits
    to "no WAL records emitted" automatically.
  - **No CLR concerns.** A crash mid-purge has nothing to recover —
    the file is wiped at next open.
  - **No cursor durability.** In-memory only; loss on crash is fine.
- **Diverge on**:
  - **Discovery**: range scan on `replaced_at_ts < lwm` rather than
    dirty-page bit-set. Natural fit for the history tree's keying.
  - **Budget**: explicit per-cycle leaf cap.
- **Per-leaf execution**:
  1. Load leaf via `loadPageOptimistic`.
  2. Scan entries; collect those with `replaced_at_ts < lwm`.
  3. If drop-set empty, skip.
  4. Otherwise, `executeInsideComponentOperation` with leaf
     exclusive latch; remove the eligible entries. Component op
     commits with no WAL records.
  5. Advance cursor.
- **LWM interaction**: each cycle calls
  `AbstractStorage.computeGlobalLowWaterMark()` once. If LWM hasn't
  advanced since previous cycle, return without iterating.

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
- New files: `HistoryBTreePurge.java`, `HistoryPurgeState.java`.
- New methods on `AbstractStorage`: `periodicHistoryPurge()` +
  helpers.
- New methods on `HistoryBTree`: `purgeRange`.
- New `GlobalConfiguration` entries.
- Registration in `DiskStorage.open()` and
  `DiskStorage.close()`.

**Out of scope:**
- Changes to `PeriodicRecordsGc` — reuse as-is.
- Tier-2 / Tier-3 for inline trees — not needed (D9).
- Durable cursor persistence (cursor is RAM-only).
- Crash-during-cycle resume logic — not needed; restart wipes
  history and resumes from empty.

Must use `executeOptimisticStorageRead` for leaf loads. Must not starve writers: per-leaf lock acquisition through
standard component-op machinery.

**Inter-track dependencies:**
- **Track H** is a hard dependency — provides the (non-durable)
  history B-Tree to purge, plus the constructor-variant on `BTree`
  that supports `durable=false`.
- **Track D** is a hard dependency — purge mutations are ordinary
  component ops; D's S7-relaxation makes pure-non-durable purge
  ops legal.
- **Track L** indirect dependency — purge's optimistic leaf loads
  route through L&Y descender.
- **Track F** downstream — purge metrics
  (`history_purge_rate_per_minute`, `noop_cycles`,
  `history_disk_bytes`) integrate.
