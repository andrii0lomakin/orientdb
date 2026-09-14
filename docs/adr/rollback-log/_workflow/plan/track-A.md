# Track A: WAL scaffolding — undo, ComponentOperationEndRecord, LogicalOperationDescriptor, tryConvertToWriteLock, StatsStatus

## Purpose / Big Picture
Adds the WAL primitives and cache-layer hooks every downstream track consumes — all additive, no runtime consumer yet.

<!-- Reserved for Move 2 — ADDED/MODIFIED/REMOVED triad. Empty until Move 2 lands. -->

Adds the WAL + StampedLock building blocks consumed by every downstream track: `undo(DurablePage)` on every `PageOperation` subclass; `ComponentOperationEndRecord` + `LogicalOperationDescriptor` (descriptor's `prev_value_with_metadata` load-bearing under D6/S14); `PageFrame.tryConvertToWriteLock` paired with the **D40** uniform-`getPageFrameOptimistic` cache-layer change (cold disk load + memory storage; S26); `StatsStatus` enum + public-API adapters for D29 signaling. All additive — no runtime consumer yet.
**Scope:** ~7 steps including L1 JMH for `tryConvertToWriteLock` (D37/S25) and the D40 cache-layer step paired with it.

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

- Add `public abstract void undo(DurablePage page)` to
  `com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.PageOperation`.
  Pure page-level logical inverse of `redo(page)` — physiological:
  logical within a page, operating through the same page-API
  abstractions `redo` uses (insert / remove / replace entry at a
  position, set a field, etc.). Must not read or write
  `page.getLsn()`, must not perform state inspection or
  self-idempotence checks. LSN management happens at the
  portion-level applier in recovery (see **D3**, **D16**).
- Implement `undo` on every concrete `PageOperation` subclass in the
  `wal.pageop.*` hierarchy (`WALRecordTypes` IDs 77–198 for the
  current set, less the `CellBTreeMultiValueV2*` subclasses being
  deleted in Track L).
- For subclasses whose inverse isn't derivable from existing payload
  fields (e.g., a replace op needs the old value), extend the wire
  format. On-disk compatibility is not a constraint.
- `initialLsn` on the `PageOperation` record stays unchanged from
  today (captured per-op at portion-create time, shared across
  PageOps of the same portion). Its role is re-described from
  "per-op CAS diagnostic" to "pre-portion page LSN, consumed by the
  portion-level UNDO applier as the post-UNDO LSN." No capture-site
  changes.
- Introduce `ComponentOperationEndRecord`: numeric type ID
  registered in `WALRecordTypes`, class in the `wal` package,
  serializer/deserializer consistent with existing records. Carries
  the atomic-operation ts (for grouping) and a component identifier
  (for recovery-time attribution).
- Introduce `LogicalOperationDescriptor`: numeric type ID,
  serializer. The schema is union-typed by `op_type` — different
  op types carry different field sets, encoded as a discriminated
  union for compact wire format. `op_type` is an enum spanning:
  - **Index ops** — `INDEX_PUT_UNIQUE`, `INDEX_REMOVE_UNIQUE`,
    `INDEX_PUT_NONUNIQUE`, `INDEX_REMOVE_NONUNIQUE`. Schema:
    `(op_type, indexId, key_bytes, prev_value_or_absent,
    new_value_or_absent, op_metadata)`. `prev_value` for UNIQUE
    includes `(prev_RID, prev_writer_tx_id, prev_start_ts)`.
    **Load-bearing under D6**: this is the **single source of truth**
    for rollback inverses on UNIQUE put/remove (the history tree is
    non-durable and is wiped before recovery REDO begins, so recovery
    cannot read it). Schema must be designed with this in mind —
    every field needed to restore in-tree to its pre-op state must
    be on the record.
  - **Linkbag ops** — `LINKBAG_ADD`, `LINKBAG_REMOVE`. Schema:
    `(op_type, treeId, edge_key_bytes, op_metadata)`.
  - **Record ops** — `RECORD_CREATE`, `RECORD_UPDATE`, `RECORD_DELETE`.
    Schema (D23):
    - `RECORD_CREATE`: `(op_type, rid)` — ~16 B. Inverse derives
      deletion semantics; no prev field needed.
    - `RECORD_UPDATE`: `(op_type, rid, prev_position_entry)` —
      ~36 B. `prev_position_entry = (pageIndex, slotOffset, version,
      status)`. **Load-bearing under D23**: the descriptor's
      position pointer is the single source of truth for the
      inverse; recovery-time rollback never reads the snapshot
      index. The chunks at `prev_position_entry`'s slot are
      guaranteed physical via the LWM gate (S15 + the snapshot-
      index visibility-key mechanism).
    - `RECORD_DELETE`: `(op_type, rid, prev_position_entry)` —
      ~36 B. Same shape and rationale as `RECORD_UPDATE`.
    The descriptor stays at fixed ~36 B regardless of record
    content size; new-content bytes ride on the existing
    `CollectionPageAppendRecordOp` PageOps, not on the descriptor.
- Update `MetaDataRecord` / `toStream` / `fromStream` registries so
  the new record types round-trip.
- Add `public long tryConvertToWriteLock(long stamp)` on
  `com.jetbrains.youtrackdb.internal.common.directmemory.PageFrame`.
  One-line delegation to the underlying
  `StampedLock.tryConvertToWriteLock(stamp)`. Returns a non-zero
  write stamp if the optimistic stamp is still valid (caller now
  holds the write lock on the frame), or `0` if any writer has
  bumped the lock version since the stamp was issued.
- Unit tests for `PageFrame.tryConvertToWriteLock`: success case,
  intervening-writer failure case, post-eviction failure case.
- **D40 — Uniform StampedLock-backed reads (paired with
  `tryConvertToWriteLock` since both are foundational cache-layer
  primitives consumed by Track D's commit-time validate-and-upgrade
  loop):**
  - **`ReadCache` interface signature change**: extend
    `getPageFrameOptimistic(long fileId, long pageIndex)` to
    `getPageFrameOptimistic(long fileId, long pageIndex,
    WriteCache writeCache, boolean verifyChecksums)` so the cache
    layer can load on miss when needed. The single caller
    (`StorageComponent.loadPageOptimistic` at
    `core/.../paginated/base/StorageComponent.java:279`) is updated
    to pass `writeCache` (already a protected field) and `true`.
  - **`LockFreeReadCache.getPageFrameOptimistic` extension**: hot
    path remains the cache-hit lookup unchanged (return
    `cacheEntry.getCachePointer().getPageFrame()` for an alive
    entry). On miss, fall through to `doLoad(fileId, (int)
    pageIndex, writeCache, verifyChecksums)` to load + pin +
    install in CHM, then immediately call
    `releaseFromRead(cacheEntry)` to drop the pin `doLoad`
    acquired, and return the underlying `PageFrame`. Pin lifetime
    is bounded to I/O wait + frame publication. Coordinate-and-
    stamp validation in the caller (the existing
    `frame.getFileId() == fileId && frame.getPageIndex() ==
    pageIndex` check at
    `StorageComponent.java:292-294`, plus the trailing
    `frame.validate(stamp)` / `tryConvertToWriteLock(stamp)` at
    scope-end) catches any eviction-and-recycle that races between
    drop-pin and the caller's `tryOptimisticRead` (S26).
  - **`DirectMemoryOnlyDiskCache.getPageFrameOptimistic`
    enablement**: replace the current hardcoded `return null` at
    `core/.../storage/memory/DirectMemoryOnlyDiskCache.java:411-416`
    with a direct lookup through the per-file `MemoryFile`'s
    `ConcurrentSkipListMap` (`MemoryFile.loadPage(pageIndex)`),
    extract the frame via
    `cacheEntry.getCachePointer().getPageFrame()`, return null
    only when the file or page is absent. Memory storage allocates
    a frame once via `MemoryFile.addNewPage` and never recycles or
    evicts it (`clear()` only fires on file deletion), so stamp
    invalidation is purely writer-driven; no eviction race exists
    and no coordinate-recycle check is structurally necessary,
    though the caller's coordinate check stays uniform across
    storage shapes.
  - **`recordOptimisticAccess` symmetry**: leave
    `DirectMemoryOnlyDiskCache.recordOptimisticAccess` as the
    existing no-op (memory storage has no eviction policy to
    update); `LockFreeReadCache.recordOptimisticAccess` is
    unchanged (frequency-sketch update on the just-validated
    frame's identity).
  - **Unit tests**:
    - `LockFreeReadCacheColdLoadOptimisticTest` — cold-miss
      returns a `PageFrame` with the requested coordinates;
      post-return pin count is zero on the new entry; subsequent
      `tryOptimisticRead` succeeds; concurrent writer between
      cold-load and validate causes stamp invalidation; concurrent
      eviction between drop-pin and `tryOptimisticRead` is caught
      by the coordinate check after the recycle.
    - `DirectMemoryOnlyDiskCacheOptimisticTest` — warm read
      returns the same frame for identical (fileId, pageIndex);
      returns null for absent file or absent page; concurrent
      writer invalidates an outstanding stamp (validate returns
      false); concurrent reader with no writer holds a valid
      stamp through validate.
  - **VMLens MT test**: `MemoryStorageOptimisticReadMTTest` —
    2-thread, single-op-per-thread structure per the project's
    VMLens convention (`MAX_ITERATIONS=100`); reader thread takes
    `tryOptimisticRead` + reads payload + validates; writer
    thread acquires exclusive lock + mutates + releases. Every
    interleaving must satisfy: reader either succeeds with a
    stable view or fails stamp validation and falls back; reader
    never observes torn data; reader never gets a stale view that
    passes validation.
- Unit tests per `PageOperation` subclass asserting inverse
  correctness via logical-equivalence comparator (entry iteration,
  counters, relevant header fields). Byte-level layout differences
  are acceptable.
- Test infrastructure: per-page-type logical-equivalence
  comparators in `test-commons` (e.g.,
  `assertLogicalEquivalent(expected, actual)`).
- Unit tests for `ComponentOperationEndRecord` and
  `LogicalOperationDescriptor` serialization round-trip.
- Helper for the S14 write-time assertion (defensive check used by
  the engine before emitting a UNIQUE-index descriptor): pure
  function `MatchAssertions.checkPrevValueCoherent(descriptor,
  inTreeValueJustRead)` returning `boolean` — extracted to a static
  helper per the Java-assert / JaCoCo guidance in CLAUDE.md so both
  true/false branches are unit-testable independently. Track H
  wires the actual call site inside the engine.
- Helper for the S15 write-time assertion (defensive check used by
  `PaginatedCollectionV2.updateRecord` / `deleteRecord` before
  emitting a `RECORD_UPDATE` / `RECORD_DELETE` descriptor): pure
  function `RecordRollbackAssertions.checkPrevPositionCoherent(
  descriptor.prevPositionEntry, cpmEntryJustRead)` returning
  `boolean` — same Java-assert / JaCoCo extraction pattern as the
  S14 helper. Track D wires the actual call sites inside
  `PaginatedCollectionV2`.
- **D29 scaffolding — `StatsStatus` enum + public-API adapters.**
  Add `public enum StatsStatus { VALID, REBUILDING }` under
  `com.jetbrains.youtrackdb.api.collection` (or a similar
  public-API package consistent with existing public types).
  Add `DatabaseSessionEmbedded.getCollectionStatsStatus(int
  collectionId): StatsStatus` and a class-level adapter
  `SchemaClass.getStatsStatus(DatabaseSessionEmbedded session):
  StatsStatus` that returns `REBUILDING` if any underlying
  collection is `REBUILDING`, `VALID` only when all are
  `VALID`. Implementations of these methods initially return a
  constant `VALID` (no per-collection state field exists yet —
  Track D adds the `volatile statsStatus` field on
  `PaginatedCollectionV2`); Track D rewires them to read the
  real field. Unit tests assert the API surface compiles, the
  adapter aggregates correctly across multiple collections, and
  the constant-`VALID` placeholder is reachable from a
  `DatabaseSessionEmbedded`. The enum and APIs are dormant —
  no DDL or planner consumer reads them yet (Track D wires those).
- **L1 JMH microbenchmark for `PageFrame.tryConvertToWriteLock`**
  (per D37/S25; consumes Track 0's harness; expected scalability
  declared in design.md §"Expected MT Scalability"). Three
  scenarios:
  - **`UpgradeAlone`** — 1 thread holds an optimistic stamp,
    calls `tryConvertToWriteLock`, releases, repeats. Measures
    the cost of the upgrade primitive itself; serves as the
    single-thread baseline for the scalability-factor denominator
    in Phase 4's report.
  - **`UpgradeOnSameFrame`** — N threads each hold an optimistic
    stamp on the **same** `PageFrame` and race to upgrade.
    Exactly one wins per round; the others fall back. Measures
    fallback rate vs. throughput as N scales; expected to plateau
    (no parallelism on a contended frame is the correct outcome).
  - **`UpgradeOnDisjointFrames`** — N threads each hold an
    optimistic stamp on a **distinct** `PageFrame` and call
    `tryConvertToWriteLock`. All succeed; throughput should scale
    near-linearly. **The point of D18 is that this scenario
    produces N× throughput on disjoint pages.** Phase 4 verifies
    the prediction; gap-analysis flags any deviation > 2×.
  Smoke-runs locally against legacy code-equivalent stub if the
  primitive is mid-implementation; final smoke-run after
  `tryConvertToWriteLock` lands. Adds `tryconvertwrite/` JMH
  subdirectory under `tests/.../benchmarks/rollbacklog/` plus
  `LoadTestExpectations` entries.

## Plan of Work

- This track is **purely additive**. No runtime consumer writes or
  reads the new records yet; no code path calls
  `PageOperation.undo()`. The new behavior is dormant until Track D
  wires it in.
- Audit the `PageOperation` subclass list. Implement `undo` for each
  as a pure page-level logical inverse, using the same
  record-payload parameters as the forward direction and the same
  page-API abstractions (`insertEntry`, `removeEntry`, `setValue`,
  etc.). Where the inverse isn't derivable from existing fields,
  extend the wire format.
- **Do not** read or write `page.getLsn()` inside `undo`. **Do not**
  add byte-inspection or self-idempotence checks. A subclass's
  `undo` has no awareness of portion boundaries, LSN state, or
  whether its effect is currently on the page — it is only invoked
  by the portion-level recovery applier (Track D), which owns LSN
  gating and exclusive-latch discipline.
- Keep `undo` independent of any snapshot index or external state —
  pure page transforms only.
- `LogicalOperationDescriptor` design: keep the schema small. The
  descriptor is read once during recovery's analysis pass per
  in-flight tx; per-PageOp ops don't read it. Bytes per descriptor
  should be < 64 typical.

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

**In scope:** every subclass under
`core/src/main/java/com/jetbrains/youtrackdb/internal/core/storage/impl/local/paginated/wal/`
that extends `PageOperation` (excluding `CellBTreeMultiValueV2*`
which Track L deletes); the two new WAL record types; the single
new method on `PageFrame`; the `StatsStatus` enum + the two
public-API adapter methods (`getCollectionStatsStatus`,
`SchemaClass.getStatsStatus`); the L1 JMH microbenchmark for
`PageFrame.tryConvertToWriteLock` (lives under
`tests/.../benchmarks/rollbacklog/tryconvertwrite/`).

**D40 in scope**: `ReadCache.getPageFrameOptimistic` signature
change; `LockFreeReadCache.getPageFrameOptimistic` cold-load
path; `DirectMemoryOnlyDiskCache.getPageFrameOptimistic`
enablement; the single call-site update in
`StorageComponent.loadPageOptimistic`; the three new test classes
(`LockFreeReadCacheColdLoadOptimisticTest`,
`DirectMemoryOnlyDiskCacheOptimisticTest`,
`MemoryStorageOptimisticReadMTTest`).

**D40 out of scope**: any other `ReadCache` API change; any
change to `loadForRead` / `loadForWrite` / `releaseFromRead` /
`releaseFromWrite`; any change to `MemoryFile` (the per-file CHM
lookup is reused verbatim); any change to `PageFramePool` or
`CachePointer` lifecycle.

**Out of scope:** `UpdatePageRecord` (binary-diff record) — deprecated
under the new model; leave its `undo` as a safe no-op or throw
`UnsupportedOperationException` pending removal in Track D.

No wiring into recovery or runtime yet — that happens in Track D.
The new `PageFrame.tryConvertToWriteLock` has no callers after
Track A; Track D adds the single call site inside the cache-layer
upgrade overload. The L1 microbenchmark has only the JMH
`@State` setup. The D40 cache-layer change has exactly one production caller
after Track A — `StorageComponent.loadPageOptimistic` — already
exercised by the existing optimistic-read sites that Track R
later expands.

**D39: `RECORD_DELETE` / `RECORD_UPDATE` descriptor schema is
fixed at `(rid, prev_position_entry)` — `prev_content` is
forbidden.** The descriptor stays at ~36 B fixed regardless of
record size for both op types. The Track A schema design must NOT
add a `prev_content` (or equivalent record-content) payload field
to `RECORD_DELETE` or `RECORD_UPDATE`. The corresponding inverse
component ops in Track D are required to be CPM bit-flip
resurrection (CPM `REMOVED` → `WRITTEN` for `RECORD_DELETE`;
redirect to `prev_position_entry` for `RECORD_UPDATE`) plus an
`approximateRecordsCount` adjustment, never a content-replay
shape that calls `findNewPageToWrite` or writes record-content
chunks. The chunks at `prev_position_entry` are guaranteed
physical via the LWM gate (runtime: snapshot-index visibility
key; recovery: D32's recovery-window `TsMinHolder`). A
`prev_content` payload would (i) bloat the descriptor with
variable-size payload, (ii) push WAL volume per inverse from
2-3 PageOps to `ceil(record_size / 8095)` `CollectionPageAppendRecordOp`
records, and (iii) force the inverse to write fresh chunks per
rollback — collectively producing the disk-exhaustion failure
mode on rollback-heavy workloads that D39 exists to forbid. See
**D39** (rationale, alternative `(a)` rejection) and **D23**
(the original alternative-`(d)` selection D39 promotes to
binding). Track-level code review verifies the descriptor
schema matches this constraint.

**Inter-track dependencies:**
- **Track D** consumes `PageOperation.undo()` via the crash-recovery
  path; emits and consumes `ComponentOperationEndRecord` and
  `LogicalOperationDescriptor`; uses
  `PageFrame.tryConvertToWriteLock` via the new cache-layer
  overload. Track D's commit-time `loadForWrite(..., frame, stamp)`
  overload is the matching write-side primitive to D40's
  uniform-read primitive — together they make D18's
  validate-and-upgrade work uniformly across warm cache, cold
  disk loads, and memory storage.
- **Track L** deletes the `CellBTreeMultiValueV2*` subclasses, so
  their `undo` implementations don't need to be written here.
- **Track R** (subsystem-read migration) benefits from D40
  immediately: each migrated reader stops paying the
  component-shared-lock fallback on memory storage and on
  cache-miss workloads, so Track R's expected throughput
  contribution becomes universal rather than disk-warm-cache-only.
- No direct interaction with Tracks B, C, V, H, E, F.
