package com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.atomicoperations;

import com.jetbrains.youtrackdb.internal.common.concur.collection.CASObjectArray;
import com.jetbrains.youtrackdb.internal.common.concur.lock.ScalableRWLock;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLong;

/// A concurrent operation tracking table for snapshot isolation.
///
/// The table stores operation states in bounded contiguous ranges. A range is one contiguous
/// interval of registered operation identifiers with its own dense array. The ordered range
/// directory locates a range by identifier without allocating unused identifier gaps.
///
/// The directory is a chunked array of range descriptors with strictly increasing start
/// identifiers. A binary search over that array locates a range with a deterministic logarithmic
/// probe count. A positional read of one descriptor costs constant work, so an ascending or a
/// descending scan over all ranges costs work proportional to the number of ranges. Appending a
/// descriptor never copies the previously published descriptors.
///
/// Registration remains serialized by [AtomicOperationsManager], while snapshots and lifecycle
/// transitions run concurrently under the shared compaction lock.
public class AtomicOperationsTable {

  private static final long UNKNOWN_TS = Long.MIN_VALUE;
  private static final int MAX_FORWARD_SCAN = 128;

  // This capacity is well below CASObjectArray's integer index boundary. A package-private
  // constructor supplies a smaller capacity to exercise splits without large allocations.
  private static final int DEFAULT_RANGE_CAPACITY = 1 << 20;

  private static final VarHandle CACHED_MIN_ACTIVE_TS;

  static {
    try {
      CACHED_MIN_ACTIVE_TS = MethodHandles.lookup().findVarHandle(
          AtomicOperationsTable.class, "cachedMinActiveTs", long.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final ScalableRWLock compactionLock = new ScalableRWLock();
  private final int tableCompactionInterval;
  private final int rangeCapacity;
  private final long initialTs;
  private final Runnable directoryReadObserver;
  private final AtomicLong operationsStarted = new AtomicLong();

  // The directory publishes only fully initialized ranges, because a descriptor becomes
  // reachable when the directory length grows, and the length grows after the descriptor
  // reference is written.
  private volatile RangeDirectory ranges;

  // Compaction records how many descriptors it copied. Tests read this number to observe
  // compaction work directly. Compaction writes the number once for each invocation, so the
  // field adds no work to registration, snapshot or transition paths.
  private volatile int lastCompactedRangeCount;

  // The high-water mark survives compaction and an empty directory. AtomicOperationsManager
  // serializes registration, so publication after the entry write preserves registration order.
  private volatile boolean hasRegisteredOperation;
  private volatile long highestRegisteredTs;
  private volatile long lastCompactionOperation;

  @SuppressWarnings("FieldMayBeFinal") // accessed through a VarHandle
  private volatile long cachedMinActiveTs = UNKNOWN_TS;

  /// An immutable snapshot used to decide whether a record version is visible.
  public record AtomicOperationsSnapshot(long minActiveOperationTs,
      long maxActiveOperationTs,
      LongOpenHashSet inProgressTxs,
      long snapshotTs) {

    public boolean isEntryVisible(long recordTs) {
      if (recordTs < minActiveOperationTs) {
        return true;
      }
      if (recordTs > snapshotTs) {
        return false;
      }
      if (recordTs > maxActiveOperationTs) {
        return true;
      }
      return !inProgressTxs.contains(recordTs);
    }
  }

  public AtomicOperationsTable(final int tableCompactionInterval, final long tsOffset) {
    this(tableCompactionInterval, tsOffset, DEFAULT_RANGE_CAPACITY, null);
  }

  AtomicOperationsTable(
      final int tableCompactionInterval, final long tsOffset, final int rangeCapacity) {
    this(tableCompactionInterval, tsOffset, rangeCapacity, null);
  }

  // Tests can observe actual descriptor reads without adding a shared counter to production.
  // The production constructors pass no observer, so the default path allocates no measurement
  // object and performs no atomic counter update.
  AtomicOperationsTable(
      final int tableCompactionInterval,
      final long tsOffset,
      final int rangeCapacity,
      final Runnable directoryReadObserver) {
    if (rangeCapacity <= 0) {
      throw new IllegalArgumentException("Range capacity must be positive");
    }
    this.tableCompactionInterval = tableCompactionInterval;
    this.initialTs = tsOffset;
    this.rangeCapacity = rangeCapacity;
    this.directoryReadObserver = directoryReadObserver;
    ranges = new RangeDirectory(directoryReadObserver);
  }

  public AtomicOperationsSnapshot snapshotAtomicOperationTableState(long currentTimestamp) {
    var minOp = Long.MAX_VALUE;
    var maxOp = Long.MIN_VALUE;
    final var inProgressTs = new LongOpenHashSet();

    compactionLock.sharedLock();
    try {
      final var cachedMin = cachedMinActiveTs;
      final var scanFromTs = cachedMin != UNKNOWN_TS ? cachedMin : Long.MIN_VALUE;
      final var directory = ranges;
      final var directorySize = directory.size();

      // Registrations publish a descriptor and its first entry before the directory length
      // grows. Descending positional traversal therefore preserves the happens-before
      // visibility argument used by the previous single-array implementation, and it reads each
      // descriptor with constant work.
      for (var rangeIndex = directorySize - 1; rangeIndex >= 0; rangeIndex--) {
        final var range = directory.get(rangeIndex);
        if (range.lastTs() < scanFromTs) {
          break;
        }
        for (var i = range.size() - 1; i >= 0; i--) {
          final var information = range.get(i);
          if (information.operationTs < scanFromTs) {
            break;
          }
          if (information.status == AtomicOperationStatus.IN_PROGRESS) {
            inProgressTs.add(information.operationTs);
            minOp = Math.min(minOp, information.operationTs);
            maxOp = Math.max(maxOp, information.operationTs);
          }
        }
      }

      final var hasActiveOperations = maxOp != Long.MIN_VALUE;
      if (!hasActiveOperations) {
        final var boundary = currentTimestamp == Long.MAX_VALUE
            ? Long.MAX_VALUE : currentTimestamp + 1;
        minOp = boundary;
        maxOp = boundary;
      }

      final var newCachedMin = hasActiveOperations ? minOp : UNKNOWN_TS;
      updateCachedMinimum(newCachedMin);
      return new AtomicOperationsSnapshot(minOp, maxOp, inProgressTs, currentTimestamp);
    } finally {
      compactionLock.sharedUnlock();
    }
  }

  public void startOperation(final long operationTs, final long segment) {
    changeOperationStatus(operationTs, null, AtomicOperationStatus.IN_PROGRESS, segment);
  }

  public void commitOperation(final long operationTs) {
    changeOperationStatus(
        operationTs, AtomicOperationStatus.IN_PROGRESS, AtomicOperationStatus.COMMITTED, -1);
  }

  public void rollbackOperation(final long operationTs) {
    changeOperationStatus(
        operationTs, AtomicOperationStatus.IN_PROGRESS, AtomicOperationStatus.ROLLED_BACK, -1);
  }

  public void persistOperation(final long operationTs) {
    changeOperationStatus(
        operationTs, AtomicOperationStatus.COMMITTED, AtomicOperationStatus.PERSISTED, -1);
  }

  public long getSegmentEarliestOperationInProgress() {
    return findEarliestSegment(false);
  }

  public long getSegmentEarliestNotPersistedOperation() {
    return findEarliestSegment(true);
  }

  private long findEarliestSegment(boolean includeCommitted) {
    compactionLock.sharedLock();
    try {
      final var directory = ranges;
      final var directorySize = directory.size();
      for (var rangeIndex = 0; rangeIndex < directorySize; rangeIndex++) {
        final var range = directory.get(rangeIndex);
        for (var i = 0; i < range.size(); i++) {
          final var information = range.get(i);
          if (information.status == AtomicOperationStatus.IN_PROGRESS
              || (includeCommitted && information.status == AtomicOperationStatus.COMMITTED)) {
            return information.segment;
          }
        }
      }
      return -1;
    } finally {
      compactionLock.sharedUnlock();
    }
  }

  private void changeOperationStatus(
      final long operationTs,
      final AtomicOperationStatus expectedStatus,
      final AtomicOperationStatus newStatus,
      final long segment) {
    if (newStatus != AtomicOperationStatus.IN_PROGRESS
        && operationsStarted.get() - lastCompactionOperation > tableCompactionInterval) {
      compactTable();
    }

    compactionLock.sharedLock();
    try {
      validateTransitionArguments(newStatus, segment);
      if (newStatus == AtomicOperationStatus.IN_PROGRESS) {
        registerOperation(operationTs, segment);
      } else {
        transitionOperation(operationTs, expectedStatus, newStatus);
      }

      if (expectedStatus == AtomicOperationStatus.IN_PROGRESS) {
        final var currentMinimum = cachedMinActiveTs;
        if (currentMinimum != UNKNOWN_TS && operationTs == currentMinimum) {
          final var nextTs = operationTs == Long.MAX_VALUE
              ? UNKNOWN_TS : findNextInProgressFrom(operationTs + 1);
          CACHED_MIN_ACTIVE_TS.compareAndSet(this, operationTs, nextTs);
        }
      }
    } finally {
      compactionLock.sharedUnlock();
    }
  }

  private static void validateTransitionArguments(
      AtomicOperationStatus newStatus, long segment) {
    if (segment >= 0 && newStatus != AtomicOperationStatus.IN_PROGRESS) {
      throw new IllegalStateException(
          "Invalid status of atomic operation, expected " + AtomicOperationStatus.IN_PROGRESS);
    }
    if (newStatus == AtomicOperationStatus.IN_PROGRESS && segment < 0) {
      throw new IllegalStateException(
          "Invalid value of transaction segment for newly started operation");
    }
  }

  private void registerOperation(long operationTs, long segment) {
    if (!hasRegisteredOperation) {
      if (operationTs < initialTs) {
        throw invalidRegistration(operationTs);
      }
    } else if (operationTs <= highestRegisteredTs) {
      throw invalidRegistration(operationTs);
    }

    final var information = new OperationInformation(
        AtomicOperationStatus.IN_PROGRESS, segment, operationTs);
    final var directory = ranges;
    final var directorySize = directory.size();
    final var lastRange = directorySize == 0 ? null : directory.get(directorySize - 1);
    if (lastRange != null && lastRange.canAppend(operationTs, rangeCapacity)) {
      // The entry array allocates any required storage before it writes the entry, and it
      // publishes the entry by growing its length last. A failed allocation therefore leaves no
      // reachable entry.
      lastRange.append(information);
    } else {
      // The descriptor and its first entry are fully built before the directory can reach them.
      // The directory also allocates its own storage before it writes the descriptor reference,
      // and it publishes the descriptor by growing its length last. A failed allocation
      // therefore leaves the identifier as a non-registered hole, which pins no range and blocks
      // no compaction.
      final var newRange = new OperationRange(operationTs, information);
      if (!directory.append(newRange, directorySize)) {
        throw new IllegalStateException("Operation range registration was not serialized");
      }
    }

    // Publish the high-water mark only after the range and entry are visible. No allocation
    // happens between publication and these writes, so an allocation failure cannot leave a
    // registered entry without its high-water mark.
    highestRegisteredTs = operationTs;
    hasRegisteredOperation = true;
    operationsStarted.incrementAndGet();
  }

  private IllegalStateException invalidRegistration(long operationTs) {
    final var boundary = hasRegisteredOperation ? highestRegisteredTs : initialTs;
    return new IllegalStateException(
        "Operation identifier " + operationTs
            + " does not exceed the registration boundary " + boundary);
  }

  private void transitionOperation(
      long operationTs, AtomicOperationStatus expectedStatus, AtomicOperationStatus newStatus) {
    final var range = findRange(operationTs);
    final var index = range.indexOf(operationTs);
    final var currentInformation = range.get(index);
    if (currentInformation.operationTs != operationTs) {
      throw missingOperation(operationTs);
    }
    if (currentInformation.status != expectedStatus) {
      throw new IllegalStateException(
          "Invalid state of atomic operation " + operationTs + ", expected " + expectedStatus
              + " but found " + currentInformation.status);
    }
    if (!range.compareAndSet(index, currentInformation,
        new OperationInformation(newStatus, currentInformation.segment, operationTs))) {
      throw new IllegalStateException("Concurrent state change of atomic operation " + operationTs);
    }
  }

  private OperationRange findRange(long operationTs) {
    final var directory = ranges;
    final var index = directory.floorIndex(operationTs, directory.size());
    if (index < 0) {
      throw missingOperation(operationTs);
    }
    final var range = directory.get(index);
    if (!range.contains(operationTs)) {
      throw missingOperation(operationTs);
    }
    return range;
  }

  private static ArrayIndexOutOfBoundsException missingOperation(long operationTs) {
    return new ArrayIndexOutOfBoundsException(
        "Atomic operation " + operationTs + " is not retained in the table");
  }

  private long findNextInProgressFrom(long startTs) {
    var scanned = 0;
    final var directory = ranges;
    final var directorySize = directory.size();
    // A range that starts below the requested identifier can still contain it, so the scan
    // starts at the floor descriptor. A descriptor that lies entirely below the requested
    // identifier contributes no entry to the scan.
    final var floorIndex = directory.floorIndex(startTs, directorySize);
    final var firstRangeIndex = floorIndex < 0 ? 0 : floorIndex;

    for (var rangeIndex = firstRangeIndex; rangeIndex < directorySize; rangeIndex++) {
      final var range = directory.get(rangeIndex);
      for (var index = range.firstIndexAtOrAfter(startTs); index < range.size(); index++) {
        if (++scanned > MAX_FORWARD_SCAN) {
          return UNKNOWN_TS;
        }
        final var information = range.get(index);
        if (information.status == AtomicOperationStatus.IN_PROGRESS) {
          return information.operationTs;
        }
      }
    }
    return UNKNOWN_TS;
  }

  private void updateCachedMinimum(long newCachedMin) {
    while (true) {
      final var currentMinimum = cachedMinActiveTs;
      if (currentMinimum == newCachedMin
          || (currentMinimum != UNKNOWN_TS && newCachedMin == UNKNOWN_TS)
          || (currentMinimum != UNKNOWN_TS && newCachedMin != UNKNOWN_TS
              && newCachedMin < currentMinimum)) {
        return;
      }
      if (CACHED_MIN_ACTIVE_TS.compareAndSet(this, currentMinimum, newCachedMin)) {
        return;
      }
    }
  }

  /// Removes the globally leading prefix of persisted, rolled-back, or empty entries.
  public void compactTable() {
    compactionLock.exclusiveLock();
    try {
      final var source = ranges;
      final var sourceSize = source.size();
      final var compacted = new RangeDirectory(directoryReadObserver);
      var copiedRangeCount = 0;
      var foundRetainedEntry = false;

      // Every retained descriptor is appended once, and an append costs constant work. The whole
      // rebuild therefore costs work proportional to the number of ranges plus the number of
      // entries inspected in the leading range.
      for (var rangeIndex = 0; rangeIndex < sourceSize; rangeIndex++) {
        final var range = source.get(rangeIndex);
        if (foundRetainedEntry) {
          appendDuringCompaction(compacted, range, copiedRangeCount);
          copiedRangeCount++;
          continue;
        }

        final var firstRetainedIndex = range.firstRetainedIndex();
        if (firstRetainedIndex < range.size()) {
          final var retainedRange = firstRetainedIndex == 0
              ? range : range.copyFrom(firstRetainedIndex);
          appendDuringCompaction(compacted, retainedRange, copiedRangeCount);
          copiedRangeCount++;
          foundRetainedEntry = true;
        }
      }

      ranges = compacted;
      lastCompactedRangeCount = copiedRangeCount;
      lastCompactionOperation = operationsStarted.get();
    } finally {
      compactionLock.exclusiveUnlock();
    }
  }

  // Compaction builds an unpublished directory while it holds the exclusive lock, so no other
  // thread can append to that directory. A rejected append would therefore mean a broken
  // invariant rather than a lost race.
  private static void appendDuringCompaction(
      RangeDirectory target, OperationRange range, int expectedIndex) {
    if (!target.append(range, expectedIndex)) {
      throw new IllegalStateException("Compacted directory was modified concurrently");
    }
  }

  // These accessors validate structural properties without exposing mutable directory state.
  int rangeCount() {
    compactionLock.sharedLock();
    try {
      return ranges.size();
    } finally {
      compactionLock.sharedUnlock();
    }
  }

  int storedEntryCount() {
    compactionLock.sharedLock();
    try {
      final var directory = ranges;
      final var directorySize = directory.size();
      var count = 0;
      for (var rangeIndex = 0; rangeIndex < directorySize; rangeIndex++) {
        count += directory.get(rangeIndex).size();
      }
      return count;
    } finally {
      compactionLock.sharedUnlock();
    }
  }

  // The number of range descriptors that the last compaction copied into the new directory.
  int lastCompactedRangeCount() {
    compactionLock.sharedLock();
    try {
      return lastCompactedRangeCount;
    } finally {
      compactionLock.sharedUnlock();
    }
  }

  /// An ordered, append-only directory of range descriptors.
  ///
  /// Start identifiers increase strictly, because registration is serialized and monotonic. The
  /// directory therefore supports binary search with a deterministic logarithmic probe count.
  /// The backing array stores descriptors in geometrically growing chunks, so an append allocates
  /// no copy of the already published descriptors, and a positional read costs constant work.
  /// Compaction replaces the whole directory instead of removing single descriptors.
  private static final class RangeDirectory {

    private final CASObjectArray<OperationRange> descriptors = new CASObjectArray<>();
    private final Runnable readObserver;

    private RangeDirectory(Runnable readObserver) {
      this.readObserver = readObserver;
    }

    private int size() {
      return descriptors.size();
    }

    private OperationRange get(int index) {
      // Every descriptor traversal in this class goes through this method. Tests may install an
      // observer to count the implementation's actual reads. Production uses a null observer.
      if (readObserver != null) {
        readObserver.run();
      }
      return descriptors.get(index);
    }

    /// Appends one descriptor at the expected position.
    ///
    /// The append returns false when the directory length already moved past the expected
    /// position. In that case the directory publishes nothing.
    private boolean append(OperationRange range, int expectedIndex) {
      return descriptors.add(range, expectedIndex);
    }

    /// Returns the position of the last descriptor whose start identifier does not exceed the
    /// requested identifier. Returns -1 when every descriptor starts above that identifier.
    ///
    /// The caller passes the directory length it already observed, so the search never reads a
    /// descriptor beyond that length.
    private int floorIndex(long operationTs, int size) {
      var low = 0;
      var high = size - 1;
      var result = -1;
      while (low <= high) {
        final var middle = (low + high) >>> 1;
        // Full long identifiers are compared here. No narrowing happens during the search.
        if (get(middle).startTs <= operationTs) {
          result = middle;
          low = middle + 1;
        } else {
          high = middle - 1;
        }
      }
      return result;
    }
  }

  private static final class OperationRange {

    private final long startTs;
    private final CASObjectArray<OperationInformation> entries = new CASObjectArray<>();

    private OperationRange(long startTs, OperationInformation firstEntry) {
      this.startTs = startTs;
      entries.add(firstEntry);
    }

    private int size() {
      return entries.size();
    }

    private OperationInformation get(int index) {
      return entries.get(index);
    }

    private boolean compareAndSet(
        int index, OperationInformation expected, OperationInformation replacement) {
      return entries.compareAndSet(index, expected, replacement);
    }

    private long lastTs() {
      final var size = size();
      // Every append checks the predecessor before addition, so this addition cannot wrap.
      return startTs + size - 1L;
    }

    private boolean canAppend(long operationTs, int capacity) {
      if (size() >= capacity) {
        return false;
      }
      final var lastTs = lastTs();
      return lastTs != Long.MAX_VALUE && operationTs == lastTs + 1;
    }

    private void append(OperationInformation information) {
      final var expectedIndex = size();
      final var actualIndex = entries.add(information);
      if (actualIndex != expectedIndex) {
        throw new IllegalStateException("Operation range registration was not serialized");
      }
    }

    private boolean contains(long operationTs) {
      // Compare full identifiers against both bounds before calculating or narrowing a difference.
      if (operationTs < startTs || operationTs > lastTs()) {
        return false;
      }
      final var difference = operationTs - startTs;
      return difference < size();
    }

    private int indexOf(long operationTs) {
      if (!contains(operationTs)) {
        throw missingOperation(operationTs);
      }
      // The range capacity bounds the proven non-negative difference before narrowing.
      return (int) (operationTs - startTs);
    }

    private int firstIndexAtOrAfter(long operationTs) {
      if (operationTs <= startTs) {
        return 0;
      }
      final var difference = operationTs - startTs;
      return difference >= size() ? size() : (int) difference;
    }

    private int firstRetainedIndex() {
      for (var i = 0; i < size(); i++) {
        final var status = get(i).status;
        if (status == AtomicOperationStatus.IN_PROGRESS
            || status == AtomicOperationStatus.COMMITTED) {
          return i;
        }
      }
      return size();
    }

    private OperationRange copyFrom(int index) {
      final var first = get(index);
      final var copy = new OperationRange(first.operationTs, first);
      for (var i = index + 1; i < size(); i++) {
        copy.entries.add(get(i));
      }
      return copy;
    }
  }

  private record OperationInformation(AtomicOperationStatus status, long segment,
      long operationTs) {
  }
}
