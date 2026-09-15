package com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.atomicoperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.Test;

public class AtomicOperationsTableTest {

  // ==================== Basic Lifecycle Tests ====================

  @Test
  public void testStartOperation() {
    var table = new AtomicOperationsTable(100, 0);

    table.startOperation(0, 10);

    assertEquals(10, table.getSegmentEarliestOperationInProgress());
    assertEquals(10, table.getSegmentEarliestNotPersistedOperation());
  }

  @Test
  public void testStartAndCommitOperation() {
    var table = new AtomicOperationsTable(100, 0);

    table.startOperation(0, 10);
    table.commitOperation(0);

    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(10, table.getSegmentEarliestNotPersistedOperation());
  }

  @Test
  public void testFullLifecycleCommitAndPersist() {
    var table = new AtomicOperationsTable(100, 0);

    table.startOperation(0, 10);
    table.commitOperation(0);
    table.persistOperation(0);

    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  @Test
  public void testStartAndRollbackOperation() {
    var table = new AtomicOperationsTable(100, 0);

    table.startOperation(0, 10);
    table.rollbackOperation(0);

    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  @Test
  public void testMultipleOperationsLifecycle() {
    var table = new AtomicOperationsTable(100, 0);

    table.startOperation(0, 10);
    table.startOperation(1, 11);
    table.startOperation(2, 12);

    assertEquals(10, table.getSegmentEarliestOperationInProgress());
    assertEquals(10, table.getSegmentEarliestNotPersistedOperation());

    table.commitOperation(0);
    assertEquals(11, table.getSegmentEarliestOperationInProgress());
    assertEquals(10, table.getSegmentEarliestNotPersistedOperation());

    table.persistOperation(0);
    assertEquals(11, table.getSegmentEarliestOperationInProgress());
    assertEquals(11, table.getSegmentEarliestNotPersistedOperation());

    table.rollbackOperation(1);
    assertEquals(12, table.getSegmentEarliestOperationInProgress());
    assertEquals(12, table.getSegmentEarliestNotPersistedOperation());

    table.commitOperation(2);
    table.persistOperation(2);
    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  // ==================== Invalid State Transition Tests ====================

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testCommitNonExistentOperationThrows() {
    var table = new AtomicOperationsTable(100, 0);
    table.commitOperation(0);
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testRollbackNonExistentOperationThrows() {
    var table = new AtomicOperationsTable(100, 0);
    table.rollbackOperation(0);
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testPersistNonExistentOperationThrows() {
    var table = new AtomicOperationsTable(100, 0);
    table.persistOperation(0);
  }

  @Test(expected = IllegalStateException.class)
  public void testPersistInProgressOperationThrows() {
    var table = new AtomicOperationsTable(100, 0);
    table.startOperation(0, 10);
    table.persistOperation(0);
  }

  @Test(expected = IllegalStateException.class)
  public void testCommitCommittedOperationThrows() {
    var table = new AtomicOperationsTable(100, 0);
    table.startOperation(0, 10);
    table.commitOperation(0);
    table.commitOperation(0);
  }

  @Test(expected = IllegalStateException.class)
  public void testRollbackCommittedOperationThrows() {
    var table = new AtomicOperationsTable(100, 0);
    table.startOperation(0, 10);
    table.commitOperation(0);
    table.rollbackOperation(0);
  }

  @Test(expected = IllegalStateException.class)
  public void testStartOperationWithNegativeSegmentThrows() {
    var table = new AtomicOperationsTable(100, 0);
    table.startOperation(0, -1);
  }

  // ==================== Snapshot Visibility Tests ====================

  @Test
  public void testSnapshotNoActiveOperations() {
    var table = new AtomicOperationsTable(100, 0);

    var snapshot = table.snapshotAtomicOperationTableState(100);

    // With no active operations, records at or below currentTimestamp should be visible
    assertTrue(snapshot.isEntryVisible(50));
    assertTrue(snapshot.isEntryVisible(100));
    assertFalse(snapshot.isEntryVisible(101));
    assertFalse(snapshot.isEntryVisible(200));
  }

  /** An exhausted identifier space saturates the empty-table boundary without signed wraparound. */
  @Test
  public void testSnapshotAtMaximumIdentifierDoesNotWrapVisibilityBoundary() {
    var table = new AtomicOperationsTable(100, Long.MAX_VALUE);

    var snapshot = table.snapshotAtomicOperationTableState(Long.MAX_VALUE);

    assertEquals(Long.MAX_VALUE, snapshot.minActiveOperationTs());
    assertEquals(Long.MAX_VALUE, snapshot.maxActiveOperationTs());
    assertTrue(snapshot.isEntryVisible(1));
    assertTrue(snapshot.isEntryVisible(Long.MAX_VALUE));
  }

  @Test
  public void testSnapshotWithSingleInProgressOperation() {
    var table = new AtomicOperationsTable(100, 0);
    table.startOperation(50, 10);

    var snapshot = table.snapshotAtomicOperationTableState(100);

    // Records below minActiveOperationTimestamp are visible
    assertTrue(snapshot.isEntryVisible(0));
    assertTrue(snapshot.isEntryVisible(49));

    // minActiveOperationTimestamp itself is not visible (it's in progress)
    assertFalse(snapshot.isEntryVisible(50));

    // Records above maxActiveOperationTs but within snapshotTs are visible
    // (they must be committed because they're not in the in-progress set)
    assertTrue(snapshot.isEntryVisible(51));
    assertTrue(snapshot.isEntryVisible(100));

    // Records above snapshotTs are not visible (truly future)
    assertFalse(snapshot.isEntryVisible(101));
  }

  @Test
  public void testSnapshotWithMultipleInProgressOperations() {
    var table = new AtomicOperationsTable(100, 0);
    table.startOperation(10, 1);
    table.startOperation(20, 2);
    table.startOperation(30, 3);

    var snapshot = table.snapshotAtomicOperationTableState(100);

    // Records below min (10) are visible
    assertTrue(snapshot.isEntryVisible(0));
    assertTrue(snapshot.isEntryVisible(9));

    // In-progress operations are not visible
    assertFalse(snapshot.isEntryVisible(10));
    assertFalse(snapshot.isEntryVisible(20));
    assertFalse(snapshot.isEntryVisible(30));

    // Records above max (30) but within snapshotTs (100) are visible (committed)
    assertTrue(snapshot.isEntryVisible(31));
    assertTrue(snapshot.isEntryVisible(100));

    // Records above snapshotTs are not visible
    assertFalse(snapshot.isEntryVisible(101));
  }

  @Test
  public void testSnapshotWithGapsInInProgressOperations() {
    var table = new AtomicOperationsTable(100, 0);
    table.startOperation(10, 1);
    table.startOperation(30, 3);
    // Note: 20 is NOT started, so it represents a committed operation

    var snapshot = table.snapshotAtomicOperationTableState(100);

    // Records below min (10) are visible
    assertTrue(snapshot.isEntryVisible(9));

    // In-progress operations are not visible
    assertFalse(snapshot.isEntryVisible(10));
    assertFalse(snapshot.isEntryVisible(30));

    // Gaps (not in-progress set) between min and max are visible
    assertTrue(snapshot.isEntryVisible(15));
    assertTrue(snapshot.isEntryVisible(20));
    assertTrue(snapshot.isEntryVisible(25));
    assertTrue(snapshot.isEntryVisible(29));

    // Above max but within snapshotTs: visible (committed)
    assertTrue(snapshot.isEntryVisible(31));

    // Above snapshotTs: not visible
    assertFalse(snapshot.isEntryVisible(101));
  }

  @Test
  public void testSnapshotAfterOperationCommits() {
    var table = new AtomicOperationsTable(100, 0);
    table.startOperation(10, 1);
    table.startOperation(20, 2);

    // Initially both are in progress
    var snapshot1 = table.snapshotAtomicOperationTableState(100);
    assertFalse(snapshot1.isEntryVisible(10));
    assertFalse(snapshot1.isEntryVisible(20));

    // Commit operation 10
    table.commitOperation(10);

    // Now only 20 is in progress
    var snapshot2 = table.snapshotAtomicOperationTableState(100);
    // Operation 10 is still not visible to snapshot2 because minActiveOperationTimestamp is now 20
    // and 10 < 20, so it should be visible
    assertTrue(snapshot2.isEntryVisible(10));
    assertFalse(snapshot2.isEntryVisible(20));
  }

  // ==================== Compaction Tests ====================

  @Test
  public void testCompactionRemovesPersistedOperations() {
    var table = new AtomicOperationsTable(10, 0);

    // Start and complete several operations
    for (var i = 0; i < 5; i++) {
      table.startOperation(i, i);
      table.commitOperation(i);
      table.persistOperation(i);
    }

    // Force compaction
    table.compactTable();

    // Verify all operations are cleared
    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  @Test
  public void testCompactionRemovesRolledBackOperations() {
    var table = new AtomicOperationsTable(10, 0);

    // Start and rollback several operations
    for (var i = 0; i < 5; i++) {
      table.startOperation(i, i);
      table.rollbackOperation(i);
    }

    // Force compaction
    table.compactTable();

    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  @Test
  public void testCompactionPreservesInProgressOperations() {
    var table = new AtomicOperationsTable(10, 0);

    // Complete some operations
    for (var i = 0; i < 3; i++) {
      table.startOperation(i, i);
      table.commitOperation(i);
      table.persistOperation(i);
    }

    // Leave one in progress
    table.startOperation(3, 30);

    // Force compaction
    table.compactTable();

    // The in-progress operation should be preserved
    assertEquals(30, table.getSegmentEarliestOperationInProgress());
  }

  @Test
  public void testCompactionPreservesCommittedOperations() {
    var table = new AtomicOperationsTable(10, 0);

    // Persist some operations
    for (var i = 0; i < 3; i++) {
      table.startOperation(i, i);
      table.commitOperation(i);
      table.persistOperation(i);
    }

    // Leave one committed but not persisted
    table.startOperation(3, 30);
    table.commitOperation(3);

    // Force compaction
    table.compactTable();

    // The committed operation should be preserved
    assertEquals(30, table.getSegmentEarliestNotPersistedOperation());
  }

  @Test
  public void testAutomaticCompactionTriggering() {
    // Small compaction interval to trigger automatic compaction
    var table = new AtomicOperationsTable(5, 0);

    // Start and complete operations to trigger compaction
    for (var i = 0; i < 10; i++) {
      table.startOperation(i, i);
      table.commitOperation(i);
      table.persistOperation(i);
    }

    // At this point, automatic compaction should have been triggered
    // Start a new operation to verify table is still functional
    table.startOperation(10, 100);
    assertEquals(100, table.getSegmentEarliestOperationInProgress());
  }

  // ==================== Non-Zero Offset Tests ====================

  @Test
  public void testNonZeroInitialOffset() {
    var table = new AtomicOperationsTable(100, 1000);

    table.startOperation(1000, 10);
    table.startOperation(1001, 11);

    assertEquals(10, table.getSegmentEarliestOperationInProgress());

    table.commitOperation(1000);
    assertEquals(11, table.getSegmentEarliestOperationInProgress());
  }

  @Test
  public void testSnapshotWithNonZeroOffset() {
    var table = new AtomicOperationsTable(100, 1000);

    table.startOperation(1010, 10);

    var snapshot = table.snapshotAtomicOperationTableState(1100);

    assertTrue(snapshot.isEntryVisible(1000));
    assertTrue(snapshot.isEntryVisible(1009));
    assertFalse(snapshot.isEntryVisible(1010));
    // 1011 is above max in-progress (1010) but within snapshotTs (1100): visible
    assertTrue(snapshot.isEntryVisible(1011));
    assertTrue(snapshot.isEntryVisible(1100));
    // Above snapshotTs: not visible
    assertFalse(snapshot.isEntryVisible(1101));
  }

  // ==================== Thread Safety Tests ====================

  // Verifies that concurrent startOperation calls under a lock (mimicking the
  // production segmentLock in AtomicOperationsManager) correctly register all
  // operations and maintain the cached min invariant.
  @Test
  public void testConcurrentStartOperations() throws InterruptedException {
    var table = new AtomicOperationsTable(1000, 0);
    var threadCount = 10;
    var operationsPerThread = 100;
    var latch = new CountDownLatch(threadCount);
    var errors = new ConcurrentLinkedQueue<Throwable>();
    // In production, startOperation is always called under segmentLock with
    // monotonically increasing timestamps from idGen.nextId(). Use a shared
    // lock + AtomicLong to replicate this guarantee.
    var nextTs = new AtomicLong(0);
    var lock = new ReentrantLock();

    try (var executor = Executors.newFixedThreadPool(threadCount)) {
      for (var t = 0; t < threadCount; t++) {
        executor.submit(() -> {
          try {
            for (var i = 0; i < operationsPerThread; i++) {
              lock.lock();
              try {
                var opTs = nextTs.getAndIncrement();
                table.startOperation(opTs, opTs);
              } finally {
                lock.unlock();
              }
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            latch.countDown();
          }
        });
      }

      assertTrue(latch.await(30, TimeUnit.SECONDS));
      executor.shutdown();
    }

    if (!errors.isEmpty()) {
      fail("Errors occurred during concurrent starts: " + errors.peek().getMessage());
    }

    // Verify all operations are in progress
    var snapshot = table.snapshotAtomicOperationTableState(threadCount * operationsPerThread);
    assertEquals(threadCount * operationsPerThread, snapshot.inProgressTxs().size());
  }

  // Verifies that concurrent start+commit cycles under a lock (mimicking
  // the production segmentLock for starts) correctly process all operations
  // and leave no operations in progress at the end.
  @Test
  public void testConcurrentStartAndCommit() throws InterruptedException {
    var table = new AtomicOperationsTable(1000, 0);
    var threadCount = 10;
    var operationsPerThread = 100;
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var nextTs = new AtomicLong(0);
    // In production, startOperation is called under segmentLock ensuring
    // monotonic TS order. commitOperation does not need the lock.
    var lock = new ReentrantLock();

    try (var executor = Executors.newFixedThreadPool(threadCount)) {
      for (var t = 0; t < threadCount; t++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < operationsPerThread; i++) {
              long opTs;
              lock.lock();
              try {
                opTs = nextTs.getAndIncrement();
                table.startOperation(opTs, opTs);
              } finally {
                lock.unlock();
              }
              table.commitOperation(opTs);
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
    }

    if (!errors.isEmpty()) {
      fail("Errors occurred during concurrent start/commit: " + errors.peek().getMessage());
    }

    // All operations should be committed (not in progress)
    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
  }

  @Test
  public void testConcurrentStartCommitAndPersist() throws InterruptedException {
    var table = new AtomicOperationsTable(1000, 0);
    var threadCount = 10;
    var operationsPerThread = 100;
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var nextTs = new AtomicLong(0);
    var lock = new ReentrantLock();

    try (var executor = Executors.newFixedThreadPool(threadCount)) {
      for (var t = 0; t < threadCount; t++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < operationsPerThread; i++) {
              long opTs;
              lock.lock();
              try {
                opTs = nextTs.getAndIncrement();
                table.startOperation(opTs, opTs);
              } finally {
                lock.unlock();
              }
              table.commitOperation(opTs);
              table.persistOperation(opTs);
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
    }

    if (!errors.isEmpty()) {
      fail("Errors occurred during concurrent lifecycle: " + errors.peek().getMessage());
    }

    // All operations should be persisted
    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  @Test
  public void testConcurrentSnapshots() throws InterruptedException {
    var table = new AtomicOperationsTable(1000, 0);

    // Start some operations
    for (var i = 0; i < 100; i++) {
      table.startOperation(i, i);
    }

    var threadCount = 10;
    var snapshotsPerThread = 100;
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);
    var errors = new ConcurrentLinkedQueue<Throwable>();

    try (var executor = Executors.newFixedThreadPool(threadCount)) {
      for (var t = 0; t < threadCount; t++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < snapshotsPerThread; i++) {
              var snapshot = table.snapshotAtomicOperationTableState(200);
              // All started operations should be in the snapshot
              assertEquals(100, snapshot.inProgressTxs().size());
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
    }

    if (!errors.isEmpty()) {
      fail("Errors occurred during concurrent snapshots: " + errors.peek().getMessage());
    }
  }

  @Test
  public void testConcurrentOperationsWithSnapshots() throws InterruptedException {
    var table = new AtomicOperationsTable(100, 0);
    var writerThreads = 5;
    var readerThreads = 5;
    var operationsPerWriter = 100;
    var snapshotsPerReader = 100;
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(writerThreads + readerThreads);
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var nextTs = new AtomicLong(0);
    var lock = new ReentrantLock();

    try (var executor = Executors.newFixedThreadPool(writerThreads + readerThreads)) {
      // Writer threads
      for (var t = 0; t < writerThreads; t++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < operationsPerWriter; i++) {
              long opTs;
              lock.lock();
              try {
                opTs = nextTs.getAndIncrement();
                table.startOperation(opTs, opTs);
              } finally {
                lock.unlock();
              }
              Thread.yield(); // Allow interleaving
              table.commitOperation(opTs);
              Thread.yield();
              table.persistOperation(opTs);
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      // Reader threads
      for (var t = 0; t < readerThreads; t++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < snapshotsPerReader; i++) {
              var snapshot = table.snapshotAtomicOperationTableState(Long.MAX_VALUE);
              verifySnapshotConsistency(snapshot);
              Thread.yield();
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(60, TimeUnit.SECONDS));
    }

    if (!errors.isEmpty()) {
      fail("Errors occurred during concurrent operations with snapshots: " + errors.peek()
          .getMessage());
    }
  }

  @Test
  public void testConcurrentCompaction() throws InterruptedException {
    var table = new AtomicOperationsTable(10, 0);
    var threadCount = 5;
    var operationsPerThread = 50;
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var nextTs = new AtomicLong(0);
    var lock = new ReentrantLock();

    try (var executor = Executors.newFixedThreadPool(threadCount)) {
      for (var t = 0; t < threadCount; t++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < operationsPerThread; i++) {
              long opTs;
              lock.lock();
              try {
                opTs = nextTs.getAndIncrement();
                table.startOperation(opTs, opTs);
              } finally {
                lock.unlock();
              }
              table.commitOperation(opTs);
              table.persistOperation(opTs);
              // Trigger manual compaction occasionally
              if (i % 10 == 0) {
                table.compactTable();
              }
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
    }

    if (!errors.isEmpty()) {
      fail("Errors occurred during concurrent compaction: " + errors.peek().getMessage());
    }

    // All operations should be complete
    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  @Test
  public void testConcurrentMixedOperations() throws InterruptedException {
    var table = new AtomicOperationsTable(50, 0);
    var threadCount = 10;
    var operationsPerThread = 100;
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var nextTs = new AtomicLong(0);
    var completedOps = new AtomicInteger(0);
    var lock = new ReentrantLock();

    try (var executor = Executors.newFixedThreadPool(threadCount)) {
      for (var t = 0; t < threadCount; t++) {
        final var threadId = t;
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < operationsPerThread; i++) {
              long opTs;
              lock.lock();
              try {
                opTs = nextTs.getAndIncrement();
                table.startOperation(opTs, opTs);
              } finally {
                lock.unlock();
              }

              // Mix of commit+persist and rollback
              if ((threadId + i) % 3 == 0) {
                table.rollbackOperation(opTs);
              } else {
                table.commitOperation(opTs);
                table.persistOperation(opTs);
              }
              completedOps.incrementAndGet();
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(60, TimeUnit.SECONDS));
    }

    if (!errors.isEmpty()) {
      fail("Errors occurred during concurrent mixed operations: " + errors.peek().getMessage());
    }

    assertEquals(threadCount * operationsPerThread, completedOps.get());

    // All operations should be complete
    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  // ==================== Edge Cases Tests ====================

  @Test
  public void testLargeNumberOfOperations() {
    var table = new AtomicOperationsTable(100, 0);
    var count = 10000;

    for (var i = 0; i < count; i++) {
      table.startOperation(i, i % 100);
    }

    var snapshot = table.snapshotAtomicOperationTableState(count + 1);
    assertEquals(count, snapshot.inProgressTxs().size());

    // Commit and persist all
    for (var i = 0; i < count; i++) {
      table.commitOperation(i);
      table.persistOperation(i);
    }

    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  @Test
  public void testOperationsWithSameSegment() {
    var table = new AtomicOperationsTable(100, 0);

    // Multiple operations in the same segment
    table.startOperation(0, 5);
    table.startOperation(1, 5);
    table.startOperation(2, 5);

    assertEquals(5, table.getSegmentEarliestOperationInProgress());

    table.commitOperation(0);
    table.persistOperation(0);

    // Still returns 5 because ops 1 and 2 are still in progress in segment 5
    assertEquals(5, table.getSegmentEarliestOperationInProgress());

    table.commitOperation(1);
    table.persistOperation(1);
    table.commitOperation(2);
    table.persistOperation(2);

    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
  }

  @Test
  public void testSnapshotVisibilityEdgeCases() {
    var table = new AtomicOperationsTable(100, 0);

    // Start operations at timestamps 5, 10, 15
    table.startOperation(5, 1);
    table.startOperation(10, 2);
    table.startOperation(15, 3);

    var snapshot = table.snapshotAtomicOperationTableState(20);

    // minActiveOperationTimestamp = 5, maxActiveOperationTimestamp = 15
    // Boundary conditions:
    assertTrue(snapshot.isEntryVisible(4)); // Just below min - visible
    assertFalse(snapshot.isEntryVisible(5)); // Exactly min - not visible
    assertTrue(snapshot.isEntryVisible(6)); // Between min and next in-progress - visible
    assertTrue(snapshot.isEntryVisible(9)); // Just below 10 - visible
    assertFalse(snapshot.isEntryVisible(10)); // In-progress - not visible
    assertTrue(snapshot.isEntryVisible(11)); // Between 10 and 15 - visible
    assertTrue(snapshot.isEntryVisible(14)); // Just below max - visible
    assertFalse(snapshot.isEntryVisible(15)); // Exactly max (in-progress) - not visible
    assertTrue(snapshot.isEntryVisible(16)); // Above max, within snapshotTs (20) - visible
    assertTrue(snapshot.isEntryVisible(20)); // At snapshotTs - visible
    assertFalse(snapshot.isEntryVisible(21)); // Above snapshotTs - not visible
  }

  @Test
  public void testEmptyTableOperations() {
    var table = new AtomicOperationsTable(100, 0);

    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());

    var snapshot = table.snapshotAtomicOperationTableState(100);
    assertTrue(snapshot.isEntryVisible(50));
    assertTrue(snapshot.isEntryVisible(100));
    assertFalse(snapshot.isEntryVisible(101));
  }

  @Test
  public void testCompactionWithEmptyTable() {
    var table = new AtomicOperationsTable(100, 0);

    // Compacting an empty table should not cause issues
    table.compactTable();

    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());

    // Table should still be functional
    table.startOperation(0, 10);
    assertEquals(10, table.getSegmentEarliestOperationInProgress());
  }

  @Test
  public void testRepeatedCompaction() {
    var table = new AtomicOperationsTable(10, 0);

    for (var round = 0; round < 5; round++) {
      // Add operations
      for (var i = 0; i < 20; i++) {
        long ts = round * 20 + i;
        table.startOperation(ts, ts);
        table.commitOperation(ts);
        table.persistOperation(ts);
      }

      // Compact
      table.compactTable();
    }

    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  /// After compaction advances the offset, attempting to start an operation with
  /// a timestamp below the new offset must throw IllegalStateException because
  /// the slot no longer exists in the table.
  @Test(expected = IllegalStateException.class)
  public void testOperationBelowOffsetAfterCompactionThrows() {
    var table = new AtomicOperationsTable(10, 0);

    // Complete operations 0-4 and compact
    for (var i = 0; i < 5; i++) {
      table.startOperation(i, i);
      table.commitOperation(i);
      table.persistOperation(i);
    }
    table.compactTable();

    // Attempt to start an operation with a timestamp below the new offset —
    // should fail because the offset has advanced past timestamp 0
    table.startOperation(0, 10);
  }

  /// Compaction must correctly prune a mix of PERSISTED and ROLLED_BACK entries
  /// at the front of the table, stopping at the first active entry.
  @Test
  public void testCompactionPrunesMixedTerminalStates() {
    var table = new AtomicOperationsTable(100, 0);

    // Create a mix: persisted, rolled-back, persisted, then in-progress
    table.startOperation(0, 10);
    table.commitOperation(0);
    table.persistOperation(0); // PERSISTED

    table.startOperation(1, 11);
    table.rollbackOperation(1); // ROLLED_BACK

    table.startOperation(2, 12);
    table.commitOperation(2);
    table.persistOperation(2); // PERSISTED

    table.startOperation(3, 13); // IN_PROGRESS

    table.compactTable();

    // Only the in-progress operation at ts=3 should remain
    assertEquals(13, table.getSegmentEarliestOperationInProgress());

    // Verify snapshot correctness after compaction
    var snapshot = table.snapshotAtomicOperationTableState(100);
    assertEquals(1, snapshot.inProgressTxs().size());
    assertTrue(snapshot.inProgressTxs().contains(3));
    assertEquals(3, snapshot.minActiveOperationTs());
  }

  /// Calling compactTable() twice in succession when the first compaction already
  /// removed all purgeable entries — the second call should be a no-op.
  @Test
  public void testDoubleCompactionIsIdempotent() {
    var table = new AtomicOperationsTable(10, 0);

    table.startOperation(0, 10);
    table.commitOperation(0);
    table.persistOperation(0);
    table.startOperation(1, 11); // IN_PROGRESS

    table.compactTable();
    // After first compaction: only ts=1 remains
    assertEquals(11, table.getSegmentEarliestOperationInProgress());

    table.compactTable();
    // Second compaction should be no-op (ts=1 is IN_PROGRESS, cannot be pruned)
    assertEquals(11, table.getSegmentEarliestOperationInProgress());

    // Verify snapshot still correct
    var snapshot = table.snapshotAtomicOperationTableState(100);
    assertEquals(1, snapshot.inProgressTxs().size());
    assertTrue(snapshot.inProgressTxs().contains(1));
  }

  @Test
  public void testOutOfOrderCommits() {
    var table = new AtomicOperationsTable(100, 0);

    // Start operations in order
    table.startOperation(0, 10);
    table.startOperation(1, 11);
    table.startOperation(2, 12);

    // Commit out of order
    table.commitOperation(2);
    table.commitOperation(0);
    table.commitOperation(1);

    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
    // Earliest not-persisted operation should map to WAL segment 10 (operation 0)
    assertEquals(10, table.getSegmentEarliestNotPersistedOperation());

    // Persist out of order
    table.persistOperation(1);
    assertEquals(10, table.getSegmentEarliestNotPersistedOperation());

    table.persistOperation(0);
    assertEquals(12, table.getSegmentEarliestNotPersistedOperation());

    table.persistOperation(2);
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  // ==================== Cached Min Tests ====================

  /// Verifies the basic lifecycle of the cached min: start ops, take snapshot to
  /// verify min/max, commit boundary ops, and verify min is updated via forward
  /// scan.
  @Test
  public void testCachedMinBasicLifecycle() {
    var table = new AtomicOperationsTable(100, 1);

    table.startOperation(1, 1);
    table.startOperation(5, 5);
    table.startOperation(10, 10);

    // First snapshot establishes cached min=1
    var snap1 = table.snapshotAtomicOperationTableState(100);
    assertEquals(1, snap1.minActiveOperationTs());
    assertEquals(10, snap1.maxActiveOperationTs());
    assertEquals(3, snap1.inProgressTxs().size());

    // Commit the min (ts=1) — forward scan should set cached min to 5
    table.commitOperation(1);
    var snap2 = table.snapshotAtomicOperationTableState(100);
    assertEquals(5, snap2.minActiveOperationTs());
    assertEquals(10, snap2.maxActiveOperationTs());
    assertEquals(2, snap2.inProgressTxs().size());

    // Commit ts=10 — snapshot finds remaining op ts=5
    table.commitOperation(10);
    var snap3 = table.snapshotAtomicOperationTableState(100);
    assertEquals(5, snap3.minActiveOperationTs());
    assertEquals(5, snap3.maxActiveOperationTs());
    assertEquals(1, snap3.inProgressTxs().size());
  }

  /// Starts 5 operations, commits the min, and verifies that the new min
  /// is the next IN_PROGRESS entry (forward-scanned, not a full rescan).
  @Test
  public void testCachedMinForwardScanOnMinCommit() {
    var table = new AtomicOperationsTable(100, 1);

    table.startOperation(1, 1);
    table.startOperation(2, 2);
    table.startOperation(3, 3);
    table.startOperation(4, 4);
    table.startOperation(5, 5);

    // Establish cached min
    table.snapshotAtomicOperationTableState(100);

    // Commit the min (ts=1) — forward scan finds ts=2
    table.commitOperation(1);
    var snap = table.snapshotAtomicOperationTableState(100);
    assertEquals(2, snap.minActiveOperationTs());
    assertEquals(5, snap.maxActiveOperationTs());
    assertEquals(4, snap.inProgressTxs().size());
    assertTrue(snap.inProgressTxs().contains(2));
    assertTrue(snap.inProgressTxs().contains(3));
    assertTrue(snap.inProgressTxs().contains(4));
    assertTrue(snap.inProgressTxs().contains(5));
  }

  /// Starts 3 operations, commits the highest one, and verifies the
  /// snapshot correctly reflects the remaining active operations.
  @Test
  public void testSnapshotAfterMaxCommit() {
    var table = new AtomicOperationsTable(100, 1);

    table.startOperation(1, 1);
    table.startOperation(5, 5);
    table.startOperation(10, 10);

    // Establish cached min
    table.snapshotAtomicOperationTableState(100);

    // Commit ts=10
    table.commitOperation(10);

    // Snapshot computes the actual max from scan
    var snap = table.snapshotAtomicOperationTableState(100);
    assertEquals(1, snap.minActiveOperationTs());
    assertEquals(5, snap.maxActiveOperationTs());
    assertEquals(2, snap.inProgressTxs().size());

    // Starting a new operation sets max correctly
    table.startOperation(20, 20);
    var snap2 = table.snapshotAtomicOperationTableState(100);
    assertEquals(1, snap2.minActiveOperationTs());
    assertEquals(20, snap2.maxActiveOperationTs());
    assertEquals(3, snap2.inProgressTxs().size());
  }

  /// Commits both the min and max boundaries and verifies correct state:
  /// min is forward-scanned, only the middle operation remains.
  @Test
  public void testCachedMinBothBoundariesCommitted() {
    var table = new AtomicOperationsTable(100, 1);

    table.startOperation(1, 1);
    table.startOperation(5, 5);
    table.startOperation(10, 10);

    // Establish cached min
    table.snapshotAtomicOperationTableState(100);

    // Commit min first, then max
    table.commitOperation(1);
    table.commitOperation(10);

    var snap = table.snapshotAtomicOperationTableState(100);
    assertEquals(5, snap.minActiveOperationTs());
    assertEquals(5, snap.maxActiveOperationTs());
    assertEquals(1, snap.inProgressTxs().size());
    assertTrue(snap.inProgressTxs().contains(5));
  }

  /// Only one active operation: commit it and verify the cached min goes to
  /// UNKNOWN. The next snapshot should return currentTimestamp + 1.
  @Test
  public void testCachedMinSingleOpCommit() {
    var table = new AtomicOperationsTable(100, 1);

    table.startOperation(5, 5);

    // Establish cached min via snapshot
    var snap1 = table.snapshotAtomicOperationTableState(100);
    assertEquals(5, snap1.minActiveOperationTs());
    assertEquals(5, snap1.maxActiveOperationTs());

    // Commit the only active op — cached min becomes UNKNOWN
    table.commitOperation(5);

    var snap2 = table.snapshotAtomicOperationTableState(100);
    // No active ops: min and max should be currentTimestamp + 1
    assertEquals(101, snap2.minActiveOperationTs());
    assertEquals(101, snap2.maxActiveOperationTs());
    assertTrue(snap2.inProgressTxs().isEmpty());
  }

  /// Commits a middle operation (not min, not max) and verifies that
  /// the cached min remains unchanged.
  @Test
  public void testCachedMinNonBoundaryCommit() {
    var table = new AtomicOperationsTable(100, 1);

    table.startOperation(1, 1);
    table.startOperation(5, 5);
    table.startOperation(10, 10);

    // Establish cached min
    table.snapshotAtomicOperationTableState(100);

    // Commit the middle op (ts=5) — neither min nor max should change
    table.commitOperation(5);

    var snap = table.snapshotAtomicOperationTableState(100);
    assertEquals(1, snap.minActiveOperationTs());
    assertEquals(10, snap.maxActiveOperationTs());
    assertEquals(2, snap.inProgressTxs().size());
    assertTrue(snap.inProgressTxs().contains(1));
    assertTrue(snap.inProgressTxs().contains(10));
  }

  /// Verifies that the cached min survives compaction correctly: after
  /// compaction prunes completed entries, snapshots still return accurate results.
  @Test
  public void testCachedMinAfterCompaction() {
    var table = new AtomicOperationsTable(10, 1);

    // Create enough operations to build up the table
    for (var i = 1; i <= 15; i++) {
      table.startOperation(i, i);
      if (i <= 10) {
        table.commitOperation(i);
        table.persistOperation(i);
      }
    }

    // Establish cached min — active ops are 11-15
    var snap1 = table.snapshotAtomicOperationTableState(100);
    assertEquals(11, snap1.minActiveOperationTs());
    assertEquals(15, snap1.maxActiveOperationTs());

    // Force compaction — table compacts but active ops don't change
    table.compactTable();

    // Snapshot after compaction should show same active set
    var snap2 = table.snapshotAtomicOperationTableState(100);
    assertEquals(11, snap2.minActiveOperationTs());
    assertEquals(15, snap2.maxActiveOperationTs());
    assertEquals(5, snap2.inProgressTxs().size());
  }

  /// Verifies that snapshot min/max always match the actual min/max of the
  /// inProgressTxs set across a sequence of operations.
  @Test
  public void testCachedMinConsistencyWithInProgressSet() {
    var table = new AtomicOperationsTable(100, 1);

    table.startOperation(3, 3);
    table.startOperation(7, 7);
    table.startOperation(12, 12);
    table.startOperation(18, 18);

    // Verify consistency after each mutation
    assertSnapshotConsistency(table, 100);

    table.commitOperation(3);
    assertSnapshotConsistency(table, 100);

    table.commitOperation(18);
    assertSnapshotConsistency(table, 100);

    table.rollbackOperation(7);
    assertSnapshotConsistency(table, 100);

    table.commitOperation(12);
    assertSnapshotConsistency(table, 100);
  }

  /// Verifies that with the cached min, the snapshot correctly builds the
  /// inProgressTxs set — all active operations are included, no extras.
  @Test
  public void testSnapshotScanRangeNarrowing() {
    var table = new AtomicOperationsTable(100, 1);

    // Start operations spread across the table
    table.startOperation(5, 5);
    table.startOperation(10, 10);
    table.startOperation(50, 50);
    table.startOperation(90, 90);

    // First snapshot: full scan establishes cached min
    var snap1 = table.snapshotAtomicOperationTableState(100);
    assertEquals(4, snap1.inProgressTxs().size());
    assertEquals(5, snap1.minActiveOperationTs());
    assertEquals(90, snap1.maxActiveOperationTs());

    // Second snapshot: scan starts at cached min=5, should find all the same ops
    var snap2 = table.snapshotAtomicOperationTableState(100);
    assertEquals(4, snap2.inProgressTxs().size());
    assertTrue(snap2.inProgressTxs().contains(5));
    assertTrue(snap2.inProgressTxs().contains(10));
    assertTrue(snap2.inProgressTxs().contains(50));
    assertTrue(snap2.inProgressTxs().contains(90));

    // Commit min and max, verify narrowed scan still works
    table.commitOperation(5);
    table.commitOperation(90);
    var snap3 = table.snapshotAtomicOperationTableState(100);
    assertEquals(2, snap3.inProgressTxs().size());
    assertEquals(10, snap3.minActiveOperationTs());
    assertEquals(50, snap3.maxActiveOperationTs());
  }

  /// All operations complete, cached min goes UNKNOWN. The next snapshot after
  /// new starts correctly re-establishes the cached min.
  @Test
  public void testCachedMinRecoveryAfterAllOpsComplete() {
    var table = new AtomicOperationsTable(100, 1);

    table.startOperation(1, 1);
    table.startOperation(5, 5);

    // Establish cached min
    table.snapshotAtomicOperationTableState(100);

    // Complete all ops
    table.commitOperation(1);
    table.commitOperation(5);

    // No active ops — snapshot returns currentTimestamp + 1
    var snap1 = table.snapshotAtomicOperationTableState(200);
    assertEquals(201, snap1.minActiveOperationTs());
    assertEquals(201, snap1.maxActiveOperationTs());
    assertTrue(snap1.inProgressTxs().isEmpty());

    // Next snapshot after new starts re-establishes cached min
    table.startOperation(10, 10);
    table.startOperation(20, 20);

    var snap2 = table.snapshotAtomicOperationTableState(200);
    assertEquals(10, snap2.minActiveOperationTs());
    assertEquals(20, snap2.maxActiveOperationTs());
    assertEquals(2, snap2.inProgressTxs().size());
  }

  /// Rolls back the min boundary and verifies that the cached min advances
  /// to the next IN_PROGRESS entry (same as commit, but via rollback path).
  @Test
  public void testCachedMinRollbackOfMinBoundary() {
    var table = new AtomicOperationsTable(100, 1);

    table.startOperation(1, 1);
    table.startOperation(5, 5);
    table.startOperation(10, 10);

    // Establish cached min
    table.snapshotAtomicOperationTableState(100);

    // Rollback the min (ts=1) — forward scan should find ts=5
    table.rollbackOperation(1);
    var snap = table.snapshotAtomicOperationTableState(100);
    assertEquals(5, snap.minActiveOperationTs());
    assertEquals(10, snap.maxActiveOperationTs());
    assertEquals(2, snap.inProgressTxs().size());

    // Rollback ts=10 — only ts=5 remains
    table.rollbackOperation(10);
    var snap2 = table.snapshotAtomicOperationTableState(100);
    assertEquals(5, snap2.minActiveOperationTs());
    assertEquals(5, snap2.maxActiveOperationTs());
    assertEquals(1, snap2.inProgressTxs().size());
  }

  /// Creates a gap larger than MAX_FORWARD_SCAN (128) between the committed
  /// min and the next IN_PROGRESS entry. Verifies that the forward scan cap
  /// causes a fallback to UNKNOWN and the snapshot still produces correct
  /// results via a broader scan.
  @Test
  public void testForwardScanCapFallbackToUnknown() {
    var table = new AtomicOperationsTable(1000, 1);

    // Start 200 operations: ts 1..200
    for (var i = 1; i <= 200; i++) {
      table.startOperation(i, i);
    }

    // Establish cached min=1
    var snap1 = table.snapshotAtomicOperationTableState(300);
    assertEquals(1, snap1.minActiveOperationTs());
    assertEquals(200, snap1.maxActiveOperationTs());

    // Commit the first 150 operations, creating a gap of 150
    // between the old min (1) and the next IN_PROGRESS (151)
    for (var i = 1; i <= 150; i++) {
      table.commitOperation(i);
    }

    // The forward scan on min=1's commit scans up to 128 entries and gives up
    // (returns UNKNOWN). The next snapshot must still find the correct min.
    var snap2 = table.snapshotAtomicOperationTableState(300);
    assertEquals(151, snap2.minActiveOperationTs());
    assertEquals(200, snap2.maxActiveOperationTs());
    assertEquals(50, snap2.inProgressTxs().size());
  }

  /// Regression test: after the forward scan cap sets cachedMin to UNKNOWN,
  /// a new startOperation must NOT blindly set cachedMin to its own TS.
  /// Doing so would hide older active operations behind the new min.
  ///
  /// Scenario: TS=1 and TS=200 are IN_PROGRESS. TS=1 commits, creating a
  /// gap of 199 entries (> MAX_FORWARD_SCAN=128). The forward scan gives up,
  /// setting cachedMin=UNKNOWN. TS=300 starts. A subsequent snapshot must
  /// still find TS=200 as the actual min, not TS=300.
  @Test
  public void testStartAfterForwardScanCapDoesNotHideOlderActiveOp() {
    var table = new AtomicOperationsTable(1000, 1);

    table.startOperation(1, 1);
    table.startOperation(200, 200);

    // Establish cached min=1
    table.snapshotAtomicOperationTableState(400);

    // Commit TS=1. The forward scan from 2 hits the cap at 128 entries
    // without finding TS=200 → cachedMin set to UNKNOWN.
    table.commitOperation(1);

    // New start: must NOT set cachedMin to 300 (hiding TS=200).
    table.startOperation(300, 300);

    // Snapshot must find TS=200 as the actual min, not TS=300.
    var snap = table.snapshotAtomicOperationTableState(400);
    assertEquals(200, snap.minActiveOperationTs());
    assertEquals(300, snap.maxActiveOperationTs());
    assertEquals(2, snap.inProgressTxs().size());
    assertTrue(snap.inProgressTxs().contains(200));
    assertTrue(snap.inProgressTxs().contains(300));
  }

  /// Creates many operations with a small compaction interval, then verifies
  /// that the snapshot correctly finds active operations after compaction
  /// prunes completed entries and narrows the scan range.
  @Test
  public void testScanRangeSkippingAfterCompaction() {
    // Small compaction interval to trigger compaction
    var table = new AtomicOperationsTable(5, 1);

    // Start 20 operations
    for (var i = 1; i <= 20; i++) {
      table.startOperation(i, i);
    }

    // Commit and persist all except ts=15 and ts=18
    for (var i = 1; i <= 20; i++) {
      if (i != 15 && i != 18) {
        table.commitOperation(i);
        table.persistOperation(i);
      }
    }

    // Force compaction to prune completed entries
    table.compactTable();

    // Snapshot should find exactly ts=15 and ts=18 despite compaction
    var snap = table.snapshotAtomicOperationTableState(100);
    assertEquals(15, snap.minActiveOperationTs());
    assertEquals(18, snap.maxActiveOperationTs());
    assertEquals(2, snap.inProgressTxs().size());
    assertTrue(snap.inProgressTxs().contains(15));
    assertTrue(snap.inProgressTxs().contains(18));

    // Second snapshot uses narrowed range [15, 18] — should still be correct
    var snap2 = table.snapshotAtomicOperationTableState(100);
    assertEquals(15, snap2.minActiveOperationTs());
    assertEquals(18, snap2.maxActiveOperationTs());
    assertEquals(2, snap2.inProgressTxs().size());
  }

  /// Verifies correctness when operations are started and committed before
  /// any snapshot is ever taken (cached min remains at UNKNOWN throughout).
  @Test
  public void testSnapshotWithNoPriorSnapshotCachesUnknown() {
    var table = new AtomicOperationsTable(100, 1);

    // Start and commit some operations without ever taking a snapshot
    table.startOperation(1, 1);
    table.startOperation(5, 5);
    table.startOperation(10, 10);
    table.commitOperation(1);
    table.commitOperation(10);

    // First-ever snapshot: cached min is UNKNOWN, must do full scan
    var snap = table.snapshotAtomicOperationTableState(100);
    assertEquals(5, snap.minActiveOperationTs());
    assertEquals(5, snap.maxActiveOperationTs());
    assertEquals(1, snap.inProgressTxs().size());
    assertTrue(snap.inProgressTxs().contains(5));
  }

  // ==================== Cached Min Thread Safety Tests ====================

  /// Multiple threads start operations concurrently. After all starts, the
  /// snapshot max must equal the highest TS and min the lowest TS.
  @Test
  public void testConcurrentStartsProduceConsistentSnapshot() throws InterruptedException {
    var table = new AtomicOperationsTable(10000, 1);
    var threadCount = 8;
    var opsPerThread = 200;
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount);
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var nextTs = new AtomicLong(1);
    var lock = new ReentrantLock();

    try (var executor = Executors.newFixedThreadPool(threadCount)) {
      for (var t = 0; t < threadCount; t++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < opsPerThread; i++) {
              lock.lock();
              try {
                var ts = nextTs.getAndIncrement();
                table.startOperation(ts, ts);
              } finally {
                lock.unlock();
              }
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(30, TimeUnit.SECONDS));
    }

    assertNoErrors(errors);

    var totalOps = threadCount * opsPerThread;
    // Take two snapshots: the first updates cached min authoritatively, the second
    // uses the narrowed range. Both must be self-consistent and include all ops.
    var snapshot1 = table.snapshotAtomicOperationTableState(totalOps + 100);
    verifySnapshotConsistency(snapshot1);
    var snapshot2 = table.snapshotAtomicOperationTableState(totalOps + 100);
    verifySnapshotConsistency(snapshot2);
    assertEquals(totalOps, snapshot2.inProgressTxs().size());
    assertEquals(1, snapshot2.minActiveOperationTs());
    assertEquals(totalOps, snapshot2.maxActiveOperationTs());
  }

  /// Thread A commits the min (triggering forward scan) while Thread B starts
  /// new operations. Every snapshot taken is self-consistent.
  @Test
  public void testConcurrentCommitMinWithStartsVerifyCacheConsistency()
      throws InterruptedException {
    var table = new AtomicOperationsTable(10000, 1);
    var totalOps = 500;
    var nextTs = new AtomicLong(1);

    // Pre-start operations
    for (var i = 0; i < totalOps; i++) {
      var ts = nextTs.getAndIncrement();
      table.startOperation(ts, ts);
    }

    // Establish cached min
    table.snapshotAtomicOperationTableState(totalOps + 100);

    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(3);
    var errors = new ConcurrentLinkedQueue<Throwable>();

    try (var executor = Executors.newFixedThreadPool(3)) {
      // Thread A: commits from the min upward
      executor.submit(() -> {
        try {
          startLatch.await();
          for (var ts = 1L; ts <= totalOps / 2; ts++) {
            table.commitOperation(ts);
          }
        } catch (Throwable e) {
          errors.add(e);
        } finally {
          doneLatch.countDown();
        }
      });

      // Thread B: starts new operations (under lock to maintain monotonic
      // TS ordering, matching production segmentLock behavior). Note: only
      // one thread does starts here, so the lock is only needed to order
      // w.r.t. the pre-start phase above.
      executor.submit(() -> {
        try {
          startLatch.await();
          for (var i = 0; i < 200; i++) {
            var ts = nextTs.getAndIncrement();
            table.startOperation(ts, ts);
          }
        } catch (Throwable e) {
          errors.add(e);
        } finally {
          doneLatch.countDown();
        }
      });

      // Thread C: takes snapshots and verifies consistency
      executor.submit(() -> {
        try {
          startLatch.await();
          for (var i = 0; i < 300; i++) {
            var snap = table.snapshotAtomicOperationTableState(Long.MAX_VALUE - 2);
            verifySnapshotConsistency(snap);
            Thread.yield();
          }
        } catch (Throwable e) {
          errors.add(e);
        } finally {
          doneLatch.countDown();
        }
      });

      startLatch.countDown();
      assertTrue(doneLatch.await(60, TimeUnit.SECONDS));
    }

    assertNoErrors(errors);
  }

  /// Reader threads take snapshots while writer threads start/commit/rollback.
  /// Every snapshot must be self-consistent.
  @Test
  public void testConcurrentSnapshotsWithStatusChanges() throws InterruptedException {
    var table = new AtomicOperationsTable(1000, 1);
    var writerThreads = 4;
    var readerThreads = 4;
    var opsPerWriter = 200;
    var snapshotsPerReader = 300;
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(writerThreads + readerThreads);
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var nextTs = new AtomicLong(1);
    var lock = new ReentrantLock();

    try (var executor = Executors.newFixedThreadPool(writerThreads + readerThreads)) {
      // Writer threads: start, then commit or rollback
      for (var t = 0; t < writerThreads; t++) {
        final var threadId = t;
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < opsPerWriter; i++) {
              long ts;
              lock.lock();
              try {
                ts = nextTs.getAndIncrement();
                table.startOperation(ts, ts);
              } finally {
                lock.unlock();
              }
              Thread.yield();
              if ((threadId + i) % 3 == 0) {
                table.rollbackOperation(ts);
              } else {
                table.commitOperation(ts);
              }
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      // Reader threads: take snapshots and verify consistency
      for (var t = 0; t < readerThreads; t++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < snapshotsPerReader; i++) {
              var snap = table.snapshotAtomicOperationTableState(Long.MAX_VALUE - 2);
              verifySnapshotConsistency(snap);
              Thread.yield();
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(60, TimeUnit.SECONDS));
    }

    assertNoErrors(errors);
  }

  /// High contention: many threads rapidly start and immediately commit operations
  /// (frequent boundary changes). Verifies no incorrect snapshots.
  @Test
  public void testConcurrentCacheInvalidationStorm() throws InterruptedException {
    var table = new AtomicOperationsTable(10000, 1);
    var threadCount = 8;
    var opsPerThread = 300;
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount + 2);
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var nextTs = new AtomicLong(1);
    var lock = new ReentrantLock();

    try (var executor = Executors.newFixedThreadPool(threadCount + 2)) {
      // Writer threads: rapid start+commit cycles
      for (var t = 0; t < threadCount; t++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < opsPerThread; i++) {
              long ts;
              lock.lock();
              try {
                ts = nextTs.getAndIncrement();
                table.startOperation(ts, ts);
              } finally {
                lock.unlock();
              }
              table.commitOperation(ts);
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      // Reader threads: continuous snapshots
      for (var r = 0; r < 2; r++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < 500; i++) {
              var snap = table.snapshotAtomicOperationTableState(Long.MAX_VALUE - 2);
              verifySnapshotConsistency(snap);
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(60, TimeUnit.SECONDS));
    }

    assertNoErrors(errors);
  }

  /// Mixed workload: threads start, commit, rollback, and take snapshots
  /// concurrently. After each snapshot, min/max match the actual set.
  @Test
  public void testConcurrentMixedOperationsVerifyCacheConsistency()
      throws InterruptedException {
    var table = new AtomicOperationsTable(500, 1);
    var writerThreads = 6;
    var readerThreads = 3;
    var opsPerWriter = 200;
    var snapshotsPerReader = 400;
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(writerThreads + readerThreads);
    var errors = new ConcurrentLinkedQueue<Throwable>();
    var nextTs = new AtomicLong(1);
    var lock = new ReentrantLock();

    try (var executor = Executors.newFixedThreadPool(writerThreads + readerThreads)) {
      for (var t = 0; t < writerThreads; t++) {
        final var threadId = t;
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < opsPerWriter; i++) {
              long ts;
              lock.lock();
              try {
                ts = nextTs.getAndIncrement();
                table.startOperation(ts, ts);
              } finally {
                lock.unlock();
              }
              Thread.yield();
              switch ((threadId + i) % 4) {
                case 0 -> table.rollbackOperation(ts);
                case 1 -> table.commitOperation(ts);
                default -> {
                  table.commitOperation(ts);
                  table.persistOperation(ts);
                }
              }
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      for (var t = 0; t < readerThreads; t++) {
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var i = 0; i < snapshotsPerReader; i++) {
              var snap = table.snapshotAtomicOperationTableState(Long.MAX_VALUE - 2);
              verifySnapshotConsistency(snap);
              Thread.yield();
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(60, TimeUnit.SECONDS));
    }

    assertNoErrors(errors);
  }

  /// Many threads commit the current min concurrently (each triggering a forward
  /// scan). Verifies min converges to the correct value.
  @Test
  public void testRapidMinForwardScansUnderContention() throws InterruptedException {
    var table = new AtomicOperationsTable(10000, 1);
    var totalOps = 1000;

    // Pre-start all operations
    for (var i = 1; i <= totalOps; i++) {
      table.startOperation(i, i);
    }

    // Establish cached min
    table.snapshotAtomicOperationTableState(totalOps + 100);

    var threadCount = 8;
    var opsPerThread = totalOps / threadCount;
    var startLatch = new CountDownLatch(1);
    var doneLatch = new CountDownLatch(threadCount + 1);
    var errors = new ConcurrentLinkedQueue<Throwable>();

    try (var executor = Executors.newFixedThreadPool(threadCount + 1)) {
      // Each thread commits a contiguous slice of operations
      for (var t = 0; t < threadCount; t++) {
        final var fromTs = t * opsPerThread + 1;
        final var toTs = (t + 1) * opsPerThread;
        executor.submit(() -> {
          try {
            startLatch.await();
            for (var ts = fromTs; ts <= toTs; ts++) {
              table.commitOperation(ts);
            }
          } catch (Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      // Reader thread verifies consistency
      executor.submit(() -> {
        try {
          startLatch.await();
          for (var i = 0; i < 500; i++) {
            var snap = table.snapshotAtomicOperationTableState(totalOps + 100);
            verifySnapshotConsistency(snap);
            Thread.yield();
          }
        } catch (Throwable e) {
          errors.add(e);
        } finally {
          doneLatch.countDown();
        }
      });

      startLatch.countDown();
      assertTrue(doneLatch.await(60, TimeUnit.SECONDS));
    }

    assertNoErrors(errors);

    // All operations should be committed
    assertEquals(-1, table.getSegmentEarliestOperationInProgress());
  }

  // ==================== Bounded Range Tests ====================

  /** Large identifier gaps create bounded ranges and preserve every lifecycle transition. */
  @Test
  public void largeIdentifierGapsDoNotMaterializeUnusedSlots() {
    var table = new AtomicOperationsTable(100, 0, 8);
    var aboveIntGap = 1L << 31;
    var aboveUnsignedIntGap = 1L << 32;

    table.startOperation(0, 10);
    table.startOperation(aboveIntGap, 20);
    table.startOperation(aboveUnsignedIntGap, 30);

    assertEquals(3, table.rangeCount());
    assertEquals(3, table.storedEntryCount());
    assertEquals(10, table.getSegmentEarliestOperationInProgress());

    table.commitOperation(0);
    table.persistOperation(0);
    table.rollbackOperation(aboveIntGap);
    table.commitOperation(aboveUnsignedIntGap);

    assertEquals(30, table.getSegmentEarliestNotPersistedOperation());
    table.persistOperation(aboveUnsignedIntGap);
    assertEquals(-1, table.getSegmentEarliestNotPersistedOperation());
  }

  /** A small test capacity forces an adjacent split, including at the identifier ceiling. */
  @Test
  public void adjacentRegistrationsSplitWithoutCeilingArithmeticWraparound() {
    var table = new AtomicOperationsTable(100, Long.MAX_VALUE - 3, 2);

    table.startOperation(Long.MAX_VALUE - 3, 1);
    table.startOperation(Long.MAX_VALUE - 2, 2);
    table.startOperation(Long.MAX_VALUE - 1, 3);
    table.startOperation(Long.MAX_VALUE, 4);

    assertEquals(2, table.rangeCount());
    assertEquals(4, table.storedEntryCount());
    var snapshot = table.snapshotAtomicOperationTableState(Long.MAX_VALUE);
    assertEquals(4, snapshot.inProgressTxs().size());
    assertTrue(snapshot.inProgressTxs().contains(Long.MAX_VALUE));

    table.commitOperation(Long.MAX_VALUE);
    table.persistOperation(Long.MAX_VALUE);
    assertEquals(1, table.getSegmentEarliestOperationInProgress());
  }

  /** Duplicate and backward registrations fail even when the requested identifier is in a gap. */
  @Test
  public void registrationHighWaterRejectsDuplicateAndBackwardIdentifiers() {
    var table = new AtomicOperationsTable(100, 0, 4);
    table.startOperation(10, 1);
    table.startOperation(30, 2);

    assertRegistrationFails(table, 30);
    assertRegistrationFails(table, 20);
    assertRegistrationFails(table, 9);
    assertEquals(2, table.storedEntryCount());
  }

  /**
   * Compaction may empty every range, but it must not reset registration monotonicity, and it
   * must leave no reachable descriptor behind.
   *
   * <p>The test repeats the whole cycle of registration, terminal transition and compaction three
   * times, because a retained empty descriptor could appear only in a later cycle. Each cycle
   * also jumps far ahead, so each cycle drains a freshly created range. After every cycle the
   * directory must hold no range and no entry, the compaction must report zero copied
   * descriptors, and the registration boundary must still reject the drained identifier and its
   * predecessor.
   */
  @Test
  public void fullDrainPreservesRegistrationHighWater() {
    var table = new AtomicOperationsTable(100, 0, 4);
    var operationTs = 5L;

    for (var cycle = 0; cycle < 3; cycle++) {
      var cycleLabel = "cycle " + cycle;
      table.startOperation(operationTs, cycle + 1);
      assertEquals(cycleLabel, 1, table.rangeCount());
      assertEquals(cycleLabel, 1, table.storedEntryCount());

      table.commitOperation(operationTs);
      table.persistOperation(operationTs);
      table.compactTable();

      assertEquals(cycleLabel, 0, table.rangeCount());
      assertEquals(cycleLabel, 0, table.storedEntryCount());
      assertEquals(cycleLabel, 0, table.lastCompactedRangeCount());
      assertRegistrationFails(table, operationTs);
      assertRegistrationFails(table, operationTs - 1);

      operationTs += 1L << 32;
    }

    table.startOperation(operationTs, 9);
    assertEquals(1, table.rangeCount());
    assertEquals(1, table.storedEntryCount());
    assertEquals(9, table.getSegmentEarliestOperationInProgress());
  }

  /** An unregistered identifier between starts remains absent and does not pin compaction. */
  @Test
  public void issuedButUnregisteredHoleDoesNotAllocateOrRetainSlack() {
    var table = new AtomicOperationsTable(100, 0, 4);
    table.startOperation(0, 1);
    // Identifier 1 represents a manager start that failed before table registration.
    table.startOperation(2, 2);

    assertEquals(2, table.rangeCount());
    assertEquals(2, table.storedEntryCount());
    table.commitOperation(0);
    table.persistOperation(0);
    table.compactTable();

    assertEquals(1, table.rangeCount());
    assertEquals(1, table.storedEntryCount());
    assertEquals(2, table.getSegmentEarliestOperationInProgress());
  }

  /** A cached boundary in a removed range resolves upward to the next retained active entry. */
  @Test
  public void cachedBoundaryFallsForwardAcrossCompactedRangeGap() {
    var table = new AtomicOperationsTable(100, 0, 2);
    table.startOperation(0, 1);
    table.startOperation(1L << 32, 2);
    assertEquals(0, table.snapshotAtomicOperationTableState(Long.MAX_VALUE).minActiveOperationTs());

    table.commitOperation(0);
    table.persistOperation(0);
    table.compactTable();

    var snapshot = table.snapshotAtomicOperationTableState(Long.MAX_VALUE);
    assertEquals(1L << 32, snapshot.minActiveOperationTs());
    assertEquals(1, snapshot.inProgressTxs().size());
    assertEquals(2, table.getSegmentEarliestNotPersistedOperation());
  }

  /** Fragmented and contiguous histories store the same entries regardless of gap magnitude. */
  @Test
  public void storageWorkDependsOnEntriesAndRangesInsteadOfIdentifierDistance() {
    var contiguous = new AtomicOperationsTable(100, 0, 16);
    var fragmented = new AtomicOperationsTable(100, 0, 16);
    var fragmentedTs = 0L;

    for (var i = 0; i < 64; i++) {
      contiguous.startOperation(i, i);
      fragmented.startOperation(fragmentedTs, i);
      fragmentedTs += 1L << 32;
    }

    assertEquals(64, contiguous.storedEntryCount());
    assertEquals(64, fragmented.storedEntryCount());
    assertEquals(4, contiguous.rangeCount());
    assertEquals(64, fragmented.rangeCount());
    assertEquals(
        contiguous.snapshotAtomicOperationTableState(Long.MAX_VALUE).inProgressTxs().size(),
        fragmented.snapshotAtomicOperationTableState(Long.MAX_VALUE).inProgressTxs().size());
  }

  /**
   * Compaction work must follow the number of retained range descriptors, not the numerical
   * distance between identifiers.
   *
   * <p>Eight registrations with a gap of two to the power of thirty-two each create their own
   * range. While the oldest operation stays in progress, compaction must copy all eight
   * descriptors. After the three oldest operations become terminal, compaction must copy exactly
   * the five remaining descriptors. The copied descriptor count comes from the table itself, so
   * the test observes the real compaction work instead of a derived structural count.
   */
  @Test
  public void compactionCopiesOnlyRetainedRangeDescriptors() {
    // A very large compaction interval keeps automatic compaction out of this test.
    var table = new AtomicOperationsTable(Integer.MAX_VALUE, 0, 2);
    var identifiers = new long[8];
    var operationTs = 0L;
    for (var index = 0; index < identifiers.length; index++) {
      identifiers[index] = operationTs;
      table.startOperation(operationTs, index + 1);
      operationTs += 1L << 32;
    }
    assertEquals(8, table.rangeCount());
    assertEquals(8, table.storedEntryCount());

    table.compactTable();
    assertEquals(8, table.lastCompactedRangeCount());
    assertEquals(8, table.rangeCount());

    for (var index = 0; index < 3; index++) {
      table.commitOperation(identifiers[index]);
      table.persistOperation(identifiers[index]);
    }
    table.compactTable();

    assertEquals(5, table.lastCompactedRangeCount());
    assertEquals(5, table.rangeCount());
    assertEquals(5, table.storedEntryCount());
    assertEquals(4, table.getSegmentEarliestOperationInProgress());
  }

  /**
   * Registration directory work must stay proportional to the number of registrations. It must
   * not follow the retained history length or the numerical distance between identifiers.
   *
   * <p>A package-private observer counts every actual range descriptor read through the directory.
   * The test covers three history sizes, contiguous ranges, fragmented ranges, and two large gap
   * magnitudes. Every operation stays in progress, so the complete directory remains retained.
   *
   * <p>A full-directory copy and a read-only pre-append scan both add a growing number of observed
   * descriptor reads. Either defect therefore makes reads per registration grow with history.
   * The assertion uses actual implementation work and no elapsed-time or JVM-specific metric.
   */
  @Test
  public void registrationDirectoryWorkDoesNotGrowWithHistoryLength() {
    var sizes = new int[] {256, 1024, 4096};
    var steps = new long[] {1, 1L << 32, 1L << 40};

    for (var step : steps) {
      var shortestHistoryWork = directoryReadsPerRegistration(sizes[0], step, 64);
      for (var sizeIndex = 1; sizeIndex < sizes.length; sizeIndex++) {
        var size = sizes[sizeIndex];
        var readsPerRegistration = directoryReadsPerRegistration(size, step, 64);
        var details = "step " + step + ", size " + size + ", shortest history work "
            + shortestHistoryWork + ", current work " + readsPerRegistration;

        assertTrue("descriptor work must not grow with retained history: " + details,
            readsPerRegistration <= 2 * shortestHistoryWork);
      }
    }

    var narrowerGapWork = directoryReadsPerRegistration(1024, 1L << 32, 64);
    var widerGapWork = directoryReadsPerRegistration(1024, 1L << 40, 64);
    assertTrue("descriptor work must not grow with numeric gap magnitude",
        widerGapWork <= 2 * narrowerGapWork && narrowerGapWork <= 2 * widerGapWork);
  }

  /** Registers one retained history and returns its observed directory reads per registration. */
  private static double directoryReadsPerRegistration(
      int operationCount, long identifierStep, int rangeCapacity) {
    var directoryReads = new AtomicLong();
    var table = new AtomicOperationsTable(
        Integer.MAX_VALUE, 0, rangeCapacity, directoryReads::incrementAndGet);
    var operationTs = 0L;
    for (var index = 0; index < operationCount; index++) {
      table.startOperation(operationTs, 1);
      operationTs += identifierStep;
    }

    // Read the count before structural assertions, because those assertions traverse the table.
    var observedReads = directoryReads.get();
    assertEquals(operationCount, table.storedEntryCount());
    var expectedRangeCount = identifierStep == 1
        ? (operationCount + rangeCapacity - 1) / rangeCapacity : operationCount;
    assertEquals(expectedRangeCount, table.rangeCount());
    return (double) observedReads / operationCount;
  }

  private static void assertRegistrationFails(AtomicOperationsTable table, long operationTs) {
    try {
      table.startOperation(operationTs, 1);
      fail("Registration should reject identifier " + operationTs);
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("registration boundary"));
    }
  }

  // ==================== Test Helpers ====================

  /// Asserts that a snapshot's min/max match the actual min/max of its
  /// inProgressTxs set. If the set is empty, min and max should be equal
  /// (both set to currentTimestamp + 1).
  private static void verifySnapshotConsistency(
      AtomicOperationsTable.AtomicOperationsSnapshot snap) {
    var set = snap.inProgressTxs();
    if (set.isEmpty()) {
      assertEquals(
          "min and max must be equal when no ops are in progress",
          snap.minActiveOperationTs(), snap.maxActiveOperationTs());
      return;
    }

    var actualMin = Long.MAX_VALUE;
    var actualMax = Long.MIN_VALUE;
    for (var it = set.iterator(); it.hasNext();) {
      var ts = it.nextLong();
      if (ts < actualMin) {
        actualMin = ts;
      }
      if (ts > actualMax) {
        actualMax = ts;
      }
    }
    assertEquals("snapshot min must match actual min of inProgressTxs set",
        actualMin, snap.minActiveOperationTs());
    assertEquals("snapshot max must match actual max of inProgressTxs set",
        actualMax, snap.maxActiveOperationTs());
  }

  /// Takes a snapshot and asserts self-consistency.
  private static void assertSnapshotConsistency(
      AtomicOperationsTable table, long currentTimestamp) {
    verifySnapshotConsistency(
        table.snapshotAtomicOperationTableState(currentTimestamp));
  }

  /// Fails the test if any errors were collected.
  private static void assertNoErrors(ConcurrentLinkedQueue<Throwable> errors) {
    if (!errors.isEmpty()) {
      var first = errors.peek();
      fail("Errors occurred: " + first.getClass().getSimpleName()
          + ": " + first.getMessage());
    }
  }

  // ==================== Snapshot Visibility Upper Bound Tests ====================

  /// Verifies the fix for the CME bug: a committed transaction with a timestamp
  /// above maxActiveOperationTs must be visible. The old rule
  /// `recordTs >= maxActiveOperationTs → NOT VISIBLE` was wrong because committed
  /// transactions can have higher timestamps than in-progress ones.
  ///
  /// Scenario: long-running op D at ts=5 is still IN_PROGRESS while short-lived
  /// op C at ts=7 has already committed. A new reader at snapshotTs=10 must see
  /// C's writes (version 7).
  @Test
  public void testCommittedVersionAboveMaxInProgressIsVisible() {
    var table = new AtomicOperationsTable(100, 0);

    // D starts at ts=5, still in progress
    table.startOperation(5, 1);
    // C starts at ts=7 and commits — record gets version 7
    table.startOperation(7, 1);
    table.commitOperation(7);

    // New reader takes snapshot at ts=10
    var snapshot = table.snapshotAtomicOperationTableState(10);

    // maxActiveOperationTs is 5 (only D is in-progress)
    assertEquals(5, snapshot.maxActiveOperationTs());
    assertEquals(10, snapshot.snapshotTs());

    // Version 7 is ABOVE maxActiveOperationTs but BELOW snapshotTs.
    // It must be visible because it's committed and within the registered range.
    assertTrue("Committed version above max in-progress must be visible",
        snapshot.isEntryVisible(7));

    // Version 5 is in-progress — not visible
    assertFalse(snapshot.isEntryVisible(5));

    // Version 11 is truly future — not visible
    assertFalse(snapshot.isEntryVisible(11));
  }

  /// Verifies that the snapshotTs field correctly reflects the currentTimestamp
  /// passed to snapshotAtomicOperationTableState.
  @Test
  public void testSnapshotTsMatchesCurrentTimestamp() {
    var table = new AtomicOperationsTable(100, 0);
    table.startOperation(0, 1);

    var snapshot = table.snapshotAtomicOperationTableState(42);
    assertEquals(42, snapshot.snapshotTs());
  }

  /// Verifies the full visibility spectrum across the timestamp space:
  /// below min (visible), in-progress (not visible), between min and max (depends),
  /// above max but below snapshotTs (visible), above snapshotTs (not visible).
  @Test
  public void testVisibilitySpectrum() {
    var table = new AtomicOperationsTable(100, 0);

    // Create a gap: ops at ts=5 and ts=15 are in-progress, ts=10 is committed
    table.startOperation(5, 1);
    table.startOperation(10, 1);
    table.commitOperation(10);
    table.startOperation(15, 1);
    table.commitOperation(15);
    table.startOperation(20, 1);

    // Snapshot at ts=25
    var snapshot = table.snapshotAtomicOperationTableState(25);
    assertEquals(5, snapshot.minActiveOperationTs());
    assertEquals(20, snapshot.maxActiveOperationTs());
    assertEquals(25, snapshot.snapshotTs());

    // Below min: visible
    assertTrue(snapshot.isEntryVisible(3));

    // At min (in-progress): not visible
    assertFalse(snapshot.isEntryVisible(5));

    // Between min and max, committed: visible
    assertTrue(snapshot.isEntryVisible(10));
    assertTrue(snapshot.isEntryVisible(15));

    // At max (in-progress): not visible
    assertFalse(snapshot.isEntryVisible(20));

    // Above max, below snapshotTs: visible (committed in registered range)
    assertTrue(snapshot.isEntryVisible(22));

    // At snapshotTs: visible
    assertTrue(snapshot.isEntryVisible(25));

    // Above snapshotTs: not visible (truly future)
    assertFalse(snapshot.isEntryVisible(26));
  }

  /// Verifies that when no operations are in progress, all versions up to
  /// snapshotTs are visible and versions above it are not.
  @Test
  public void testNoActiveOpsVisibility() {
    var table = new AtomicOperationsTable(100, 0);

    table.startOperation(0, 1);
    table.commitOperation(0);

    var snapshot = table.snapshotAtomicOperationTableState(5);
    assertEquals(5, snapshot.snapshotTs());

    assertTrue(snapshot.isEntryVisible(0));
    assertTrue(snapshot.isEntryVisible(5));
    assertFalse(snapshot.isEntryVisible(6));
  }

  /// Reproduces the exact CME scenario from CI: a long-running background operation
  /// (e.g., GC at ts=140) is IN_PROGRESS while a frontend transaction committed a
  /// vertex at ts=142. A new reader must see version 142.
  @Test
  public void testBackgroundOpDoesNotHideCommittedVersion() {
    var table = new AtomicOperationsTable(100, 100);

    // Background GC starts at ts=140
    table.startOperation(140, 1);
    // Frontend tx commits a record at ts=142
    table.startOperation(142, 1);
    table.commitOperation(142);

    // New frontend tx takes snapshot at ts=145
    var snapshot = table.snapshotAtomicOperationTableState(145);
    assertEquals(140, snapshot.maxActiveOperationTs());

    // Version 142 must be visible — it's committed and within the registered range
    assertTrue("Committed version 142 must be visible despite GC at 140",
        snapshot.isEntryVisible(142));
  }
}
