package com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.atomicoperations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.vmlens.api.AllInterleavings;
import com.vmlens.api.AllInterleavingsBuilder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Multi-threaded tests for {@link AtomicOperationsTable} using VMLens for
 * systematic interleaving exploration. VMLens exhaustively explores thread
 * schedules, so small operation counts are sufficient for thorough coverage.
 *
 * <p>Each test focuses on a specific concurrent interaction with the cached
 * min field and snapshot scan range narrowing.
 *
 * <p>The class deadline turns a hang into a test failure. A deadlock between a registration, a
 * snapshot and a compaction would otherwise block the build without any report. The deadline is
 * far above the measured runtime of the whole class, which stays near ten seconds, so it fails
 * only on a real hang. Surefire runs this class, and the VMLens plugin extends the Surefire
 * plugin, so both executions apply the deadline.
 */
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class AtomicOperationsTableMTTest {

  // VMLens exhaustively explores all thread interleavings, so small counts
  // provide thorough coverage (unlike random sampling which needs many iterations).
  // AtomicOperationsTable has many synchronization points (VarHandle CAS +
  // ScalableRWLock + CASObjectArray), which creates a large interleaving space.
  // A lower limit avoids hitting VMLens internal bugs when the alternating order
  // exploration is very large.
  private static final int MAX_ITERATIONS = 100;
  private static final long CLEANUP_GRACE_MILLIS = TimeUnit.SECONDS.toMillis(5);
  // A cleanup handshake step waits for evidence in the suppressed list. The bound only keeps a
  // broken cleanup pass from blocking the class, so a passing run never reaches it.
  private static final long HANDSHAKE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(2);
  // The retry test needs a grace period far above its handshake budget of two bounded steps, so
  // that a failure there reports cleanup behavior and never reports machine load.
  private static final long RETRY_TEST_GRACE_MILLIS = TimeUnit.SECONDS.toMillis(60);
  // The collective deadline test uses several workers that ignore interruption. A correct pass
  // spends one grace period in total, while a per worker deadline would spend one per worker.
  private static final int DEADLINE_TEST_WORKERS = 4;
  private static final long DEADLINE_TEST_GRACE_MILLIS = 500;

  private AllInterleavings allInterleavings(String name) throws Exception {
    return new AllInterleavingsBuilder()
        .withMaximumIterations(MAX_ITERATIONS)
        .build(name);
  }

  /**
   * Runs every task in its own thread and fails the test when a task fails or hangs.
   *
   * <p>The VMLens API class {@code com.vmlens.api.Runner} is not sufficient here. Its sources for
   * version 1.2.28 show that it records only {@link RuntimeException}, so it drops every
   * {@link Error}, including an {@link AssertionError} raised inside a worker thread.
   *
   * <p>The joins carry no deadline on purpose. A join without a deadline creates the memory
   * ordering that VMLens models, so VMLens does not report the result handover from a worker
   * thread as a data race. The class deadline instead turns a hang into a test failure, and it
   * interrupts this thread, which ends a blocked join with a failure.
   *
   * <p>Each worker writes only its own array slot, and the main thread reads those slots after
   * the joins. The helper therefore adds no shared synchronization that would enlarge the
   * interleaving space that VMLens explores.
   */
  private static void runConcurrently(Runnable... tasks) {
    final var failures = new Throwable[tasks.length];
    final var threads = new Thread[tasks.length];
    for (var index = 0; index < tasks.length; index++) {
      final var task = tasks[index];
      final var slot = index;
      threads[index] = new Thread(() -> {
        try {
          task.run();
        } catch (Throwable failure) {
          // Every failure kind must reach the main thread, including an assertion error.
          failures[slot] = failure;
        }
      });
    }

    var startedCount = 0;
    try {
      for (var thread : threads) {
        thread.start();
        startedCount++;
      }

      // Untimed joins preserve the happens-before edge that VMLens models on the normal path.
      for (var thread : threads) {
        thread.join();
      }
    } catch (Throwable originalFailure) {
      cleanupWorkers(threads, startedCount, originalFailure,
          TimeUnit.MILLISECONDS.toNanos(CLEANUP_GRACE_MILLIS));
      rethrow(originalFailure);
    }

    for (var failure : failures) {
      if (failure != null) {
        throw new AssertionError("a worker thread failed", failure);
      }
    }
  }

  // This path runs only after thread startup or joining fails. Interruption gives cooperative
  // workers a termination request, and bounded joins prevent cleanup from hiding the first failure.
  private static void cleanupWorkers(Thread[] threads, int startedCount,
      Throwable originalFailure, long cleanupGraceNanos) {
    var restoreInterrupt = Thread.interrupted();
    for (var index = 0; index < startedCount; index++) {
      threads[index].interrupt();
    }

    // Every retry uses one shared deadline. Repeated interruption cannot abandon the current
    // worker, and it cannot give that worker or a later worker a fresh grace period.
    final var deadline = System.nanoTime() + cleanupGraceNanos;
    for (var index = 0; index < startedCount; index++) {
      while (threads[index].isAlive()) {
        var remainingNanos = deadline - System.nanoTime();
        if (remainingNanos <= 0) {
          break;
        }
        try {
          TimeUnit.NANOSECONDS.timedJoin(threads[index], remainingNanos);
        } catch (InterruptedException cleanupInterruption) {
          originalFailure.addSuppressed(cleanupInterruption);
          restoreInterrupt = true;
        }
      }
    }

    for (var index = 0; index < startedCount; index++) {
      if (threads[index].isAlive()) {
        originalFailure.addSuppressed(new AssertionError(
            "worker did not terminate after interruption: " + threads[index].getName()));
      }
    }
    if (restoreInterrupt || originalFailure instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  private static void rethrow(Throwable failure) {
    if (failure instanceof RuntimeException runtimeException) {
      throw runtimeException;
    }
    if (failure instanceof Error error) {
      throw error;
    }
    throw new AssertionError("the main test thread failed while joining workers", failure);
  }

  /** A worker assertion must reach the main test thread after all workers terminate. */
  @Test
  public void concurrentHelperPropagatesWorkerErrors() {
    var failure = assertThrows(AssertionError.class,
        () -> runConcurrently(() -> {
          throw new AssertionError("worker failure control");
        }));
    assertTrue(failure.getCause() instanceof AssertionError);
  }

  /** Main-thread interruption must interrupt and join a cooperative worker before returning. */
  @Test
  public void concurrentHelperCleansUpCooperativeWorkersAfterInterruption() {
    var mainThread = Thread.currentThread();
    var workerStarted = new CountDownLatch(1);
    var workerStopped = new AtomicBoolean();

    assertThrows(AssertionError.class, () -> runConcurrently(
        () -> {
          workerStarted.countDown();
          try {
            Thread.sleep(Long.MAX_VALUE);
          } catch (InterruptedException expected) {
            workerStopped.set(true);
          }
        },
        () -> {
          try {
            assertTrue(workerStarted.await(5, TimeUnit.SECONDS));
          } catch (InterruptedException interruption) {
            throw new AssertionError(interruption);
          }
          mainThread.interrupt();
        }));

    // The helper restores the original interrupt after cleanup. Clear it for the test runner.
    assertTrue(Thread.interrupted());
    assertTrue(workerStopped.get(), "the cooperative worker must stop before the helper returns");
  }

  /**
   * Two interruptions arriving during cleanup must not abandon the worker that cleanup is joining.
   *
   * <p>The worker starts its cooperative shutdown when cleanup interrupts it, and it stays alive
   * during that shutdown. While alive it delivers two interruptions to the cleanup thread. It
   * delivers the second interruption only after the suppressed list of the original failure shows
   * that cleanup consumed the first one. The timed join clears the interrupted status before it
   * throws, and it records the suppressed entry afterwards, so that evidence proves the status was
   * already clear. The two interruptions therefore cannot collapse into one, and no sleep orders
   * the two threads.
   *
   * <p>The worker stops only after both interruptions reached cleanup. A single join attempt per
   * worker would leave the shutdown handshake unfinished, would report the worker as a survivor,
   * and would lose the second suppressed interruption. The assertions demand the opposite of all
   * three, and they also demand the original failure instance and the restored interrupted status.
   */
  @Test
  public void concurrentHelperRetriesCleanupAfterRepeatedInterruption() {
    var cleanupThread = Thread.currentThread();
    var originalFailure = new IllegalStateException("original cleanup failure");
    var deliveredInterruptions = new AtomicInteger();
    var workerStopped = new AtomicBoolean();
    var handshakeFailure = new AtomicReference<String>();

    var worker = new Thread(() -> {
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException expectedTerminationRequest) {
        // Cleanup requested termination, so the observable shutdown handshake starts here.
      }
      for (var attempt = 1; attempt <= 2; attempt++) {
        cleanupThread.interrupt();
        deliveredInterruptions.set(attempt);
        if (!awaitSuppressedInterruptions(originalFailure, attempt)) {
          handshakeFailure.set("cleanup never recorded interruption " + attempt
              + ", so it stopped joining this worker");
          return;
        }
      }
      workerStopped.set(true);
    }, "cleanup-retry-worker");

    worker.start();
    boolean interruptRestored;
    try {
      cleanupWorkers(new Thread[] {worker}, 1, originalFailure,
          TimeUnit.MILLISECONDS.toNanos(RETRY_TEST_GRACE_MILLIS));
    } finally {
      // Read the status before the join, because the join would consume it.
      interruptRestored = Thread.interrupted();
      joinQuietly(worker);
      // A failing run can leave a driven interruption behind, which must not reach the next test.
      Thread.interrupted();
    }

    var handshakeProblem = handshakeFailure.get();
    assertNull(handshakeProblem, handshakeProblem);
    assertEquals(2, deliveredInterruptions.get(),
        "the worker must deliver both interruptions while it is still terminating");
    var rethrown = assertThrows(IllegalStateException.class, () -> rethrow(originalFailure));
    assertSame(originalFailure, rethrown, "cleanup must preserve the original failure instance");
    assertTrue(interruptRestored, "cleanup must restore the interrupted status");
    assertTrue(workerStopped.get(), "cleanup must retry the same worker until that worker stops");
    assertEquals(2, countSuppressedInterruptions(originalFailure),
        "both cleanup interruptions must remain suppressed on the original failure");
    assertEquals(0, countSuppressedSurvivorReports(originalFailure),
        "a worker that stops inside the grace must not be reported as surviving cleanup");
  }

  /**
   * One grace deadline must bound the whole cleanup pass, not one deadline per worker.
   *
   * <p>Every worker here ignores interruption until the test releases it, so cleanup can only
   * leave its join loop when a deadline expires. A shared deadline makes the pass cost one grace
   * period for all four workers. A deadline that restarts for each worker would cost four grace
   * periods, which is four times the lower bound and twice the upper bound asserted below.
   *
   * <p>A deadline is a duration, so the pass duration carries the evidence. The lower bound also
   * rejects a pass that skips the grace period, and the upper bound leaves one further grace
   * period of scheduling slack for a correct pass.
   */
  @Test
  public void concurrentHelperUsesCollectiveCleanupDeadline() throws Exception {
    var releaseWorkers = new CountDownLatch(1);
    var workersStarted = new CountDownLatch(DEADLINE_TEST_WORKERS);
    var workers = new Thread[DEADLINE_TEST_WORKERS];
    for (var index = 0; index < workers.length; index++) {
      workers[index] = uninterruptibleTestWorker(workersStarted, releaseWorkers);
    }
    var originalFailure = new IllegalStateException("bounded cleanup control");

    for (var worker : workers) {
      worker.start();
    }
    assertTrue(workersStarted.await(5, TimeUnit.SECONDS));
    long elapsedMillis;
    var startedNanos = System.nanoTime();
    try {
      cleanupWorkers(workers, workers.length, originalFailure,
          TimeUnit.MILLISECONDS.toNanos(DEADLINE_TEST_GRACE_MILLIS));
      elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    } finally {
      releaseWorkers.countDown();
      for (var worker : workers) {
        joinQuietly(worker);
      }
    }

    assertTrue(elapsedMillis >= DEADLINE_TEST_GRACE_MILLIS,
        "cleanup must wait out the grace period on workers that ignore interruption, but the "
            + "pass took " + elapsedMillis + " ms");
    assertTrue(elapsedMillis < 2 * DEADLINE_TEST_GRACE_MILLIS,
        "one deadline must bound cleanup for all " + workers.length + " workers, but the pass "
            + "took " + elapsedMillis + " ms");
    assertEquals(workers.length, countSuppressedSurvivorReports(originalFailure),
        "every worker still alive at the collective deadline must be reported");
    assertEquals(0, countSuppressedInterruptions(originalFailure),
        "nothing interrupted the cleanup thread in this test");
  }

  /**
   * Waits until the suppressed list of the failure holds the expected number of interruptions.
   *
   * <p>{@link Throwable#addSuppressed} and {@link Throwable#getSuppressed} both synchronize on the
   * throwable, so this poll observes the cleanup pass without a further test only assumption.
   * The result reports whether the evidence arrived before the bound.
   */
  private static boolean awaitSuppressedInterruptions(Throwable failure, int expectedCount) {
    var deadline = System.nanoTime() + HANDSHAKE_TIMEOUT_NANOS;
    while (countSuppressedInterruptions(failure) < expectedCount) {
      if (System.nanoTime() - deadline >= 0) {
        return false;
      }
      Thread.onSpinWait();
    }
    return true;
  }

  private static int countSuppressedInterruptions(Throwable failure) {
    var count = 0;
    for (var suppressed : failure.getSuppressed()) {
      if (suppressed instanceof InterruptedException) {
        count++;
      }
    }
    return count;
  }

  /** Counts the reports that cleanup adds for a worker that outlived the grace period. */
  private static int countSuppressedSurvivorReports(Throwable failure) {
    var count = 0;
    for (var suppressed : failure.getSuppressed()) {
      if (suppressed instanceof AssertionError) {
        count++;
      }
    }
    return count;
  }

  /**
   * Joins a test worker without leaking it, even when a late interruption arrives.
   *
   * <p>A test that drives interruption can deliver one after the measured call returned. Such an
   * interruption must not abandon a worker thread, and it must not reach the next test, so this
   * teardown retries the join and then drops the status.
   */
  private static void joinQuietly(Thread thread) {
    var interrupted = false;
    while (true) {
      try {
        thread.join();
        break;
      } catch (InterruptedException lateInterruption) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.interrupted();
    }
  }

  private static Thread uninterruptibleTestWorker(CountDownLatch started,
      CountDownLatch release) {
    return new Thread(() -> {
      started.countDown();
      while (true) {
        try {
          release.await();
          return;
        } catch (InterruptedException ignored) {
          // Stay alive until the test releases this worker after the bounded cleanup call.
        }
      }
    });
  }

  /**
   * Verifies that a snapshot's min/max match the actual min/max of its
   * inProgressTxs set. If the set is empty, min and max must be equal
   * (both set to currentTimestamp + 1).
   */
  private static void verifySnapshotConsistency(
      AtomicOperationsTable.AtomicOperationsSnapshot snap) {
    var set = snap.inProgressTxs();
    if (set.isEmpty()) {
      assertEquals(snap.minActiveOperationTs(), snap.maxActiveOperationTs(),
          "min and max must be equal when no ops are in progress");
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
    assertEquals(actualMin, snap.minActiveOperationTs(),
        "snapshot min must match actual min of inProgressTxs set");
    assertEquals(actualMax, snap.maxActiveOperationTs(),
        "snapshot max must match actual max of inProgressTxs set");
  }

  /**
   * One thread starts a new operation while another takes a snapshot on a table
   * that already has active operations. The snapshot must be self-consistent.
   */
  @Test
  public void concurrentStartWithSnapshotShouldBeConsistent() throws Exception {
    try (var allInterleavings =
        allInterleavings("concurrentStartWithSnapshotShouldBeConsistent")) {
      while (allInterleavings.hasNext()) {
        var table = new AtomicOperationsTable(100, 1);
        // Pre-establish one operation and cached min
        table.startOperation(1, 1);
        table.snapshotAtomicOperationTableState(100);

        var snapHolder = new AtomicOperationsTable.AtomicOperationsSnapshot[1];

        // Thread 1 starts a new operation, Thread 2 takes a snapshot
        runConcurrently(
            () -> table.startOperation(2, 2),
            () -> snapHolder[0] = table.snapshotAtomicOperationTableState(100));

        verifySnapshotConsistency(snapHolder[0]);

        // After both threads: both ops should be visible
        var snapAfter = table.snapshotAtomicOperationTableState(100);
        verifySnapshotConsistency(snapAfter);
        assertEquals(2, snapAfter.inProgressTxs().size());
      }
    }
  }

  /**
   * One thread starts an operation while another commits a different one.
   * Every interleaving must produce a self-consistent snapshot.
   */
  @Test
  public void concurrentStartAndCommitShouldProduceConsistentSnapshot()
      throws Exception {
    try (var allInterleavings =
        allInterleavings("concurrentStartAndCommitShouldProduceConsistentSnapshot")) {
      while (allInterleavings.hasNext()) {
        var table = new AtomicOperationsTable(100, 1);
        // Pre-start two operations
        table.startOperation(1, 1);
        table.startOperation(2, 2);
        // Establish cached min
        table.snapshotAtomicOperationTableState(100);

        // Thread 1 commits ts=1 (the min), Thread 2 starts ts=3
        runConcurrently(
            () -> table.commitOperation(1),
            () -> table.startOperation(3, 3));

        var snap = table.snapshotAtomicOperationTableState(100);
        verifySnapshotConsistency(snap);
        // ts=1 is committed, ts=2 and ts=3 should be in progress
        assertTrue(snap.inProgressTxs().contains(2));
        assertTrue(snap.inProgressTxs().contains(3));
      }
    }
  }

  /**
   * One thread rolls back the min while another takes a snapshot. The snapshot
   * must be self-consistent regardless of the interleaving.
   */
  @Test
  public void concurrentRollbackMinWithSnapshotShouldBeConsistent()
      throws Exception {
    try (var allInterleavings =
        allInterleavings("concurrentRollbackMinWithSnapshotShouldBeConsistent")) {
      while (allInterleavings.hasNext()) {
        var table = new AtomicOperationsTable(100, 1);
        table.startOperation(1, 1);
        table.startOperation(2, 2);
        table.startOperation(3, 3);
        table.snapshotAtomicOperationTableState(100);

        var snapHolder = new AtomicOperationsTable.AtomicOperationsSnapshot[1];

        runConcurrently(
            () -> table.rollbackOperation(1),
            () -> snapHolder[0] = table.snapshotAtomicOperationTableState(100));

        verifySnapshotConsistency(snapHolder[0]);
      }
    }
  }

  /**
   * One thread commits the min (triggering a forward scan for the new min)
   * while another thread takes a snapshot. Every interleaving must produce
   * a self-consistent snapshot.
   */
  @Test
  public void concurrentCommitMinWithSnapshotShouldBeConsistent()
      throws Exception {
    try (var allInterleavings =
        allInterleavings("concurrentCommitMinWithSnapshotShouldBeConsistent")) {
      while (allInterleavings.hasNext()) {
        var table = new AtomicOperationsTable(100, 1);
        table.startOperation(1, 1);
        table.startOperation(2, 2);
        table.startOperation(3, 3);
        // Establish cached min
        table.snapshotAtomicOperationTableState(100);

        var snapHolder = new AtomicOperationsTable.AtomicOperationsSnapshot[1];

        // Thread 1 commits ts=1 (triggers forward scan for new min)
        // Thread 2 takes a snapshot concurrently
        runConcurrently(
            () -> table.commitOperation(1),
            () -> snapHolder[0] = table.snapshotAtomicOperationTableState(100));

        // The concurrent snapshot must be self-consistent
        verifySnapshotConsistency(snapHolder[0]);

        // A snapshot after both threads complete must also be consistent
        var snapAfter = table.snapshotAtomicOperationTableState(100);
        verifySnapshotConsistency(snapAfter);
        assertTrue(snapAfter.inProgressTxs().contains(2));
        assertTrue(snapAfter.inProgressTxs().contains(3));
      }
    }
  }

  /**
   * One thread commits the highest active operation while another starts a new
   * operation. Every interleaving must produce a self-consistent snapshot.
   */
  @Test
  public void concurrentCommitMaxWithStartShouldBeConsistent()
      throws Exception {
    try (var allInterleavings =
        allInterleavings("concurrentCommitMaxWithStartShouldBeConsistent")) {
      while (allInterleavings.hasNext()) {
        var table = new AtomicOperationsTable(100, 1);
        table.startOperation(1, 1);
        table.startOperation(3, 3);
        table.snapshotAtomicOperationTableState(100);

        // Thread 1 commits ts=3, Thread 2 starts ts=5
        runConcurrently(
            () -> table.commitOperation(3),
            () -> table.startOperation(5, 5));

        var snap = table.snapshotAtomicOperationTableState(100);
        verifySnapshotConsistency(snap);
        assertTrue(snap.inProgressTxs().contains(1));
        assertTrue(snap.inProgressTxs().contains(5));
      }
    }
  }

  /**
   * Commits the only active operation while another thread starts a new
   * operation. Every interleaving must produce a self-consistent snapshot.
   */
  @Test
  public void concurrentCommitSoleOpWithStartShouldBeConsistent()
      throws Exception {
    try (var allInterleavings =
        allInterleavings("concurrentCommitSoleOpWithStartShouldBeConsistent")) {
      while (allInterleavings.hasNext()) {
        var table = new AtomicOperationsTable(100, 1);
        table.startOperation(1, 1);
        table.snapshotAtomicOperationTableState(100);

        // Thread 1 commits the only op, Thread 2 starts a new one
        runConcurrently(
            () -> table.commitOperation(1),
            () -> table.startOperation(2, 2));

        var snap = table.snapshotAtomicOperationTableState(100);
        verifySnapshotConsistency(snap);
        // ts=1 committed, ts=2 should be in progress
        assertEquals(1, snap.inProgressTxs().size());
        assertTrue(snap.inProgressTxs().contains(2));
      }
    }
  }

  /**
   * Two threads take snapshots concurrently while operations are in progress.
   * Both snapshots must be self-consistent.
   */
  @Test
  public void concurrentSnapshotsShouldBothBeConsistent() throws Exception {
    try (var allInterleavings =
        allInterleavings("concurrentSnapshotsShouldBothBeConsistent")) {
      while (allInterleavings.hasNext()) {
        var table = new AtomicOperationsTable(100, 1);
        table.startOperation(1, 1);
        table.startOperation(3, 3);
        table.startOperation(5, 5);

        var snap1Holder = new AtomicOperationsTable.AtomicOperationsSnapshot[1];
        var snap2Holder = new AtomicOperationsTable.AtomicOperationsSnapshot[1];

        runConcurrently(
            () -> snap1Holder[0] = table.snapshotAtomicOperationTableState(100),
            () -> snap2Holder[0] = table.snapshotAtomicOperationTableState(100));

        verifySnapshotConsistency(snap1Holder[0]);
        verifySnapshotConsistency(snap2Holder[0]);
        assertEquals(3, snap1Holder[0].inProgressTxs().size());
        assertEquals(3, snap2Holder[0].inProgressTxs().size());
      }
    }
  }

  /**
   * One thread commits a non-boundary operation while another takes a snapshot.
   * The cached min should remain unchanged since the committed operation is not
   * the min. Every interleaving must produce a self-consistent snapshot.
   */
  @Test
  public void concurrentNonBoundaryCommitWithSnapshotShouldBeConsistent()
      throws Exception {
    try (var allInterleavings = allInterleavings(
        "concurrentNonBoundaryCommitWithSnapshotShouldBeConsistent")) {
      while (allInterleavings.hasNext()) {
        var table = new AtomicOperationsTable(100, 1);
        table.startOperation(1, 1);
        table.startOperation(3, 3);
        table.startOperation(5, 5);
        // Establish cached min via snapshot
        table.snapshotAtomicOperationTableState(100);

        var snapHolder = new AtomicOperationsTable.AtomicOperationsSnapshot[1];

        // Thread 1 commits middle op (ts=3), Thread 2 takes snapshot
        runConcurrently(
            () -> table.commitOperation(3),
            () -> snapHolder[0] = table.snapshotAtomicOperationTableState(100));

        verifySnapshotConsistency(snapHolder[0]);

        // After both threads: min=1, max=5, only ts=3 committed
        var snapAfter = table.snapshotAtomicOperationTableState(100);
        verifySnapshotConsistency(snapAfter);
        assertEquals(2, snapAfter.inProgressTxs().size());
        assertTrue(snapAfter.inProgressTxs().contains(1));
        assertTrue(snapAfter.inProgressTxs().contains(5));
      }
    }
  }

  /**
   * Regression test: verifies that a snapshot never reports a
   * {@code maxActiveOperationTs} lower than any actually-found IN_PROGRESS
   * entry. Before the fix, the snapshot scan used a cached upper bound
   * ({@code cachedMaxActiveTs}) that could be stale when a new operation was
   * concurrently started. The scan would stop too early and produce a snapshot
   * with a {@code maxActiveOperationTs} that excluded the just-started entry.
   * This caused committed operations between the stale max and the true max
   * to be erroneously visible, violating Snapshot Isolation.
   *
   * <p>The test starts two operations, establishes cached scan bounds, then
   * concurrently starts a third operation (beyond the cached max) while
   * taking a snapshot. The snapshot's {@code maxActiveOperationTs} must be
   * at least as high as every entry in its {@code inProgressTxs} set.
   */
  @Test
  public void snapshotShouldNeverMissStartBeyondCachedMax()
      throws Exception {
    try (var allInterleavings =
        allInterleavings("snapshotShouldNeverMissStartBeyondCachedMax")) {
      while (allInterleavings.hasNext()) {
        var table = new AtomicOperationsTable(100, 1);
        // Pre-establish two operations and populate the cached min bound.
        table.startOperation(1, 1);
        table.startOperation(2, 2);
        table.snapshotAtomicOperationTableState(100);

        var snapHolder = new AtomicOperationsTable.AtomicOperationsSnapshot[1];

        // Thread 1 starts ts=3 (beyond the previously scanned max=2).
        // Thread 2 takes a snapshot concurrently.
        runConcurrently(
            () -> table.startOperation(3, 3),
            () -> snapHolder[0] = table.snapshotAtomicOperationTableState(100));

        // The concurrent snapshot must be self-consistent: min/max must
        // match the actual bounds of inProgressTxs.
        verifySnapshotConsistency(snapHolder[0]);

        // Stronger check: maxActiveOperationTs must be >= every entry.
        // With the old cachedMax-based scan, certain interleavings would
        // produce a snapshot with max=2 even though ts=3 was IN_PROGRESS,
        // because the scan stopped at the stale cachedMax=2.
        var snap = snapHolder[0];
        for (var it = snap.inProgressTxs().iterator(); it.hasNext();) {
          var ts = it.nextLong();
          assertTrue(ts <= snap.maxActiveOperationTs(),
              "snapshot max (" + snap.maxActiveOperationTs()
                  + ") must be >= every in-progress entry (" + ts + ")");
          assertTrue(ts >= snap.minActiveOperationTs(),
              "snapshot min (" + snap.minActiveOperationTs()
                  + ") must be <= every in-progress entry (" + ts + ")");
        }

        // After both threads complete, all three operations must be visible.
        var snapAfter = table.snapshotAtomicOperationTableState(100);
        verifySnapshotConsistency(snapAfter);
        assertEquals(3, snapAfter.inProgressTxs().size());
        assertTrue(snapAfter.inProgressTxs().contains(1));
        assertTrue(snapAfter.inProgressTxs().contains(2));
        assertTrue(snapAfter.inProgressTxs().contains(3));
      }
    }
  }

  /**
   * Persistence, snapshot, and compaction operations remain safe when retained entries span
   * separate ranges.
   */
  @Test
  public void concurrentPersistenceSnapshotAndCompactionAcrossRangesShouldBeConsistent()
      throws Exception {
    try (var allInterleavings = allInterleavings(
        "concurrentPersistenceSnapshotAndCompactionAcrossRangesShouldBeConsistent")) {
      while (allInterleavings.hasNext()) {
        var table = new AtomicOperationsTable(100, 1, 2);
        table.startOperation(1, 1);
        table.startOperation(2, 2);
        table.startOperation(1L << 32, 3);
        table.commitOperation(1);

        var snapshotHolder = new AtomicOperationsTable.AtomicOperationsSnapshot[1];
        runConcurrently(
            () -> table.persistOperation(1),
            () -> snapshotHolder[0] = table.snapshotAtomicOperationTableState(Long.MAX_VALUE),
            table::compactTable);

        verifySnapshotConsistency(snapshotHolder[0]);
        var finalSnapshot = table.snapshotAtomicOperationTableState(Long.MAX_VALUE);
        assertTrue(finalSnapshot.inProgressTxs().contains(2));
        assertTrue(finalSnapshot.inProgressTxs().contains(1L << 32));
        assertEquals(2, finalSnapshot.inProgressTxs().size());
      }
    }
  }

  /**
   * A far identifier gap makes one thread create and publish a new range descriptor, while
   * another thread takes a snapshot.
   *
   * <p>The operation manager serializes registration under its own segment lock, so exactly one
   * thread registers an operation here. The snapshot thread runs the same descending scan that a
   * reader runs after that handshake.
   *
   * <p>The snapshot thread must never observe a partly built descriptor. A descriptor published
   * before its first entry would make the snapshot thread read an entry slot that holds no value,
   * which either raises an exception inside that thread or blocks it. The concurrent helper turns
   * both outcomes into a test failure. The assertions also require the two earlier operations to
   * stay visible in the concurrent snapshot, and all three operations to be visible afterwards.
   */
  @Test
  public void concurrentFarGapRangePublicationWithSnapshotShouldBeConsistent() throws Exception {
    try (var allInterleavings = allInterleavings(
        "concurrentFarGapRangePublicationWithSnapshotShouldBeConsistent")) {
      while (allInterleavings.hasNext()) {
        var table = new AtomicOperationsTable(100, 1);
        var farGapTs = 1L << 32;
        table.startOperation(1, 1);
        table.startOperation(2, 2);
        // This snapshot establishes the cached scan boundary at identifier 1.
        table.snapshotAtomicOperationTableState(100);

        var snapshotHolder = new AtomicOperationsTable.AtomicOperationsSnapshot[1];
        runConcurrently(
            () -> table.startOperation(farGapTs, 3),
            () -> snapshotHolder[0] = table.snapshotAtomicOperationTableState(Long.MAX_VALUE));

        var concurrentSnapshot = snapshotHolder[0];
        verifySnapshotConsistency(concurrentSnapshot);
        assertTrue(concurrentSnapshot.inProgressTxs().contains(1),
            "the concurrent snapshot must keep operation 1 visible");
        assertTrue(concurrentSnapshot.inProgressTxs().contains(2),
            "the concurrent snapshot must keep operation 2 visible");

        var snapshotAfter = table.snapshotAtomicOperationTableState(Long.MAX_VALUE);
        verifySnapshotConsistency(snapshotAfter);
        assertEquals(3, snapshotAfter.inProgressTxs().size());
        assertTrue(snapshotAfter.inProgressTxs().contains(farGapTs),
            "the far gap operation must be visible after both threads finish");
        assertEquals(2, table.rangeCount(), "the far gap must create a second range");
      }
    }
  }

  /**
   * A full range makes one thread publish a new descriptor through the capacity split, while a
   * second thread takes a snapshot and a third thread changes the status of an older entry.
   *
   * <p>The test capacity of two entries fills the first range with identifiers 1 and 2, so the
   * registration of identifier 3 must create a second range. Registration stays on one thread,
   * which matches the serialized registration of the operation manager. The older entry
   * transition commits identifier 1, which also moves the cached scan boundary while the split
   * happens.
   *
   * <p>Operation 2 stays in progress during the whole run, so every snapshot must report it.
   * A descriptor published before its first entry, or a lost cached boundary repair, would raise
   * an exception in a worker thread, block a worker thread, or drop operation 2 from a snapshot.
   */
  @Test
  public void concurrentCapacitySplitWithSnapshotAndOlderTransitionShouldBeConsistent()
      throws Exception {
    try (var allInterleavings = allInterleavings(
        "concurrentCapacitySplitWithSnapshotAndOlderTransitionShouldBeConsistent")) {
      while (allInterleavings.hasNext()) {
        // A capacity of two entries makes the first range full after two registrations.
        var table = new AtomicOperationsTable(100, 1, 2);
        table.startOperation(1, 1);
        table.startOperation(2, 2);
        table.snapshotAtomicOperationTableState(100);

        var snapshotHolder = new AtomicOperationsTable.AtomicOperationsSnapshot[1];
        runConcurrently(
            () -> table.startOperation(3, 3),
            () -> snapshotHolder[0] = table.snapshotAtomicOperationTableState(Long.MAX_VALUE),
            () -> table.commitOperation(1));

        verifySnapshotConsistency(snapshotHolder[0]);
        assertTrue(snapshotHolder[0].inProgressTxs().contains(2),
            "the concurrent snapshot must keep operation 2 visible");

        var snapshotAfter = table.snapshotAtomicOperationTableState(Long.MAX_VALUE);
        verifySnapshotConsistency(snapshotAfter);
        assertEquals(2, snapshotAfter.inProgressTxs().size());
        assertTrue(snapshotAfter.inProgressTxs().contains(2));
        assertTrue(snapshotAfter.inProgressTxs().contains(3));
        assertEquals(2, table.rangeCount(), "the capacity split must create a second range");
      }
    }
  }
}
