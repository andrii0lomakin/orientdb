package com.jetbrains.youtrackdb.internal.core.gremlin;

import com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression tests for {@link YTDBTransaction#executeInTX} and
 * {@link YTDBTransaction#computeInTx} — specifically, the behavior of the
 * internal {@code finishTx} helper that wraps commit/rollback.
 *
 * <p>Historically {@code finishTx} caught every exception from
 * {@code tx.commit()} and only logged it, so commit-time failures (e.g.
 * {@link ConcurrentModificationException} from MVCC) were silently swallowed
 * and never reached the caller. That made retry loops in driver code
 * (e.g. the YCSB YQL client) ineffective. These tests lock in the corrected
 * behavior: commit-time exceptions propagate, body exceptions still
 * propagate with the transaction rolled back, and the happy path still
 * commits.
 */
public class YTDBTransactionFinishTxTest extends GraphBaseTest {

  @Before
  @Override
  public void setupGraphDB() {
    super.setupGraphDB();
    session.command("CREATE CLASS Person EXTENDS V");
    session.command("CREATE PROPERTY Person.name STRING");
  }

  /**
   * Happy path: {@code executeInTx} commits the body's changes.
   */
  @Test
  public void executeInTxCommitsOnSuccess() {
    graph.executeInTx(g -> g.addV("Person").property("name", "alice").iterate());

    session.begin();
    try {
      Assert.assertEquals(1, session.countClass("Person"));
    } finally {
      session.rollback();
    }
  }

  /**
   * A {@link RuntimeException} thrown from the body must propagate to the
   * caller and the transaction must be rolled back, leaving no changes.
   */
  @Test
  public void executeInTxRollsBackAndRethrowsBodyException() {
    var thrown = Assert.assertThrows(IllegalStateException.class, () -> graph.executeInTx(g -> {
      g.addV("Person").property("name", "doomed").iterate();
      throw new IllegalStateException("body boom");
    }));
    Assert.assertEquals("body boom", thrown.getMessage());

    session.begin();
    try {
      Assert.assertEquals(0, session.countClass("Person"));
    } finally {
      session.rollback();
    }
  }

  /**
   * Regression: a {@link ConcurrentModificationException} raised at commit
   * time must propagate to the caller of {@code executeInTx}. Without this,
   * retry loops built on top of {@code executeInTx} cannot detect MVCC
   * conflicts and silently drop the update.
   */
  @Test
  public void executeInTxPropagatesCommitCME() throws InterruptedException {
    createPersonAndCommit("original");

    var cme = runWithConcurrentBump(waitForBump -> graph.executeInTx(g -> {
      var v = (Vertex) g.V().hasLabel("Person").next();
      v.property("name", "main");
      waitForBump.run();
    }));

    Assert.assertNotNull("commit-time CME did not propagate from executeInTx", cme);
  }

  /**
   * Happy path: {@code computeInTx} commits the body's changes and returns
   * the computed value.
   */
  @Test
  public void computeInTxCommitsAndReturnsValue() {
    var returned = graph.computeInTx(g -> {
      g.addV("Person").property("name", "alice").iterate();
      return "result";
    });
    Assert.assertEquals("result", returned);

    session.begin();
    try {
      Assert.assertEquals(1, session.countClass("Person"));
    } finally {
      session.rollback();
    }
  }

  /**
   * Body exception from {@code computeInTx} propagates; no changes persist.
   */
  @Test
  public void computeInTxRollsBackAndRethrowsBodyException() {
    var thrown = Assert.assertThrows(IllegalStateException.class, () -> graph.computeInTx(g -> {
      g.addV("Person").property("name", "doomed").iterate();
      throw new IllegalStateException("body boom");
    }));
    Assert.assertEquals("body boom", thrown.getMessage());

    session.begin();
    try {
      Assert.assertEquals(0, session.countClass("Person"));
    } finally {
      session.rollback();
    }
  }

  /**
   * Regression: commit-time CME must also propagate out of
   * {@code computeInTx}, not be silently swallowed.
   */
  @Test
  public void computeInTxPropagatesCommitCME() throws InterruptedException {
    createPersonAndCommit("original");

    var cme = runWithConcurrentBump(waitForBump -> {
      @SuppressWarnings("unused")
      var ignored = graph.computeInTx(g -> {
        var v = (Vertex) g.V().hasLabel("Person").next();
        v.property("name", "main");
        waitForBump.run();
        return "unused";
      });
    });

    Assert.assertNotNull("commit-time CME did not propagate from computeInTx", cme);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Creates a {@code Person} vertex with the given {@code name} and commits.
   */
  private void createPersonAndCommit(String name) {
    graph.addVertex(T.label, "Person", "name", name);
    graph.tx().commit();
  }

  /**
   * Runs {@code body} on the current thread, but gives it access to a
   * {@code waitForBump} hook. The hook signals a background thread that
   * concurrently updates the {@code Person} record via an independent
   * {@code DatabaseSessionEmbedded} and waits for that concurrent commit to
   * finish before returning. After {@code body} returns, its surrounding
   * {@code executeInTx}/{@code computeInTx} will attempt to commit a stale
   * version and — with the finishTx fix — a
   * {@link ConcurrentModificationException} must propagate out. This helper
   * captures and returns that exception, or {@code null} if no CME was seen.
   *
   * <p>The concurrent update runs on a separate thread because YTDB sessions
   * are activated per-thread; modifying from the main thread while the graph
   * transaction is in flight would steal the active session and trip the
   * {@code assertIfNotActive} guard inside commit.
   *
   * <p>It uses a fresh {@code DatabaseSessionEmbedded} rather than a second
   * graph because closing a {@link YTDBGraph} also closes the shared cached
   * {@code SessionPool}, which would invalidate the main thread's session.
   */
  private ConcurrentModificationException runWithConcurrentBump(
      Consumer<Runnable> body) throws InterruptedException {
    var readyToBump = new CountDownLatch(1);
    var bumpDone = new CountDownLatch(1);
    var bumperError = new AtomicReference<Throwable>();

    var bumper = new Thread(() -> {
      try {
        if (!readyToBump.await(30, TimeUnit.SECONDS)) {
          bumperError.set(new AssertionError("body never signaled bumper"));
          return;
        }
        var s = openDatabase();
        try {
          s.activateOnCurrentThread();
          s.begin();
          s.command("UPDATE Person SET name = 'concurrent'");
          s.commit();
        } finally {
          s.close();
        }
      } catch (Throwable t) {
        bumperError.set(t);
      } finally {
        bumpDone.countDown();
      }
    }, "finishTx-bumper");

    Runnable waitForBump = () -> {
      readyToBump.countDown();
      try {
        Assert.assertTrue("bumper did not finish within timeout",
            bumpDone.await(30, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    };

    ConcurrentModificationException captured = null;
    try {
      bumper.start();
      body.accept(waitForBump);
    } catch (ConcurrentModificationException e) {
      captured = e;
    } finally {
      bumper.join(TimeUnit.SECONDS.toMillis(5));
      Assert.assertNull("concurrent bumper failed: " + bumperError.get(),
          bumperError.get());
    }
    return captured;
  }
}
