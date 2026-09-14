package com.jetbrains.youtrackdb.internal.core.storage.disk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.internal.common.io.FileUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers the cleanup policy of the prepared copies of one restore request.
 *
 * <p>One request owns one prepared chain. The close of that chain removes every prepared copy and
 * the temporary directory of the request. A failed removal never decides the outcome of the
 * request, because leftover temporary files change no database.
 *
 * <p>Each test of this class states one scenario and one expected outcome in its own comment.
 */
public class PreparedBackupChainTest {

  private static final String DATABASE = "preparedChainDatabase";

  private Path root;

  @Before
  public void createRoot() throws Exception {
    root = Files.createTempDirectory("prepared-chain-");
  }

  @After
  public void deleteRoot() {
    FileUtils.deleteRecursively(root.toFile());
  }

  /**
   * One close of a finished request removes every prepared copy.
   *
   * <p>The scenario prepares one temporary directory with one copy and closes the chain. The
   * expected outcome has two parts. The close reports no failure. The temporary directory leaves.
   */
  @Test
  public void closeRemovesEveryPreparedCopy() throws Exception {
    var temporaryDirectory = Files.createDirectories(root.resolve("request"));
    Files.writeString(temporaryDirectory.resolve("unit.ibu"), "prepared copy");
    var chain = new PreparedBackupChain(DATABASE, temporaryDirectory);

    chain.close();

    assertFalse("the close must remove the temporary directory",
        Files.exists(temporaryDirectory));
  }

  /**
   * One failed cleanup after a successful request reports no failure to the caller.
   *
   * <p>A successful restore already activated its target when the cleanup runs. A cleanup failure
   * therefore stays a warning of the log, and the restore keeps its success. The scenario removes
   * the temporary directory of the request from outside, which fails the later cleanup. The
   * expected outcome is one close without any failure.
   */
  @Test
  public void failedCleanupAfterSuccessReportsNoFailure() throws Exception {
    var temporaryDirectory = Files.createDirectories(root.resolve("request"));
    var chain = new PreparedBackupChain(DATABASE, temporaryDirectory);
    var warning = new AtomicReference<LogRecord>();
    var logger = Logger.getLogger(PreparedBackupChain.class.getName());
    var previousLevel = logger.getLevel();
    var expectedPath = temporaryDirectory.toAbsolutePath().toString();
    var handler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        if (record.getLevel().intValue() >= Level.WARNING.intValue()
            && record.getMessage().contains(expectedPath)) {
          warning.set(record);
        }
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    handler.setLevel(Level.ALL);
    logger.setLevel(Level.ALL);
    logger.addHandler(handler);
    // The removal from outside fails the cleanup of the request, because the cleanup then finds
    // no directory of its own.
    Files.delete(temporaryDirectory);

    try {
      chain.close();
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
    }

    assertFalse("the temporary directory must stay absent", Files.exists(temporaryDirectory));
    assertNotNull("the failed cleanup must report one warning", warning.get());
    assertTrue("the warning must name the temporary directory, saw: " + warning.get().getMessage(),
        warning.get().getMessage().contains(temporaryDirectory.toAbsolutePath().toString()));
  }

  /**
   * One failed cleanup of a failed request keeps the primary failure primary.
   *
   * <p>The request reports its own failure, and the cleanup failure joins that failure as one
   * suppressed failure. The scenario removes the temporary directory from outside and then closes
   * the chain behind one primary failure.
   *
   * <p>The expected outcome has three parts. The primary
   * failure stays unchanged. The primary failure carries one suppressed cleanup failure. That
   * suppressed failure names the temporary directory.
   */
  @Test
  public void failedCleanupOfAFailedRequestStaysSuppressed() throws Exception {
    var temporaryDirectory = Files.createDirectories(root.resolve("request"));
    var chain = new PreparedBackupChain(DATABASE, temporaryDirectory);
    var primaryFailure = new IllegalStateException("the primary failure of this request");
    Files.delete(temporaryDirectory);

    chain.closeSuppressing(primaryFailure);

    assertEquals("the primary failure must keep its message",
        "the primary failure of this request", primaryFailure.getMessage());
    assertEquals("the cleanup failure must join the primary failure",
        1, primaryFailure.getSuppressed().length);
    assertTrue(
        "the suppressed failure must name the temporary directory, saw: "
            + primaryFailure.getSuppressed()[0].getMessage(),
        primaryFailure.getSuppressed()[0].getMessage()
            .contains(temporaryDirectory.toAbsolutePath().toString()));
  }

  /**
   * One second close of one chain removes nothing and reports nothing.
   *
   * <p>The scenario closes one chain twice. The expected outcome is one silent second close, so
   * an owner that closes twice never reports a failure of an absent directory.
   */
  @Test
  public void secondCloseReportsNoFailure() throws Exception {
    var temporaryDirectory = Files.createDirectories(root.resolve("request"));
    var chain = new PreparedBackupChain(DATABASE, temporaryDirectory);

    chain.close();
    chain.close();

    assertFalse("the temporary directory must stay absent", Files.exists(temporaryDirectory));
  }

  /**
   * One replay of a closed chain reports the closed state.
   *
   * <p>The completeness check of one replay is defense in depth. The scenario closes one complete
   * chain and then requests a replay of that chain. The expected outcome is one refusal that
   * names the closed chain.
   */
  @Test
  public void replayOfAClosedChainIsRefused() throws Exception {
    var temporaryDirectory = Files.createDirectories(root.resolve("request"));
    var chain = new PreparedBackupChain(DATABASE, temporaryDirectory);
    chain.completePreparation(7);
    chain.close();

    var refusal = assertThrows(IllegalStateException.class, chain::requireComplete);

    assertTrue("the refusal must name the closed chain, saw: " + refusal.getMessage(),
        refusal.getMessage().contains("closed already"));
  }

  /**
   * One replay of an unvalidated chain reports the missing validation.
   *
   * <p>The scenario requests a replay of one chain without any complete preparation. The expected
   * outcome is one refusal that names the missing validation.
   */
  @Test
  public void replayOfAnUnvalidatedChainIsRefused() throws Exception {
    var temporaryDirectory = Files.createDirectories(root.resolve("request"));
    var chain = new PreparedBackupChain(DATABASE, temporaryDirectory);

    var refusal = assertThrows(IllegalStateException.class, chain::requireComplete);

    assertTrue("the refusal must name the missing validation, saw: " + refusal.getMessage(),
        refusal.getMessage().contains("no complete validation"));
  }

  /**
   * One prepared chain reports its units in the order of the chain.
   *
   * <p>The scenario adds one full backup and one increment to the chain.
   *
   * <p>The expected outcome has
   * three parts. The unit list keeps the order of the chain. The full-backup flag of each unit
   * survives. The last transaction identifier of the chain reaches the owner.
   */
  @Test
  public void preparedChainReportsItsUnitsInChainOrder() throws Exception {
    var temporaryDirectory = Files.createDirectories(root.resolve("request"));
    var chain = new PreparedBackupChain(DATABASE, temporaryDirectory);
    var fullBackup = temporaryDirectory.resolve("full.ibu");
    var increment = temporaryDirectory.resolve("increment.ibu");

    chain.addUnit(fullBackup, true);
    chain.addUnit(increment, false);
    chain.completePreparation(11);

    assertEquals("the chain must keep two units", 2, chain.units().size());
    assertSame("the full backup must open the chain", fullBackup, chain.units().getFirst().left());
    assertTrue("the first unit must carry the full-backup flag",
        chain.units().getFirst().rightBoolean());
    assertFalse("the second unit must carry no full-backup flag",
        chain.units().get(1).rightBoolean());
    assertEquals("the chain must report its last transaction identifier", 11, chain.lastTxId());
  }
}
