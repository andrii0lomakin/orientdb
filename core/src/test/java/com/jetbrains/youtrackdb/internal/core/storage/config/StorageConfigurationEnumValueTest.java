package com.jetbrains.youtrackdb.internal.core.storage.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB.LocalUserCredential;
import com.jetbrains.youtrackdb.api.YouTrackDB.PredefinedLocalRole;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.DbTestBase;
import com.jetbrains.youtrackdb.internal.LogRecordCollector;
import com.jetbrains.youtrackdb.internal.SequentialTest;
import com.jetbrains.youtrackdb.internal.common.io.FileUtils;
import com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrackdb.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrackdb.internal.core.exception.DatabaseException;
import com.jetbrains.youtrackdb.internal.core.sql.OrderByNullsUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Round-trip tests for the two enum-typed, storage-local null placement settings.
 *
 * <p>The value is written as its constant name when the storage closes and converted back by its
 * declared type when the storage loads. Enum-typed keys need their own conversion on that read path,
 * and a value that names no constant must not make the database unopenable. Such a value is skipped,
 * and the next clean close drops it from disk.
 *
 * <p>Marked {@code @Category(SequentialTest)} because the tests mutate process-wide placement
 * globals. Parallel execution could leak those mutations between classes.
 */
@Category(SequentialTest.class)
public class StorageConfigurationEnumValueTest {

  private static final GlobalConfiguration ASC_NULLS_KEY =
      GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC;
  private static final GlobalConfiguration DESC_NULLS_KEY =
      GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_DESC;

  /** A non-enum key used to prove the tolerant read stays bounded to enum-typed keys. */
  private static final GlobalConfiguration INT_KEY =
      GlobalConfiguration.QUERY_INDEX_ORDERED_MIN_LINKBAG;

  @Rule
  public TestName name = new TestName();

  private YouTrackDBImpl youTrackDB;
  private String databaseName;
  private Object previousAscending;
  private Object previousDescending;
  private Object previousInteger;

  @Before
  public void before() {
    previousAscending = ASC_NULLS_KEY.getValue();
    previousDescending = DESC_NULLS_KEY.getValue();
    previousInteger = INT_KEY.getValue();
    databaseName = name.getMethodName();
    youTrackDB = openContext();
    youTrackDB.create(
        databaseName,
        DatabaseType.DISK,
        new LocalUserCredential("admin", DbTestBase.ADMIN_PASSWORD, PredefinedLocalRole.ADMIN));
  }

  @After
  public void after() {
    ASC_NULLS_KEY.setValue(previousAscending);
    DESC_NULLS_KEY.setValue(previousDescending);
    INT_KEY.setValue(previousInteger);
    try {
      if (youTrackDB.isOpen() && youTrackDB.exists(databaseName)) {
        youTrackDB.drop(databaseName);
      }
    } catch (RuntimeException e) {
      // A deliberately corrupted database may be impossible to drop through the storage API.
    } finally {
      try {
        if (youTrackDB.isOpen()) {
          youTrackDB.close();
        }
      } finally {
        FileUtils.deleteRecursively(DbTestBase.getBaseDirectoryPath(getClass()).toFile());
      }
    }
  }

  /**
   * Regression test for the reopen blocker. Both enum values must survive one close and reopen
   * cycle without making the disk database unopenable.
   */
  @Test
  public void diskDatabaseReopensAfterBothPlacementValuesAreStored() {
    try (var session = youTrackDB.open(databaseName, "admin", DbTestBase.ADMIN_PASSWORD)) {
      var configuration = session.getStorage().getContextConfiguration();
      configuration.setValue(ASC_NULLS_KEY, OrderByNullsPlacement.LAST);
      configuration.setValue(DESC_NULLS_KEY, OrderByNullsPlacement.FIRST);
    }
    youTrackDB.close();

    reopenContext();

    var configuration = storageConfiguration();
    assertEquals(OrderByNullsPlacement.LAST, configuration.getValue(ASC_NULLS_KEY));
    assertEquals(OrderByNullsPlacement.FIRST, configuration.getValue(DESC_NULLS_KEY));
  }

  /**
   * A value persisted in lower case still names the constant, and a server property carries plain
   * text. The load path matches constant names while ignoring case, like the global setter does. The
   * storage therefore honours LAST rather than falling back without a word.
   */
  @Test
  public void storedLowerCaseValueIsAcceptedAndHonoured() {
    ASC_NULLS_KEY.setValue(OrderByNullsPlacement.FIRST);
    storeOnStorage(ASC_NULLS_KEY, "last");

    reopenContext();

    var configuration = storageConfiguration();
    assertEquals(OrderByNullsPlacement.LAST, configuration.getValue(ASC_NULLS_KEY));
    assertEquals(
        OrderByNullsPlacement.LAST, OrderByNullsUtil.resolvePlacements(configuration).ascending());
  }

  /**
   * A stored value that names no constant is skipped, and the database opens. The key is absent from
   * the storage configuration, so the runtime global stays in force. The global is set to
   * LAST here, so a fallback to the declared default would fail the assertion too. The
   * logged warning has to name the database, the key, the value and the consequence, because it is
   * the operator's only signal.
   */
  @Test
  public void storedInvalidValueIsReportedAndGlobalDefaultApplies() {
    ASC_NULLS_KEY.setValue(OrderByNullsPlacement.LAST);
    storeOnStorage(ASC_NULLS_KEY, "NOT_A_CONSTANT");

    reopenContext();

    ContextConfiguration configuration;
    try (var logs = LogRecordCollector.attachTo(CollectionBasedStorageConfiguration.class)) {
      configuration = storageConfiguration();
      assertTrue(
          "the skipped value must be reported, captured: " + logs.messages(),
          logs.warnedWithAll(
              databaseName, ASC_NULLS_KEY.getKey(), "NOT_A_CONSTANT", "global default applies"));
    }
    assertFalse(configuration.getContextKeys().contains(ASC_NULLS_KEY.getKey()));
    assertEquals(OrderByNullsPlacement.LAST,
        OrderByNullsUtil.resolvePlacements(configuration).ascending());
  }

  /**
   * A skipped value is forgotten at the next clean close. The store path writes only what the
   * effective configuration holds, and a skipped key is not part of it. The reopen after that close
   * is therefore silent, because there is no stored value left to report.
   */
  @Test
  public void storedInvalidValueIsForgottenAtTheNextCleanClose() {
    storeOnStorage(ASC_NULLS_KEY, "NOT_A_CONSTANT");

    reopenContext();
    loadStorage();
    // Clean close: this is the write-back that drops the key.
    youTrackDB.close();

    reopenContext();
    try (var logs = LogRecordCollector.attachTo(CollectionBasedStorageConfiguration.class)) {
      loadStorage();
      assertFalse(
          "the skipped value must be gone from disk, captured: " + logs.messages(),
          logs.warnedWithAll(ASC_NULLS_KEY.getKey(), "NOT_A_CONSTANT"));
    }
    assertFalse(storageConfiguration().getContextKeys().contains(ASC_NULLS_KEY.getKey()));
  }

  /**
   * Surrounding whitespace is not tolerated, because the global setter does not tolerate it either.
   * A padded value is treated as unreadable, so the global default applies.
   */
  @Test
  public void storedPaddedValueIsRejectedLikeTheGlobalSetter() {
    ASC_NULLS_KEY.setValue(OrderByNullsPlacement.FIRST);
    storeOnStorage(ASC_NULLS_KEY, " LAST ");

    reopenContext();

    var configuration = storageConfiguration();
    assertFalse(configuration.getContextKeys().contains(ASC_NULLS_KEY.getKey()));
    assertEquals(
        OrderByNullsPlacement.FIRST, OrderByNullsUtil.resolvePlacements(configuration).ascending());
  }

  /**
   * The tolerance is bounded to enum-typed keys. A damaged value of a non-enum key still fails the
   * open. The failure has to come from the configuration load, not from anything else the open does.
   * The open reports it as a refusal to open the database, with the load error as a cause.
   */
  @Test
  public void storedInvalidValueOfNonEnumKeyStillFailsTheOpen() {
    storeOnStorage(INT_KEY, "not-a-number");

    reopenContext();

    var failure =
        assertThrows(
            DatabaseException.class,
            () -> youTrackDB.open(databaseName, "admin", DbTestBase.ADMIN_PASSWORD).close());
    assertTrue(
        "the open must be refused, message was: " + failure.getMessage(),
        failure.getMessage().contains("Cannot open database"));
    assertTrue(
        "the configuration load must be the cause, chain was: " + causeMessages(failure),
        causeMessages(failure).contains("Can not load storage configuration"));
  }

  /** Joins the messages of a failure and of every cause below it, for one readable assertion. */
  private static String causeMessages(Throwable failure) {
    var text = new StringBuilder();
    for (Throwable current = failure; current != null; current = current.getCause()) {
      text.append(current.getMessage()).append(" | ");
    }
    return text.toString();
  }

  /**
   * Sets a value on the open storage's context configuration and closes the whole embedded context,
   * which is what flushes the configuration property to disk.
   */
  private void storeOnStorage(GlobalConfiguration key, Object value) {
    try (var session = youTrackDB.open(databaseName, "admin", DbTestBase.ADMIN_PASSWORD)) {
      session.getStorage().getContextConfiguration().setValue(key, value);
    }
    youTrackDB.close();
  }

  /** Reads the persisted configuration back through a freshly opened session. */
  private ContextConfiguration storageConfiguration() {
    try (var session = youTrackDB.open(databaseName, "admin", DbTestBase.ADMIN_PASSWORD)) {
      return session.getStorage().getContextConfiguration();
    }
  }

  /** Opens and closes a session, which makes the current context load the storage. */
  private void loadStorage() {
    youTrackDB.open(databaseName, "admin", DbTestBase.ADMIN_PASSWORD).close();
  }

  private void reopenContext() {
    youTrackDB = openContext();
  }

  private YouTrackDBImpl openContext() {
    return (YouTrackDBImpl) YourTracks.instance(DbTestBase.getBaseDirectoryPath(getClass()));
  }
}
