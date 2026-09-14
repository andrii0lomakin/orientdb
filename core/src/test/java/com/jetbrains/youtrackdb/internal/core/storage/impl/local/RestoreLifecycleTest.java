package com.jetbrains.youtrackdb.internal.core.storage.impl.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.internal.common.io.FileUtils;
import com.jetbrains.youtrackdb.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseLifecycleListener;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.SharedContext;
import com.jetbrains.youtrackdb.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrackdb.internal.core.db.YouTrackDBInternalEmbedded;
import com.jetbrains.youtrackdb.internal.core.storage.config.CollectionBasedStorageConfiguration;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.FeatureFormatIdentity;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageAdmissionException;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageBootstrapMetadata;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageIdentity;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageLineageIdentity;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.apache.commons.configuration2.BaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers the restore order and the destructive restart entry of Track 24.
 *
 * <p>Restore creates the restore target without genesis and without activation. Genesis is the
 * creation of the initial database metadata. Genesis creates the schema, the index manager, the
 * index statistics, and the default users. A destructive restart is the full deletion of an
 * interrupted restore target and a fresh restore of the same backup.
 *
 * <p>Storage admission is the decision that accepts an existing storage image for use. Admission
 * reports one named reason for every rejected image. The lifecycle state of a restore target is
 * therefore observable through the admission reason of that target.
 *
 * <p>Each test of this class states one scenario and one expected outcome in its own comment.
 */
public class RestoreLifecycleTest {

  private static final String ADMIN = "admin";
  private static final String PASSWORD = "adminpwd";
  private static final String RECORD_CLASS = "RestoredClass";
  private static final String SOURCE = "restoreSource";
  private static final String UNUSABLE_SOURCE = "unusableRestoreSource";
  private static final String TARGET = "restoreTarget";
  private static final FeatureFormatIdentity FEATURE_FORMAT = new FeatureFormatIdentity(1);

  private Path root;
  private Path databasesPath;
  private Path backupPath;
  private Path emptyBackupPath;
  private Path unusableBackupPath;
  private boolean unusableBackupCreated;

  @Before
  public void createDirectories() throws Exception {
    root = Files.createTempDirectory("restore-lifecycle-");
    databasesPath = Files.createDirectories(root.resolve("databases"));
    backupPath = Files.createDirectories(root.resolve("backup"));
    emptyBackupPath = Files.createDirectories(root.resolve("empty-backup"));
    unusableBackupPath = Files.createDirectories(root.resolve("unusable-backup"));
  }

  @After
  public void deleteDirectories() {
    FileUtils.deleteRecursively(root.toFile());
  }

  /**
   * No restore target exists while the restore reads its source, and the finished target is
   * active.
   *
   * <p>The restore prepares and validates the complete backup chain before any change of the
   * target. The scenario probes the target directory from inside the read of every backup file.
   * The expected outcome has three parts. No probe sees a target directory, which proves that
   * every source read precedes the target creation. The finished restore leaves an accepted image,
   * which proves the active lifecycle state. The restored database opens and carries the content
   * of the backup.
   */
  @Test
  public void restoreCreatesNoTargetBeforeSourceValidationAndActivatesAfterward()
      throws Exception {
    createSourceDatabaseAndBackup();
    var observedTargets = new ArrayList<Boolean>();

    try (var youTrackDB = openManager()) {
      internalOf(youTrackDB).restore(
          TARGET,
          this::backupFileNames,
          fileName -> {
            observedTargets.add(Files.exists(databasesPath.resolve(TARGET)));
            return openBackupFile(fileName);
          },
          null,
          null);

      assertFalse("the restore must read at least one backup file", observedTargets.isEmpty());
      for (var targetExists : observedTargets) {
        assertFalse(
            "no source read may follow the creation of the restore target", targetExists);
      }
      assertNull(
          "the finished restore must publish the active lifecycle state",
          admissionReasonOf(TARGET));
    }

    try (var youTrackDB = openManager();
        var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
      assertTrue(
          "the restored database must carry the content of the backup",
          session.getMetadata().getSchema().existsClass(RECORD_CLASS));
    }
  }

  /**
   * A restore without any backup content creates no target at all.
   *
   * <p>The chain validation of the restore refuses an empty source before any target change. The
   * scenario restores from an empty backup directory. The expected outcome has two parts. The
   * restore reports a failure. No database and no directory of the target name exist afterwards.
   */
  @Test
  public void restoreWithoutBackupContentCreatesNoTarget() throws Exception {
    try (var youTrackDB = openManager()) {
      assertThrows(
          RuntimeException.class,
          () -> internalOf(youTrackDB).restore(TARGET, emptyBackupPath.toString(), null, null));

      assertFalse("the refused restore must create no database", youTrackDB.exists(TARGET));
    }
    assertFalse(
        "the refused restore must create no directory",
        Files.exists(databasesPath.resolve(TARGET)));
  }

  /**
   * A restore of a truncated source creates no target at all.
   *
   * <p>A truncated unit holds no valid hash code, so the chain validation refuses that unit before
   * any target change. The scenario truncates the stream of every backup file. The expected
   * outcome has two parts. The restore reports a failure. No directory of the target name exists
   * afterwards.
   */
  @Test
  public void restoreOfTruncatedSourceCreatesNoTarget() throws Exception {
    createSourceDatabaseAndBackup();

    try (var youTrackDB = openManager()) {
      assertThrows(
          RuntimeException.class,
          () -> internalOf(youTrackDB).restore(
              TARGET,
              this::backupFileNames,
              this::openTruncatedBackupFile,
              null,
              null));

      assertFalse("the refused restore must create no database", youTrackDB.exists(TARGET));
    }
    assertFalse(
        "the refused restore must create no directory",
        Files.exists(databasesPath.resolve(TARGET)));
  }

  /**
   * A failure inside the replay leaves the target in the restore-in-progress state.
   *
   * <p>The scenario restores one admitted chain whose content carries an unsupported storage
   * layout version. The chain passes admission, so the replay starts and then fails.
   *
   * <p>The expected outcome has four parts. The restore reports a failure. The target stays
   * visible and reports the interrupted-restore reason. An open of the target repeats that same
   * rejection. No temporary directory of the failed request survives, because the request owns
   * its prepared copies until the request ends.
   */
  @Test
  public void replayFailureLeavesRestoreInProgressTarget() throws Exception {
    var temporaryDirectoriesBefore = temporaryChainDirectories();

    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);

      assertEquals(
          "the failed replay must remove every prepared copy",
          temporaryDirectoriesBefore,
          temporaryChainDirectories());

      assertTrue("the interrupted restore target must stay visible", youTrackDB.exists(TARGET));
      assertEquals(
          StorageAdmissionException.Reason.INTERRUPTED_RESTORE, admissionReasonOf(TARGET));
      var openFailure =
          assertThrows(RuntimeException.class, () -> youTrackDB.open(TARGET, ADMIN, PASSWORD));
      assertEquals(
          "an open must reject the interrupted restore target",
          StorageAdmissionException.Reason.INTERRUPTED_RESTORE,
          admissionReason(openFailure));
    }
  }

  /**
   * A backup of a database without a finished creation never reaches any target.
   *
   * <p>The genesis marker is the durable property that records a finished genesis. Every backup
   * header carries the creation completion evidence of its source database. The scenario clears
   * that marker in the source database, takes a backup, and restores that backup.
   *
   * <p>The expected outcome has three parts. The restore names the missing creation completion
   * evidence. No directory of the target name exists afterwards. The source database keeps every
   * file.
   */
  @Test
  public void restoreOfBackupWithoutCreationEvidenceCreatesNoTarget() throws Exception {
    try (var youTrackDB = openManager()) {
      createSourceDatabase(youTrackDB);
      storageOf(youTrackDB, SOURCE).setProperty(SharedContext.GENESIS_COMPLETED_PROPERTY, "false");
      storageOf(youTrackDB, SOURCE).fullBackup(backupPath);
    }

    try (var youTrackDB = openManager()) {
      var failure =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB).restore(TARGET, backupPath.toString(), null, null));

      assertTrue(
          "the failure must name the missing creation completion evidence, saw: "
              + rootMessage(failure),
          rootMessage(failure).contains("no accepted creation completion evidence"));
      assertFalse("the refused restore must create no database", youTrackDB.exists(TARGET));
    }
    assertFalse(
        "the refused restore must create no directory",
        Files.exists(databasesPath.resolve(TARGET)));
    assertFalse(
        "the source database must keep every file",
        entryNames(databasesPath.resolve(SOURCE)).isEmpty());
  }

  /**
   * A backup of an unsupported storage layout version stays discardable.
   *
   * <p>The storage layout version is the version of the on-disk storage layout. The scenario
   * rewrites that version in the source database, takes a backup, and restores that backup. The
   * expected outcome has four parts. The restore reports a failure. The destructive restart
   * repeats that same failure and names the disagreeing storage layout versions, so the restart
   * alone cannot clear the target. The refused restart leaves the restore-in-progress state. A
   * drop discards the target and reports success.
   *
   * <p>The reachable refusal comes from the layout consistency check of the configuration load.
   * That load runs inside the restore, before the restore validation of the restored content.
   */
  @Test
  public void restoreOfUnsupportedStorageLayoutVersionStaysDiscardable() throws Exception {
    try (var youTrackDB = openManager()) {
      createSourceDatabase(youTrackDB);
      var storage = storageOf(youTrackDB, SOURCE);
      var configuration = (CollectionBasedStorageConfiguration) storage.configuration;
      storage.getAtomicOperationsManager().executeInsideAtomicOperation(
          atomicOperation -> configuration.updateVersionForTesting(atomicOperation, 23));
      storage.fullBackup(backupPath);
    }

    try (var youTrackDB = openManager()) {
      assertThrows(
          RuntimeException.class,
          () -> internalOf(youTrackDB).restore(TARGET, backupPath.toString(), null, null));
      assertEquals(
          StorageAdmissionException.Reason.INTERRUPTED_RESTORE, admissionReasonOf(TARGET));

      var restartFailure =
          assertThrows(
              "the destructive restart must repeat the failure of the unusable backup",
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore(TARGET, backupPath.toString(), null, null));
      assertTrue(
          "the restart must name the unsupported layout version of the backup, saw: "
              + rootMessage(restartFailure),
          rootMessage(restartFailure)
              .contains("The storage configuration reports storage layout version 23"));
      assertEquals(
          "the repeated restart must leave the restore-in-progress state",
          StorageAdmissionException.Reason.INTERRUPTED_RESTORE,
          admissionReasonOf(TARGET));

      youTrackDB.drop(TARGET);
      assertFalse("the drop must discard the unusable restore target", youTrackDB.exists(TARGET));
    }
  }

  /**
   * The destructive restart deletes an interrupted restore target and restores the backup.
   *
   * <p>The scenario interrupts one restore and then restarts that restore with a complete backup.
   * The expected outcome has two parts. The restart leaves an accepted image, which proves the
   * active lifecycle state. The restarted database opens and carries the content of the backup.
   */
  @Test
  public void restartDeletesInterruptedTargetAndRestoresTheBackup() throws Exception {
    createSourceDatabaseAndBackup();

    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);

      internalOf(youTrackDB).restartInterruptedRestore(TARGET, backupPath.toString(), null, null);

      assertNull("the restart must publish the active lifecycle state", admissionReasonOf(TARGET));
      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "the restarted database must carry the content of the backup",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * The destructive restart refuses a healthy database and deletes nothing.
   *
   * <p>The scenario restarts the restore of a healthy database. The expected outcome has two
   * parts. The restart names the reason for a target that is no interrupted restore. The healthy
   * database still opens and still carries its own content.
   */
  @Test
  public void restartRefusesHealthyDatabaseAndKeepsEveryFile() throws Exception {
    try (var youTrackDB = openManager()) {
      createSourceDatabase(youTrackDB);
      storageOf(youTrackDB, SOURCE).fullBackup(backupPath);

      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore(SOURCE, backupPath.toString(), null, null));

      assertEquals(
          "the restart must refuse an active target by name",
          StorageAdmissionException.Reason.RESTART_TARGET_NOT_RESTORE_IN_PROGRESS,
          admissionReason(refusal));
      try (var session = youTrackDB.open(SOURCE, ADMIN, PASSWORD)) {
        assertTrue(
            "the refused database must keep its own content",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * The destructive restart refuses an interrupted storage birth and deletes nothing.
   *
   * <p>The scenario publishes one birth record without any further creation step, which is the
   * residue of an interrupted creation. The expected outcome has two parts. The restart names the
   * interrupted-birth reason, which a drop tolerates. The bootstrap authority artifact survives
   * the refusal.
   */
  @Test
  public void restartRefusesInterruptedBirthTarget() throws Exception {
    createSourceDatabaseAndBackup();
    var birthDirectory = Files.createDirectories(databasesPath.resolve("birthResidue"));
    new StorageBootstrapMetadata(birthDirectory, FEATURE_FORMAT)
        .createBirth(StorageIdentity.random(), StorageLineageIdentity.random());

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore(
                      "birthResidue", backupPath.toString(), null, null));

      assertEquals(
          StorageAdmissionException.Reason.INTERRUPTED_BIRTH, admissionReason(refusal));
      assertFalse(
          "the refused image must keep its bootstrap authority artifact",
          bootstrapArtifactNames(birthDirectory).isEmpty());
    }
  }

  /**
   * The destructive restart accepts the residue of a crash inside an earlier restart deletion.
   *
   * <p>The deletion of a restart removes every content file first and every authority copy last,
   * and the deletion keeps the authority lock file. A crash in the tail of that deletion
   * therefore leaves one directory whose only entry is the authority lock file. The scenario
   * builds exactly that shape. The expected outcome has two parts. The restart accepts that
   * residue. The restarted database opens and carries the content of the backup.
   */
  @Test
  public void restartAcceptsResidueOfInterruptedDeletion() throws Exception {
    createSourceDatabaseAndBackup();

    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);
    }
    deleteEveryEntryExceptTheAuthorityLockFile(databasesPath.resolve(TARGET));

    try (var youTrackDB = openManager()) {
      internalOf(youTrackDB).restartInterruptedRestore(TARGET, backupPath.toString(), null, null);

      assertNull("the restart must publish the active lifecycle state", admissionReasonOf(TARGET));
      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "the restarted database must carry the content of the backup",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * The destructive restart refuses a directory without any bootstrap authority artifact.
   *
   * <p>A bootstrap authority record names one storage identity, one storage lineage, and one
   * lifecycle state. A database of an earlier format holds real content and no such record. The
   * scenario names such a directory in a restart. The expected outcome has three parts. The
   * restart refuses the target with the missing-record reason. Every content file survives the
   * refusal. The refusal creates no bootstrap authority artifact.
   */
  @Test
  public void restartRefusesDirectoryWithoutAuthorityRecordAndKeepsEveryFile() throws Exception {
    createSourceDatabaseAndBackup();
    var earlierFormatDirectory = Files.createDirectories(databasesPath.resolve("earlierFormat"));
    Files.writeString(earlierFormatDirectory.resolve("database.ocf"), "payload");
    Files.writeString(earlierFormatDirectory.resolve("config.bd"), "payload");

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore(
                      "earlierFormat", backupPath.toString(), null, null));

      assertEquals(
          "the restart must refuse a target without any readable authority record",
          StorageAdmissionException.Reason.AUTHORITY_MISSING,
          admissionReason(refusal));
    }

    assertEquals(
        "the refused directory must keep every file",
        List.of("config.bd", "database.ocf"),
        entryNames(earlierFormatDirectory));
  }

  /**
   * The destructive restart refuses a directory that holds content and one authority lock file.
   *
   * <p>The authority lock file is the durable file that gives mutual exclusion over the authority
   * copies. A directory that holds that lock file and content files carries no readable authority
   * record, so the directory can hold a database of an earlier format. The scenario names such a
   * directory in a restart. The expected outcome has two parts. The restart refuses the target
   * with the interrupted-birth reason. Every content file survives the refusal.
   */
  @Test
  public void restartRefusesContentWithLockFileAndKeepsEveryFile() throws Exception {
    createSourceDatabaseAndBackup();
    var residueDirectory = Files.createDirectories(databasesPath.resolve("lockedResidue"));
    Files.writeString(residueDirectory.resolve("database.ocf"), "payload");
    Files.createFile(residueDirectory.resolve("storage-bootstrap.bsml"));

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore("lockedResidue", backupPath.toString(), null, null));

      assertEquals(
          "the restart must refuse content without any readable authority record",
          StorageAdmissionException.Reason.INTERRUPTED_BIRTH,
          admissionReason(refusal));
    }

    assertEquals(
        "the refused directory must keep every file",
        List.of("database.ocf", "storage-bootstrap.bsml"),
        entryNames(residueDirectory));
  }

  /**
   * The destructive restart refuses a database name that escapes the databases directory.
   *
   * <p>The name of a single dot resolves to the databases directory itself. A deletion of that
   * target would destroy every database of the manager. The scenario names a single dot and a
   * double dot in two restarts. The expected outcome has two parts. Both restarts refuse the
   * name. The database directory of the source database survives both refusals.
   */
  @Test
  public void restartRefusesPathEscapingNameAndKeepsEveryDatabase() throws Exception {
    createSourceDatabaseAndBackup();
    var sourceEntriesBeforeRestart = entryNames(databasesPath.resolve(SOURCE));
    assertFalse("the source database must hold files", sourceEntriesBeforeRestart.isEmpty());

    try (var youTrackDB = openManager()) {
      for (var escapingName : List.of(".", "..")) {
        var refusal =
            assertThrows(
                "the restart must refuse the escaping name " + escapingName,
                RuntimeException.class,
                () -> internalOf(youTrackDB)
                    .restartInterruptedRestore(escapingName, backupPath.toString(), null, null));
        assertTrue(
            "the refusal must name the invalid database name, saw: " + rootMessage(refusal),
            rootMessage(refusal).contains("Invalid database name"));
      }
    }

    assertTrue("the databases directory must survive", Files.isDirectory(databasesPath));
    assertEquals(
        "the source database must keep every file",
        sourceEntriesBeforeRestart,
        entryNames(databasesPath.resolve(SOURCE)));
  }

  /**
   * The destructive restart refuses a reserved name prefix before any deletion.
   *
   * <p>The two reserved prefixes are the prefix for graph traversal names and the prefix for
   * server names. Both name checks of the restart use one lower-case rule, so an upper-case
   * spelling reaches the same refusal. The scenario names a directory with an upper-case reserved
   * prefix. The expected outcome has two parts. The restart refuses the name. The content file of
   * that directory survives the refusal.
   */
  @Test
  public void restartRefusesReservedNamePrefixBeforeAnyDeletion() throws Exception {
    createSourceDatabaseAndBackup();
    var reservedDirectory = Files.createDirectories(databasesPath.resolve("SERVERtarget"));
    Files.writeString(reservedDirectory.resolve("database.ocf"), "payload");

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore("SERVERtarget", backupPath.toString(), null, null));

      assertTrue(
          "the refusal must name the reserved prefix, saw: " + rootMessage(refusal),
          rootMessage(refusal).contains("server"));
    }

    assertEquals(
        "the refused directory must keep every file",
        List.of("database.ocf"),
        entryNames(reservedDirectory));
  }

  /**
   * A refused destructive restart keeps every live session of the named database.
   *
   * <p>The acceptance check of the restart runs before the restart touches any in-memory state.
   * The scenario holds one open session of a healthy database and restarts that database. The
   * expected outcome has three parts. The restart refuses the healthy target. The held session
   * still reads its own content. The registered storage of that database survives the refusal.
   */
  @Test
  public void restartRefusalKeepsFilesAndLiveSession() throws Exception {
    try (var youTrackDB = openManager()) {
      createSourceDatabase(youTrackDB);
      storageOf(youTrackDB, SOURCE).fullBackup(backupPath);

      try (var liveSession = youTrackDB.open(SOURCE, ADMIN, PASSWORD)) {
        var registeredStorage = storageOf(youTrackDB, SOURCE);

        var refusal =
            assertThrows(
                RuntimeException.class,
                () -> internalOf(youTrackDB)
                    .restartInterruptedRestore(SOURCE, backupPath.toString(), null, null));

        assertEquals(
            "the restart must refuse an active target by name",
            StorageAdmissionException.Reason.RESTART_TARGET_NOT_RESTORE_IN_PROGRESS,
            admissionReason(refusal));
        assertTrue(
            "the live session must still read its own content",
            liveSession.getMetadata().getSchema().existsClass(RECORD_CLASS));
        assertEquals(
            "the refusal must keep the registered storage of the healthy database",
            registeredStorage,
            internalOf(youTrackDB).getStorage(SOURCE));
      }
    }
  }

  /**
   * The destructive restart excludes a concurrent creation of the same database name.
   *
   * <p>The restart holds one exclusion over the deletion and over the new restore. A creation
   * that runs between the deletion and the new restore would leave an empty active database under
   * the name of the restore target. The scenario runs one restart while a second thread creates
   * the same name in a loop.
   *
   * <p>The barrier of this test removes every vacuous pass. The restart starts only after the
   * competitor finished its first attempt. The test also counts the attempts of the competitor
   * during the restart, so a competitor that never ran fails this test.
   *
   * <p>The expected outcome has
   * three parts. The restart finishes. The competitor attempts at least one creation during the
   * restart. The database of that name carries the content of the backup, so no empty database
   * survived.
   */
  @Test(timeout = 300_000)
  public void restartExcludesAConcurrentCreationOfTheSameName() throws Exception {
    createSourceDatabaseAndBackup();

    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);

      var competitorRuns = new java.util.concurrent.atomic.AtomicBoolean(true);
      var competitorAttempts = new AtomicInteger();
      var firstAttemptFinished = new java.util.concurrent.CountDownLatch(1);
      var competitor =
          new Thread(
              () -> {
                while (competitorRuns.get()) {
                  try {
                    youTrackDB.createIfNotExists(TARGET, DatabaseType.DISK, ADMIN, PASSWORD, ADMIN);
                  } catch (RuntimeException expectedFailure) {
                    // Every refusal of the competing creation is expected. Only the final state
                    // of the database decides this test.
                  }
                  competitorAttempts.incrementAndGet();
                  firstAttemptFinished.countDown();
                }
              });
      competitor.start();
      // The restart starts after the first attempt of the competitor, so the competitor is
      // running when the exclusive part of the restart begins.
      assertTrue("the competitor must reach its first attempt",
          firstAttemptFinished.await(2, java.util.concurrent.TimeUnit.MINUTES));
      var attemptsBeforeRestart = competitorAttempts.get();
      try {
        internalOf(youTrackDB).restartInterruptedRestore(TARGET, backupPath.toString(), null, null);
      } finally {
        competitorRuns.set(false);
        competitor.join();
      }

      assertTrue(
          "the competitor must attempt at least one creation during the restart",
          competitorAttempts.get() > attemptsBeforeRestart);
      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "no empty database may survive the restart",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * The destructive restart refuses a symbolic link that carries a database name.
   *
   * <p>A symbolic link would redirect the deletion of the restart outside of the databases
   * directory. The scenario links one database name to a directory outside of the databases
   * directory. The expected outcome has two parts. The restart refuses the link. The content of
   * the linked directory survives the refusal.
   */
  @Test
  public void restartRefusesSymbolicLinkTargetAndKeepsTheLinkedContent() throws Exception {
    createSourceDatabaseAndBackup();
    var outsideDirectory = Files.createDirectories(root.resolve("outside"));
    var outsideFile = Files.writeString(outsideDirectory.resolve("precious.txt"), "payload");
    try {
      Files.createSymbolicLink(databasesPath.resolve("linkedTarget"), outsideDirectory);
    } catch (UnsupportedOperationException | IOException linksUnsupported) {
      org.junit.Assume.assumeNoException("symbolic links are unsupported", linksUnsupported);
    }

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore("linkedTarget", backupPath.toString(), null, null));

      assertTrue(
          "the refusal must name the missing real directory, saw: " + rootMessage(refusal),
          rootMessage(refusal).contains("no real directory"));
    }

    assertTrue("the linked content must survive the refusal", Files.exists(outsideFile));
  }

  /**
   * Two opens of one unchanged image report the same admission reason.
   *
   * <p>An admission that creates the authority lock file would turn a directory without any
   * bootstrap authority artifact into birth residue. The second open would then report the
   * interrupted-birth reason and would advise a drop. The scenario opens one directory of an
   * earlier format twice. The expected outcome has two parts. Both opens report the
   * missing-record reason. The two opens create no bootstrap authority artifact.
   */
  @Test
  public void twoOpensOfOneUnchangedImageReportTheSameAdmissionReason() throws Exception {
    var earlierFormatDirectory = Files.createDirectories(databasesPath.resolve("earlierFormat"));
    Files.writeString(earlierFormatDirectory.resolve("database.ocf"), "payload");

    try (var youTrackDB = openManager()) {
      var firstFailure =
          assertThrows(
              RuntimeException.class,
              () -> youTrackDB.open("earlierFormat", ADMIN, PASSWORD));
      var secondFailure =
          assertThrows(
              RuntimeException.class,
              () -> youTrackDB.open("earlierFormat", ADMIN, PASSWORD));

      assertEquals(
          "the first open must report the missing-record reason",
          StorageAdmissionException.Reason.AUTHORITY_MISSING,
          admissionReason(firstFailure));
      assertEquals(
          "the second open must report the same reason as the first open",
          StorageAdmissionException.Reason.AUTHORITY_MISSING,
          admissionReason(secondFailure));
    }

    assertEquals(
        "an admission must create no bootstrap authority artifact",
        List.of(),
        bootstrapArtifactNames(earlierFormatDirectory));
  }

  /**
   * The public restart entry deletes an interrupted target and restores the backup.
   *
   * <p>Every earlier restart test of this class calls the internal manager directly. The public
   * entry converts the configuration of the caller and then delegates. The scenario interrupts one
   * restore and then calls the public entry of the manager with one Apache configuration. The
   * expected outcome has three parts. The public entry finishes. The target reaches the active
   * lifecycle state. The restarted database opens and carries the content of the backup.
   */
  @Test
  public void publicRestartEntryDeletesInterruptedTargetAndRestoresTheBackup() throws Exception {
    createSourceDatabaseAndBackup();

    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);

      // The public entry accepts an Apache configuration, which the embedded entry never sees.
      youTrackDB.restartInterruptedRestore(
          TARGET, backupPath.toString(), null, new BaseConfiguration());

      assertNull(
          "the public restart must publish the active lifecycle state",
          admissionReasonOf(TARGET));
      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "the restarted database must carry the content of the backup",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * The public restart entry accepts an absent configuration and uses the default configuration.
   *
   * <p>The scenario interrupts one restore and then calls the public entry with a null
   * configuration. The expected outcome has two parts. The public entry finishes. The restarted
   * database opens and carries the content of the backup.
   */
  @Test
  public void publicRestartEntryAcceptsAnAbsentConfiguration() throws Exception {
    createSourceDatabaseAndBackup();

    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);

      youTrackDB.restartInterruptedRestore(TARGET, backupPath.toString(), null, null);

      assertNull(
          "the public restart must publish the active lifecycle state",
          admissionReasonOf(TARGET));
      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "the restarted database must carry the content of the backup",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * Every connection other than an embedded connection refuses the restart entry.
   *
   * <p>The restart deletes a whole database directory of the host that stores the database, so a
   * remote connection must never run the restart. The interface therefore carries a refusing
   * default method, and only the embedded implementation overrides that method. The scenario calls
   * the default method through a proxy that implements the interface and nothing else. The
   * expected outcome has two parts. The call reports an unsupported operation. The message names
   * the database and prescribes the embedded connection.
   */
  @Test
  public void nonEmbeddedRestartEntryRefusesAndNamesTheDatabase() {
    var connectionWithoutOverride =
        (YouTrackDB) Proxy.newProxyInstance(
            YouTrackDB.class.getClassLoader(),
            new Class<?>[] {YouTrackDB.class},
            (proxy, method, arguments) -> InvocationHandler.invokeDefault(proxy, method,
                arguments));

    var refusal =
        assertThrows(
            UnsupportedOperationException.class,
            () -> connectionWithoutOverride.restartInterruptedRestore(
                TARGET, backupPath.toString(), null, null));

    assertTrue(
        "the refusal must name the database, saw: " + refusal.getMessage(),
        refusal.getMessage().contains(TARGET));
    assertTrue(
        "the refusal must prescribe an embedded connection, saw: " + refusal.getMessage(),
        refusal.getMessage().contains("embedded connection"));
  }

  /**
   * The restored target carries a fresh storage identity and a fresh storage lineage.
   *
   * <p>A storage identity is the unique identifier of one storage image. A storage lineage is the
   * identifier of the ancestry of one storage image. A restored image never shares either value
   * with the backup source, because the restore target adopts both values at its birth
   * publication. The scenario restores one backup into a new database name. The expected outcome
   * has two parts. The restored target carries another storage identity than the source database.
   * The restored target carries another storage lineage than the source database.
   */
  @Test
  public void restoredTargetCarriesFreshIdentityAndFreshLineage() throws Exception {
    StorageIdentity sourceIdentity;
    StorageLineageIdentity sourceLineage;
    try (var youTrackDB = openManager()) {
      createSourceDatabase(youTrackDB);
      var sourceStorage = storageOf(youTrackDB, SOURCE);
      sourceIdentity = sourceStorage.getStorageIdentity();
      sourceLineage = sourceStorage.getStorageLineageIdentity();
      sourceStorage.fullBackup(backupPath);
    }

    try (var youTrackDB = openManager()) {
      internalOf(youTrackDB).restore(TARGET, backupPath.toString(), null, null);
      var restoredStorage = storageOf(youTrackDB, TARGET);

      assertNotEquals(
          "the restored target must carry a fresh storage identity",
          sourceIdentity,
          restoredStorage.getStorageIdentity());
      assertNotEquals(
          "the restored target must carry a fresh storage lineage",
          sourceLineage,
          restoredStorage.getStorageLineageIdentity());
    }
  }

  /**
   * A restore refuses a database name that already exists, and keeps every file of that database.
   *
   * <p>The scenario opens the source database and then restores one backup into the name of that
   * healthy database. The expected outcome has three parts. The restore refuses the name and names
   * the existing database. The refusal keeps the registered storage of the healthy database, so no
   * fresh storage of the refused restore replaces that registration. The source database still
   * opens and still carries its own content.
   */
  @Test
  public void restoreRefusesAnExistingDatabaseName() throws Exception {
    createSourceDatabaseAndBackup();

    try (var youTrackDB = openManager()) {
      try (var ignored = youTrackDB.open(SOURCE, ADMIN, PASSWORD)) {
        var registeredStorage = storageOf(youTrackDB, SOURCE);

        var refusal =
            assertThrows(
                RuntimeException.class,
                () -> internalOf(youTrackDB).restore(SOURCE, backupPath.toString(), null, null));

        assertTrue(
            "the refusal must name the existing database, saw: " + rootMessage(refusal),
            rootMessage(refusal).contains("already exists"));
        assertSame(
            "the refusal must keep the registered storage of the existing database",
            registeredStorage,
            internalOf(youTrackDB).getStorage(SOURCE));
      }
      try (var session = youTrackDB.open(SOURCE, ADMIN, PASSWORD)) {
        assertTrue(
            "the refused database must keep its own content",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * A restore refuses a reserved database name prefix and creates no directory.
   *
   * <p>The two reserved prefixes are the prefix for graph traversal names and the prefix for
   * server names. The scenario restores one backup into a name with an upper-case reserved prefix.
   * The expected outcome has two parts. The restore refuses the name. The restore creates no
   * directory under that name.
   */
  @Test
  public void restoreRefusesAReservedNamePrefix() throws Exception {
    createSourceDatabaseAndBackup();

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restore("SERVERrestore", backupPath.toString(), null, null));

      assertTrue(
          "the refusal must name the reserved prefix, saw: " + rootMessage(refusal),
          rootMessage(refusal).contains("server"));
    }

    assertFalse(
        "the refused restore must create no directory",
        Files.exists(databasesPath.resolve("SERVERrestore")));
  }

  /**
   * A restore runs no database creation listener, and a plain creation still runs that listener.
   *
   * <p>A database creation listener runs after a creation published a usable database. A restore
   * target is no such database, because the backup content replaces every genesis artifact. The
   * scenario registers one counting listener and then runs one restore and one plain creation. The
   * expected outcome has two parts. The restore runs the listener zero times. The plain creation
   * runs the listener at least one time.
   *
   * <p>The listener registry belongs to the whole virtual machine, and a sibling test class can
   * create a database at the same time. The counting listener therefore counts one call per
   * database name, and both assertions read the counter of one name of this test only. The name of
   * the plain creation also carries the name of this test, so no sibling class can raise that
   * counter.
   *
   * <p>The creation assertion names no exact count on purpose. The create path of the manager
   * calls the listener set twice for one plain creation, which predates Track 24. This test
   * therefore proves the presence of the call and never freezes the count of that call.
   */
  @Test
  public void restoreRunsNoCreationListenerAndCreationStillRunsThatListener() throws Exception {
    createSourceDatabaseAndBackup();
    var plainCreationName = "restoreLifecycleListenerPlainCreation";
    var creationCountsByName = new ConcurrentHashMap<String, AtomicInteger>();
    DatabaseLifecycleListener countingListener =
        new DatabaseLifecycleListener() {
          @Override
          public void onCreate(@Nonnull DatabaseSessionEmbedded session) {
            creationCountsByName
                .computeIfAbsent(session.getDatabaseName(), name -> new AtomicInteger())
                .incrementAndGet();
          }
        };

    YouTrackDBEnginesManager.instance().addDbLifecycleListener(countingListener);
    try (var youTrackDB = openManager()) {
      internalOf(youTrackDB).restore(TARGET, backupPath.toString(), null, null);

      assertEquals(
          "a restore must run no database creation listener for the restore target",
          0,
          creationCountOf(creationCountsByName, TARGET));

      youTrackDB.create(plainCreationName, DatabaseType.DISK, ADMIN, PASSWORD, ADMIN);
      assertTrue(
          "a plain creation must still run the database creation listener, saw "
              + creationCountOf(creationCountsByName, plainCreationName)
              + " calls",
          creationCountOf(creationCountsByName, plainCreationName) >= 1);
    } finally {
      YouTrackDBEnginesManager.instance().removeDbLifecycleListener(countingListener);
    }
  }

  /** Returns the counted creation listener calls of one database name. */
  private static int creationCountOf(
      ConcurrentHashMap<String, AtomicInteger> creationCountsByName, String databaseName) {
    var counter = creationCountsByName.get(databaseName);
    return counter == null ? 0 : counter.get();
  }

  /**
   * The destructive restart refuses a live memory database and keeps that database working.
   *
   * <p>The registration map of the manager holds one storage per database name, and a memory
   * database keeps no directory at all. A restart that trusts the registration map alone would
   * shut down that memory database without any evidence on disk. The scenario creates one memory
   * database and then names that memory database in a restart. The expected outcome has three
   * parts. The restart refuses the name with the missing-record reason. The registered storage of
   * the memory database survives the refusal. The memory database still opens and still carries
   * its own content.
   */
  @Test
  public void restartRefusesALiveMemoryDatabaseAndKeepsThatDatabaseWorking() throws Exception {
    createSourceDatabaseAndBackup();
    var memoryName = "memoryRestartTarget";

    try (var youTrackDB = openManager()) {
      youTrackDB.create(memoryName, DatabaseType.MEMORY, ADMIN, PASSWORD, ADMIN);
      try (var session = youTrackDB.open(memoryName, ADMIN, PASSWORD)) {
        session.getMetadata().getSchema().createClass(RECORD_CLASS);
      }
      var registeredStorage = storageOf(youTrackDB, memoryName);

      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore(memoryName, backupPath.toString(), null, null));

      assertEquals(
          "the restart must refuse a name without any evidence on disk",
          StorageAdmissionException.Reason.AUTHORITY_MISSING,
          admissionReason(refusal));
      assertSame(
          "the refusal must keep the registered storage of the memory database",
          registeredStorage,
          internalOf(youTrackDB).getStorage(memoryName));
      try (var session = youTrackDB.open(memoryName, ADMIN, PASSWORD)) {
        assertTrue(
            "the memory database must still carry its own content",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * The plain restore refuses a database name that escapes the databases directory.
   *
   * <p>The name of a single dot resolves to the databases directory itself, and the name of a
   * double dot resolves to the parent of that directory. A restore into such a target would write
   * storage files over the databases directory. The scenario names a single dot and a double dot
   * in two plain restores. The expected outcome has two parts. Both restores refuse the name. The
   * database directory of the source database survives both refusals.
   */
  @Test
  public void plainRestoreRefusesPathEscapingNameAndKeepsEveryDatabase() throws Exception {
    createSourceDatabaseAndBackup();
    var sourceEntriesBeforeRestore = entryNames(databasesPath.resolve(SOURCE));
    assertFalse("the source database must hold files", sourceEntriesBeforeRestore.isEmpty());

    try (var youTrackDB = openManager()) {
      for (var escapingName : List.of(".", "..")) {
        var refusal =
            assertThrows(
                "the plain restore must refuse the escaping name " + escapingName,
                RuntimeException.class,
                () -> internalOf(youTrackDB)
                    .restore(escapingName, backupPath.toString(), null, null));
        assertTrue(
            "the refusal must name the invalid database name, saw: " + rootMessage(refusal),
            rootMessage(refusal).contains("Invalid database name"));
      }
    }

    assertTrue("the databases directory must survive", Files.isDirectory(databasesPath));
    assertEquals(
        "the source database must keep every file",
        sourceEntriesBeforeRestore,
        entryNames(databasesPath.resolve(SOURCE)));
  }

  /**
   * The destructive restart accepts a directory that holds the authority lock file alone.
   *
   * <p>Two events produce that directory shape. The deletion of an earlier restart produces the
   * shape, because the deletion keeps the authority lock file. An interrupted storage birth also
   * produces the shape, because the birth creates the authority lock file before the birth writes
   * the first record. The shape holds no content file under either event, so the restart accepts
   * the shape and destroys no data.
   *
   * <p>The scenario builds the shape of an interrupted storage birth, which holds the authority
   * lock file alone. The expected outcome has two parts. The restart accepts that directory. The
   * restarted database opens and carries the content of the backup.
   */
  @Test
  public void restartAcceptsTheLockFileOnlyShapeOfAnInterruptedBirth() throws Exception {
    createSourceDatabaseAndBackup();
    var birthResidueName = "lockFileOnlyBirthResidue";
    var birthResidueDirectory = Files.createDirectories(databasesPath.resolve(birthResidueName));
    // The name of the authority lock file. A storage birth creates that file first.
    Files.createFile(birthResidueDirectory.resolve("storage-bootstrap.bsml"));

    try (var youTrackDB = openManager()) {
      internalOf(youTrackDB)
          .restartInterruptedRestore(birthResidueName, backupPath.toString(), null, null);

      assertNull(
          "the restart must publish the active lifecycle state",
          admissionReasonOf(birthResidueName));
      try (var session = youTrackDB.open(birthResidueName, ADMIN, PASSWORD)) {
        assertTrue(
            "the restarted database must carry the content of the backup",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * The destructive restart refuses a registered storage that another directory backs.
   *
   * <p>The registration map of the manager holds one storage per database name. A restart that
   * trusts that map alone would shut down a storage of another directory. The scenario registers
   * one memory database under the target name and then builds the accepted restart shape on disk
   * under the same name. The accepted restart shape is a directory whose only entry is the
   * authority lock file. The disk evidence therefore passes the acceptance check, and the restart
   * reaches the registration check afterwards.
   *
   * <p>The expected outcome has four parts. The restart refuses the name and names the resolved
   * target directory. The registered storage of the memory database survives the refusal. The
   * memory database still opens and still carries its own content. The target directory keeps its
   * single entry, so the refusal deletes no file.
   *
   * <p>This test reaches the registration check directly. The sibling test of a live memory
   * database without any directory never reaches the registration check, because the absent
   * directory refuses first.
   */
  @Test
  public void restartRefusesARegisteredStorageOfAnotherDirectory() throws Exception {
    createSourceDatabaseAndBackup();
    var mismatchName = "registrationMismatchTarget";

    try (var youTrackDB = openManager()) {
      // The memory database occupies the name first. A memory database creates no directory, so
      // the creation sees no existing database of that name.
      youTrackDB.create(mismatchName, DatabaseType.MEMORY, ADMIN, PASSWORD, ADMIN);
      try (var session = youTrackDB.open(mismatchName, ADMIN, PASSWORD)) {
        session.getMetadata().getSchema().createClass(RECORD_CLASS);
      }
      var registeredStorage = storageOf(youTrackDB, mismatchName);

      // The disk evidence of an interrupted storage birth follows the registration. The restart
      // accepts this directory shape, so the acceptance check passes and the guard decides.
      var targetDirectory = Files.createDirectories(databasesPath.resolve(mismatchName));
      Files.createFile(targetDirectory.resolve("storage-bootstrap.bsml"));

      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore(mismatchName, backupPath.toString(), null, null));

      assertTrue(
          "the refusal must name the resolved target directory, saw: " + rootMessage(refusal),
          rootMessage(refusal).contains(targetDirectory.toAbsolutePath().normalize().toString()));
      assertSame(
          "the refusal must keep the registered storage of the memory database",
          registeredStorage,
          internalOf(youTrackDB).getStorage(mismatchName));
      try (var session = youTrackDB.open(mismatchName, ADMIN, PASSWORD)) {
        assertTrue(
            "the memory database must still carry its own content",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
      assertEquals(
          "the refusal must delete no file of the target directory",
          List.of("storage-bootstrap.bsml"),
          entryNames(targetDirectory));
    }
  }

  /** Creates the source database with one class and one record, and then takes one full backup. */
  private void createSourceDatabaseAndBackup() {
    try (var youTrackDB = openManager()) {
      createSourceDatabase(youTrackDB);
      assertNotNull(storageOf(youTrackDB, SOURCE).fullBackup(backupPath));
    }
  }

  /**
   * Interrupts one restore of the target and leaves the restore-in-progress lifecycle state.
   *
   * <p>The prepared chain passes the admission of the restore. Every header of that chain carries
   * the accepted database format and the accepted creation completion evidence. The replay then
   * fails, because the content of that backup carries an unsupported storage layout version. The
   * target therefore keeps every file and reports the interrupted-restore reason.
   */
  private void interruptRestoreOfTarget(YouTrackDBImpl youTrackDB) throws Exception {
    createUnusableBackup(youTrackDB);

    assertThrows(
        RuntimeException.class,
        () -> internalOf(youTrackDB).restore(TARGET, unusableBackupPath.toString(), null, null));
    assertEquals(
        "the interrupted restore must leave the restore-in-progress state",
        StorageAdmissionException.Reason.INTERRUPTED_RESTORE,
        admissionReasonOf(TARGET));
  }

  /**
   * Creates one backup whose headers pass admission and whose content fails the replay.
   *
   * <p>The source database of that backup carries an unsupported storage layout version. The
   * backup header records the layout version of this build, so the admission of a later restore
   * accepts the chain. The configuration load of the replay then refuses the restored content.
   */
  private void createUnusableBackup(YouTrackDBImpl youTrackDB) throws Exception {
    if (unusableBackupCreated) {
      return;
    }
    unusableBackupCreated = true;
    youTrackDB.create(UNUSABLE_SOURCE, DatabaseType.DISK, ADMIN, PASSWORD, ADMIN);
    var storage = storageOf(youTrackDB, UNUSABLE_SOURCE);
    var configuration = (CollectionBasedStorageConfiguration) storage.configuration;
    storage.getAtomicOperationsManager().executeInsideAtomicOperation(
        atomicOperation -> configuration.updateVersionForTesting(atomicOperation, 23));
    storage.fullBackup(unusableBackupPath);
  }

  /** Creates the source database with one class and one record. */
  private void createSourceDatabase(YouTrackDBImpl youTrackDB) {
    youTrackDB.create(SOURCE, DatabaseType.DISK, ADMIN, PASSWORD, ADMIN);
    try (var session = youTrackDB.open(SOURCE, ADMIN, PASSWORD)) {
      session.getMetadata().getSchema().createClass(RECORD_CLASS);
      session.begin();
      var entity = session.newEntity(RECORD_CLASS);
      entity.setProperty("value", "restored");
      session.commit();
    }
  }

  private YouTrackDBImpl openManager() {
    return (YouTrackDBImpl) YourTracks.instance(databasesPath.toString());
  }

  private static YouTrackDBInternalEmbedded internalOf(YouTrackDBImpl youTrackDB) {
    return (YouTrackDBInternalEmbedded) youTrackDB.internal;
  }

  private static AbstractStorage storageOf(YouTrackDBImpl youTrackDB, String databaseName) {
    var storage = internalOf(youTrackDB).getStorage(databaseName);
    assertNotNull("the database must hold one registered storage", storage);
    return storage;
  }

  /** Returns the temporary chain directories of restore requests of the target name. */
  private static List<String> temporaryChainDirectories() throws IOException {
    var temporaryRoot = Path.of(System.getProperty("java.io.tmpdir"));
    try (var paths = Files.list(temporaryRoot)) {
      return paths
          .map(path -> path.getFileName().toString())
          .filter(name -> name.startsWith(TARGET + "-ytdb-backup"))
          .sorted()
          .toList();
    }
  }

  /** Returns the admission reason of one database directory, or null for an accepted image. */
  private StorageAdmissionException.Reason admissionReasonOf(String databaseName) {
    try {
      new StorageBootstrapMetadata(databasesPath.resolve(databaseName), FEATURE_FORMAT)
          .readActiveRequired();
      return null;
    } catch (StorageAdmissionException rejection) {
      return rejection.reason();
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  /** Returns the admission reason inside the cause chain of one failure. */
  private static StorageAdmissionException.Reason admissionReason(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof StorageAdmissionException admission) {
        return admission.reason();
      }
    }
    return null;
  }

  /** Joins every message of the cause chain of one failure. */
  private static String rootMessage(Throwable failure) {
    var message = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      message.append(cause.getMessage()).append(' ');
    }
    return message.toString();
  }

  private Iterator<String> backupFileNames() {
    try (var paths = Files.list(backupPath)) {
      List<String> names =
          paths.map(path -> path.getFileName().toString()).filter(name -> name.endsWith(".ibu"))
              .sorted()
              .toList();
      return names.iterator();
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  private InputStream openBackupFile(String fileName) {
    try {
      return Files.newInputStream(backupPath.resolve(fileName));
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  /** Returns a truncated stream of one backup file, which interrupts the content stage. */
  private InputStream openTruncatedBackupFile(String fileName) {
    try {
      var content = Files.readAllBytes(backupPath.resolve(fileName));
      return new ByteArrayInputStream(Arrays.copyOf(content, Math.min(64, content.length)));
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  /** Removes every entry of one target and keeps the authority lock file. */
  private static void deleteEveryEntryExceptTheAuthorityLockFile(Path storageDirectory)
      throws IOException {
    try (var paths = Files.list(storageDirectory)) {
      for (var path : paths.toList()) {
        if (!path.getFileName().toString().endsWith(".bsml")) {
          FileUtils.deleteRecursively(path.toFile());
        }
      }
    }
  }

  /** Returns the sorted names of every entry of one directory. */
  private static List<String> entryNames(Path directory) throws IOException {
    try (var paths = Files.list(directory)) {
      return paths.map(path -> path.getFileName().toString()).sorted().toList();
    }
  }

  private static List<String> bootstrapArtifactNames(Path storageDirectory) throws IOException {
    try (var paths = Files.list(storageDirectory)) {
      return paths
          .map(path -> path.getFileName().toString())
          .filter(StorageBootstrapMetadata::isBootstrapArtifactName)
          .sorted()
          .toList();
    }
  }
}
