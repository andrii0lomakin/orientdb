package com.jetbrains.youtrackdb.internal.core.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.internal.common.io.FileUtils;
import com.jetbrains.youtrackdb.internal.core.exception.UnsupportedBackupException;
import com.jetbrains.youtrackdb.internal.core.storage.disk.BackupUnitFiles;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageAdmissionException;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageBootstrapMetadata;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers the backup admission of every restore entry before any change of the restore target.
 *
 * <p>A backup header is the metadata record at the tail of one backup unit file. Every supported
 * header carries the semantic database format of the backed-up database and evidence of a finished
 * creation of that database. A restore prepares and validates the complete selected chain before
 * the restore creates, deletes, or writes any target.
 *
 * <p>One request owns the prepared copies. The replay reads those copies alone, so a change of the
 * source after the validation never reaches the target. The request removes every copy on every
 * ordinary exit.
 *
 * <p>Each test of this class states one scenario and one expected outcome in its own comment.
 */
public class BackupAdmissionRestoreTest {

  private static final String ADMIN = "admin";
  private static final String PASSWORD = "adminpwd";
  private static final String RECORD_CLASS = "AdmittedClass";
  private static final String SOURCE = "backupAdmissionSource";
  private static final String TARGET = "backupAdmissionTarget";

  private Path root;
  private Path databasesPath;
  private Path backupPath;
  private Path syntheticBackupPath;

  @Before
  public void createDirectories() throws Exception {
    root = Files.createTempDirectory("backup-admission-");
    databasesPath = Files.createDirectories(root.resolve("databases"));
    backupPath = Files.createDirectories(root.resolve("backup"));
    syntheticBackupPath = Files.createDirectories(root.resolve("synthetic-backup"));
  }

  @After
  public void deleteDirectories() {
    FileUtils.deleteRecursively(root.toFile());
  }

  /**
   * One path restore refuses a chain of old headers and changes no target.
   *
   * <p>An old header carries no semantic database format and no creation completion evidence. The
   * scenario restores one chain of such headers through the path entry. The expected outcome has
   * three parts. The restore reports the unsupported chain. No database of the target name exists.
   * No directory of the target name exists.
   */
  @Test
  public void pathRestoreRefusesAnOldHeaderChainBeforeAnyTargetChange() throws Exception {
    BackupUnitFiles.writeLegacyVersion2Unit(
        syntheticBackupPath, syntheticDatabaseId, SOURCE, 0, true);

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              UnsupportedBackupException.class,
              () -> internalOf(youTrackDB)
                  .restore(TARGET, syntheticBackupPath.toString(), null, null));

      assertTrue(
          "the refusal must name the unsupported backup format version, saw: "
              + refusal.getMessage(),
          refusal.getMessage().contains("backup format version"));
      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One supplier restore refuses a chain of old headers and changes no target.
   *
   * <p>The supplier entry uses another listing rule and another read rule than the path entry, so
   * the admission of that entry needs its own coverage. The scenario restores one chain of old
   * headers through the supplier entry. The expected outcome has two parts. The restore reports
   * the unsupported chain. No database and no directory of the target name exist.
   */
  @Test
  public void supplierRestoreRefusesAnOldHeaderChainBeforeAnyTargetChange() throws Exception {
    BackupUnitFiles.writeLegacyVersion2Unit(
        syntheticBackupPath, syntheticDatabaseId, SOURCE, 0, true);
    var source = new TrackingBackupSource(syntheticBackupPath);

    try (var youTrackDB = openManager()) {
      assertThrows(
          UnsupportedBackupException.class,
          () -> internalOf(youTrackDB)
              .restore(TARGET, source.names(), source.streams(), null, null));

      assertNoTargetExists(youTrackDB);
      assertTrue("every opened source stream must close", source.everyStreamClosed());
    }
  }

  /**
   * One path restore refuses an otherwise valid chain without creation completion evidence.
   *
   * <p>The chain of this scenario carries the supported backup format, the feature format of this
   * build, and the layout version of this build. The chain carries no creation completion
   * evidence. The expected outcome has three parts. The restore names the missing evidence. No
   * database of the target name exists. No directory of the target name exists.
   */
  @Test
  public void pathRestoreRefusesAChainWithoutCreationEvidenceBeforeAnyTargetChange()
      throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.ABSENT_CREATION_EVIDENCE);

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              UnsupportedBackupException.class,
              () -> internalOf(youTrackDB)
                  .restore(TARGET, syntheticBackupPath.toString(), null, null));

      assertTrue(
          "the refusal must name the missing creation completion evidence, saw: "
              + refusal.getMessage(),
          refusal.getMessage().contains("no accepted creation completion evidence"));
      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One supplier restore refuses an otherwise valid chain without creation completion evidence.
   *
   * <p>The scenario repeats the isolated creation-evidence case through the supplier entry. The
   * expected outcome has two parts. The restore names the missing evidence. No database and no
   * directory of the target name exist.
   */
  @Test
  public void supplierRestoreRefusesAChainWithoutCreationEvidenceBeforeAnyTargetChange()
      throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.ABSENT_CREATION_EVIDENCE);
    var source = new TrackingBackupSource(syntheticBackupPath);

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              UnsupportedBackupException.class,
              () -> internalOf(youTrackDB)
                  .restore(TARGET, source.names(), source.streams(), null, null));

      assertTrue(
          "the refusal must name the missing creation completion evidence, saw: "
              + refusal.getMessage(),
          refusal.getMessage().contains("no accepted creation completion evidence"));
      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One path restore refuses a chain of mixed backup formats.
   *
   * <p>A mixed chain holds one supported unit and one unsupported unit. The whole chain validation
   * runs before any target change, so the unsupported unit refuses the request. The scenario
   * writes one supported full backup and one old increment. The expected outcome has two parts.
   * The restore reports the unsupported chain. No database and no directory of the target name
   * exist.
   */
  @Test
  public void pathRestoreRefusesAMixedFormatChain() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    writeSyntheticUnit(1, false, BackupUnitFiles.OLD_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);

    try (var youTrackDB = openManager()) {
      assertThrows(
          UnsupportedBackupException.class,
          () -> internalOf(youTrackDB)
              .restore(TARGET, syntheticBackupPath.toString(), null, null));

      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One path restore refuses a chain of another database feature format.
   *
   * <p>A database feature format names the storage features of one database. A chain of another
   * feature format can hold a backup of a newer build. The expected outcome has two parts. The
   * restore names the foreign feature format. No database and no directory of the target name
   * exist.
   */
  @Test
  public void pathRestoreRefusesAnIncompatibleFeatureFormatChain() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat() + 1, BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              UnsupportedBackupException.class,
              () -> internalOf(youTrackDB)
                  .restore(TARGET, syntheticBackupPath.toString(), null, null));

      assertTrue(
          "the refusal must name the foreign feature format, saw: " + refusal.getMessage(),
          refusal.getMessage().contains("database feature format"));
      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One path restore refuses a chain of another storage layout version.
   *
   * <p>A storage layout version names the on-disk layout of one database. The expected outcome has
   * two parts. The restore names the foreign layout version. No database and no directory of the
   * target name exist.
   */
  @Test
  public void pathRestoreRefusesAnIncompatibleStorageLayoutChain() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion() + 1,
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              UnsupportedBackupException.class,
              () -> internalOf(youTrackDB)
                  .restore(TARGET, syntheticBackupPath.toString(), null, null));

      assertTrue(
          "the refusal must name the foreign layout version, saw: " + refusal.getMessage(),
          refusal.getMessage().contains("storage layout version"));
      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One path restore refuses output without any readable header.
   *
   * <p>A crash inside one backup write leaves such output. The expected outcome has two parts. The
   * restore reports the unsupported chain. No database and no directory of the target name exist.
   */
  @Test
  public void pathRestoreRefusesUnreadableResidue() throws Exception {
    BackupUnitFiles.writeUnreadableUnit(syntheticBackupPath, UUID.randomUUID(), SOURCE, 0);

    try (var youTrackDB = openManager()) {
      assertThrows(
          UnsupportedBackupException.class,
          () -> internalOf(youTrackDB)
              .restore(TARGET, syntheticBackupPath.toString(), null, null));

      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One path restore refuses a truncated unit of a real backup.
   *
   * <p>A truncated unit carries no valid hash code, so the chain validation refuses the unit. The
   * scenario truncates the file of one real full backup. The expected outcome has two parts. The
   * restore reports the invalid content. No database and no directory of the target name exist.
   */
  @Test
  public void pathRestoreRefusesATruncatedUnitOfARealBackup() throws Exception {
    createSourceDatabaseAndBackup();
    var unitPath = backupPath.resolve(realUnitNames().getFirst());
    BackupUnitFiles.truncateUnit(unitPath, (int) Files.size(unitPath) - 1);

    try (var youTrackDB = openManager()) {
      var refusal = assertThrows(
          UnsupportedBackupException.class,
          () -> internalOf(youTrackDB).restore(TARGET, backupPath.toString(), null, null));

      assertTrue("the refusal must name invalid content, saw: " + refusal.getMessage(),
          refusal.getMessage().contains("contains invalid content"));
      assertFalse("the refusal must not misname a broken unit as unsupported, saw: "
          + refusal.getMessage(), refusal.getMessage().contains("no supported backup header"));
      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One path restore refuses a real chain with concurrently written trailing output.
   *
   * <p>A backup that runs during one restore leaves one unit without a complete header. The
   * restore refuses that chain before any target change and adds no waiting behavior. The expected
   * outcome has two parts. The restore reports the unsupported chain. No database and no directory
   * of the target name exist.
   */
  @Test
  public void pathRestoreRefusesConcurrentlyWrittenTrailingOutput() throws Exception {
    var sourceId = createSourceDatabaseAndBackup();
    BackupUnitFiles.writeUnreadableUnit(backupPath, sourceId, SOURCE, 1,
        BackupUnitFiles.unitFileName(sourceId, SOURCE, 1, BackupUnitFiles.FUTURE_DATE_STAMP));

    try (var youTrackDB = openManager()) {
      assertThrows(
          RuntimeException.class,
          () -> internalOf(youTrackDB).restore(TARGET, backupPath.toString(), null, null));

      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One destructive restart refuses an old chain and keeps the interrupted target.
   *
   * <p>The restart validates the complete chain before the restart deletes the interrupted target.
   * The scenario interrupts one restore and then restarts that restore from a chain of old
   * headers. The expected outcome has three parts. The restart reports the unsupported chain. The
   * interrupted target keeps every file. The target still reports the interrupted-restore reason.
   */
  @Test
  public void restartRefusesAnOldHeaderChainAndKeepsTheInterruptedTarget() throws Exception {
    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);
      var entriesBeforeRestart = entryNames(databasesPath.resolve(TARGET));
      var oldChain = Files.createDirectories(root.resolve("old-chain"));
      BackupUnitFiles.writeLegacyVersion2Unit(
          oldChain, UUID.randomUUID(), SOURCE, 0, true);

      assertThrows(
          UnsupportedBackupException.class,
          () -> internalOf(youTrackDB)
              .restartInterruptedRestore(TARGET, oldChain.toString(), null, null));

      assertEquals(
          "the refused restart must keep every file of the interrupted target",
          entriesBeforeRestart,
          entryNames(databasesPath.resolve(TARGET)));
      assertEquals(
          "the refused restart must keep the restore-in-progress state",
          StorageAdmissionException.Reason.INTERRUPTED_RESTORE,
          admissionReasonOf(TARGET));
    }
  }

  /**
   * One destructive restart refuses a chain without creation completion evidence and keeps the
   * target.
   *
   * <p>The scenario interrupts one restore and then restarts that restore from a chain whose
   * headers carry the accepted database format without any creation completion evidence. The
   * expected outcome has three parts. The restart names the missing evidence. The interrupted
   * target keeps every file. The target still reports the interrupted-restore reason.
   */
  @Test
  public void restartRefusesAChainWithoutCreationEvidenceAndKeepsTheInterruptedTarget()
      throws Exception {
    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);
      var entriesBeforeRestart = entryNames(databasesPath.resolve(TARGET));
      var chainWithoutEvidence = Files.createDirectories(root.resolve("chain-without-evidence"));
      BackupUnitFiles.writeUnit(chainWithoutEvidence, UUID.randomUUID(), SOURCE, 0, true,
          BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION, BackupUnitFiles.supportedFeatureFormat(),
          BackupUnitFiles.supportedLayoutVersion(), BackupUnitFiles.ABSENT_CREATION_EVIDENCE,
          true);

      var refusal =
          assertThrows(
              UnsupportedBackupException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore(TARGET, chainWithoutEvidence.toString(), null, null));

      assertTrue(
          "the refusal must name the missing creation completion evidence, saw: "
              + refusal.getMessage(),
          refusal.getMessage().contains("no accepted creation completion evidence"));
      assertEquals(
          "the refused restart must keep every file of the interrupted target",
          entriesBeforeRestart,
          entryNames(databasesPath.resolve(TARGET)));
      assertEquals(
          StorageAdmissionException.Reason.INTERRUPTED_RESTORE, admissionReasonOf(TARGET));
    }
  }

  /**
   * One destructive restart of a supplier source refuses an old chain and keeps the target.
   *
   * <p>The restart of a supplier source uses the same admission as the restart of a path source.
   * The scenario interrupts one restore and restarts that restore from a supplier source of old
   * headers. The expected outcome has three parts. The restart reports the unsupported chain. The
   * interrupted target keeps every file. Every opened source stream closes.
   */
  @Test
  public void supplierRestartRefusesAnOldHeaderChainAndKeepsTheInterruptedTarget()
      throws Exception {
    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);
      var entriesBeforeRestart = entryNames(databasesPath.resolve(TARGET));
      var oldChain = Files.createDirectories(root.resolve("old-chain"));
      BackupUnitFiles.writeLegacyVersion2Unit(
          oldChain, UUID.randomUUID(), SOURCE, 0, true);
      var source = new TrackingBackupSource(oldChain);

      assertThrows(
          UnsupportedBackupException.class,
          () -> internalOf(youTrackDB)
              .restartInterruptedRestore(TARGET, source.names(), source.streams(), null, null));

      assertEquals(
          "the refused restart must keep every file of the interrupted target",
          entriesBeforeRestart,
          entryNames(databasesPath.resolve(TARGET)));
      assertTrue("every opened source stream must close", source.everyStreamClosed());
    }
  }

  /**
   * One destructive restart refuses a target that became healthy during the preparation.
   *
   * <p>The restart repeats the target-eligibility check immediately before the first destructive
   * step. That repetition also protects the live registration of the target, because the in-memory
   * discard of the restart follows the repeated check. The scenario replaces the interrupted
   * target by one healthy database from inside the source read of the restart.
   *
   * <p>The expected outcome has three parts. The restart refuses the target by its lifecycle
   * state. The refusal keeps the registered storage of the healthy database. The healthy database
   * keeps its content.
   */
  @Test
  public void restartRefusesATargetThatBecameHealthyDuringThePreparation() throws Exception {
    createSourceDatabaseAndBackup();
    var temporaryDirectoriesBefore = temporaryChainDirectories();

    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);
      var source = new TrackingBackupSource(backupPath);
      var healthyStorage = new java.util.concurrent.atomic.AtomicReference<Object>();
      // The replacement runs inside the source read of the restart, which precedes every
      // destructive step of that restart.
      source.afterFirstOpen(
          () -> {
            youTrackDB.drop(TARGET);
            internalOf(youTrackDB).restore(TARGET, backupPath.toString(), null, null);
            healthyStorage.set(internalOf(youTrackDB).getStorage(TARGET));
          });

      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore(TARGET, source.names(), source.streams(), null,
                      null));

      assertEquals(
          "the repeated check must refuse the healthy target",
          StorageAdmissionException.Reason.RESTART_TARGET_NOT_RESTORE_IN_PROGRESS,
          admissionReason(refusal));
      assertNotNull("the replacement must register one storage", healthyStorage.get());
      assertSame(
          "the refusal must keep the registered storage of the healthy database",
          healthyStorage.get(),
          internalOf(youTrackDB).getStorage(TARGET));
      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "the healthy database must keep its content",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
      assertEquals(
          "the refusal at the final check must remove every prepared copy",
          temporaryDirectoriesBefore,
          temporaryChainDirectories());
    }
  }

  /**
   * One restore of an existing database name reads no source file.
   *
   * <p>The cheap nonmutating checks of one restore precede the chain-sized preparation. The
   * scenario restores one backup into the name of one existing database. The expected outcome has
   * two parts. The restore names the existing database. The restore opens no source file.
   */
  @Test
  public void restoreOfAnExistingNameReadsNoSourceFile() throws Exception {
    createSourceDatabaseAndBackup();
    var source = new TrackingBackupSource(backupPath);

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restore(SOURCE, source.names(), source.streams(), null, null));

      assertTrue(
          "the refusal must name the existing database, saw: " + rootMessage(refusal),
          rootMessage(refusal).contains("already exists"));
      assertEquals("the refused restore must open no source file", List.of(),
          source.openedNames());
    }
  }

  /**
   * One restore of an invalid expected identifier reads no source file.
   *
   * <p>The scenario restores one backup with one malformed expected database identifier. The
   * expected outcome has three parts. The restore reports the invalid identifier. The restore
   * opens no source file. No directory of the target name exists.
   */
  @Test
  public void restoreOfAnInvalidExpectedIdentifierReadsNoSourceFile() throws Exception {
    createSourceDatabaseAndBackup();
    var source = new TrackingBackupSource(backupPath);

    try (var youTrackDB = openManager()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> internalOf(youTrackDB)
              .restore(TARGET, source.names(), source.streams(), "not-a-uuid", null));

      assertEquals("the refused restore must open no source file", List.of(),
          source.openedNames());
      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One restore of a reserved name prefix reads no source file.
   *
   * <p>The two reserved prefixes are the prefix for graph traversal names and the prefix for
   * server names. The scenario restores one backup into a name with a reserved prefix. The
   * expected outcome has two parts. The restore names the reserved prefix. The restore opens no
   * source file.
   */
  @Test
  public void restoreOfAReservedNamePrefixReadsNoSourceFile() throws Exception {
    createSourceDatabaseAndBackup();
    var source = new TrackingBackupSource(backupPath);

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restore("SERVERadmission", source.names(), source.streams(), null, null));

      assertTrue(
          "the refusal must name the reserved prefix, saw: " + rootMessage(refusal),
          rootMessage(refusal).contains("server"));
      assertEquals("the refused restore must open no source file", List.of(),
          source.openedNames());
    }
  }

  /**
   * One refused restart of a directory of an earlier format creates no authority file.
   *
   * <p>A bootstrap authority record names one storage identity, one storage lineage, and one
   * lifecycle state. A database of an earlier format holds content without such a record. The
   * scenario restarts the restore of such a directory.
   *
   * <p>The expected outcome has three parts. The restart refuses the target. The refusal creates
   * no bootstrap authority artifact. The restart opens no source file.
   */
  @Test
  public void refusedRestartOfAnEarlierFormatDirectoryCreatesNoAuthorityFile() throws Exception {
    createSourceDatabaseAndBackup();
    var earlierFormatDirectory = Files.createDirectories(databasesPath.resolve("earlierFormat"));
    Files.writeString(earlierFormatDirectory.resolve("database.ocf"), "payload");
    var source = new TrackingBackupSource(backupPath);

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              RuntimeException.class,
              () -> internalOf(youTrackDB)
                  .restartInterruptedRestore("earlierFormat", source.names(), source.streams(),
                      null, null));

      assertEquals(
          StorageAdmissionException.Reason.AUTHORITY_MISSING, admissionReason(refusal));
      assertEquals(
          "the refusal must create no bootstrap authority artifact",
          List.of("database.ocf"),
          entryNames(earlierFormatDirectory));
      assertEquals("the refused restart must open no source file", List.of(),
          source.openedNames());
    }
  }

  /**
   * One path restore keeps the existing listing rules of its backup directory.
   *
   * <p>The listing rule of one local backup directory skips every directory entry and every entry
   * without the backup unit extension. The scenario places one text file and one subdirectory next
   * to one real chain. The expected outcome has two parts. The restore finishes. The restored
   * database carries the content of the backup.
   */
  @Test
  public void pathRestoreIgnoresUnrelatedEntriesOfTheBackupDirectory() throws Exception {
    createSourceDatabaseAndBackup();
    Files.writeString(backupPath.resolve("README.txt"), "notes of the operator");
    Files.createDirectories(backupPath.resolve("archive"));

    try (var youTrackDB = openManager()) {
      internalOf(youTrackDB).restore(TARGET, backupPath.toString(), null, null);

      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "the restored database must carry the content of the backup",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * One path restore of a named database identifier ignores the units of another database.
   *
   * <p>The scenario places one supported unit of another database identifier next to one real
   * chain. The restore names the identifier of the real chain. The expected outcome has two parts.
   * The restore finishes. The restored database carries the content of the backup.
   */
  @Test
  public void pathRestoreOfANamedIdentifierIgnoresUnitsOfAnotherDatabase() throws Exception {
    var sourceId = createSourceDatabaseAndBackup();
    BackupUnitFiles.writeSupportedUnit(backupPath, UUID.randomUUID(), "otherDatabase", 0, true);

    try (var youTrackDB = openManager()) {
      internalOf(youTrackDB).restore(TARGET, backupPath.toString(), sourceId.toString(), null);

      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "the restored database must carry the content of the backup",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * The replay reads the prepared copies after every change of the source.
   *
   * <p>One request owns the prepared copies of its chain. The scenario replaces every source file
   * by unreadable residue after the last source read of the preparation. The expected outcome has
   * two parts. The restore finishes. The restored database carries the content of the backup.
   */
  @Test
  public void replayReadsThePreparedCopiesAfterTheSourceChanges() throws Exception {
    createSourceDatabaseAndBackup();
    var source = new TrackingBackupSource(backupPath);
    // The replacement runs after the last source read, which is the end of the preparation.
    source.afterEachClose(this::replaceEveryBackupUnitByResidue);

    try (var youTrackDB = openManager()) {
      internalOf(youTrackDB).restore(TARGET, source.names(), source.streams(), null, null);

      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "the restored database must carry the content of the prepared copies",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * One successful restore closes every source stream and removes every prepared copy.
   *
   * <p>The scenario restores one real chain through the supplier entry. The expected outcome has
   * three parts. The restore finishes. Every opened source stream closes. No temporary directory
   * of the request survives.
   */
  @Test
  public void successfulRestoreClosesEveryStreamAndRemovesEveryPreparedCopy() throws Exception {
    createSourceDatabaseAndBackup();
    var source = new TrackingBackupSource(backupPath);
    var temporaryDirectoriesBefore = temporaryChainDirectories();

    try (var youTrackDB = openManager()) {
      internalOf(youTrackDB).restore(TARGET, source.names(), source.streams(), null, null);
    }

    assertFalse("the restore must read at least one source file", source.openedNames().isEmpty());
    assertTrue("every opened source stream must close", source.everyStreamClosed());
    assertEquals(
        "no temporary directory of the request may survive",
        temporaryDirectoriesBefore,
        temporaryChainDirectories());
  }

  /**
   * One refused restore closes every source stream and removes every prepared copy.
   *
   * <p>The scenario restores one chain whose second unit carries an old header. The expected
   * outcome has three parts. The restore reports the unsupported chain. Every opened source stream
   * closes. No temporary directory of the request survives.
   */
  @Test
  public void refusedRestoreClosesEveryStreamAndRemovesEveryPreparedCopy() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    writeSyntheticUnit(1, false, BackupUnitFiles.OLD_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    var source = new TrackingBackupSource(syntheticBackupPath);
    var temporaryDirectoriesBefore = temporaryChainDirectories();

    try (var youTrackDB = openManager()) {
      assertThrows(
          UnsupportedBackupException.class,
          () -> internalOf(youTrackDB)
              .restore(TARGET, source.names(), source.streams(), null, null));
    }

    assertEquals("the refused restore must read both units", 2, source.openedNames().size());
    assertTrue("every opened source stream must close", source.everyStreamClosed());
    assertEquals(
        "no temporary directory of the request may survive",
        temporaryDirectoriesBefore,
        temporaryChainDirectories());
  }

  /**
   * One failed source open removes every partial copy and creates no target.
   *
   * <p>The failure of this scenario is the refused open of the second source unit. The sibling
   * tests of this class cover the failed source read and the failed output write of the copy
   * stage.
   *
   * <p>The expected outcome has four parts. The restore reports the failure. No directory of
   * the target name exists. Every opened source stream closes. No temporary directory of the
   * request survives.
   */
  @Test
  public void failedSourceOpenRemovesEveryPartialCopyAndCreatesNoTarget() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    writeSyntheticUnit(1, false, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    var source = new TrackingBackupSource(syntheticBackupPath);
    source.failOnOpenNumber(2);
    var temporaryDirectoriesBefore = temporaryChainDirectories();

    try (var youTrackDB = openManager()) {
      assertThrows(
          RuntimeException.class,
          () -> internalOf(youTrackDB)
              .restore(TARGET, source.names(), source.streams(), null, null));

      assertNoTargetExists(youTrackDB);
    }

    assertTrue("every opened source stream must close", source.everyStreamClosed());
    assertEquals(
        "no temporary directory of the failed request may survive",
        temporaryDirectoriesBefore,
        temporaryChainDirectories());
  }

  /**
   * One destructive restart replays the prepared copies after every change of the source.
   *
   * <p>The restart prepares the complete chain before the restart deletes the interrupted target.
   * The scenario replaces every source file by unreadable residue after the source read of the
   * restart. The expected outcome has two parts. The restart finishes. The restarted database
   * carries the content of the backup.
   */
  @Test
  public void supplierRestartReplaysThePreparedCopiesAfterTheSourceChanges() throws Exception {
    createSourceDatabaseAndBackup();

    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);
      var source = new TrackingBackupSource(backupPath);
      // The replacement runs after the source read, which precedes the deletion of the target.
      source.afterEachClose(this::replaceEveryBackupUnitByResidue);

      internalOf(youTrackDB)
          .restartInterruptedRestore(TARGET, source.names(), source.streams(), null, null);

      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "the restarted database must carry the content of the prepared copies",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /**
   * One failed preparation of a restart keeps the interrupted target unchanged.
   *
   * <p>The preparation of one restart precedes every destructive step of that restart. The
   * scenario fails the first source read of one restart. The expected outcome has three parts. The
   * restart reports the failure. The interrupted target keeps every file. The target still reports
   * the interrupted-restore reason.
   */
  @Test
  public void failedPreparationOfARestartKeepsTheInterruptedTarget() throws Exception {
    createSourceDatabaseAndBackup();

    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);
      var entriesBeforeRestart = entryNames(databasesPath.resolve(TARGET));
      var source = new TrackingBackupSource(backupPath);
      source.failOnOpenNumber(1);

      assertThrows(
          RuntimeException.class,
          () -> internalOf(youTrackDB)
              .restartInterruptedRestore(TARGET, source.names(), source.streams(), null, null));

      assertEquals(
          "the failed preparation must keep every file of the interrupted target",
          entriesBeforeRestart,
          entryNames(databasesPath.resolve(TARGET)));
      assertEquals(
          StorageAdmissionException.Reason.INTERRUPTED_RESTORE, admissionReasonOf(TARGET));
    }
  }

  /**
   * One supplier restore refuses an absolute unit name before any copy of that unit.
   *
   * <p>A stream source controls the names of its units completely. An absolute name would leave
   * the request-owned temporary directory, so the preparation refuses that name before it opens
   * any output. The scenario lists the absolute path of one unrelated file as the only unit of
   * the chain.
   *
   * <p>The expected outcome has three parts. The restore reports the refused name. The
   * unrelated file keeps its content. No database and no directory of the target name exist.
   */
  @Test
  public void supplierRestoreRefusesAnAbsoluteUnitName() throws Exception {
    createSourceDatabaseAndBackup();
    var unrelatedFile = root.resolve("precious-payload.ibu");
    Files.writeString(unrelatedFile, "payload of the operator");
    var escapingName = unrelatedFile.toAbsolutePath().toString();
    var sourceOpens = new AtomicInteger();

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              UnsupportedBackupException.class,
              () -> internalOf(youTrackDB)
                  .restore(TARGET, () -> List.of(escapingName).iterator(),
                      name -> openFirstRealUnit(name, sourceOpens), null, null));

      assertEquals("the refused name must open no source stream", 0, sourceOpens.get());
      assertTrue(
          "the refusal must name the refused unit, saw: " + refusal.getMessage(),
          refusal.getMessage().contains("no plain backup unit file name"));
      assertEquals(
          "the refused name must keep the content of the unrelated file",
          "payload of the operator",
          Files.readString(unrelatedFile));
      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One supplier restore refuses repeated names before opening either possible source unit.
   *
   * <p>The source can answer the repeated name with a valid full unit first and its contiguous
   * increment second. The expected refusal occurs before either answer can replace an earlier
   * prepared copy, so no target appears.
   */
  @Test
  public void supplierRestoreRefusesARepeatedFullAndIncrementNameBeforeAnyCopy() throws Exception {
    var fullName = writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    var incrementName = writeSyntheticUnit(1, false,
        BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    var repeatedName = BackupUnitFiles.unitFileName(syntheticDatabaseId, SOURCE, 0);
    var sourceOpens = new AtomicInteger();

    try (var youTrackDB = openManager()) {
      var refusal = assertThrows(UnsupportedBackupException.class,
          () -> internalOf(youTrackDB).restore(TARGET,
              () -> List.of(repeatedName, repeatedName).iterator(),
              ignored -> {
                var sourceName = sourceOpens.getAndIncrement() == 0 ? fullName : incrementName;
                try {
                  return Files.newInputStream(syntheticBackupPath.resolve(sourceName));
                } catch (IOException failure) {
                  throw new UncheckedIOException(failure);
                }
              }, null, null));

      assertTrue("the refusal must name duplicate unit names, saw: " + refusal.getMessage(),
          refusal.getMessage().contains("duplicate unit names"));
      assertEquals("duplicate names must open no source stream", 0, sourceOpens.get());
      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One supplier restore refuses a repeated change-free increment before any prepared copy.
   *
   * <p>A valid full unit precedes one no-op increment listed twice. Replaying that shared path
   * twice previously reached complete preparation. The expected refusal opens no source and
   * changes no target.
   */
  @Test
  public void supplierRestoreRefusesARepeatedNoOpIncrementBeforeAnyCopy() throws Exception {
    var fullName = writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    var noOpName = BackupUnitFiles.writeNoOpIncrementUnit(
        syntheticBackupPath, syntheticDatabaseId, SOURCE, 1);
    var sourceOpens = new AtomicInteger();

    try (var youTrackDB = openManager()) {
      assertThrows(UnsupportedBackupException.class,
          () -> internalOf(youTrackDB).restore(TARGET,
              () -> List.of(fullName, noOpName, noOpName).iterator(),
              name -> {
                sourceOpens.incrementAndGet();
                try {
                  return Files.newInputStream(syntheticBackupPath.resolve(name));
                } catch (IOException failure) {
                  throw new UncheckedIOException(failure);
                }
              }, null, null));

      assertEquals("duplicate names must refuse before every source open", 0, sourceOpens.get());
      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One supplier restore refuses a unit name with a parent directory step.
   *
   * <p>Such a name would place one prepared copy above the request-owned temporary directory. The
   * scenario lists one name with a parent directory step.
   *
   * <p>The expected outcome has three parts.
   * The restore reports the refused name. No copy appears beside the temporary directories. No
   * database and no directory of the target name exist.
   */
  @Test
  public void supplierRestoreRefusesAUnitNameWithAParentStep() throws Exception {
    createSourceDatabaseAndBackup();
    var escapingName = "../escaped-0-" + TARGET + ".ibu";
    var escapedCopy = Path.of(System.getProperty("java.io.tmpdir")).resolve("escaped-0-"
        + TARGET + ".ibu");
    var sourceOpens = new AtomicInteger();

    try (var youTrackDB = openManager()) {
      var refusal =
          assertThrows(
              UnsupportedBackupException.class,
              () -> internalOf(youTrackDB)
                  .restore(TARGET, () -> List.of(escapingName).iterator(),
                      name -> openFirstRealUnit(name, sourceOpens), null, null));

      assertEquals("the refused name must open no source stream", 0, sourceOpens.get());
      assertTrue(
          "the refusal must name the refused unit, saw: " + refusal.getMessage(),
          refusal.getMessage().contains("no plain backup unit file name"));
      assertFalse(
          "the refused name must create no copy outside the temporary directory",
          Files.exists(escapedCopy));
      assertNoTargetExists(youTrackDB);
    }
  }

  /**
   * One failed replay removes every prepared copy of its request.
   *
   * <p>The request owns its prepared copies until the request ends. The scenario replays one
   * admitted chain whose content no replay can read.
   *
   * <p>The expected outcome has two parts. The
   * target keeps the restore-in-progress state, which proves that the replay started. No
   * temporary directory of the failed request survives.
   */
  @Test
  public void failedReplayRemovesEveryPreparedCopy() throws Exception {
    var temporaryDirectoriesBefore = temporaryChainDirectories();

    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);

      assertEquals(
          "the failed replay must remove every prepared copy",
          temporaryDirectoriesBefore,
          temporaryChainDirectories());
    }
  }

  /**
   * One observed cancellation removes every prepared copy and creates no target.
   *
   * <p>A cancelled request observes the cancellation inside one source read. The scenario sets
   * the interrupt flag of the restoring thread and fails the source read with an interrupted
   * read failure.
   *
   * <p>The expected outcome has three parts. The restore reports the failure. No
   * database and no directory of the target name exist. No temporary directory of the cancelled
   * request survives.
   */
  @Test
  public void cancelledPreparationRemovesEveryPreparedCopy() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    var source = new TrackingBackupSource(syntheticBackupPath);
    source.failEveryReadWith(() -> new InterruptedIOException("The request observed a cancel"));
    var temporaryDirectoriesBefore = temporaryChainDirectories();

    try (var youTrackDB = openManager()) {
      RuntimeException cancellation = null;
      Thread.currentThread().interrupt();
      try {
        internalOf(youTrackDB).restore(TARGET, source.names(), source.streams(), null, null);
        fail("the cancelled restore must report one failure");
      } catch (RuntimeException observedCancellation) {
        cancellation = observedCancellation;
      } finally {
        // The flag leaves the thread before any assertion, so no later file operation of this
        // test observes the cancellation.
        Thread.interrupted();
      }

      assertNotNull("the cancelled restore must report one failure", cancellation);
      assertNoTargetExists(youTrackDB);
    }

    assertTrue("every opened source stream must close", source.everyStreamClosed());
    assertEquals(
        "no temporary directory of the cancelled request may survive",
        temporaryDirectoriesBefore,
        temporaryChainDirectories());
  }

  /**
   * One failed source read removes every partial copy and creates no target.
   *
   * <p>The copy stage reads the source and writes the prepared copy. A read failure of the source
   * therefore ends the copy stage with one checked failure. The scenario fails every read of the
   * second source unit after the first unit reached its prepared copy.
   *
   * <p>The expected outcome has
   * three parts. The restore reports the failure. No database and no directory of the target name
   * exist. No temporary directory of the failed request survives.
   */
  @Test
  public void failedSourceReadRemovesEveryPartialCopyAndCreatesNoTarget() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    writeSyntheticUnit(1, false, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    var source = new TrackingBackupSource(syntheticBackupPath);
    source.failReadsOfOpenNumber(2, () -> new IOException("The source of this test fails a read"));
    var temporaryDirectoriesBefore = temporaryChainDirectories();

    try (var youTrackDB = openManager()) {
      assertThrows(
          RuntimeException.class,
          () -> internalOf(youTrackDB)
              .restore(TARGET, source.names(), source.streams(), null, null));

      assertNoTargetExists(youTrackDB);
    }

    assertEquals("the failed restore must open both units", 2, source.openedNames().size());
    assertTrue("every opened source stream must close", source.everyStreamClosed());
    assertEquals(
        "no temporary directory of the failed request may survive",
        temporaryDirectoriesBefore,
        temporaryChainDirectories());
  }

  /**
   * One failed open of a prepared copy removes every partial copy and creates no target.
   *
   * <p>The scenario blocks the prepared copy of the second unit with one directory of that name.
   * The output of that copy cannot open after the first copy completed.
   *
   * <p>The expected outcome has three parts. The restore reports the failure. No database and no
   * directory of the target name exist. No temporary directory of the failed request survives.
   */
  @Test
  public void failedPreparedCopyOpenRemovesEveryPartialCopyAndCreatesNoTarget() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    writeSyntheticUnit(1, false, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    var secondUnitName =
        BackupUnitFiles.unitFileName(syntheticDatabaseId, SOURCE, 1);
    var source = new TrackingBackupSource(syntheticBackupPath);
    // The block appears after the first prepared copy, so the second output open fails.
    source.afterEachClose(() -> blockPreparedCopyOpen(secondUnitName));
    var temporaryDirectoriesBefore = temporaryChainDirectories();

    try (var youTrackDB = openManager()) {
      assertThrows(
          RuntimeException.class,
          () -> internalOf(youTrackDB)
              .restore(TARGET, source.names(), source.streams(), null, null));

      assertNoTargetExists(youTrackDB);
    }

    assertTrue("every opened source stream must close", source.everyStreamClosed());
    assertEquals(
        "no temporary directory of the failed request may survive",
        temporaryDirectoriesBefore,
        temporaryChainDirectories());
  }

  /**
   * One checked write failure removes every partial prepared copy and changes no target.
   *
   * <p>A request-scoped decorator writes one byte to the second open output and then throws a
   * checked failure. The fault therefore proves cleanup after an output open and a partial write.
   *
   * <p>The expected outcome has four parts. Both source streams open. The write reports a failure.
   * No target appears. No temporary directory of the failed request survives.
   */
  @Test
  public void failedPreparedCopyWriteRemovesEveryPartialCopyAndCreatesNoTarget() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    writeSyntheticUnit(1, false, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    var source = new TrackingBackupSource(syntheticBackupPath);
    var openedOutputs = new AtomicInteger();
    var partialWrites = new AtomicInteger();
    var temporaryDirectoriesBefore = temporaryChainDirectories();

    try (var youTrackDB = openManager()) {
      var internal = internalOf(youTrackDB);
      internal
          .setPreparedCopyOutputDecoratorForTesting(output -> openedOutputs.incrementAndGet() == 2
              ? failingAfterOneByte(output, partialWrites)
              : output);
      RuntimeException failure;
      try {
        failure = assertThrows(RuntimeException.class,
            () -> internal.restore(TARGET, source.names(), source.streams(), null, null));
      } finally {
        internal.setPreparedCopyOutputDecoratorForTesting(null);
      }

      assertTrue("the failed copy must report a checked write failure, saw: "
          + rootMessage(failure), causeChainContains(failure, IOException.class));
      assertNoTargetExists(youTrackDB);
    }

    assertEquals("the failed write must open both source units", 2, source.openedNames().size());
    assertEquals("the failing output must receive one partial byte", 1, partialWrites.get());
    assertTrue("every opened source stream must close", source.everyStreamClosed());
    assertEquals(
        "no temporary directory of the failed request may survive",
        temporaryDirectoriesBefore,
        temporaryChainDirectories());
  }

  /**
   * One substituted symbolic link cannot replace a request-owned prepared output.
   *
   * <p>The first source close links the second destination to an owned victim. Exclusive creation
   * must refuse that destination before opening its source, preserve the victim, and change no
   * target.
   */
  @Test
  public void substitutedPreparedCopyLinkPreservesVictimAndChangesNoTarget() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    writeSyntheticUnit(1, false, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    assumeSymbolicLinksSupported();
    var victim = Files.writeString(root.resolve("prepared-copy-victim"), "preserved victim");
    var secondUnitName = BackupUnitFiles.unitFileName(syntheticDatabaseId, SOURCE, 1);
    var source = new TrackingBackupSource(syntheticBackupPath);
    source.afterEachClose(() -> linkPreparedCopyToVictim(secondUnitName, victim));

    try (var youTrackDB = openManager()) {
      assertThrows(RuntimeException.class,
          () -> internalOf(youTrackDB)
              .restore(TARGET, source.names(), source.streams(), null, null));

      assertEquals("the substituted output must preserve the victim", "preserved victim",
          Files.readString(victim));
      assertNoTargetExists(youTrackDB);
    }

    assertEquals("the refused destination must not open its source", 1,
        source.openedNames().size());
    assertTrue("every opened source stream must close", source.everyStreamClosed());
  }

  /**
   * Every unsupported chain kind refuses one supplier restore before any target change.
   *
   * <p>The four kinds are a mixed chain, unreadable output, incomplete output, and an incompatible
   * database format. The expected outcome of every kind has three parts. The restore reports the
   * unsupported chain. No database and no directory of the target name exist. Every opened source
   * stream closes.
   */
  @Test
  public void supplierRestoreRefusesEveryUnsupportedChainKind() throws Exception {
    try (var youTrackDB = openManager()) {
      for (var kind : unsupportedChainKinds()) {
        var chainDirectory = Files.createDirectories(root.resolve("supplier-" + kind.name()));
        kind.writer().write(chainDirectory);
        var source = new TrackingBackupSource(chainDirectory);

        assertThrows(
            "the " + kind.name() + " chain must refuse the supplier restore",
            UnsupportedBackupException.class,
            () -> internalOf(youTrackDB)
                .restore(TARGET, source.names(), source.streams(), null, null));

        assertNoTargetExists(youTrackDB);
        assertTrue(
            "every opened source stream of the " + kind.name() + " chain must close",
            source.everyStreamClosed());
      }
    }
  }

  /**
   * Every unsupported chain kind refuses one destructive restart before any target change.
   *
   * <p>The restart validates the complete chain before it deletes the interrupted target. The
   * expected outcome of every kind has three parts. The restart reports the unsupported chain.
   * The interrupted target keeps every file. The target still reports the interrupted-restore
   * reason.
   */
  @Test
  public void restartRefusesEveryUnsupportedChainKind() throws Exception {
    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);
      var entriesBeforeRestart = entryNames(databasesPath.resolve(TARGET));

      for (var kind : unsupportedChainKinds()) {
        var chainDirectory = Files.createDirectories(root.resolve("restart-" + kind.name()));
        kind.writer().write(chainDirectory);
        var source = new TrackingBackupSource(chainDirectory);

        assertThrows(
            "the " + kind.name() + " chain must refuse the restart",
            UnsupportedBackupException.class,
            () -> internalOf(youTrackDB)
                .restartInterruptedRestore(TARGET, source.names(), source.streams(), null, null));

        assertEquals(
            "the refused restart of the " + kind.name() + " chain must keep every file",
            entriesBeforeRestart,
            entryNames(databasesPath.resolve(TARGET)));
        assertEquals(
            "the refused restart of the " + kind.name() + " chain must keep the target state",
            StorageAdmissionException.Reason.INTERRUPTED_RESTORE,
            admissionReasonOf(TARGET));
      }
    }
  }

  /**
   * One supplier restore refuses a source unit changed concurrently during preparation.
   *
   * <p>The source starts with two supported units. Another thread replaces the second unit while
   * the restore holds the first source stream open. The restore then refuses the changed chain.
   *
   * <p>The expected outcome has three parts. The mutation runs during preparation. The restore
   * changes no target. Every opened source stream closes.
   */
  @Test
  public void supplierRestoreRefusesAUnitChangedConcurrentlyDuringPreparation() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    writeSyntheticUnit(1, false, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    var source = new TrackingBackupSource(syntheticBackupPath);
    source.afterFirstOpen(() -> replaceSecondUnitConcurrently(syntheticBackupPath));

    try (var youTrackDB = openManager()) {
      assertThrows(
          UnsupportedBackupException.class,
          () -> internalOf(youTrackDB)
              .restore(TARGET, source.names(), source.streams(), null, null));

      assertNoTargetExists(youTrackDB);
    }
    assertTrue("every opened source stream must close", source.everyStreamClosed());
  }

  /**
   * One restart refuses a source unit changed concurrently during preparation.
   *
   * <p>Another thread replaces the second unit while the restart holds the first source stream
   * open. The refusal must precede every mutation of the interrupted target.
   */
  @Test
  public void restartRefusesAUnitChangedConcurrentlyDuringPreparation() throws Exception {
    writeSyntheticUnit(0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    writeSyntheticUnit(1, false, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
        BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
        BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
    var source = new TrackingBackupSource(syntheticBackupPath);
    source.afterFirstOpen(() -> replaceSecondUnitConcurrently(syntheticBackupPath));

    try (var youTrackDB = openManager()) {
      interruptRestoreOfTarget(youTrackDB);
      var entriesBeforeRestart = entryNames(databasesPath.resolve(TARGET));

      assertThrows(
          UnsupportedBackupException.class,
          () -> internalOf(youTrackDB)
              .restartInterruptedRestore(TARGET, source.names(), source.streams(), null, null));

      assertEquals("the refused restart must keep every target file", entriesBeforeRestart,
          entryNames(databasesPath.resolve(TARGET)));
      assertEquals(StorageAdmissionException.Reason.INTERRUPTED_RESTORE,
          admissionReasonOf(TARGET));
    }
    assertTrue("every opened source stream must close", source.everyStreamClosed());
  }

  /**
   * Two restarts of one name exclude each other at the repeated eligibility check.
   *
   * <p>The scenario blocks the preparation of the first restart inside its source read. The
   * second restart then completes its own deletion, replay, and activation. The first restart
   * resumes afterwards and reaches the repeated eligibility check.
   *
   * <p>The expected outcome has three
   * parts. The first restart refuses the now active target. The database carries the content of
   * the backup. No temporary directory of either request survives.
   */
  @Test(timeout = 300_000)
  public void twoRestartsOfOneNameExcludeEachOtherAtTheFinalCheck() throws Exception {
    createSourceDatabaseAndBackup();
    var temporaryDirectoriesBefore = temporaryChainDirectories();

    try (var youTrackDB = openManager()) {
      var internal = internalOf(youTrackDB);
      interruptRestoreOfTarget(youTrackDB);
      var firstRestartPrepares = new CountDownLatch(1);
      var secondRestartFinished = new CountDownLatch(1);
      var blockedSource = new TrackingBackupSource(backupPath);
      // The block sits inside the preparation of the first restart, which holds no exclusion of
      // the manager.
      blockedSource.afterFirstOpen(
          () -> {
            firstRestartPrepares.countDown();
            awaitLatch(secondRestartFinished);
          });
      var firstRefusal = new AtomicReference<Throwable>();
      var firstRestart =
          new Thread(
              () -> {
                try {
                  internal.restartInterruptedRestore(TARGET, blockedSource.names(),
                      blockedSource.streams(), null, null);
                } catch (Throwable refusal) {
                  firstRefusal.set(refusal);
                }
              });
      firstRestart.start();
      try {
        awaitLatch(firstRestartPrepares);
        internal.restartInterruptedRestore(TARGET, backupPath.toString(), null, null);
        var activeStorage = internal.getStorage(TARGET);
        try (var activeSession = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
          // Holding the monitor after releasing preparation proves that the first restart reaches
          // the manager boundary before it can run the repeated eligibility check.
          synchronized (internal) {
            secondRestartFinished.countDown();
            awaitThreadBlockedOnOwner(firstRestart, Thread.currentThread());
          }
          joinThread(firstRestart);
          assertSame("the final manager check must preserve the existing storage registration",
              activeStorage, internal.getStorage(TARGET));
          assertTrue(
              "the final manager check must preserve the existing live session",
              activeSession.getMetadata().getSchema().existsClass(RECORD_CLASS));
        }
      } finally {
        secondRestartFinished.countDown();
        if (firstRestart.isAlive()) {
          joinThread(firstRestart);
        }
      }

      assertEquals(
          "the repeated check of the first restart must refuse the active target",
          StorageAdmissionException.Reason.RESTART_TARGET_NOT_RESTORE_IN_PROGRESS,
          admissionReason(firstRefusal.get()));
      assertEquals(
          "no temporary directory of either request may survive",
          temporaryDirectoriesBefore,
          temporaryChainDirectories());
    }
  }

  /**
   * One competing creation blocks throughout the target-changing restart phase.
   *
   * <p>The exclusion of the manager covers deletion, recreation, replay, and activation. The
   * scenario starts one competing creation after deletion, while the restart still owns that
   * exclusion.
   *
   * <p>The expected outcome has three parts. The competitor reaches the manager monitor and
   * blocks. It never observes an active database without backup content. The restart finishes with
   * the backup content.
   */
  @Test(timeout = 300_000)
  public void competingCreationBlocksThroughoutRestartTargetMutation() throws Exception {
    createSourceDatabaseAndBackup();

    try (var youTrackDB = openManager()) {
      var internal = internalOf(youTrackDB);
      interruptRestoreOfTarget(youTrackDB);
      var deletionFinished = new CountDownLatch(1);
      var competitorAtCreation = new CountDownLatch(1);
      var competitorHolder = new AtomicReference<Thread>();
      // The restart pauses after deletion while it still owns the manager monitor. The competing
      // creation must reach that monitor and block until replay and activation finish.
      internal.setRestartAfterDeletionForTesting(
          () -> {
            deletionFinished.countDown();
            awaitLatch(competitorAtCreation);
            awaitThreadBlockedOnOwner(competitorHolder.get(), Thread.currentThread());
          });
      var observedByCompetitor = new AtomicReference<String>();
      var competitorFailure = new AtomicReference<Throwable>();
      var competitor =
          new Thread(
              () -> {
                try {
                  awaitLatch(deletionFinished);
                  competitorAtCreation.countDown();
                  try {
                    youTrackDB.createIfNotExists(
                        TARGET, DatabaseType.DISK, ADMIN, PASSWORD, ADMIN);
                  } catch (RuntimeException expectedFailure) {
                    // Every refusal of the competing creation is expected here.
                  }
                  observedByCompetitor.set(observeTarget(youTrackDB));
                } catch (Throwable failure) {
                  competitorFailure.set(failure);
                }
              });
      competitorHolder.set(competitor);
      competitor.start();

      try {
        internal.restartInterruptedRestore(TARGET, backupPath.toString(), null, null);
      } finally {
        internal.setRestartAfterDeletionForTesting(null);
        deletionFinished.countDown();
        competitorAtCreation.countDown();
        joinThread(competitor);
      }

      assertEquals("the competing thread must report no unexpected failure", null,
          competitorFailure.get());
      assertNotEquals(
          "the competitor must never observe an active database without the backup content",
          "active without content",
          observedByCompetitor.get());
      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "the restarted database must carry the content of the backup",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    }
  }

  /** Reports what one competing thread observes of the target name. */
  private static String observeTarget(YouTrackDBImpl youTrackDB) {
    try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
      return session.getMetadata().getSchema().existsClass(RECORD_CLASS)
          ? "active with content"
          : "active without content";
    } catch (RuntimeException refusal) {
      return "refused";
    }
  }

  /** Awaits one latch of a deterministic test, and fails the test on every timeout. */
  private static void awaitLatch(CountDownLatch latch) {
    try {
      assertTrue("the awaited latch must open", latch.await(2, TimeUnit.MINUTES));
    } catch (InterruptedException interruption) {
      Thread.currentThread().interrupt();
      throw new AssertionError("the wait of this test was interrupted", interruption);
    }
  }

  /** Replaces the second source unit from another thread while preparation holds the first open. */
  private void replaceSecondUnitConcurrently(Path directory) {
    var failure = new AtomicReference<Throwable>();
    var finished = new CountDownLatch(1);
    var writer = new Thread(
        () -> {
          try {
            var secondUnit = BackupUnitFiles.unitFileName(syntheticDatabaseId, SOURCE, 1);
            BackupUnitFiles.writeUnreadableUnit(
                directory, syntheticDatabaseId, SOURCE, 1, secondUnit);
          } catch (Throwable writeFailure) {
            failure.set(writeFailure);
          } finally {
            finished.countDown();
          }
        });
    writer.start();
    try {
      awaitLatch(finished);
    } finally {
      joinThread(writer);
    }
    if (failure.get() != null) {
      throw new AssertionError("the concurrent source mutation must succeed", failure.get());
    }
  }

  /** Waits until one thread blocks on the monitor owned by the expected thread. */
  private static void awaitThreadBlockedOnOwner(Thread thread, Thread expectedOwner) {
    var threadBean = ManagementFactory.getThreadMXBean();
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    var blockedOnExpectedOwner = false;
    while (!blockedOnExpectedOwner && System.nanoTime() < deadline) {
      var threadInfo = threadBean.getThreadInfo(thread.threadId());
      blockedOnExpectedOwner = threadInfo != null
          && threadInfo.getThreadState() == Thread.State.BLOCKED
          && threadInfo.getLockOwnerId() == expectedOwner.threadId();
      if (!blockedOnExpectedOwner) {
        Thread.onSpinWait();
      }
    }
    assertTrue("the thread must block on the monitor owned by the expected thread",
        blockedOnExpectedOwner);
  }

  /** Joins one test thread with a bound and fails if that thread survives. */
  private static void joinThread(Thread thread) {
    try {
      thread.join(TimeUnit.SECONDS.toMillis(30));
    } catch (InterruptedException interruption) {
      Thread.currentThread().interrupt();
      throw new AssertionError("the join of this test was interrupted", interruption);
    }
    assertFalse("the test thread must finish before the join bound", thread.isAlive());
  }

  /** Names one unsupported chain kind and the writer of that chain. */
  private record ChainKind(String name, ChainWriter writer) {

  }

  /** Writes one chain of backup units into one directory. */
  private interface ChainWriter {

    void write(Path directory) throws Exception;
  }

  /**
   * Returns the four static unsupported chain kinds of the admission tests.
   *
   * <p>A mixed chain holds one supported unit and one old unit. Unreadable output carries no
   * readable header. Incomplete output carries a broken hash code. An incompatible chain names
   * another database feature format. Separate tests mutate a source while preparation runs.
   */
  private List<ChainKind> unsupportedChainKinds() {
    return List.of(
        new ChainKind("mixed",
            directory -> {
              writeUnitInto(directory, 0, true, BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
                  BackupUnitFiles.supportedFeatureFormat(),
                  BackupUnitFiles.supportedLayoutVersion(),
                  BackupUnitFiles.COMPLETED_CREATION_EVIDENCE, true);
              writeUnitInto(directory, 1, false, BackupUnitFiles.OLD_BACKUP_FORMAT_VERSION,
                  BackupUnitFiles.supportedFeatureFormat(),
                  BackupUnitFiles.supportedLayoutVersion(),
                  BackupUnitFiles.COMPLETED_CREATION_EVIDENCE, true);
            }),
        new ChainKind("unreadable",
            directory -> BackupUnitFiles.writeUnreadableUnit(directory, syntheticDatabaseId,
                SOURCE, 0)),
        new ChainKind("incomplete",
            directory -> writeUnitInto(directory, 0, true,
                BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
                BackupUnitFiles.supportedFeatureFormat(),
                BackupUnitFiles.supportedLayoutVersion(),
                BackupUnitFiles.COMPLETED_CREATION_EVIDENCE, false)),
        new ChainKind("incompatible",
            directory -> writeUnitInto(directory, 0, true,
                BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
                BackupUnitFiles.supportedFeatureFormat() + 1,
                BackupUnitFiles.supportedLayoutVersion(),
                BackupUnitFiles.COMPLETED_CREATION_EVIDENCE, true)));
  }

  /** Writes one unit of the fixed synthetic database identifier into one directory. */
  private void writeUnitInto(Path directory, int sequenceNumber, boolean fullBackup,
      int backupFormatVersion, int featureFormat, int layoutVersion, int creationEvidence,
      boolean validHash) throws IOException {
    BackupUnitFiles.writeUnit(directory, syntheticDatabaseId, SOURCE, sequenceNumber, fullBackup,
        backupFormatVersion, featureFormat, layoutVersion, creationEvidence, validHash);
  }

  /** Opens the first real backup unit and records that the source reached an open. */
  private InputStream openFirstRealUnit(String ignoredName, AtomicInteger sourceOpens) {
    sourceOpens.incrementAndGet();
    try {
      return Files.newInputStream(backupPath.resolve(realUnitNames().getFirst()));
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  /**
   * Blocks the prepared copy of one unit by one directory of the same name.
   *
   * <p>The temporary directory of the running request carries the name of the target. The open of
   * the blocked output then fails, which stands for an exhausted temporary volume.
   */
  private void blockPreparedCopyOpen(String unitName) {
    try {
      var temporaryRoot = Path.of(System.getProperty("java.io.tmpdir"));
      try (var paths = Files.list(temporaryRoot)) {
        for (var path : paths.toList()) {
          if (path.getFileName().toString().startsWith(TARGET + "-ytdb-backup")
              && Files.isDirectory(path)) {
            Files.createDirectories(path.resolve(unitName));
          }
        }
      }
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  /** Links one prepared-copy path to an owned victim after another copy finishes. */
  private void linkPreparedCopyToVictim(String unitName, Path victim) {
    try {
      var temporaryRoot = Path.of(System.getProperty("java.io.tmpdir"));
      try (var paths = Files.list(temporaryRoot)) {
        for (var path : paths.toList()) {
          if (path.getFileName().toString().startsWith(TARGET + "-ytdb-backup")
              && Files.isDirectory(path)
              && !Files.exists(path.resolve(unitName))) {
            Files.createSymbolicLink(path.resolve(unitName), victim);
          }
        }
      }
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  /** Skips one symbolic-link scenario when the temporary filesystem cannot create links. */
  private void assumeSymbolicLinksSupported() throws IOException {
    var target = Files.writeString(root.resolve("symlink-capability-target"), "probe");
    var link = root.resolve("symlink-capability-link");
    try {
      Files.createSymbolicLink(link, target);
    } catch (UnsupportedOperationException | IOException unsupported) {
      org.junit.Assume.assumeNoException(
          "this platform must support symbolic links for this scenario", unsupported);
    } finally {
      Files.deleteIfExists(link);
      Files.deleteIfExists(target);
    }
  }

  /** Returns an output that writes one byte and then fails every write with a checked failure. */
  private static OutputStream failingAfterOneByte(OutputStream output,
      AtomicInteger partialWrites) {
    return new FilterOutputStream(output) {
      @Override
      public void write(byte[] buffer, int offset, int length) throws IOException {
        if (partialWrites.get() == 0 && length > 0) {
          out.write(buffer, offset, 1);
          partialWrites.incrementAndGet();
        }
        throw new IOException("The prepared output of this test fails after one byte");
      }

      @Override
      public void write(int value) throws IOException {
        if (partialWrites.get() == 0) {
          out.write(value);
          partialWrites.incrementAndGet();
        }
        throw new IOException("The prepared output of this test fails after one byte");
      }
    };
  }

  /**
   * One failed cleanup after a successful restore keeps the restore successful.
   *
   * <p>The cleanup of the prepared copies runs after the activation of the restored database. A
   * failed cleanup therefore changes no database, and the restore reports success. The scenario
   * takes the write permission of the temporary directory of the running request, which fails
   * every later removal inside that directory.
   *
   * <p>The expected outcome has three parts. The restore
   * reports success. The restored database carries the content of the backup. The temporary
   * directory survives for a manual removal.
   */
  @Test
  public void failedCleanupAfterASuccessfulRestoreKeepsTheSuccess() throws Exception {
    createSourceDatabaseAndBackup();
    var source = new TrackingBackupSource(backupPath);
    var readOnlyDirectory = new AtomicReference<Path>();
    var permissionsEnforced = new java.util.concurrent.atomic.AtomicBoolean(true);
    // The permission change runs after the source read of the only unit. The prepared copy still
    // reaches the temporary directory, because the write of that copy uses an open file.
    source.afterEachClose(
        () -> makeRunningRequestDirectoryReadOnly(readOnlyDirectory, permissionsEnforced));

    var leftoverUnitExists = new java.util.concurrent.atomic.AtomicBoolean();
    try (var youTrackDB = openManager()) {
      internalOf(youTrackDB).restore(TARGET, source.names(), source.streams(), null, null);

      org.junit.Assume.assumeTrue(
          "this platform enforces no directory permissions for this user",
          permissionsEnforced.get());
      try (var session = youTrackDB.open(TARGET, ADMIN, PASSWORD)) {
        assertTrue(
            "the restored database must carry the content of the backup",
            session.getMetadata().getSchema().existsClass(RECORD_CLASS));
      }
    } finally {
      var leftover = readOnlyDirectory.get();
      if (leftover != null && Files.exists(leftover)) {
        try {
          leftoverUnitExists.set(Files.exists(leftover.resolve(realUnitNames().getFirst())));
          Files.setPosixFilePermissions(leftover,
              java.nio.file.attribute.PosixFilePermissions.fromString("rwx------"));
        } finally {
          FileUtils.deleteRecursively(leftover.toFile());
        }
      }
    }
    assertTrue(
        "the failed cleanup must leave the temporary directory for a manual removal",
        leftoverUnitExists.get());
  }

  /**
   * Takes the write permission of the temporary directory of the running restore request.
   *
   * <p>The probe of this method reports a platform or a user that enforces no permission. Such a
   * run cannot fail the cleanup, so the calling test skips itself.
   */
  private void makeRunningRequestDirectoryReadOnly(AtomicReference<Path> directoryHolder,
      java.util.concurrent.atomic.AtomicBoolean permissionsEnforced) {
    try {
      var temporaryRoot = Path.of(System.getProperty("java.io.tmpdir"));
      try (var paths = Files.list(temporaryRoot)) {
        for (var path : paths.toList()) {
          if (!path.getFileName().toString().startsWith(TARGET + "-ytdb-backup")
              || !Files.isDirectory(path)) {
            continue;
          }
          directoryHolder.set(path);
          Files.setPosixFilePermissions(path,
              java.nio.file.attribute.PosixFilePermissions.fromString("r-x------"));
          var probe = path.resolve("permission-probe");
          try {
            Files.createFile(probe);
            Files.delete(probe);
            permissionsEnforced.set(false);
          } catch (IOException expectedRefusal) {
            // The refused creation proves that the removal of this directory fails as well.
          }
        }
      }
    } catch (UnsupportedOperationException | IOException unsupported) {
      permissionsEnforced.set(false);
    }
  }

  /** Writes one synthetic unit of one fixed database identifier into the synthetic directory. */
  private String writeSyntheticUnit(int sequenceNumber, boolean fullBackup,
      int backupFormatVersion, int featureFormat, int layoutVersion, int creationEvidence)
      throws IOException {
    return BackupUnitFiles.writeUnit(syntheticBackupPath, syntheticDatabaseId, SOURCE,
        sequenceNumber, fullBackup, backupFormatVersion, featureFormat, layoutVersion,
        creationEvidence, true);
  }

  private final UUID syntheticDatabaseId = UUID.randomUUID();

  /**
   * Interrupts one restore of the target and leaves the restore-in-progress lifecycle state.
   *
   * <p>The chain of this interruption carries the accepted header of this build over content that
   * no replay can read. The admission therefore accepts the chain, and the replay then fails. The
   * target keeps every file and reports the interrupted-restore reason.
   */
  private void interruptRestoreOfTarget(YouTrackDBImpl youTrackDB) throws Exception {
    var interruptingChain = Files.createDirectories(root.resolve("interrupting-chain"));
    BackupUnitFiles.writeSupportedUnit(interruptingChain, UUID.randomUUID(), SOURCE, 0, true);

    assertThrows(
        RuntimeException.class,
        () -> internalOf(youTrackDB).restore(TARGET, interruptingChain.toString(), null, null));
    assertEquals(
        "the interrupted restore must leave the restore-in-progress state",
        StorageAdmissionException.Reason.INTERRUPTED_RESTORE,
        admissionReasonOf(TARGET));
  }

  /** Replaces every real backup unit by unreadable residue of the same length. */
  private void replaceEveryBackupUnitByResidue() {
    try {
      for (var unitName : realUnitNames()) {
        var unitPath = backupPath.resolve(unitName);
        var residue = new byte[(int) Files.size(unitPath)];
        Files.write(unitPath, residue);
      }
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  /** Creates the source database with one class and one record, and takes one full backup. */
  private UUID createSourceDatabaseAndBackup() {
    try (var youTrackDB = openManager()) {
      youTrackDB.create(SOURCE, DatabaseType.DISK, ADMIN, PASSWORD, ADMIN);
      try (var session = youTrackDB.open(SOURCE, ADMIN, PASSWORD)) {
        session.getMetadata().getSchema().createClass(RECORD_CLASS);
        session.begin();
        var entity = session.newEntity(RECORD_CLASS);
        entity.setProperty("value", "admitted");
        session.commit();
      }
      var storage = internalOf(youTrackDB).getStorage(SOURCE);
      assertNotNull("the database must hold one registered storage", storage);
      assertNotNull(storage.fullBackup(backupPath));
      return storage.getUuid();
    }
  }

  /** Returns the sorted names of every real backup unit of the backup directory. */
  private List<String> realUnitNames() throws IOException {
    try (var paths = Files.list(backupPath)) {
      return paths
          .map(path -> path.getFileName().toString())
          .filter(name -> name.endsWith(".ibu"))
          .sorted()
          .toList();
    }
  }

  /** Returns the sorted names of every entry of one directory. */
  private static List<String> entryNames(Path directory) throws IOException {
    try (var paths = Files.list(directory)) {
      return paths.map(path -> path.getFileName().toString()).sorted().toList();
    }
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

  /** Refuses every database and every directory of the target name. */
  private void assertNoTargetExists(YouTrackDBImpl youTrackDB) {
    assertFalse("the refused request must create no database", youTrackDB.exists(TARGET));
    assertFalse(
        "the refused request must create no directory",
        Files.exists(databasesPath.resolve(TARGET)));
  }

  private YouTrackDBImpl openManager() {
    return (YouTrackDBImpl) YourTracks.instance(databasesPath.toString());
  }

  private static YouTrackDBInternalEmbedded internalOf(YouTrackDBImpl youTrackDB) {
    return (YouTrackDBInternalEmbedded) youTrackDB.internal;
  }

  /** Returns the admission reason of one database directory, or null for an accepted image. */
  private StorageAdmissionException.Reason admissionReasonOf(String databaseName) {
    try {
      new StorageBootstrapMetadata(databasesPath.resolve(databaseName),
          new com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.FeatureFormatIdentity(
              1))
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

  /** Reports whether one cause chain contains the requested failure type. */
  private static boolean causeChainContains(Throwable failure,
      Class<? extends Throwable> failureType) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (failureType.isInstance(cause)) {
        return true;
      }
    }
    return false;
  }

  /** Joins every message of the cause chain of one failure. */
  private static String rootMessage(Throwable failure) {
    var message = new StringBuilder();
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      message.append(cause.getMessage()).append(' ');
    }
    return message.toString();
  }

  /**
   * Lists and reads one backup directory, and records every opened stream of one request.
   *
   * <p>The recorded streams prove that every cheap refusal precedes the first source read. The
   * recorded close state proves the stream lifetime of one request. One optional failure of the
   * chosen open stands for every failure of the copy stage.
   */
  private static final class TrackingBackupSource {

    private final Path directory;
    private final List<String> openedNames = new ArrayList<>();
    private final List<TrackingStream> openedStreams = new ArrayList<>();

    private int failOnOpenNumber = -1;
    private int failReadsOfOpenNumber = -1;
    private Supplier<IOException> readFailure;
    private Runnable afterEachClose = () -> {
    };
    private Runnable afterFirstOpen = () -> {
    };

    private TrackingBackupSource(Path directory) {
      this.directory = directory;
    }

    /** Fails the given open of this source, counted from one. */
    void failOnOpenNumber(int openNumber) {
      this.failOnOpenNumber = openNumber;
    }

    /**
     * Fails every read of the stream of the given open, counted from one.
     *
     * <p>The failure is one checked read failure, which is the failure form of a real source.
     */
    void failReadsOfOpenNumber(int openNumber, Supplier<IOException> failure) {
      this.failReadsOfOpenNumber = openNumber;
      this.readFailure = failure;
    }

    /** Fails every read of every stream of this source with one checked read failure. */
    void failEveryReadWith(Supplier<IOException> failure) {
      failReadsOfOpenNumber(0, failure);
    }

    /** Runs the given action after every close of one stream of this source. */
    void afterEachClose(Runnable action) {
      this.afterEachClose = action;
    }

    /** Runs the given action after the first open of one stream of this source. */
    void afterFirstOpen(Runnable action) {
      this.afterFirstOpen = action;
    }

    List<String> openedNames() {
      return List.copyOf(openedNames);
    }

    boolean everyStreamClosed() {
      return openedStreams.stream().allMatch(stream -> stream.closed);
    }

    Supplier<Iterator<String>> names() {
      return () -> {
        try (var paths = Files.list(directory)) {
          return paths
              .filter(path -> !Files.isDirectory(path))
              .map(path -> path.getFileName().toString())
              .filter(name -> name.endsWith(".ibu"))
              .sorted()
              .toList()
              .iterator();
        } catch (IOException failure) {
          throw new UncheckedIOException(failure);
        }
      };
    }

    Function<String, InputStream> streams() {
      return fileName -> {
        openedNames.add(fileName);
        if (openedNames.size() == failOnOpenNumber) {
          throw new UncheckedIOException(
              new IOException("The source of this test fails the read of " + fileName));
        }
        try {
          var failsReads =
              failReadsOfOpenNumber == 0 || openedNames.size() == failReadsOfOpenNumber;
          var stream = new TrackingStream(Files.newInputStream(directory.resolve(fileName)),
              afterEachClose, failsReads ? readFailure : null);
          openedStreams.add(stream);
          if (openedNames.size() == 1) {
            afterFirstOpen.run();
          }
          return stream;
        } catch (IOException failure) {
          throw new UncheckedIOException(failure);
        }
      };
    }
  }

  /**
   * Records the close of one source stream and runs one action after that close.
   *
   * <p>One optional read failure stands for every checked failure of one real source. The first
   * read of such a stream still succeeds, so the prepared copy of the failed request holds
   * partial content.
   */
  private static final class TrackingStream extends FilterInputStream {

    private final Runnable afterClose;
    private final Supplier<IOException> readFailure;
    private int reads;
    private boolean closed;

    private TrackingStream(InputStream delegate, Runnable afterClose,
        Supplier<IOException> readFailure) {
      super(delegate);
      this.afterClose = afterClose;
      this.readFailure = readFailure;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      if (readFailure != null && reads > 0) {
        throw readFailure.get();
      }
      reads++;
      return super.read(buffer, offset, length);
    }

    @Override
    public void close() throws IOException {
      super.close();
      closed = true;
      afterClose.run();
    }
  }
}
