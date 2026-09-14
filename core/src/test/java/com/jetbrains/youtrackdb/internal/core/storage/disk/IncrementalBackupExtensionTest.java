package com.jetbrains.youtrackdb.internal.core.storage.disk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.internal.common.io.FileUtils;
import com.jetbrains.youtrackdb.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrackdb.internal.core.db.YouTrackDBInternalEmbedded;
import com.jetbrains.youtrackdb.internal.core.exception.UnsupportedBackupException;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.AbstractStorage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers the admission of an existing backup chain before one incremental backup extends it.
 *
 * <p>A backup header is the metadata record at the tail of one backup unit file. Every supported
 * header carries the semantic database format of the backed-up database and evidence of a finished
 * creation of that database.
 *
 * <p>The extension inspects the existing chain without any change of that chain. Automatic removal
 * covers recognized incomplete output of this build alone. Every other unreadable or
 * unclassifiable unit stays in place and refuses the extension, because such a unit can hold a
 * valuable backup of another build.
 *
 * <p>Each test of this class states one scenario and one expected outcome in its own comment.
 */
public class IncrementalBackupExtensionTest {

  private static final String ADMIN = "admin";
  private static final String PASSWORD = "adminpwd";
  private static final String RECORD_CLASS = "BackedUpClass";
  private static final String SOURCE = "backupExtensionSource";

  private Path root;
  private Path databasesPath;
  private Path backupPath;

  @Before
  public void createDirectories() throws Exception {
    root = Files.createTempDirectory("backup-extension-");
    databasesPath = Files.createDirectories(root.resolve("databases"));
    backupPath = Files.createDirectories(root.resolve("backup"));
  }

  @After
  public void deleteDirectories() {
    FileUtils.deleteRecursively(root.toFile());
  }

  /**
   * One incremental backup extends a chain of supported units.
   *
   * <p>The scenario takes one full backup and then two incremental backups of the same database.
   * The second increment admits the unit below the head of the chain from its header alone. The
   * expected outcome has two parts. The backup directory holds three units. Every unit carries
   * the accepted header of this build, so every unit passes the admission of a later restore.
   */
  @Test
  public void incrementalBackupExtendsAChainOfSupportedUnits() throws Exception {
    try (var youTrackDB = openManager()) {
      var storage = createSourceDatabase(youTrackDB);
      assertNotNull(storage.fullBackup(backupPath));
      addOneRecord(youTrackDB);
      assertNotNull(storage.backup(backupPath));
      addOneRecord(youTrackDB);
      // The third unit proves that the header-only admission of the units below the head
      // accepts a supported chain.
      assertNotNull(storage.backup(backupPath));

      var units = unitNames();
      assertEquals("the chain must hold the full backup and two increments", 3, units.size());
      for (var unit : units) {
        assertNotNull(
            "every written unit must carry the accepted header of this build",
            inspectUnit(unit, storage.getUuid()).metadata());
      }
    }
  }

  /**
   * One incremental backup removes recognized incomplete trailing output.
   *
   * <p>An interrupted backup of this build can leave a complete header of this database over
   * broken content. The scenario appends such a unit after one full backup. The expected outcome
   * has three parts. The backup removes that trailing unit. The backup writes one new unit. The
   * full backup of the chain survives.
   */
  @Test
  public void incrementalBackupRemovesRecognizedIncompleteTrailingUnit() throws Exception {
    try (var youTrackDB = openManager()) {
      var storage = createSourceDatabase(youTrackDB);
      var fullBackupUnit = storage.fullBackup(backupPath);
      var incompleteUnit = writeTrailingUnit(storage.getUuid(), false);

      addOneRecord(youTrackDB);
      var newUnit = storage.backup(backupPath);

      assertFalse(
          "the backup must remove the recognized incomplete trailing unit",
          Files.exists(backupPath.resolve(incompleteUnit)));
      assertTrue("the backup must write one new unit", Files.exists(backupPath.resolve(newUnit)));
      assertTrue(
          "the full backup of the chain must survive",
          Files.exists(backupPath.resolve(fullBackupUnit)));
    }
  }

  /**
   * One incremental backup refuses an old trailing unit and changes no file.
   *
   * <p>An old header carries no semantic database format and no creation completion evidence. The
   * scenario appends such a unit after one full backup. The expected outcome has three parts. The
   * backup reports the unsupported chain. Every existing unit keeps its bytes. The backup writes no
   * new unit.
   */
  @Test
  public void incrementalBackupRefusesAnOldTrailingUnitAndKeepsEveryFile() throws Exception {
    try (var youTrackDB = openManager()) {
      var storage = createSourceDatabase(youTrackDB);
      storage.fullBackup(backupPath);
      writeTrailingUnitOfFormat(storage.getUuid(), BackupUnitFiles.OLD_BACKUP_FORMAT_VERSION,
          BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
          BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
      var contentBeforeBackup = unitContent();

      var refusal =
          assertThrows(UnsupportedBackupException.class, () -> storage.backup(backupPath));

      assertTrue(
          "the refusal must name the unsupported header, saw: " + refusal.getMessage(),
          refusal.getMessage().contains("carries no supported backup header"));
      assertEquals(
          "the refused backup must keep every existing unit unchanged",
          contentBeforeBackup,
          unitContent());
    }
  }

  /**
   * One incremental backup refuses unreadable trailing residue and changes no file.
   *
   * <p>A crash inside one backup write can leave output without any readable header. This build
   * cannot classify such output, so the output stays in place and needs an operator decision. The
   * scenario appends such residue after one full backup. The expected outcome has two parts. The
   * backup reports the unsupported chain. Every existing file keeps its bytes.
   */
  @Test
  public void incrementalBackupRefusesUnreadableTrailingResidueAndKeepsEveryFile()
      throws Exception {
    try (var youTrackDB = openManager()) {
      var storage = createSourceDatabase(youTrackDB);
      storage.fullBackup(backupPath);
      BackupUnitFiles.writeUnreadableUnit(backupPath, storage.getUuid(), SOURCE, 1,
          BackupUnitFiles.unitFileName(storage.getUuid(), SOURCE, 1,
              BackupUnitFiles.FUTURE_DATE_STAMP));
      var contentBeforeBackup = unitContent();

      assertThrows(UnsupportedBackupException.class, () -> storage.backup(backupPath));

      assertEquals(
          "the refused backup must keep every existing file unchanged",
          contentBeforeBackup,
          unitContent());
    }
  }

  /**
   * One incremental backup refuses a trailing unit of another database feature format.
   *
   * <p>A database feature format names the storage features of one database. A unit of another
   * feature format can hold a backup of a newer build, so this build never removes that unit. The
   * scenario appends such a unit after one full backup. The expected outcome has two parts. The
   * backup reports the unsupported chain. Every existing file keeps its bytes.
   */
  @Test
  public void incrementalBackupRefusesATrailingUnitOfAnotherFeatureFormat() throws Exception {
    try (var youTrackDB = openManager()) {
      var storage = createSourceDatabase(youTrackDB);
      storage.fullBackup(backupPath);
      writeTrailingUnitOfFormat(storage.getUuid(), BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
          BackupUnitFiles.supportedFeatureFormat() + 1, BackupUnitFiles.supportedLayoutVersion(),
          BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);
      var contentBeforeBackup = unitContent();

      assertThrows(UnsupportedBackupException.class, () -> storage.backup(backupPath));

      assertEquals(
          "the refused backup must keep every existing file unchanged",
          contentBeforeBackup,
          unitContent());
    }
  }

  /**
   * One incremental backup refuses a trailing unit without creation completion evidence.
   *
   * <p>Such a unit is complete output of a database without a finished creation. The unit is no
   * incomplete output of this build, so no automatic removal covers it. The scenario appends such
   * a unit after one full backup. The expected outcome has two parts. The backup reports the
   * unsupported chain. Every existing file keeps its bytes.
   */
  @Test
  public void incrementalBackupRefusesATrailingUnitWithoutCreationEvidence() throws Exception {
    try (var youTrackDB = openManager()) {
      var storage = createSourceDatabase(youTrackDB);
      storage.fullBackup(backupPath);
      writeTrailingUnitOfFormat(storage.getUuid(), BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION,
          BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
          BackupUnitFiles.ABSENT_CREATION_EVIDENCE);
      var contentBeforeBackup = unitContent();

      assertThrows(UnsupportedBackupException.class, () -> storage.backup(backupPath));

      assertEquals(
          "the refused backup must keep every existing file unchanged",
          contentBeforeBackup,
          unitContent());
    }
  }

  /**
   * One full backup still replaces every existing unit of its database.
   *
   * <p>The overwrite behavior of the full backup stays unchanged. An operator therefore migrates
   * by writing one full backup into a new empty location. The scenario writes one old unit and
   * then takes one full backup into the same directory. The expected outcome has two parts. The
   * old unit leaves. The new full backup unit is the only unit of the directory.
   */
  @Test
  public void fullBackupStillReplacesEveryExistingUnitOfItsDatabase() throws Exception {
    try (var youTrackDB = openManager()) {
      var storage = createSourceDatabase(youTrackDB);
      var oldUnit =
          writeTrailingUnitOfFormat(storage.getUuid(),
              BackupUnitFiles.OLD_BACKUP_FORMAT_VERSION,
              BackupUnitFiles.supportedFeatureFormat(), BackupUnitFiles.supportedLayoutVersion(),
              BackupUnitFiles.COMPLETED_CREATION_EVIDENCE);

      var newUnit = storage.fullBackup(backupPath);

      assertFalse(
          "the full backup must replace the existing unit",
          Files.exists(backupPath.resolve(oldUnit)));
      assertEquals("the directory must hold the new full backup alone", List.of(newUnit),
          unitNames());
    }
  }

  /**
   * One incremental backup refuses an unsupported unit below a supported chain head.
   *
   * <p>The inspection covers every existing unit, not the trailing units alone. An old unit below
   * one supported increment therefore refuses the extension. The scenario replaces the full
   * backup of one two-unit chain by an old unit and keeps the supported increment as the newest
   * unit.
   *
   * <p>The expected outcome has three parts. The backup reports the unsupported chain. Every
   * existing unit keeps its bytes. The backup writes no new unit.
   */
  @Test
  public void incrementalBackupRefusesAnUnsupportedUnitBelowASupportedHead() throws Exception {
    try (var youTrackDB = openManager()) {
      var storage = createSourceDatabase(youTrackDB);
      var fullBackupUnit = storage.fullBackup(backupPath);
      addOneRecord(youTrackDB);
      var incrementUnit = storage.backup(backupPath);
      // The old unit takes the name and the position of the full backup, so the newest unit of
      // the chain stays supported.
      BackupUnitFiles.writeUnit(backupPath, storage.getUuid(), SOURCE, 0, true,
          BackupUnitFiles.OLD_BACKUP_FORMAT_VERSION, BackupUnitFiles.supportedFeatureFormat(),
          BackupUnitFiles.supportedLayoutVersion(), BackupUnitFiles.COMPLETED_CREATION_EVIDENCE,
          true, fullBackupUnit);
      var contentBeforeBackup = unitContent();

      var refusal =
          assertThrows(UnsupportedBackupException.class, () -> storage.backup(backupPath));

      assertTrue(
          "the refusal must name the unsupported unit " + fullBackupUnit + ", saw: "
              + refusal.getMessage(),
          refusal.getMessage().contains(fullBackupUnit));
      assertEquals(
          "the refused backup must keep every existing unit unchanged",
          contentBeforeBackup,
          unitContent());
      assertEquals(
          "the refused backup must write no new unit",
          List.of(fullBackupUnit, incrementUnit).stream().sorted().toList(),
          unitNames());
    }
  }

  /**
   * One incremental backup refuses an authentic version 2 unit below a supported chain head.
   *
   * <p>A unit of the earlier release carries a shorter header tail, and its stored hash code
   * still matches its content. Such a unit can hold a valuable backup, so no automatic removal
   * covers it. The scenario replaces the full backup of one two-unit chain by an authentic
   * version 2 unit.
   *
   * <p>The expected outcome has two parts. The backup reports the unsupported chain.
   * Every existing unit keeps its bytes.
   */
  @Test
  public void incrementalBackupRefusesAnAuthenticVersion2UnitBelowASupportedHead()
      throws Exception {
    try (var youTrackDB = openManager()) {
      var storage = createSourceDatabase(youTrackDB);
      var fullBackupUnit = storage.fullBackup(backupPath);
      addOneRecord(youTrackDB);
      storage.backup(backupPath);
      BackupUnitFiles.writeLegacyVersion2Unit(backupPath, storage.getUuid(), SOURCE, 0, true,
          fullBackupUnit);
      var contentBeforeBackup = unitContent();

      assertThrows(UnsupportedBackupException.class, () -> storage.backup(backupPath));

      assertEquals(
          "the refused backup must keep every existing unit unchanged",
          contentBeforeBackup,
          unitContent());
    }
  }

  /**
   * One incremental backup inspects the whole chain before it removes any trailing output.
   *
   * <p>The removal of recognized incomplete output follows the complete inspection. The scenario
   * combines one unclassifiable full backup with one recognized incomplete trailing unit. The
   * expected outcome has three parts. The backup reports the unsupported chain. The trailing unit
   * survives, which proves that no removal precedes the refusal. Every existing unit keeps its
   * bytes.
   */
  @Test
  public void incrementalBackupKeepsRecognizedIncompleteOutputOfARefusedChain() throws Exception {
    try (var youTrackDB = openManager()) {
      var storage = createSourceDatabase(youTrackDB);
      var fullBackupUnit = storage.fullBackup(backupPath);
      // The full backup becomes unreadable residue, which this build cannot classify.
      BackupUnitFiles.writeUnreadableUnit(backupPath, storage.getUuid(), SOURCE, 0,
          fullBackupUnit);
      var incompleteUnit = writeTrailingUnit(storage.getUuid(), false);
      var contentBeforeBackup = unitContent();

      assertThrows(UnsupportedBackupException.class, () -> storage.backup(backupPath));

      assertTrue(
          "the refused backup must keep the recognized incomplete trailing unit",
          Files.exists(backupPath.resolve(incompleteUnit)));
      assertEquals(
          "the refused backup must keep every existing unit unchanged",
          contentBeforeBackup,
          unitContent());
    }
  }

  /** Writes one trailing unit of this build with a chosen validity of its hash code. */
  private String writeTrailingUnit(UUID databaseId, boolean validHash) throws IOException {
    return BackupUnitFiles.writeUnit(backupPath, databaseId, SOURCE, 1, false,
        BackupUnitFiles.CURRENT_BACKUP_FORMAT_VERSION, BackupUnitFiles.supportedFeatureFormat(),
        BackupUnitFiles.supportedLayoutVersion(), BackupUnitFiles.COMPLETED_CREATION_EVIDENCE,
        validHash,
        BackupUnitFiles.unitFileName(databaseId, SOURCE, 1, BackupUnitFiles.FUTURE_DATE_STAMP));
  }

  /** Writes one trailing unit with a chosen header and a valid hash code. */
  private String writeTrailingUnitOfFormat(UUID databaseId, int backupFormatVersion,
      int featureFormat, int layoutVersion, int creationEvidence) throws IOException {
    return BackupUnitFiles.writeUnit(backupPath, databaseId, SOURCE, 1, false,
        backupFormatVersion, featureFormat, layoutVersion, creationEvidence, true,
        BackupUnitFiles.unitFileName(databaseId, SOURCE, 1, BackupUnitFiles.FUTURE_DATE_STAMP));
  }

  /** Inspects one unit of the backup directory. */
  private DiskStorage.BackupUnitInspection inspectUnit(String unitName, UUID databaseId)
      throws IOException {
    try (var stream = Files.newInputStream(backupPath.resolve(unitName))) {
      return DiskStorage.inspectBackupUnit(unitName, SOURCE, databaseId, stream, null);
    }
  }

  /** Returns the sorted names of every backup unit of the backup directory. */
  private List<String> unitNames() throws IOException {
    try (var paths = Files.list(backupPath)) {
      return paths
          .map(path -> path.getFileName().toString())
          .filter(name -> name.endsWith(".ibu"))
          .sorted()
          .toList();
    }
  }

  /**
   * Returns one digest of every backup unit of the backup directory, keyed by file name.
   *
   * <p>The digest keeps one failure message short. The backup lock file of the directory stays out
   * of the result, because every backup of one directory opens that file.
   */
  private Map<String, String> unitContent() throws Exception {
    var content = new HashMap<String, String>();
    var digest = java.security.MessageDigest.getInstance("SHA-256");
    try (var paths = Files.list(backupPath)) {
      for (var path : paths.toList()) {
        var name = path.getFileName().toString();
        if (!name.endsWith(".ibu")) {
          continue;
        }
        content.put(name,
            java.util.Base64.getEncoder()
                .encodeToString(digest.digest(Files.readAllBytes(path))));
      }
    }
    return content;
  }

  private YouTrackDBImpl openManager() {
    return (YouTrackDBImpl) YourTracks.instance(databasesPath.toString());
  }

  /** Creates the source database with one class and one record, and returns its storage. */
  private AbstractStorage createSourceDatabase(YouTrackDBImpl youTrackDB) {
    youTrackDB.create(SOURCE, DatabaseType.DISK, ADMIN, PASSWORD, ADMIN);
    try (var session = youTrackDB.open(SOURCE, ADMIN, PASSWORD)) {
      session.getMetadata().getSchema().createClass(RECORD_CLASS);
    }
    addOneRecord(youTrackDB);
    var storage =
        ((YouTrackDBInternalEmbedded) youTrackDB.internal).getStorage(SOURCE);
    assertNotNull("the database must hold one registered storage", storage);
    return storage;
  }

  /** Adds one record to the source database, so the next increment carries changes. */
  private void addOneRecord(YouTrackDBImpl youTrackDB) {
    try (var session = youTrackDB.open(SOURCE, ADMIN, PASSWORD)) {
      session.begin();
      var entity = session.newEntity(RECORD_CLASS);
      entity.setProperty("value", "backed up");
      session.commit();
    }
  }
}
