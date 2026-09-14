/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */

package com.jetbrains.youtrackdb.internal.core.storage.disk;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.internal.common.collection.closabledictionary.ClosableLinkedContainer;
import com.jetbrains.youtrackdb.internal.common.directmemory.ByteBufferPool;
import com.jetbrains.youtrackdb.internal.common.io.FileUtils;
import com.jetbrains.youtrackdb.internal.common.io.IOUtils;
import com.jetbrains.youtrackdb.internal.common.log.LogManager;
import com.jetbrains.youtrackdb.internal.common.parser.SystemVariableResolver;
import com.jetbrains.youtrackdb.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.LongSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.ShortSerializer;
import com.jetbrains.youtrackdb.internal.core.YouTrackDBConstants;
import com.jetbrains.youtrackdb.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrackdb.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.SharedContext;
import com.jetbrains.youtrackdb.internal.core.db.YouTrackDBInternalEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.engine.local.EngineLocalPaginated;
import com.jetbrains.youtrackdb.internal.core.exception.BackupInProgressException;
import com.jetbrains.youtrackdb.internal.core.exception.BaseException;
import com.jetbrains.youtrackdb.internal.core.exception.DatabaseException;
import com.jetbrains.youtrackdb.internal.core.exception.InvalidStorageEncryptionKeyException;
import com.jetbrains.youtrackdb.internal.core.exception.SecurityException;
import com.jetbrains.youtrackdb.internal.core.exception.StorageException;
import com.jetbrains.youtrackdb.internal.core.exception.UnsupportedBackupException;
import com.jetbrains.youtrackdb.internal.core.id.RecordIdInternal;
import com.jetbrains.youtrackdb.internal.core.index.engine.IndexHistogramManager;
import com.jetbrains.youtrackdb.internal.core.index.engine.v1.BTreeMultiValueIndexEngine;
import com.jetbrains.youtrackdb.internal.core.storage.ChecksumMode;
import com.jetbrains.youtrackdb.internal.core.storage.cache.ReadCache;
import com.jetbrains.youtrackdb.internal.core.storage.cache.local.WOWCache;
import com.jetbrains.youtrackdb.internal.core.storage.cache.local.doublewritelog.DoubleWriteLog;
import com.jetbrains.youtrackdb.internal.core.storage.cache.local.doublewritelog.DoubleWriteLogGL;
import com.jetbrains.youtrackdb.internal.core.storage.cache.local.doublewritelog.DoubleWriteLogNoOP;
import com.jetbrains.youtrackdb.internal.core.storage.collection.CollectionPositionMap;
import com.jetbrains.youtrackdb.internal.core.storage.collection.v2.FreeSpaceMap;
import com.jetbrains.youtrackdb.internal.core.storage.config.CollectionBasedStorageConfiguration;
import com.jetbrains.youtrackdb.internal.core.storage.fs.File;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.AbstractStorage;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.StartupMetadata;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.FeatureFormatIdentity;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.LineageFloorAdoption;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageAdmissionException;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageBootstrapMetadata;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageIdentity;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageLineageIdentity;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageStartupMetadata;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.atomicoperations.operationsfreezer.FreezeKind;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.base.DurablePage;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.LogSequenceNumber;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.WriteAheadLog;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.cas.CASDiskWriteAheadLog;
import com.jetbrains.youtrackdb.internal.core.storage.ridbag.AbsoluteChange;
import com.jetbrains.youtrackdb.internal.core.storage.ridbag.LinkCollectionsBTreeManagerShared;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.io.file.PathUtils;
import org.apache.commons.io.output.ProxyOutputStream;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskStorage extends AbstractStorage {

  public static final XXHash64 XX_HASH_64 = XXHashFactory.fastestInstance().hash64();
  public static final long XX_HASH_SEED = 0xAEF5634;

  private static final Logger logger = LoggerFactory.getLogger(DiskStorage.class);
  private static final AtomicBoolean fsyncWarningLogged = new AtomicBoolean();

  private static final String BACKUP_LOCK = "backup.ibl";

  private static final String ALGORITHM_NAME = "AES";
  private static final String TRANSFORMATION = "AES/CTR/NoPadding";

  private static final ThreadLocal<Cipher> CIPHER =
      ThreadLocal.withInitial(DiskStorage::getCipherInstance);

  static final String IBU_EXTENSION = ".ibu";
  /// The metadata is located at the tail and has the following format:
  ///
  /// 1. Version of the backup metadata format is stored as a short value.
  /// 2. Database UUID (generated during database creation) is stored as two long values with 16
  /// bytes.
  /// 3. Number of the backup unit in sequence (stored as an int value).
  /// 4. Start LSN - value of LSN (exclusive) from which delta changes in the backup are stored. It
  /// is stored as long + int values, stored as (-1, -1) for the first (full) backup.
  /// 5. End LSN - the value of the last change stored in the backup (inclusive).
  /// 6. Last transaction ID (idGen counter) at the time of backup. Stored as a long value.
  /// 7. Feature format version of the backed-up database. Stored as an int value. A feature format
  /// names the storage features of one database.
  /// 8. Storage layout version of the backed-up database. Stored as an int value. A storage layout
  /// version names the on-disk layout of one database.
  /// 9. Creation completion evidence of the backed-up database. Stored as an int value. The value
  /// [#CREATION_COMPLETED_EVIDENCE] states a finished creation, and every other value states
  /// absent evidence.
  /// 10. The hash code of the file's content. Stored as a long value. The XX_HASH algorithm is used
  /// for hash code calculation.
  private static final int IBU_METADATA_SIZE =
      Short.BYTES + 2 * Long.BYTES + Integer.BYTES + 2 * (Long.BYTES + Integer.BYTES)
          + Long.BYTES + 3 * Integer.BYTES + Long.BYTES;

  private static final int IBU_METADATA_VERSION_OFFSET = 0;

  private static final int IBU_METADATA_UUID_LOW_OFFSET = IBU_METADATA_VERSION_OFFSET + Short.BYTES;
  private static final int IBU_METADATA_UUID_HIGH_OFFSET =
      IBU_METADATA_UUID_LOW_OFFSET + Long.BYTES;

  private static final int IBU_METADATA_SEQUENCE_OFFSET =
      IBU_METADATA_UUID_HIGH_OFFSET + Long.BYTES;

  private static final int IBU_METADATA_FIRST_LSN_SEGMENT_OFFSET =
      IBU_METADATA_SEQUENCE_OFFSET + Integer.BYTES;
  private static final int IBU_METADATA_START_LSN_POSITION_OFFSET =
      IBU_METADATA_FIRST_LSN_SEGMENT_OFFSET + Long.BYTES;

  private static final int IBU_METADATA_LAST_LSN_SEGMENT_OFFSET =
      IBU_METADATA_START_LSN_POSITION_OFFSET + Integer.BYTES;
  private static final int IBU_METADATA_LAST_LSN_POSITION_OFFSET =
      IBU_METADATA_LAST_LSN_SEGMENT_OFFSET + Long.BYTES;
  private static final int IBU_METADATA_LAST_TX_ID_OFFSET =
      IBU_METADATA_LAST_LSN_POSITION_OFFSET + Integer.BYTES;
  private static final int IBU_METADATA_FEATURE_FORMAT_OFFSET =
      IBU_METADATA_LAST_TX_ID_OFFSET + Long.BYTES;
  private static final int IBU_METADATA_LAYOUT_VERSION_OFFSET =
      IBU_METADATA_FEATURE_FORMAT_OFFSET + Integer.BYTES;
  private static final int IBU_METADATA_CREATION_EVIDENCE_OFFSET =
      IBU_METADATA_LAYOUT_VERSION_OFFSET + Integer.BYTES;
  private static final int IBU_METADATA_HASH_CODE_OFFSET =
      IBU_METADATA_CREATION_EVIDENCE_OFFSET + Integer.BYTES;

  /**
   * The backup metadata format version of this build.
   *
   * <p>Version 3 added the semantic database format and the creation completion evidence. This
   * build reads and writes that version only, so every earlier unit stays unsupported.
   */
  static final int CURRENT_BACKUP_FORMAT_VERSION = 3;

  /**
   * States that the creation of the backed-up database finished.
   *
   * <p>The writer stores this value only for a source database that carries the durable genesis
   * completion marker. The value is one fixed marker, so an absent or truncated field never passes
   * as accepted creation completion evidence.
   */
  static final int CREATION_COMPLETED_EVIDENCE = 0x47454E43;

  /** States that the backed-up database carries no creation completion evidence. */
  static final int CREATION_EVIDENCE_ABSENT = 0;

  private static final String CONF_ENTRY_NAME = "database.ocf";
  static final String BACKUP_DATEFORMAT = "yyyy-MM-dd-HH-mm-ss";
  private static final String CONF_UTF_8_ENTRY_NAME = "database_utf8.ocf";
  private static final int UUID_LENGTH = 36;

  private static final String ENCRYPTION_IV = "encryption.iv";

  protected static final long IV_SEED = 234120934;

  private static final String IV_EXT = ".iv";
  protected static final String IV_NAME = "data" + IV_EXT;

  private static final String[] ALL_FILE_EXTENSIONS = {
      ".cm",
      ".ocf",
      ".pls",
      ".pcl",
      ".oda",
      ".odh",
      ".otx",
      ".ocs",
      ".oef",
      ".oem",
      ".oet",
      ".fl",
      ".flb",
      IV_EXT,
      CASDiskWriteAheadLog.WAL_SEGMENT_EXTENSION,
      CASDiskWriteAheadLog.MASTER_RECORD_EXTENSION,
      CollectionPositionMap.DEF_EXTENSION,
      LinkCollectionsBTreeManagerShared.FILE_EXTENSION,
      CollectionBasedStorageConfiguration.MAP_FILE_EXTENSION,
      CollectionBasedStorageConfiguration.DATA_FILE_EXTENSION,
      CollectionBasedStorageConfiguration.TREE_DATA_FILE_EXTENSION,
      CollectionBasedStorageConfiguration.TREE_NULL_FILE_EXTENSION,
      BTreeMultiValueIndexEngine.DATA_FILE_EXTENSION,
      BTreeMultiValueIndexEngine.NULL_BUCKET_FILE_EXTENSION,
      BTreeMultiValueIndexEngine.M_CONTAINER_EXTENSION,
      IndexHistogramManager.IXS_EXTENSION,
      DoubleWriteLogGL.EXTENSION,
      FreeSpaceMap.DEF_EXTENSION,
      ".bsm",
      ".bsm.tmp",
      ".bsml"
  };

  private static final int ONE_KB = 1024;

  private final int deleteMaxRetries;
  private final int deleteWaitTime;

  private static final FeatureFormatIdentity FEATURE_FORMAT = new FeatureFormatIdentity(1);

  private final StorageStartupMetadata startupMetadata;
  @Nullable private StorageBootstrapMetadata bootstrapMetadata;
  @Nullable private StorageBootstrapMetadata.Snapshot bootstrapSnapshot;

  private final Path storagePath;
  private final ClosableLinkedContainer<Long, File> files;

  private Future<?> fuzzyCheckpointTask;
  private Future<?> recordsGcTask;

  private final long walMaxSegSize;
  private final long doubleWriteLogMaxSegSize;

  protected volatile byte[] iv;

  public DiskStorage(
      final String name,
      final String filePath,
      final int id,
      final ReadCache readCache,
      final ClosableLinkedContainer<Long, File> files,
      final long walMaxSegSize,
      long doubleWriteLogMaxSegSize,
      YouTrackDBInternalEmbedded context) {
    super(name, filePath, id, context, false);

    this.walMaxSegSize = walMaxSegSize;
    this.files = files;
    this.doubleWriteLogMaxSegSize = doubleWriteLogMaxSegSize;
    this.readCache = readCache;

    final var sp =
        SystemVariableResolver.resolveSystemVariables(
            FileUtils.getPath(new java.io.File(url).getPath()));

    storagePath = Paths.get(sp).normalize().toAbsolutePath();

    deleteMaxRetries = GlobalConfiguration.FILE_DELETE_RETRY.getValueAsInteger();
    deleteWaitTime = GlobalConfiguration.FILE_DELETE_DELAY.getValueAsInteger();

    startupMetadata =
        new StorageStartupMetadata(
            storagePath.resolve("dirty.fl"), storagePath.resolve("dirty.flb"));
  }

  @SuppressWarnings("CanBeFinal")
  @Override
  public void create(final ContextConfiguration contextConfiguration) {
    try {
      stateLock.writeLock().lock();
      try {
        doCreate(contextConfiguration);
      } finally {
        stateLock.writeLock().unlock();
      }
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }

    var fsyncAfterCreate =
        contextConfiguration.getValueAsBoolean(
            GlobalConfiguration.STORAGE_MAKE_FULL_CHECKPOINT_AFTER_CREATE);
    if (fsyncAfterCreate) {
      synch();
    }

    final var additionalArgs = new Object[] {getURL(), YouTrackDBConstants.getVersion()};
    LogManager.instance()
        .info(this, "Storage '%s' is created under YouTrackDB distribution : %s", additionalArgs);
  }

  @Override
  protected void doCreate(ContextConfiguration contextConfiguration)
      throws IOException, java.lang.InterruptedException {
    final var storageFolder = storagePath;
    if (!Files.exists(storageFolder)) {
      Files.createDirectories(storageFolder);
    }

    super.doCreate(contextConfiguration);
  }

  @Override
  protected void prepareStorageBirth() {
    try {
      var birth = bootstrapMetadata().createBirth(StorageIdentity.random(),
          StorageLineageIdentity.random());
      bootstrapSnapshot = birth;
      updateStorageIdentity(birth.storageIdentity(), birth.lineageIdentity());
    } catch (IOException exception) {
      throw BaseException.wrapException(
          new StorageException(name, "Cannot publish the storage bootstrap birth"),
          exception,
          name);
    }
  }

  /**
   * Forces every genesis artifact of this disk image to durable media.
   *
   * <p>The barrier flushes dirty index histogram data first. An index histogram is index
   * statistics data that the index engine keeps in a separate file. The barrier then flushes every
   * index engine. The barrier then flushes the write-ahead log. The barrier then flushes the write
   * cache, which synchronizes every storage file.
   *
   * <p>The plain flush helper {@code flushAllData} is insufficient alone. That helper skips index
   * histogram data by contract. The barrier therefore uses the wider path.
   *
   * <p>The barrier fails closed. A storage in the internal error state and a failed flush both
   * reach the caller. The creation path therefore publishes no active lifecycle state over content
   * that never reached durable media.
   */
  @Override
  protected void barrierOverGenesisArtifacts() {
    barrierWithOwnStateLock();
  }

  @Override
  protected void activateStorageBirth() {
    activateBootstrapSnapshot("Cannot activate the storage bootstrap birth");
  }

  /**
   * Publishes the restore-in-progress lifecycle state of this fresh restore target.
   *
   * <p>The restore path calls this method directly after the creation of the storage files. The
   * creation published the birth-in-progress lifecycle state, and this method replaces that state
   * without any activation. A restore target therefore never appears as an empty active database.
   */
  @Override
  public void beginRestoreLifecycle() {
    try {
      var current = java.util.Objects.requireNonNull(bootstrapSnapshot, "bootstrapSnapshot");
      bootstrapSnapshot = bootstrapMetadata().beginRestoreFromBirth(current);
    } catch (IOException exception) {
      throw BaseException.wrapException(
          new StorageException(name, "Cannot publish the restore-in-progress lifecycle state"),
          exception,
          name);
    }
  }

  /**
   * Deletes one interrupted restore target inside one exclusive unit.
   *
   * <p>A destructive restart is the full deletion of an interrupted restore target and a fresh
   * restore. This method performs the deletion part of that restart. The acceptance check and the
   * deletion run under the authority lock of the target, so two concurrent restarts never destroy
   * a healthy restored database. Before deletion, the unit attempts the existing normal database
   * lock without waiting and holds an acquired lock through content deletion. The deletion removes
   * every content file first and removes the authority copies last.
   *
   * <p>An absent target directory carries no evidence of an interrupted restore, so this method
   * refuses an absent target directory.
   *
   * @param storagePath the directory of the restore target
   * @throws StorageAdmissionException when the target is no interrupted restore target
   */
  public static void deleteInterruptedRestoreTarget(Path storagePath) throws IOException {
    requireExistingRestoreTargetDirectory(storagePath);
    new StorageBootstrapMetadata(storagePath, FEATURE_FORMAT)
        .deleteInterruptedRestoreTarget(DiskStorage::deleteRestoreTargetContent);
  }

  /**
   * Reports whether this image already holds content that a creation must not overwrite.
   *
   * <p>The deletion of a destructive restart keeps the authority lock file of the target. A
   * directory whose only entry is that lock file therefore holds no storage content, and a
   * creation over that directory overwrites nothing. The existence probe of the manager still
   * reports such a directory as one database, so the birth residue stays visible to an operator.
   */
  @Override
  protected boolean existsBeforeCreation() {
    try {
      if (status == STATUS.CLOSED
          && StorageBootstrapMetadata.holdsOnlyTheAuthorityLockFile(storagePath)) {
        return false;
      }
    } catch (IOException listingFailure) {
      throw BaseException.wrapException(
          new StorageException(name, "Cannot list the files of the storage directory"),
          listingFailure,
          name);
    }
    return exists();
  }

  /**
   * Checks that one directory is a valid destructive restart target and changes no content.
   *
   * <p>The caller runs this check before the caller discards any in-memory state of the named
   * database. A refused restart therefore keeps every file and every live session of a healthy
   * database. The deletion repeats the same check inside its own exclusive unit.
   *
   * <p>The check requires positive evidence on disk. An absent target directory holds no such
   * evidence, so the check refuses an absent target directory. The caller therefore never discards
   * the in-memory state of a database that the named directory does not back. One example of such
   * a database is a memory database of the same name.
   *
   * @param storagePath the directory of the restore target
   * @throws StorageAdmissionException when the target is no interrupted restore target
   */
  public static void requireInterruptedRestoreTarget(Path storagePath) throws IOException {
    requireExistingRestoreTargetDirectory(storagePath);
    new StorageBootstrapMetadata(storagePath, FEATURE_FORMAT).requireInterruptedRestoreTarget();
  }

  /**
   * Refuses a restore target whose directory exists nowhere.
   *
   * <p>The destructive restart reads the evidence of an interrupted restore from the target
   * directory. An absent directory holds no bootstrap authority record, so the restart reports the
   * missing-record admission reason for an absent directory.
   *
   * @param storagePath the directory of the restore target
   * @throws StorageAdmissionException when the directory exists nowhere
   */
  private static void requireExistingRestoreTargetDirectory(Path storagePath) throws IOException {
    if (isRealDirectory(storagePath)) {
      return;
    }
    throw new StorageAdmissionException(
        StorageAdmissionException.Reason.AUTHORITY_MISSING,
        storagePath,
        "The destructive restore restart requires an existing target directory");
  }

  /**
   * Reports whether the given path is a real directory.
   *
   * <p>The check reads the attributes of the path itself and follows no symbolic link. A symbolic
   * link named like a database would otherwise redirect a deletion outside of the databases
   * directory.
   *
   * @return false when the path exists nowhere
   * @throws IOException when the path exists and is no real directory
   */
  private static boolean isRealDirectory(Path storagePath) throws IOException {
    final BasicFileAttributes attributes;
    try {
      attributes =
          Files.readAttributes(storagePath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    } catch (NoSuchFileException absentPath) {
      return false;
    }
    if (!attributes.isDirectory()) {
      throw new IOException(
          "The destructive restore restart accepts a real directory only, and the path "
              + storagePath
              + " is no real directory");
    }
    return true;
  }

  /** Removes every file of one storage directory that is no bootstrap authority artifact. */
  private static void deleteRestoreTargetContent(Path storageDirectory) throws IOException {
    try (var entries = Files.list(storageDirectory)) {
      for (var entry : entries.toList()) {
        if (StorageBootstrapMetadata.isBootstrapArtifactName(
            entry.getFileName().toString())) {
          // The authority copies and the authority lock file leave after every content file.
          continue;
        }
        if (Files.isDirectory(entry)) {
          PathUtils.deleteDirectory(entry);
        } else {
          Files.deleteIfExists(entry);
        }
      }
    }
  }

  /**
   * Runs the single admission decision of this storage image.
   *
   * <p>Storage open calls this method before write-ahead log initialization and before recovery. A
   * write-ahead log records a change before that change reaches its final file. A rejected image
   * therefore never reaches recovery.
   */
  @Override
  protected void readStorageIdentity() {
    try {
      var active = bootstrapMetadata().readActiveRequired();
      bootstrapSnapshot = active;
      updateStorageIdentity(active.storageIdentity(), active.lineageIdentity());
    } catch (StorageAdmissionException rejection) {
      // The cause already names the admission reason and the storage path. This message therefore
      // states the cause in plain words and never repeats the reason identifier.
      throw BaseException.wrapException(
          new StorageException(
              name,
              "Cannot open database '" + name + "'. " + rejection.reason().description()),
          rejection,
          name);
    } catch (IOException exception) {
      throw BaseException.wrapException(
          new StorageException(name, "Cannot read the storage bootstrap authority"),
          exception,
          name);
    }
  }

  private StorageBootstrapMetadata bootstrapMetadata() throws IOException {
    if (bootstrapMetadata == null) {
      bootstrapMetadata = new StorageBootstrapMetadata(storagePath, FEATURE_FORMAT);
    }
    return bootstrapMetadata;
  }

  void activateBootstrapSnapshot(String errorMessage) {
    try {
      var active = bootstrapMetadata().activate(
          java.util.Objects.requireNonNull(bootstrapSnapshot, "bootstrapSnapshot"));
      bootstrapSnapshot = active;
      updateStorageIdentity(active.storageIdentity(), active.lineageIdentity());
    } catch (IOException exception) {
      throw BaseException.wrapException(new StorageException(name, errorMessage), exception, name);
    }
  }

  @Override
  public final boolean exists() {
    try {
      if (status == STATUS.OPEN || isInError() || status == STATUS.MIGRATION) {
        return true;
      }

      return exists(storagePath);
    } catch (final RuntimeException e) {
      throw logAndPrepareForRethrow(e);
    } catch (final Error e) {
      throw logAndPrepareForRethrow(e, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public String getURL() {
    return EngineLocalPaginated.NAME + ":" + url;
  }

  public final Path getStoragePath() {
    return storagePath;
  }

  @Override
  public String getType() {
    return EngineLocalPaginated.NAME;
  }

  @Override
  protected LogSequenceNumber copyWALToBackup(
      final ZipOutputStream zipOutputStream, final long startSegment) throws IOException {

    java.io.File[] nonActiveSegments;

    LogSequenceNumber lastLSN;
    // A TRANSIENT self-quiesce (the incremental-backup WAL copy): bounded by the copy body, so
    // schema commits may park behind it exactly like data commits.
    final var freezeId =
        getAtomicOperationsManager().freezeWriteOperations(FreezeKind.TRANSIENT_QUIESCE, null);
    try {
      lastLSN = writeAheadLog.end();
      writeAheadLog.flush();
      writeAheadLog.appendNewSegment();
      nonActiveSegments = writeAheadLog.nonActiveSegments(startSegment);
    } finally {
      getAtomicOperationsManager().unfreezeWriteOperations(freezeId);
    }

    for (final var nonActiveSegment : nonActiveSegments) {
      try (final var fileInputStream = new FileInputStream(nonActiveSegment)) {
        try (final var bufferedInputStream =
            new BufferedInputStream(fileInputStream)) {
          final var entry = new ZipEntry(nonActiveSegment.getName());
          zipOutputStream.putNextEntry(entry);
          try {
            final var buffer = new byte[4096];

            int br;

            while ((br = bufferedInputStream.read(buffer)) >= 0) {
              zipOutputStream.write(buffer, 0, br);
            }
          } finally {
            zipOutputStream.closeEntry();
          }
        }
      }
    }

    return lastLSN;
  }

  @Override
  protected java.io.File createWalTempDirectory() {
    final var walDirectory =
        new java.io.File(storagePath.toFile(), "walIncrementalBackupRestoreDirectory");

    if (walDirectory.exists()) {
      FileUtils.deleteRecursively(walDirectory);
    }

    if (!walDirectory.mkdirs()) {
      throw new StorageException(name,
          "Can not create temporary directory to store files created during incremental backup");
    }

    return walDirectory;
  }

  private static void addFileToDirectory(final String name, final InputStream stream,
      final java.io.File directory)
      throws IOException {
    final var buffer = new byte[4096];

    var rb = -1;
    var bl = 0;

    final var walBackupFile = new java.io.File(directory, name);
    if (!walBackupFile.toPath().normalize().startsWith(directory.toPath().normalize())) {
      throw new IllegalStateException("Bad zip entry " + name);
    }

    try (final var outputStream = new FileOutputStream(walBackupFile)) {
      try (final var bufferedOutputStream =
          new BufferedOutputStream(outputStream)) {
        do {
          while (bl < buffer.length && (rb = stream.read(buffer, bl, buffer.length - bl)) > -1) {
            bl += rb;
          }

          bufferedOutputStream.write(buffer, 0, bl);
          bl = 0;

        } while (rb >= 0);
      }
    }
  }

  @Override
  protected WriteAheadLog createWalFromIBUFiles(
      final java.io.File directory,
      final ContextConfiguration contextConfiguration,
      final Locale locale,
      byte[] iv)
      throws IOException {
    final var aesKeyEncoded =
        contextConfiguration.getValueAsString(GlobalConfiguration.STORAGE_ENCRYPTION_KEY);
    final var aesKey =
        Optional.ofNullable(aesKeyEncoded)
            .map(keyEncoded -> Base64.getDecoder().decode(keyEncoded))
            .orElse(null);

    return new CASDiskWriteAheadLog(
        name,
        storagePath,
        directory.toPath(),
        contextConfiguration.getValueAsString(ContextConfiguration.WAL_BASE_NAME,
            ContextConfiguration.WAL_DEFAULT_NAME),
        contextConfiguration.getValueAsInteger(GlobalConfiguration.WAL_CACHE_SIZE),
        contextConfiguration.getValueAsInteger(GlobalConfiguration.WAL_BUFFER_SIZE),
        aesKey,
        iv,
        contextConfiguration.getValueAsLong(GlobalConfiguration.WAL_SEGMENTS_INTERVAL)
            * 60
            * 1_000_000_000L,
        contextConfiguration.getValueAsInteger(GlobalConfiguration.WAL_MAX_SEGMENT_SIZE)
            * 1024
            * 1024L,
        10,
        true,
        locale,
        GlobalConfiguration.WAL_MAX_SIZE.getValueAsLong() * 1024 * 1024,
        contextConfiguration.getValueAsInteger(GlobalConfiguration.WAL_COMMIT_TIMEOUT),
        contextConfiguration.getValueAsBoolean(GlobalConfiguration.WAL_KEEP_SINGLE_SEGMENT),
        contextConfiguration.getValueAsBoolean(GlobalConfiguration.STORAGE_CALL_FSYNC),
        contextConfiguration.getValueAsBoolean(
            GlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_STATISTICS),
        contextConfiguration.getValueAsInteger(
            GlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_INTERVAL));
  }

  @Override
  protected StartupMetadata checkIfStorageDirty() throws IOException {
    if (startupMetadata.exists()) {
      startupMetadata.open(YouTrackDBConstants.getRawVersion());
    } else {
      startupMetadata.create(YouTrackDBConstants.getRawVersion());
      startupMetadata.makeDirty(YouTrackDBConstants.getRawVersion());
    }

    return new StartupMetadata(startupMetadata.getLastTxId());
  }

  @Override
  protected void initConfiguration(
      final ContextConfiguration contextConfiguration,
      AtomicOperation atomicOperation) {
    configuration = new CollectionBasedStorageConfiguration(this);
    ((CollectionBasedStorageConfiguration) configuration)
        .load(contextConfiguration, atomicOperation);
  }

  @Override
  protected Map<String, Object> preCloseSteps() {
    final var params = super.preCloseSteps();

    if (fuzzyCheckpointTask != null) {
      fuzzyCheckpointTask.cancel(false);
    }
    if (recordsGcTask != null) {
      recordsGcTask.cancel(false);
    }

    return params;
  }

  @Override
  protected void preCreateSteps() throws IOException {
    startupMetadata.create(YouTrackDBConstants.getRawVersion());
  }

  @Override
  protected void postCloseSteps(
      final boolean onDelete, final boolean internalError, final long lastTxId) throws IOException {
    if (onDelete) {
      startupMetadata.delete();
    } else {
      if (!internalError) {
        startupMetadata.setLastTxId(lastTxId);
        startupMetadata.clearDirty();
      }
      startupMetadata.close();
    }
  }

  @Override
  protected void postDeleteSteps() {
    var databasePath =
        SystemVariableResolver.resolveSystemVariables(url);
    deleteFilesFromDisc(name, deleteMaxRetries, deleteWaitTime, databasePath);
  }

  public static void deleteFilesFromDisc(
      final String name, final int maxRetries, final int waitTime, final String databaseDirectory) {
    var dbDir = new java.io.File(databaseDirectory);
    if (!dbDir.exists() || !dbDir.isDirectory()) {
      dbDir = dbDir.getParentFile();
    }

    // RETRIES
    for (var i = 0; i < maxRetries; ++i) {
      if (dbDir != null && dbDir.exists() && dbDir.isDirectory()) {
        var notDeletedFiles = 0;

        final var storageFiles = dbDir.listFiles();
        if (storageFiles == null) {
          continue;
        }

        // TRY TO DELETE ALL THE FILES
        for (final var f : storageFiles) {
          // DELETE ONLY THE SUPPORTED FILES
          for (final var ext : ALL_FILE_EXTENSIONS) {
            if (f.getPath().endsWith(ext)) {
              if (!f.delete()) {
                notDeletedFiles++;
              }
              break;
            }
          }
        }

        if (notDeletedFiles == 0) {
          // TRY TO DELETE ALSO THE DIRECTORY IF IT'S EMPTY
          if (!dbDir.delete()) {
            LogManager.instance()
                .error(
                    DiskStorage.class,
                    "Cannot delete storage directory with path "
                        + dbDir.getAbsolutePath()
                        + " because directory is not empty. Files: "
                        + Arrays.toString(dbDir.listFiles()),
                    null);
          }
          return;
        }
      } else {
        return;
      }
      LogManager.instance()
          .debug(
              DiskStorage.class,
              "Cannot delete database files because they are still locked by the YouTrackDB process:"
                  + " waiting %d ms and retrying %d/%d...",
              logger, waitTime,
              i,
              maxRetries);
    }

    throw new StorageException(name,
        "Cannot delete database '"
            + name
            + "' located in: "
            + dbDir
            + ". Database files seem locked");
  }

  @Override
  protected void makeStorageDirty() throws IOException {
    startupMetadata.makeDirty(YouTrackDBConstants.getRawVersion());
  }

  @Override
  protected void clearStorageDirty() throws IOException {
    if (!isInError()) {
      startupMetadata.clearDirty();
    }
  }

  @Override
  protected boolean isDirty() {
    return startupMetadata.isDirty();
  }

  @Override
  protected String getOpenedAtVersion() {
    return startupMetadata.getOpenedAtVersion();
  }

  @Override
  protected void initIv() throws IOException {
    try (final var ivFile =
        new RandomAccessFile(storagePath.resolve(IV_NAME).toAbsolutePath().toFile(), "rw")) {
      final var iv = new byte[16];

      final var random = new SecureRandom();
      random.nextBytes(iv);

      final var hashFactory = XXHashFactory.fastestInstance();
      final var hash64 = hashFactory.hash64();

      final var hash = hash64.hash(iv, 0, iv.length, IV_SEED);
      ivFile.write(iv);
      ivFile.writeLong(hash);
      ivFile.getFD().sync();

      this.iv = iv;
    }
  }

  @Override
  protected void readIv() throws IOException {
    final var ivPath = storagePath.resolve(IV_NAME).toAbsolutePath();
    if (!Files.exists(ivPath)) {
      LogManager.instance().info(this, "IV file is absent, will create new one.");
      initIv();
      return;
    }

    try (final var ivFile = new RandomAccessFile(ivPath.toFile(), "r")) {
      final var iv = new byte[16];
      ivFile.readFully(iv);

      final var storedHash = ivFile.readLong();

      final var hashFactory = XXHashFactory.fastestInstance();
      final var hash64 = hashFactory.hash64();

      final var expectedHash = hash64.hash(iv, 0, iv.length, IV_SEED);
      if (storedHash != expectedHash) {
        throw new StorageException(name, "iv data are broken");
      }

      this.iv = iv;
    }
  }

  @Override
  protected byte[] getIv() {
    return iv;
  }

  @Override
  protected void initWalAndDiskCache(final ContextConfiguration contextConfiguration)
      throws IOException, java.lang.InterruptedException {
    final var callFsync =
        contextConfiguration.getValueAsBoolean(GlobalConfiguration.STORAGE_CALL_FSYNC);
    warnIfFsyncDisabled(callFsync);

    final var aesKeyEncoded =
        contextConfiguration.getValueAsString(GlobalConfiguration.STORAGE_ENCRYPTION_KEY);
    final var aesKey =
        Optional.ofNullable(aesKeyEncoded)
            .map(keyEncoded -> Base64.getDecoder().decode(keyEncoded))
            .orElse(null);

    fuzzyCheckpointTask =
        YouTrackDBEnginesManager.instance().getFuzzyCheckpointExecutor().scheduleWithFixedDelay(
            new PeriodicFuzzyCheckpoint(this),
            contextConfiguration.getValueAsInteger(
                GlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL),
            contextConfiguration.getValueAsInteger(
                GlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL),
            TimeUnit.SECONDS);

    int gcInterval = Math.max(1, contextConfiguration.getValueAsInteger(
        GlobalConfiguration.STORAGE_COLLECTION_GC_PAUSE_INTERVAL));
    recordsGcTask =
        YouTrackDBEnginesManager.instance().getFuzzyCheckpointExecutor().scheduleWithFixedDelay(
            new PeriodicRecordsGc(this),
            gcInterval,
            gcInterval,
            TimeUnit.SECONDS);

    final var configWalPath =
        contextConfiguration.getValueAsString(GlobalConfiguration.WAL_LOCATION);
    final Path walPath;
    if (configWalPath == null) {
      walPath = null;
    } else {
      walPath = Paths.get(configWalPath);
    }

    writeAheadLog =
        new CASDiskWriteAheadLog(
            name,
            storagePath,
            walPath,
            contextConfiguration.getValueAsString(ContextConfiguration.WAL_BASE_NAME,
                ContextConfiguration.WAL_DEFAULT_NAME),
            contextConfiguration.getValueAsInteger(GlobalConfiguration.WAL_CACHE_SIZE),
            contextConfiguration.getValueAsInteger(GlobalConfiguration.WAL_BUFFER_SIZE),
            aesKey,
            iv,
            contextConfiguration.getValueAsLong(GlobalConfiguration.WAL_SEGMENTS_INTERVAL)
                * 60
                * 1_000_000_000L,
            walMaxSegSize,
            10,
            true,
            Locale.getDefault(),
            contextConfiguration.getValueAsLong(GlobalConfiguration.WAL_MAX_SIZE) * 1024 * 1024,
            contextConfiguration.getValueAsInteger(GlobalConfiguration.WAL_COMMIT_TIMEOUT),
            contextConfiguration.getValueAsBoolean(GlobalConfiguration.WAL_KEEP_SINGLE_SEGMENT),
            callFsync,
            contextConfiguration.getValueAsBoolean(
                GlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_STATISTICS),
            contextConfiguration.getValueAsInteger(
                GlobalConfiguration.STORAGE_PRINT_WAL_PERFORMANCE_INTERVAL));
    writeAheadLog.addCheckpointListener(this);

    final var pageSize =
        contextConfiguration.getValueAsInteger(GlobalConfiguration.DISK_CACHE_PAGE_SIZE) * ONE_KB;
    final var diskCacheSize =
        contextConfiguration.getValueAsLong(GlobalConfiguration.DISK_CACHE_SIZE) * 1024 * 1024;
    final var writeCacheSize =
        (long) (contextConfiguration.getValueAsInteger(GlobalConfiguration.DISK_WRITE_CACHE_PART)
            / 100.0
            * diskCacheSize);
    final DoubleWriteLog doubleWriteLog;
    if (contextConfiguration.getValueAsBoolean(
        GlobalConfiguration.STORAGE_USE_DOUBLE_WRITE_LOG)) {
      doubleWriteLog = new DoubleWriteLogGL(doubleWriteLogMaxSegSize, callFsync);
    } else {
      doubleWriteLog = new DoubleWriteLogNoOP();
    }

    final var wowCache =
        new WOWCache(
            pageSize,
            contextConfiguration.getValueAsBoolean(GlobalConfiguration.FILE_LOG_DELETION),
            ByteBufferPool.instance(null),
            writeAheadLog,
            doubleWriteLog,
            contextConfiguration.getValueAsInteger(
                GlobalConfiguration.DISK_WRITE_CACHE_PAGE_FLUSH_INTERVAL),
            contextConfiguration.getValueAsInteger(GlobalConfiguration.WAL_SHUTDOWN_TIMEOUT),
            writeCacheSize,
            storagePath,
            getName(),
            files,
            getId(),
            contextConfiguration.getValueAsString(ContextConfiguration.DOUBLE_WRITE_LOG_NAME,
                ContextConfiguration.DOUBLE_WRITE_LOG_DEFAULT_NAME),
            contextConfiguration.getValueAsEnum(
                GlobalConfiguration.STORAGE_CHECKSUM_MODE, ChecksumMode.class),
            iv,
            aesKey,
            callFsync,
            context.getIoExecutor());

    wowCache.loadRegisteredFiles();
    wowCache.addBackgroundExceptionListener(this);
    wowCache.addPageIsBrokenListener(this);

    writeCache = wowCache;
  }

  static boolean warnIfFsyncDisabled(final boolean callFsync) {
    if (callFsync || !fsyncWarningLogged.compareAndSet(false, true)) {
      return false;
    }

    logger.warn(
        "Storage durability barriers are disabled by youtrackdb.storage.callFsync. "
            + "A power loss can lose data. Use this mode only for tests.");
    return true;
  }

  public static boolean exists(final Path path) {
    try {
      final var exists = new boolean[1];
      if (Files.exists(path.normalize().toAbsolutePath())) {
        try (final var stream = Files.newDirectoryStream(path)) {
          stream.forEach(
              (p) -> {
                final var fileName = p.getFileName().toString();
                // A bootstrap authority artifact also proves an existing storage image. That
                // artifact set covers the authority copies, the authority lock file, and the
                // unfinished record candidate. An interrupted storage birth therefore stays
                // visible instead of reporting a missing storage.
                if (fileName.equals("database.ocf")
                    || (fileName.startsWith("config") && fileName.endsWith(".bd"))
                    || fileName.startsWith("dirty.fl")
                    || fileName.startsWith("dirty.flb")
                    || StorageBootstrapMetadata.isBootstrapArtifactName(fileName)) {
                  exists[0] = true;
                }
              });
        }
        return exists[0];
      }

      return false;
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new StorageException(null, "Error during fetching list of files"), e, (String) null);
    }
  }

  @Override
  public String fullBackup(Path backupDirectory) {
    return fullBackup(new IBULocalFileNamesSupplier(backupDirectory, name, uuid.toString()),
        new IBULocalFileOutputStreamSupplier(backupDirectory, name),
        new IBULocalFileRemover(backupDirectory, name));
  }

  @Override
  public String fullBackup(Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, OutputStream> ibuOutputStreamSupplier, Consumer<String> ibuFileRemover) {
    var dbUuidString = uuid.toString();
    var ibuFilesIterator = IteratorUtils.filter(ibuFilesSupplier.get(),
        fileName -> fileName.startsWith(dbUuidString) && fileName.endsWith(IBU_EXTENSION));
    var ibuFiles = IteratorUtils.list(ibuFilesIterator);

    for (var ibuFile : ibuFiles) {
      ibuFileRemover.accept(ibuFile);
    }

    return backup(org.apache.commons.collections4.IteratorUtils::emptyIterator,
        ibuFileName -> {
          throw new UnsupportedOperationException("File " + ibuFileName + " does not exist.");
        }, ibuOutputStreamSupplier,
        ibuFileName -> {
          throw new UnsupportedOperationException("File " + ibuFileName + " does not exist.");
        });
  }

  @Override
  public String backup(final Path backupDirectory) {
    checkBackupIsNotPerformedInStorageDir(backupDirectory);
    try {
      if (!Files.exists(backupDirectory)) {
        Files.createDirectories(backupDirectory);
      }
    } catch (IOException e) {
      throw BaseException.wrapException(new DatabaseException(name,
          "Can not create directories are needed to perform database backup."), e, name);
    }

    var dbUUIDString = uuid.toString();
    final var fileLockPath = backupDirectory.resolve(dbUUIDString + "-" + BACKUP_LOCK);
    try (final var lockChannel = FileChannel.open(fileLockPath, StandardOpenOption.CREATE,
        StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      try (var ignored = lockChannel.lock()) {
        var currentTimeMillis = System.currentTimeMillis();

        var timeStampBuffer = ByteBuffer.allocate(Long.BYTES);
        if (lockChannel.size() == Long.BYTES) {
          IOUtils.readByteBuffer(timeStampBuffer, lockChannel);
          timeStampBuffer.rewind();

          var lastBackupMillis = timeStampBuffer.getLong();
          var minimalBackupTime = TimeUnit.MILLISECONDS.convert(
              configuration.getContextConfiguration()
                  .getValueAsInteger(GlobalConfiguration.STORAGE_BACKUP_MINIMUM_TIMEOUT_INTERVAL),
              TimeUnit.SECONDS);
          if (currentTimeMillis - lastBackupMillis < minimalBackupTime) {
            Thread.sleep(minimalBackupTime);
          }
        } else if (lockChannel.size() > 0) {
          lockChannel.truncate(0);
          lockChannel.force(true);

          var minimalBackupTime = TimeUnit.SECONDS.convert(configuration.getContextConfiguration()
              .getValueAsInteger(GlobalConfiguration.STORAGE_BACKUP_MINIMUM_TIMEOUT_INTERVAL),
              TimeUnit.MILLISECONDS);
          Thread.sleep(minimalBackupTime);
        }

        var newIbuFileName = backup(
            new IBULocalFileNamesSupplier(backupDirectory, name, dbUUIDString),
            new IBULocalFileInputStreamSupplier(backupDirectory, name),
            new IBULocalFileOutputStreamSupplier(backupDirectory, name),
            new IBULocalFileRemover(backupDirectory, name));

        timeStampBuffer.rewind();
        timeStampBuffer.putLong(System.currentTimeMillis());
        IOUtils.writeByteBuffer(timeStampBuffer, lockChannel, 0);
        lockChannel.force(true);

        return newIbuFileName;
      } catch (OverlappingFileLockException ofle) {
        LogManager.instance().error(this, "Can not lock file '%s'."
            + " File likely already locked by another process that performs database backup.",
            ofle, fileLockPath, ofle);
        throw ofle;
      } catch (InterruptedException e) {
        throw BaseException.wrapException(new DatabaseException(name, "Backup was interrupted"), e,
            name);
      }
    } catch (IOException e) {
      throw BaseException.wrapException(
          new DatabaseException(name, "Error during creation of database backup."), e,
          name);
    }
  }

  @Override
  public String backup(Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, InputStream> ibuInputStreamSupplier,
      Function<String, OutputStream> ibuOutputStreamSupplier,
      Consumer<String> ibuFileRemover) {
    checkOpennessAndMigration();

    String ibuNextFile;
    stateLock.readLock().lock();
    try {
      checkOpennessAndMigration();

      if (backupLock.tryLock()) {
        try {
          var uuid = getUuid();
          var uuidString = uuid.toString();

          var existingFiles =
              IteratorUtils.list(IteratorUtils.filter(ibuFilesSupplier.get(),
                  fileName -> fileName.startsWith(uuidString)));

          existingFiles.sort(new IBUFileNamesComparator());

          // The inspection of the existing chain changes no existing backup file. The removal of
          // recognized incomplete output follows the complete inspection.
          var extension = inspectChainForExtension(existingFiles, ibuInputStreamSupplier, uuid);
          for (var removableFile : extension.removableFiles()) {
            LogManager.instance()
                .error(this, "Backup unit file %s is broken and will be removed.", null,
                    removableFile);
            ibuFileRemover.accept(removableFile);
          }
          var backupMetadata = extension.chainHead();

          int nextFileIndex;
          LogSequenceNumber fromLsn = null;

          if (backupMetadata == null) {
            nextFileIndex = 0;
          } else {
            nextFileIndex = backupMetadata.sequenceNumber + 1;
            fromLsn = backupMetadata.endLsn;
          }

          ibuNextFile = createIbuFileName(nextFileIndex, uuidString);
          LogManager.instance()
              .info(this, "Backup unit file %s will be created.", ibuNextFile);
          // The semantic identity comes from this open source database, and the read precedes
          // the content copy. The new unit therefore carries the identity of its own database.
          var semanticIdentity = currentBackupSemanticIdentity();
          try (var fileStream = ibuOutputStreamSupplier.apply(ibuNextFile)) {
            var xxHashStream = new XXHashOutputStream(fileStream);
            var lastLsn = storeBackupDataToStream(xxHashStream, fromLsn);

            writeBackupMetadata(xxHashStream, uuid, nextFileIndex, fromLsn, lastLsn,
                getIdGen().getLastId(), semanticIdentity);
          }

          LogManager.instance()
              .info(this,
                  "Backup of database '%s' is completed. Backup unit file %s was created.",
                  name, ibuNextFile);
        } finally {
          backupLock.unlock();
        }
      } else {
        throw new BackupInProgressException(name,
            "You are trying to start incremental backup but it is in progress now, please wait till it will be finished");
      }
    } catch (final IOException ioe) {
      throw BaseException.wrapException(
          new DatabaseException(name, "Error during creation of database backup."), ioe, name);
    } finally {
      stateLock.readLock().unlock();
    }

    return ibuNextFile;
  }

  /**
   * Inspects every unit of one existing backup chain without any change of that chain.
   *
   * <p>The inspection walks from the newest unit to the oldest unit. The first supported unit
   * becomes the head of the extended chain. A unit above that head that this build recognizes as
   * its own incomplete output joins the removable set. The walk then continues below the head,
   * because one unsupported older unit refuses the extension of the whole chain.
   *
   * <p>The two inspections of this walk differ on purpose. Every unit down to the head keeps the
   * full content check, because an interrupted backup writes exactly there. Every older unit gets
   * the header-only admission of {@link #inspectBackupUnitHeader}, which reads no content hash.
   * One extension therefore never hashes the complete chain.
   *
   * <p>An unreadable or unclassifiable unit refuses the extension. Such a unit can hold an old
   * backup of real value, so this build never removes it. An operator resolves that unit and then
   * repeats the backup. A refusal reaches the caller before any removal, so a refused chain keeps
   * every file.
   *
   * @param existingFiles every existing unit of this database, ordered from oldest to newest
   * @param ibuInputStreamSupplier opens one existing unit for reading
   * @param uuid the database identifier (UUID) of this database
   * @throws UnsupportedBackupException when one inspected unit refuses the extension
   */
  private ChainExtension inspectChainForExtension(List<String> existingFiles,
      Function<String, InputStream> ibuInputStreamSupplier, UUID uuid) throws IOException {
    var remainingFiles = new ArrayList<>(existingFiles);
    var removableFiles = new ArrayList<String>();
    BackupMetadata chainHead = null;

    while (!remainingFiles.isEmpty()) {
      var ibuLastFile = remainingFiles.removeLast();
      var headFound = chainHead != null;
      BackupUnitInspection inspection;
      try (var ibuStream = ibuInputStreamSupplier.apply(ibuLastFile)) {
        inspection = headFound
            ? inspectBackupUnitHeader(ibuLastFile, getName(), uuid, ibuStream)
            : inspectBackupUnit(ibuLastFile, getName(), uuid, ibuStream, null);
      }

      switch (inspection.classification()) {
        case SUPPORTED -> {
          if (!headFound) {
            chainHead = inspection.metadata();
          }
        }
        case RECOGNIZED_INCOMPLETE -> {
          if (headFound) {
            // Defense in depth. The header-only admission of an older unit runs no content
            // check, so no removal ever covers a unit below the head of the chain.
            throw refusedChainExtension(ibuLastFile, inspection.detail());
          }
          removableFiles.add(ibuLastFile);
        }
        case UNCLASSIFIABLE -> throw refusedChainExtension(ibuLastFile, inspection.detail());
      }
    }

    return new ChainExtension(chainHead, removableFiles);
  }

  /** Builds the refusal of one chain extension. The refusal changes no backup unit file. */
  private UnsupportedBackupException refusedChainExtension(String ibuFileName, String detail) {
    return new UnsupportedBackupException(name,
        "The incremental backup of database '" + name + "' refuses to extend the chain,"
            + " because the backup unit file " + ibuFileName
            + " carries no supported backup header. " + detail
            + " This backup keeps every existing backup unit file. Resolve that file, or"
            + " create a full backup in a new empty location.");
  }

  /**
   * Reads the semantic identity of this open source database for one new backup unit.
   *
   * <p>The feature format comes from this build, and the storage open of this database accepted
   * that same feature format. The storage layout version comes from this build as well, because a
   * storage open accepts one layout version only.
   *
   * <p>The creation completion evidence comes from the durable genesis completion marker of this
   * database. A database without that marker therefore produces a backup unit without accepted
   * creation completion evidence. Every later restore of that unit refuses the unit.
   */
  private BackupSemanticIdentity currentBackupSemanticIdentity() {
    var genesisCompleted =
        Boolean.parseBoolean(configuration.getProperty(SharedContext.GENESIS_COMPLETED_PROPERTY));
    return new BackupSemanticIdentity(
        FEATURE_FORMAT.version(),
        StorageConfiguration.CURRENT_VERSION,
        genesisCompleted ? CREATION_COMPLETED_EVIDENCE : CREATION_EVIDENCE_ABSENT);
  }

  /** Returns the semantic identity that this build accepts in one backup header. */
  static BackupSemanticIdentity supportedBackupSemanticIdentity() {
    return new BackupSemanticIdentity(
        FEATURE_FORMAT.version(),
        StorageConfiguration.CURRENT_VERSION,
        CREATION_COMPLETED_EVIDENCE);
  }

  private static void writeBackupMetadata(XXHashOutputStream xxHashStream, UUID uuid,
      int nextFileIndex, LogSequenceNumber fromLsn, LogSequenceNumber lastLsn,
      long lastTxId, BackupSemanticIdentity semanticIdentity) throws IOException {
    var dataOutputStream = new DataOutputStream(xxHashStream);
    dataOutputStream.writeShort(CURRENT_BACKUP_FORMAT_VERSION);

    dataOutputStream.writeLong(uuid.getLeastSignificantBits());
    dataOutputStream.writeLong(uuid.getMostSignificantBits());

    dataOutputStream.writeInt(nextFileIndex);
    if (fromLsn == null) {
      dataOutputStream.writeLong(-1);
      dataOutputStream.writeInt(-1);
    } else {
      dataOutputStream.writeLong(fromLsn.getSegment());
      dataOutputStream.writeInt(fromLsn.getPosition());
    }
    dataOutputStream.writeLong(lastLsn.getSegment());
    dataOutputStream.writeInt(lastLsn.getPosition());

    dataOutputStream.writeLong(lastTxId);

    dataOutputStream.writeInt(semanticIdentity.featureFormatVersion());
    dataOutputStream.writeInt(semanticIdentity.storageLayoutVersion());
    dataOutputStream.writeInt(semanticIdentity.creationEvidence());

    dataOutputStream.flush();

    var hashCode = xxHashStream.xxHash64.getValue();
    dataOutputStream.writeLong(hashCode);
    dataOutputStream.flush();
  }

  private String createIbuFileName(int backupNumber, String uuid) {
    var date = new Date();
    var strDate = new SimpleDateFormat(BACKUP_DATEFORMAT, Locale.ROOT).format(date);
    return uuid + "-" + strDate + "-" + backupNumber + "-" + name + IBU_EXTENSION;
  }

  /**
   * Validates one backup unit and returns its metadata, or null for every rejected unit.
   *
   * <p>This method reports the decision alone. The caller that needs the reason of one rejection
   * calls {@link #inspectBackupUnit} instead.
   */
  @Nullable protected static BackupMetadata validateFileAndFetchBackupMetadata(String ibuFileName,
      String storageName,
      @Nullable UUID dbUUID,
      @Nonnull InputStream inputStream, @Nullable OutputStream copyStream)
      throws IOException {
    return inspectBackupUnit(ibuFileName, storageName, dbUUID, inputStream, copyStream).metadata();
  }

  /**
   * Reads one backup unit, copies the read bytes, and classifies the unit.
   *
   * <p>The classification separates three cases. A supported unit passes every header check and
   * every content check. A recognized incomplete unit carries the complete supported header of
   * this database and fails the content check. Every other unit is unclassifiable.
   *
   * <p>The unclassifiable outcome covers an old header and a header of another build. It also
   * covers a header without accepted creation completion evidence. It covers a disagreeing file
   * name and unreadable output too.
   *
   * <p>The copy stream receives every read byte, including the bytes of a rejected unit. The
   * caller therefore validates and copies in one pass.
   *
   * @param ibuFileName the name of the inspected unit
   * @param storageName the database name that owns the inspection, used in the log only
   * @param dbUUID the expected database identifier (UUID), or null when the caller expects any
   * @param inputStream the source of the unit
   * @param copyStream the optional destination of every read byte
   */
  static BackupUnitInspection inspectBackupUnit(String ibuFileName,
      String storageName,
      @Nullable UUID dbUUID,
      @Nonnull InputStream inputStream, @Nullable OutputStream copyStream)
      throws IOException {
    try (var xxHash64 = XXHashFactory.fastestInstance().newStreamingHash64(XX_HASH_SEED)) {
      var trailer =
          readBackupUnitTrailer(ibuFileName, storageName, inputStream, copyStream, xxHash64);
      if (trailer.header() == null) {
        return unclassifiableUnit(trailer.failureDetail());
      }

      // The stored hash code stays outside its own calculation, so the hash covers the content
      // and the header up to that stored value.
      xxHash64.update(trailer.header(), 0, IBU_METADATA_HASH_CODE_OFFSET);
      return classifyBackupUnit(ibuFileName, storageName, dbUUID, trailer.header(),
          xxHash64.getValue());
    }
  }

  /**
   * Reads the header of one backup unit and admits that header without any content check.
   *
   * <p>This inspection serves one unit below the head of an existing chain. The boundary between
   * the two inspections of the extension is deliberate. The trailing units down to the head keep
   * the full content check, because an interrupted backup writes exactly there. Every older unit
   * passed that same content check when the backup of the unit above it read the whole chain.
   * This header-only admission therefore keeps the cost of one extension bounded.
   *
   * <p>The admission checks the backup format version, the database feature format, the storage
   * layout version, the creation completion evidence, and the database identifier (UUID). The
   * outcome is supported or unclassifiable, because no content check runs here.
   *
   * @param ibuFileName the name of the inspected unit
   * @param storageName the database name that owns the inspection, used in the log only
   * @param dbUUID the expected database identifier (UUID), or null when the caller expects any
   * @param inputStream the source of the unit
   */
  static BackupUnitInspection inspectBackupUnitHeader(String ibuFileName, String storageName,
      @Nullable UUID dbUUID, @Nonnull InputStream inputStream) throws IOException {
    var trailer = readBackupUnitTrailer(ibuFileName, storageName, inputStream, null, null);
    if (trailer.header() == null) {
      return unclassifiableUnit(trailer.failureDetail());
    }
    return classifyBackupUnit(ibuFileName, storageName, dbUUID, trailer.header(), null);
  }

  /**
   * Reads one backup unit to its end and keeps the trailing header bytes of that unit.
   *
   * <p>The optional hash receives every byte outside the returned header. The optional copy
   * stream receives every read byte. A null hash therefore skips the content check of the caller.
   */
  private static BackupUnitTrailer readBackupUnitTrailer(String ibuFileName, String storageName,
      @Nonnull InputStream inputStream, @Nullable OutputStream copyStream,
      @Nullable StreamingXXHash64 xxHash64) throws IOException {
    byte[] metaDataCandidate = null;

    var buffer = new byte[(64 << 10)];
    var read = 0;

    while (true) {
      read = inputStream.read(buffer);

      if (read == -1) {
        break;
      } else if (read == 0) {
        continue;
      }

      if (metaDataCandidate == null) {
        if (read < IBU_METADATA_SIZE) {
          //read till we will not have to read metadata information
          var bytesLeftToRead = IBU_METADATA_SIZE - read;
          while (bytesLeftToRead > 0) {
            var r = inputStream.read(buffer, read, buffer.length - read);
            if (r == -1) {
              LogManager.instance().warn(DiskStorage.class, storageName,
                  "Size of the file %s is less than needed to store information about metadata. "
                      + "Size should be at least %d but real size is %d.",
                  ibuFileName,
                  IBU_METADATA_SIZE, read);
              return new BackupUnitTrailer(null,
                  "The file holds fewer bytes than one backup header needs.");
            }

            read += r;
            bytesLeftToRead -= r;
          }
        }

        metaDataCandidate = new byte[IBU_METADATA_SIZE];
        System.arraycopy(buffer, read - IBU_METADATA_SIZE, metaDataCandidate, 0,
            IBU_METADATA_SIZE);

        //calculate hash code everything except metadata
        //hash code from metadata will be calculated at the end
        updateHash(xxHash64, buffer, 0, read - IBU_METADATA_SIZE);
      } else {
        if (read >= IBU_METADATA_SIZE) {
          //tail of data, not metadata for sure
          updateHash(xxHash64, metaDataCandidate, 0, IBU_METADATA_SIZE);
          //hash code from metadata will be calculated at the end
          updateHash(xxHash64, buffer, 0, read - IBU_METADATA_SIZE);
          //potential metadata content
          System.arraycopy(buffer, read - IBU_METADATA_SIZE,
              metaDataCandidate, 0, IBU_METADATA_SIZE);
        } else {
          //part of metadata that will be replaced
          updateHash(xxHash64, metaDataCandidate, 0, read);
          //shift metadata to the left
          System.arraycopy(metaDataCandidate, read, metaDataCandidate, 0,
              IBU_METADATA_SIZE - read);
          //add new bytes that can be treated as metadata if they are the last ones
          System.arraycopy(buffer, 0, metaDataCandidate, IBU_METADATA_SIZE - read, read);
        }
      }

      if (copyStream != null && read > 0) {
        copyStream.write(buffer, 0, read);
      }
    }

    if (metaDataCandidate == null) {
      LogManager.instance()
          .warn(DiskStorage.class, storageName, "File %s does not contain backup metadata.",
              ibuFileName);
      return new BackupUnitTrailer(null, "The file contains no backup header at all.");
    }

    return new BackupUnitTrailer(metaDataCandidate, null);
  }

  /** Updates one optional hash, so a header-only read skips every hash update. */
  private static void updateHash(@Nullable StreamingXXHash64 xxHash64, byte[] bytes, int offset,
      int length) {
    if (xxHash64 != null) {
      xxHash64.update(bytes, offset, length);
    }
  }

  /**
   * Classifies one backup unit from its header, and from the content hash of the caller.
   *
   * <p>A null content hash states that no content check ran. The classification then reports a
   * supported unit or an unclassifiable unit, because only a content check produces recognized
   * incomplete output.
   *
   * @param metaDataCandidate the trailing header bytes of the unit
   * @param calculatedHashCode the hash of the read content, or null for a header-only admission
   */
  private static BackupUnitInspection classifyBackupUnit(String ibuFileName, String storageName,
      @Nullable UUID dbUUID, byte[] metaDataCandidate, @Nullable Long calculatedHashCode) {
    var metadataVersion = ShortSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_VERSION_OFFSET);
    var metadataUUIDLowerBits = LongSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_UUID_LOW_OFFSET);
    var metadataUUIDHigherBits = LongSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_UUID_HIGH_OFFSET);
    var metadataSequenceNumber = IntegerSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_SEQUENCE_OFFSET);
    var metadataStartLsnSegment = LongSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_FIRST_LSN_SEGMENT_OFFSET);
    var metadataStartLsnPosition = IntegerSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_START_LSN_POSITION_OFFSET);
    var metadataLastLsnSegment = LongSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_LAST_LSN_SEGMENT_OFFSET);
    var metadataEndLsnPosition = IntegerSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_LAST_LSN_POSITION_OFFSET);
    var metadataLastTxId = LongSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_LAST_TX_ID_OFFSET);
    var metadataFeatureFormat = IntegerSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_FEATURE_FORMAT_OFFSET);
    var metadataLayoutVersion = IntegerSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_LAYOUT_VERSION_OFFSET);
    var metadataCreationEvidence = IntegerSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_CREATION_EVIDENCE_OFFSET);
    var metadataHashCode = LongSerializer.deserializeLiteral(metaDataCandidate,
        IBU_METADATA_HASH_CODE_OFFSET);

    var semanticIdentity = new BackupSemanticIdentity(metadataFeatureFormat,
        metadataLayoutVersion, metadataCreationEvidence);
    // A recognized unit carries the complete supported header of this very database. Only such
    // a unit becomes removable output of an interrupted backup of this build. Every other unit
    // can hold a valuable old backup, so no automatic removal ever covers it.
    var recognized = metadataVersion == CURRENT_BACKUP_FORMAT_VERSION
        && semanticIdentity.equals(supportedBackupSemanticIdentity())
        && dbUUID != null
        && dbUUID.getLeastSignificantBits() == metadataUUIDLowerBits
        && dbUUID.getMostSignificantBits() == metadataUUIDHigherBits;

    // The failed content check of a recognized header is the only removable outcome. Every
    // other failure below keeps its unit, because that unit can hold a valuable old backup.
    if (calculatedHashCode != null && calculatedHashCode != metadataHashCode) {
      LogManager.instance()
          .warn(DiskStorage.class, storageName,
              "Hash code of the file %s is broken. Calculated hash code is %d but stored hash code is %d.",
              ibuFileName, calculatedHashCode, metadataHashCode);
      return incompleteOrUnclassifiableUnit(recognized,
          "The stored hash code of the file does not match its content.");
    }

    if (dbUUID != null) {
      if (dbUUID.getLeastSignificantBits() != metadataUUIDLowerBits
          || dbUUID.getMostSignificantBits() != metadataUUIDHigherBits) {
        var storedUUID = new UUID(metadataUUIDLowerBits, metadataUUIDHigherBits);
        LogManager.instance()
            .warn(DiskStorage.class, storageName,
                "Database identifier (UUID) of the file %s stored in metadata %s does not match "
                    + "the database identifier %s.",
                ibuFileName, storedUUID, dbUUID);
        return unclassifiableUnit("The header names another database than the expected one.");
      }
    }

    if (ibuFileName.length() < UUID_LENGTH) {
      LogManager.instance().warn(DiskStorage.class, storageName,
          "File name %s does not contain the database identifier (UUID).", ibuFileName);
      return unclassifiableUnit("The file name carries no database identifier (UUID).");
    }

    if (metadataVersion != CURRENT_BACKUP_FORMAT_VERSION) {
      LogManager.instance()
          .warn(DiskStorage.class, storageName,
              "Version of the file %s stored in metadata %d does not match supported version %d.",
              ibuFileName, metadataVersion, CURRENT_BACKUP_FORMAT_VERSION);
      return unclassifiableUnit("The header carries backup format version "
          + metadataVersion + ", and this build supports backup format version "
          + CURRENT_BACKUP_FORMAT_VERSION + " only.");
    }

    if (metadataFeatureFormat != FEATURE_FORMAT.version()) {
      LogManager.instance()
          .warn(DiskStorage.class, storageName,
              "Feature format %d of the file %s does not match supported feature format %d.",
              metadataFeatureFormat, ibuFileName, FEATURE_FORMAT.version());
      return unclassifiableUnit("The header carries database feature format "
          + metadataFeatureFormat + ", and this build supports database feature format "
          + FEATURE_FORMAT.version() + " only.");
    }

    if (metadataLayoutVersion != StorageConfiguration.CURRENT_VERSION) {
      LogManager.instance()
          .warn(DiskStorage.class, storageName,
              "Storage layout version %d of the file %s does not match supported version %d.",
              metadataLayoutVersion, ibuFileName, StorageConfiguration.CURRENT_VERSION);
      return unclassifiableUnit("The header carries storage layout version "
          + metadataLayoutVersion + ", and this build supports storage layout version "
          + StorageConfiguration.CURRENT_VERSION + " only.");
    }

    if (metadataCreationEvidence != CREATION_COMPLETED_EVIDENCE) {
      LogManager.instance()
          .warn(DiskStorage.class, storageName,
              "File %s carries no accepted creation completion evidence.", ibuFileName);
      return unclassifiableUnit("The header carries no accepted creation completion evidence.");
    }

    UUID fileNameUUID;
    try {
      fileNameUUID = UUID.fromString(ibuFileName.substring(0, UUID_LENGTH));
    } catch (IllegalArgumentException e) {
      LogManager.instance().warn(DiskStorage.class, storageName,
          "Database identifier (UUID) of the file %s is incorrect.", ibuFileName);
      return unclassifiableUnit("The file name carries no valid database identifier (UUID).");
    }

    if (fileNameUUID.getLeastSignificantBits() != metadataUUIDLowerBits
        || fileNameUUID.getMostSignificantBits() != metadataUUIDHigherBits) {
      LogManager.instance()
          .warn(DiskStorage.class, storageName,
              "Database identifier (UUID) of the file %s does not match the database identifier"
                  + " %s.",
              ibuFileName, dbUUID);
    }

    //uuid-date-sequence number
    var sequenceNumberStart = UUID_LENGTH + 1 + BACKUP_DATEFORMAT.length() + 1;
    var afterSequenceDashIndex = ibuFileName.indexOf('-', sequenceNumberStart);
    // A name without that separator carries no readable sequence number. The refusal below is
    // controlled, so no index arithmetic of a foreign name ever reaches the caller.
    if (afterSequenceDashIndex == -1) {
      LogManager.instance()
          .warn(DiskStorage.class, storageName,
              "File %s does not contain backup sequence number.", ibuFileName);
      return unclassifiableUnit("The file name carries no backup sequence number.");
    }

    int sequenceNumber;
    try {
      sequenceNumber = Integer.parseInt(
          ibuFileName.substring(sequenceNumberStart, afterSequenceDashIndex));
    } catch (NumberFormatException e) {
      LogManager.instance()
          .warn(DiskStorage.class, storageName,
              "Sequence number of the file %s is incorrect.", ibuFileName);
      return unclassifiableUnit("The file name carries no valid backup sequence number.");
    }

    // A disagreement of the file name and the header is ambiguous, because an operator can have
    // renamed complete output. Such a unit therefore stays in place.
    if (metadataSequenceNumber != sequenceNumber) {
      LogManager.instance()
          .warn(DiskStorage.class, storageName,
              "Sequence number of the file %s stored in metadata %s does not match DB "
                  + "sequence number %s.",
              ibuFileName, sequenceNumber, metadataSequenceNumber);
      return unclassifiableUnit(
          "The header and the file name name different backup sequence numbers.");
    }

    LogSequenceNumber startLsn = null;
    if (metadataStartLsnSegment != -1 && metadataStartLsnPosition != -1) {
      startLsn = new LogSequenceNumber(metadataStartLsnSegment, metadataStartLsnPosition);
    }
    if (metadataLastLsnSegment == -1 || metadataEndLsnPosition == -1) {
      LogManager.instance()
          .warn(DiskStorage.class, storageName,
              "Last LSN of the file %s stored in metadata is incorrect.",
              ibuFileName);
      return unclassifiableUnit("The header carries no valid last log sequence number.");
    }

    var lastLsn = new LogSequenceNumber(metadataLastLsnSegment, metadataEndLsnPosition);

    return new BackupUnitInspection(
        BackupUnitClassification.SUPPORTED,
        new BackupMetadata(metadataVersion, fileNameUUID, sequenceNumber, startLsn, lastLsn,
            metadataLastTxId, semanticIdentity),
        false,
        calculatedHashCode == null
            ? "The unit passes every header check."
            : "The unit passes every header check and every content check.");
  }

  /** Builds the inspection of one unit that this build cannot classify. */
  private static BackupUnitInspection unclassifiableUnit(String detail) {
    return new BackupUnitInspection(BackupUnitClassification.UNCLASSIFIABLE, null, false, detail);
  }

  /**
   * Builds the inspection of one unit that failed its content check.
   *
   * @param recognized true when the unit carries the complete supported header of this database
   */
  private static BackupUnitInspection incompleteOrUnclassifiableUnit(boolean recognized,
      String detail) {
    return new BackupUnitInspection(
        recognized
            ? BackupUnitClassification.RECOGNIZED_INCOMPLETE
            : BackupUnitClassification.UNCLASSIFIABLE,
        null,
        true,
        detail);
  }

  private LogSequenceNumber storeBackupDataToStream(OutputStream stream,
      LogSequenceNumber fromLsn)
      throws IOException {
    final var zipOutputStream = new ZipOutputStream(stream,
        Charset.forName(configuration.getCharset()));
    try {
      final long startSegment;
      final LogSequenceNumber freezeLsn;
      // A TRANSIENT self-quiesce (the backup segment cut): bounded by the cut body, so schema
      // commits may park behind it exactly like data commits.
      final var newSegmentFreezeId =
          atomicOperationsManager.freezeWriteOperations(FreezeKind.TRANSIENT_QUIESCE, null);
      try {
        final var startLsn = writeAheadLog.end();
        if (startLsn != null) {
          freezeLsn = startLsn;
        } else {
          freezeLsn = new LogSequenceNumber(0, 0);
        }

        writeAheadLog.addCutTillLimit(freezeLsn);

        writeAheadLog.appendNewSegment();
        startSegment = writeAheadLog.activeSegment();
      } finally {
        atomicOperationsManager.unfreezeWriteOperations(newSegmentFreezeId);
      }

      try {
        backupIv(zipOutputStream);

        final var encryptionIv = new byte[16];
        final var secureRandom = new SecureRandom();
        secureRandom.nextBytes(encryptionIv);

        backupIBUEncryptionIv(zipOutputStream, encryptionIv);

        final var aesKeyEncoded =
            configuration
                .getContextConfiguration()
                .getValueAsString(GlobalConfiguration.STORAGE_ENCRYPTION_KEY);
        final var aesKey =
            aesKeyEncoded == null ? null : Base64.getDecoder().decode(aesKeyEncoded);

        if (aesKey != null
            && aesKey.length != 16
            && aesKey.length != 24
            && aesKey.length != 32) {
          throw new InvalidStorageEncryptionKeyException(name,
              "Invalid length of the encryption key, provided size is " + aesKey.length);
        }

        var lastLsn = backupPagesWithChanges(fromLsn, zipOutputStream, encryptionIv, aesKey);
        final var lastWALLsn =
            copyWALToBackup(zipOutputStream, startSegment);

        if (lastWALLsn != null && (lastLsn == null || lastWALLsn.compareTo(lastLsn) > 0)) {
          lastLsn = lastWALLsn;
        }

        return lastLsn;
      } finally {
        writeAheadLog.removeCutTillLimit(freezeLsn);
      }
    } finally {
      try {
        zipOutputStream.finish();
        zipOutputStream.flush();
      } catch (IOException e) {
        LogManager.instance().warn(this, "Failed to flush data during incremental backup. ", e);
      }
    }
  }

  private void checkBackupIsNotPerformedInStorageDir(final Path backupDirectory) {
    var invalid = backupDirectory.equals(storagePath);
    if (invalid) {
      throw new StorageException(name, "Backup cannot be performed in the storage path");
    }
  }

  private void doEncryptionDecryption(
      final int mode,
      final byte[] aesKey,
      final long pageIndex,
      final long fileId,
      final byte[] backUpPage,
      final byte[] encryptionIv) {
    try {
      final var cipher = CIPHER.get();
      final SecretKey secretKey = new SecretKeySpec(aesKey, ALGORITHM_NAME);

      final var updatedIv = new byte[16];
      for (var i = 0; i < LongSerializer.LONG_SIZE; i++) {
        updatedIv[i] = (byte) (encryptionIv[i] ^ ((pageIndex >>> i) & 0xFF));
      }

      for (var i = 0; i < LongSerializer.LONG_SIZE; i++) {
        updatedIv[i + LongSerializer.LONG_SIZE] =
            (byte) (encryptionIv[i + LongSerializer.LONG_SIZE] ^ ((fileId >>> i) & 0xFF));
      }

      cipher.init(mode, secretKey, new IvParameterSpec(updatedIv));

      final var data =
          cipher.doFinal(
              backUpPage, LongSerializer.LONG_SIZE,
              backUpPage.length - LongSerializer.LONG_SIZE);
      System.arraycopy(
          data,
          0,
          backUpPage,
          LongSerializer.LONG_SIZE,
          backUpPage.length - LongSerializer.LONG_SIZE);
    } catch (InvalidKeyException e) {
      throw BaseException.wrapException(
          new InvalidStorageEncryptionKeyException(name, e.getMessage()),
          e, name);
    } catch (InvalidAlgorithmParameterException e) {
      throw new IllegalArgumentException("Invalid IV.", e);
    } catch (IllegalBlockSizeException | BadPaddingException e) {
      throw new IllegalStateException("Unexpected exception during CRT encryption.", e);
    }
  }

  private static void backupIBUEncryptionIv(final ZipOutputStream zipOutputStream,
      final byte[] encryptionIv)
      throws IOException {
    final var zipEntry = new ZipEntry(ENCRYPTION_IV);
    zipOutputStream.putNextEntry(zipEntry);

    zipOutputStream.write(encryptionIv);
    zipOutputStream.closeEntry();
  }

  private void backupIv(final ZipOutputStream zipOutputStream) throws IOException {
    final var zipEntry = new ZipEntry(IV_NAME);
    zipOutputStream.putNextEntry(zipEntry);

    zipOutputStream.write(this.iv);
    zipOutputStream.closeEntry();
  }

  private static byte[] restoreIv(final ZipInputStream zipInputStream) throws IOException {
    final var iv = new byte[16];
    IOUtils.readFully(zipInputStream, iv, 0, iv.length);

    return iv;
  }

  private LogSequenceNumber backupPagesWithChanges(
      final LogSequenceNumber changeLsn,
      final ZipOutputStream stream,
      final byte[] encryptionIv,
      final byte[] aesKey)
      throws IOException {
    var lastLsn = changeLsn;

    final var files = writeCache.files();
    final var pageSize = writeCache.pageSize();

    for (var entry : files.entrySet()) {
      final var fileName = entry.getKey();

      long fileId = entry.getValue();
      fileId = writeCache.externalFileId(writeCache.internalFileId(fileId));

      final var filledUpTo = writeCache.physicalSizeForBackupSnapshot(fileId);
      final var zipEntry = new ZipEntry(fileName);

      stream.putNextEntry(zipEntry);

      final var binaryFileId = new byte[LongSerializer.LONG_SIZE];
      LongSerializer.serializeLiteral(fileId, binaryFileId, 0);
      stream.write(binaryFileId, 0, binaryFileId.length);

      for (var pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
        final var cacheEntry =
            readCache.silentLoadForRead(fileId, pageIndex, writeCache, true);
        long sharedStamp = cacheEntry.acquireSharedLock();
        try {
          var cachePointer = cacheEntry.getCachePointer();
          assert cachePointer != null;

          var cachePointerBuffer = cachePointer.getBuffer();
          assert cachePointerBuffer != null;

          final var pageLsn =
              DurablePage.getLogSequenceNumberFromPage(cachePointerBuffer);

          if (changeLsn == null || pageLsn.compareTo(changeLsn) > 0) {

            final var data = new byte[pageSize + LongSerializer.LONG_SIZE];
            LongSerializer.serializeNative(pageIndex, data, 0);
            DurablePage.getPageData(cachePointerBuffer, data, LongSerializer.LONG_SIZE,
                pageSize);

            if (aesKey != null) {
              doEncryptionDecryption(
                  Cipher.ENCRYPT_MODE, aesKey, fileId, pageIndex, data, encryptionIv);
            }

            stream.write(data);

            if (lastLsn == null || pageLsn.compareTo(lastLsn) > 0) {
              lastLsn = pageLsn;
            }
          }
        } finally {
          cacheEntry.releaseSharedLock(sharedStamp);
          readCache.releaseFromRead(cacheEntry);
        }
      }

      stream.closeEntry();
    }

    return lastLsn;
  }

  @Override
  public void restoreFromBackup(Path backupDirectory, String expectedUUID) {
    try (var prepared = prepareBackupChain(name, backupDirectory, expectedUUID)) {
      restoreFromPreparedBackupChain(prepared);
    }
  }

  @Override
  public void restoreFromBackup(Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, InputStream> ibuInputStreamSupplier, @Nullable String expectedUUID) {
    try (var prepared =
        prepareBackupChain(name, ibuFilesSupplier, ibuInputStreamSupplier, expectedUUID)) {
      restoreFromPreparedBackupChain(prepared);
    }
  }

  /**
   * Returns the existing listing rule of one local backup directory.
   *
   * <p>The rule skips every directory entry and every entry without the backup unit extension. An
   * expected database identifier (UUID) additionally restricts the rule to the units of that
   * database.
   * The rule therefore keeps every unrelated file of the directory out of one restore.
   */
  public static Supplier<Iterator<String>> localBackupFileNames(String storageName,
      Path backupDirectory, @Nullable String expectedUUID) {
    return () -> {
      try (var filesStream = Files.list(backupDirectory)) {
        return filesStream.filter(path -> {
          if (Files.isDirectory(path)) {
            return false;
          }

          var fileName = path.getFileName();
          var strFileName = fileName.toString();
          return strFileName.endsWith(IBU_EXTENSION) && (expectedUUID == null
              || strFileName.startsWith(
                  expectedUUID));
        }).map(path -> path.getFileName().toString()).toList().iterator();
      } catch (IOException e) {
        throw BaseException.wrapException(new DatabaseException(storageName,
            "Can not list backup unit files in directory '" + backupDirectory + "'"), e,
            storageName);
      }
    };
  }

  /** Returns the existing read rule of one local backup directory. */
  public static Function<String, InputStream> localBackupFileStreams(String storageName,
      Path backupDirectory) {
    return new IBULocalFileInputStreamSupplier(backupDirectory, storageName);
  }

  /** Prepares the selected chain of one local backup directory. */
  public static PreparedBackupChain prepareBackupChain(String storageName, Path backupDirectory,
      @Nullable String expectedUUID) {
    return prepareBackupChain(storageName,
        localBackupFileNames(storageName, backupDirectory, expectedUUID),
        localBackupFileStreams(storageName, backupDirectory),
        expectedUUID);
  }

  /**
   * Copies one selected backup chain into request-owned temporary files and validates every copy.
   *
   * <p>The preparation runs before every target change. The preparation refuses an unsupported
   * header and a broken unit. The preparation also refuses a chain of two databases, a chain
   * without a full backup, and a chain with a gap. A refusal therefore precedes the creation, the
   * deletion, and the recreation of any restore target.
   *
   * <p>The returned chain owns the temporary copies and the temporary directory of one request.
   * The caller closes the chain, which removes both. The replay reads those same copies, so a
   * later change of the source never reaches the target.
   *
   * <p>The preparation holds no lock of the target, and the preparation opens one source stream at
   * a time. The preparation copies through one small buffer, so no chain-sized memory is needed.
   *
   * @param storageName the database name of the restore request
   * @param ibuFilesSupplier lists the candidate unit names of the source
   * @param ibuInputStreamSupplier opens one source unit for reading
   * @param expectedUUID the expected database identifier (UUID) of the source, or null for any
   * @throws UnsupportedBackupException when one selected unit carries no supported header
   */
  public static PreparedBackupChain prepareBackupChain(String storageName,
      Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, InputStream> ibuInputStreamSupplier,
      @Nullable String expectedUUID) {
    return prepareBackupChain(storageName, ibuFilesSupplier, ibuInputStreamSupplier, expectedUUID,
        null);
  }

  /**
   * Prepares one chain with an optional request-scoped output decorator used by failure tests.
   *
   * <p>Production callers pass no decorator. The decorator lets a manager test inject a checked
   * write failure after an output opens without changing source or filesystem semantics.
   */
  public static PreparedBackupChain prepareBackupChain(String storageName,
      Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, InputStream> ibuInputStreamSupplier,
      @Nullable String expectedUUID, @Nullable UnaryOperator<OutputStream> outputDecorator) {
    // The cheap checks of the request run before the first source read. An invalid expected
    // identifier therefore never triggers a chain-sized copy.
    UUID uuidExpectedInBackup = null;
    if (expectedUUID != null) {
      expectedUUID = expectedUUID.trim();

      if (!expectedUUID.isEmpty()) {
        try {
          uuidExpectedInBackup = UUID.fromString(expectedUUID);
        } catch (IllegalArgumentException e) {
          LogManager.instance()
              .error(DiskStorage.class,
                  "Expected database identifier (UUID) %s of the backup of database '%s' is"
                      + " incorrect.",
                  e,
                  expectedUUID, storageName);
          throw e;
        }
      }
    }

    // The existing source-specific listing and naming rules select the units of the chain.
    List<String> ibuFiles;
    if (expectedUUID != null) {
      var expectedUUIDString = expectedUUID;
      ibuFiles = IteratorUtils.list(IteratorUtils.filter(ibuFilesSupplier.get(),
          fileName -> fileName.startsWith(expectedUUIDString)));
    } else {
      ibuFiles = IteratorUtils.list(ibuFilesSupplier.get());
    }
    ibuFiles.sort(new IBUFileNamesComparator());
    validateSelectedUnitNames(storageName, ibuFiles);

    final PreparedBackupChain chain;
    try {
      chain = new PreparedBackupChain(storageName,
          Files.createTempDirectory(storageName + "-ytdb-backup"));
    } catch (IOException creationFailure) {
      throw BaseException.wrapException(
          new StorageException(storageName, "Error during restore from backup"), creationFailure,
          storageName);
    }
    LogManager.instance()
        .info(DiskStorage.class,
            "Temporary directory for the backup restore of database '%s' created in %s.",
            storageName, chain.temporaryDirectory().toAbsolutePath());

    try {
      copyAndValidateChain(chain, ibuFiles, ibuInputStreamSupplier, uuidExpectedInBackup,
          outputDecorator);
      return chain;
    } catch (IOException copyFailure) {
      var failure = BaseException.wrapException(
          new StorageException(storageName, "Error during restore from backup"), copyFailure,
          storageName);
      chain.closeSuppressing(failure);
      throw failure;
    } catch (RuntimeException | Error failure) {
      // A partial preparation owns temporary copies as well, so the cleanup covers every exit.
      chain.closeSuppressing(failure);
      throw failure;
    }
  }

  /** Validates every selected name and refuses duplicates before creating any prepared copy. */
  private static void validateSelectedUnitNames(String storageName, List<String> ibuFiles) {
    var selectedNames = new HashSet<Path>();
    for (var ibuFile : ibuFiles) {
      var bareName = preparedCopyName(storageName, ibuFile);
      if (!selectedNames.add(bareName)) {
        throw new UnsupportedBackupException(storageName,
            "The backup source repeats the unit name '" + ibuFile
                + "'. The restore refuses duplicate unit names before any copy.");
      }
    }
  }

  /**
   * Resolves the prepared copy of one selected unit inside the temporary directory.
   *
   * <p>The name of one unit comes from the source of the request, and a stream source controls
   * that name completely. This method therefore accepts one bare file-name component alone. An
   * absolute name and a name with a directory component both reach a controlled refusal. A name
   * of one directory reaches that same refusal before any output stream opens.
   *
   * @param storageName the database name of the restore request
   * @param temporaryDirectory the request-owned directory of every prepared copy
   * @param ibuFile the selected name of one source unit
   * @throws UnsupportedBackupException when the name escapes the temporary directory
   */
  private static Path preparedCopyPath(String storageName, Path temporaryDirectory,
      String ibuFile) {
    var bareName = preparedCopyName(storageName, ibuFile);
    var ownedDirectory = temporaryDirectory.toAbsolutePath().normalize();
    var preparedCopy = ownedDirectory.resolve(bareName).normalize();
    // The containment check is the last word. A prepared copy always stays one direct child of
    // the request-owned directory, so no restore ever writes outside that directory.
    if (!ownedDirectory.equals(preparedCopy.getParent())) {
      throw refusedUnitName(storageName, ibuFile);
    }
    return preparedCopy;
  }

  /** Returns one validated bare unit name that a request-owned directory may contain. */
  private static Path preparedCopyName(String storageName, String ibuFile) {
    Path candidate;
    try {
      candidate = Path.of(ibuFile);
    } catch (InvalidPathException invalidName) {
      throw refusedUnitName(storageName, ibuFile);
    }

    var bareName = candidate.getFileName();
    if (candidate.isAbsolute()
        || candidate.getNameCount() != 1
        || bareName == null
        || !bareName.toString().equals(ibuFile)) {
      throw refusedUnitName(storageName, ibuFile);
    }
    return bareName;
  }

  /** Builds the refusal of one source unit name that no prepared copy can own. */
  private static UnsupportedBackupException refusedUnitName(String storageName, String ibuFile) {
    return new UnsupportedBackupException(storageName,
        "The backup source names the unit '" + ibuFile
            + "', which is no plain backup unit file name. The restore of database '"
            + storageName + "' refuses that name before any copy of it.");
  }

  /** Copies every selected unit into the prepared chain and validates the complete chain. */
  private static void copyAndValidateChain(PreparedBackupChain chain, List<String> ibuFiles,
      Function<String, InputStream> ibuInputStreamSupplier, @Nullable UUID uuidExpectedInBackup,
      @Nullable UnaryOperator<OutputStream> outputDecorator) throws IOException {
    var storageName = chain.storageName();
    UUID metadataUUID = null;
    LogSequenceNumber lastLsn = null;
    long backupLastTxId = -1;

    for (var ibuFile : ibuFiles) {
      var tmpIBUFile = preparedCopyPath(storageName, chain.temporaryDirectory(), ibuFile);

      var isFullBackup = false;
      try (var ownedCopyStream = Files.newOutputStream(tmpIBUFile, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
        var copyStream = outputDecorator == null
            ? ownedCopyStream
            : outputDecorator.apply(ownedCopyStream);
        try (copyStream; var bufferedCopyStream = new BufferedOutputStream(copyStream)) {

          BackupUnitInspection inspection;
          try (var ibuStream = ibuInputStreamSupplier.apply(ibuFile)) {
            inspection = inspectBackupUnit(ibuFile, storageName,
                uuidExpectedInBackup, ibuStream, bufferedCopyStream);
          }

          // The two refusals below stay separate. A failed content check names broken content,
          // even when no expected database identifier can prove that the unit is removable.
          if (inspection.contentCheckFailed()) {
            throw new UnsupportedBackupException(storageName, "Backup unit file " + ibuFile
                + " contains invalid content, restore from this backup is impossible. "
                + inspection.detail());
          }
          if (inspection.classification() == BackupUnitClassification.UNCLASSIFIABLE) {
            throw new UnsupportedBackupException(storageName, "Backup unit file " + ibuFile
                + " carries no supported backup header, restore from this backup is impossible. "
                + inspection.detail());
          }
          var backupMetadata = inspection.metadata();
          if (metadataUUID == null) {
            metadataUUID = backupMetadata.databaseId;
          } else if (!metadataUUID.equals(backupMetadata.databaseId)) {
            throw new DatabaseException(storageName, "Backup unit files from different databases "
                + "cannot be restored in the same database.");
          }

          if (lastLsn != null) {
            if (backupMetadata.startLsn == null) {
              throw new DatabaseException(storageName,
                  "There are two full backups in the backup, restore is impossible. ");
            }

            if (!backupMetadata.startLsn.equals(lastLsn)) {
              throw new DatabaseException(
                  "Backup files are not contiguous, some changes are missing, restore is impossible.");
            }
          }

          isFullBackup = backupMetadata.startLsn == null;
          lastLsn = backupMetadata.endLsn;
          if (backupMetadata.lastTxId > backupLastTxId) {
            backupLastTxId = backupMetadata.lastTxId;
          }
        }
      }

      chain.addUnit(tmpIBUFile, isFullBackup);
    }

    if (chain.units().isEmpty()) {
      throw new DatabaseException(storageName, "No backup unit files found in the backup.");
    }
    var firstBackupUnit = chain.units().getFirst();
    if (!firstBackupUnit.rightBoolean()) {
      throw new DatabaseException(storageName,
          "Full backup file is absent in the backup, restore is "
              + "impossible.");
    }
    chain.completePreparation(backupLastTxId);
  }

  /**
   * Replays one prepared backup chain into this target and activates the target.
   *
   * <p>The caller prepared and validated every copy of the chain before this method runs. This
   * method therefore performs no source selection and no further copy. The replay consumes the
   * prepared copies alone, so a change of the source after the preparation never reaches this
   * target.
   *
   * <p>The existing final checks follow the replay. The restored content validation, the
   * durability barrier, and the activation keep their existing order and their existing scope.
   */
  @Override
  public void restoreFromPreparedBackupChain(PreparedBackupChain chain) {
    chain.requireComplete();
    stateLock.writeLock().lock();
    try {
      final var aesKeyEncoded =
          configuration
              .getContextConfiguration()
              .getValueAsString(GlobalConfiguration.STORAGE_ENCRYPTION_KEY);
      final var aesKey =
          aesKeyEncoded == null ? null : Base64.getDecoder().decode(aesKeyEncoded);

      if (aesKey != null && aesKey.length != 16 && aesKey.length != 24 && aesKey.length != 32) {
        throw new InvalidStorageEncryptionKeyException(name,
            "Invalid length of the encryption key, provided size is " + aesKey.length);
      }

      // A restored image never shares its storage lineage with the backup source. A storage
      // lineage is the identifier of the ancestry of one storage image. The production restore
      // path creates a fresh target. The birth publication of that fresh target already
      // generates a random storage identity and a random storage lineage. The call below is
      // therefore a no-operation for the production restore path, because the target already
      // carries the restore-in-progress lifecycle state. The call serves an in-place restore
      // into an already active storage, which only a test performs today.
      beginLineageReplacement();
      var result = preprocessingIncrementalRestore();
      for (var ibuFilePair : chain.units()) {
        var ibuPath = ibuFilePair.left();

        try (var inputStream = Files.newInputStream(ibuPath)) {
          try (var bufferedInputStream = new BufferedInputStream(inputStream)) {
            var isFullBackup = ibuFilePair.rightBoolean();
            restoreFromIncrementalBackup(
                result.charset,
                result.locale,
                result.contextConfiguration,
                aesKey,
                bufferedInputStream,
                isFullBackup);
          }
        }
      }

      var backupLastTxId = chain.lastTxId();
      if (backupLastTxId >= 0 && backupLastTxId >= getIdGen().getLastId()) {
        getIdGen().setStartId(backupLastTxId + 1);
      }

      postProcessIncrementalRestore(result.contextConfiguration);
      // Track 24 restore order. The restore target reaches the active lifecycle state only
      // after the validation of the restored content and after the durability barrier over
      // that content. A failed validation therefore leaves the restore-in-progress state, and
      // the destructive restart entry accepts that state.
      validateRestoredContent();
      barrierWhileCallerOwnsStateLock();
      activateBootstrapSnapshot("Cannot activate the restored storage lineage");
      dropStaleIndexLifecycles();
    } catch (IOException e) {
      throw BaseException.wrapException(
          new StorageException(name, "Error during restore from backup"), e, name);
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  /**
   * Adopts a fresh storage lineage for an in-place restore into an already active storage.
   *
   * <p>A storage lineage is the identifier of the ancestry of one storage image. A restored image
   * never shares its storage lineage with the backup source. An in-place restore therefore
   * replaces the lineage of the target before the first content write.
   *
   * <p>This method performs no work for a target that already carries the restore-in-progress
   * lifecycle state. The production restore path creates a fresh target. The birth publication of
   * that fresh target already generated a random storage identity and a random storage lineage.
   * That fresh target therefore needs no replacement.
   *
   * <p>The logical sequence floor needs no adoption either. A fresh target starts at the lowest
   * floor, and no production reader of the durable floor exists today.
   */
  void beginLineageReplacement() {
    try {
      var current = java.util.Objects.requireNonNull(bootstrapSnapshot, "bootstrapSnapshot");
      var pending = bootstrapMetadata().beginLineageReplacement(
          current, new LineageFloorAdoption(current.format(), current.sequenceFloor()));
      bootstrapSnapshot = pending;
    } catch (IOException exception) {
      throw BaseException.wrapException(
          new StorageException(name, "Cannot publish the storage restore authority"),
          exception,
          name);
    }
  }

  private IncrementalRestorePreprocessingResult preprocessingIncrementalRestore()
      throws IOException {
    final var contextConfiguration = configuration.getContextConfiguration();
    final var charset = configuration.getCharset();
    final var locale = configuration.getLocaleInstance();

    atomicOperationsManager.executeInsideAtomicOperation(
        atomicOperation -> {
          closeCollections();
          closeIndexes(atomicOperation);
          ((CollectionBasedStorageConfiguration) configuration).close(atomicOperation);
        });

    configuration = null;

    return new IncrementalRestorePreprocessingResult(contextConfiguration, charset, locale);
  }

  /**
   * Validates the restored content before the activation of this restore target.
   *
   * <p>The validation covers two durable values of the restored image. The first value is the
   * storage layout version of the restored storage configuration. The storage layout version is
   * the version of the on-disk storage layout. The second value is the genesis marker. The genesis
   * marker is the durable property that records a finished genesis.
   *
   * <p>A failed validation leaves this target in the restore-in-progress lifecycle state, because
   * the activation follows this method.
   */
  private void validateRestoredContent() throws IOException {
    final var restoredConfiguration = (CollectionBasedStorageConfiguration) configuration;
    final var restoredLayoutVersion =
        atomicOperationsManager.calculateInsideAtomicOperation(restoredConfiguration::getVersion);
    validateRestoredStorageLayoutVersion(name, restoredLayoutVersion);
    validateRestoredGenesisMarker(
        name, restoredConfiguration.getProperty(SharedContext.GENESIS_COMPLETED_PROPERTY));
  }

  /**
   * Rejects a restored storage layout version that this build does not support.
   *
   * <p>A backup of an older or a newer build carries such a version. The refusal keeps the target
   * in the restore-in-progress lifecycle state. A drop discards that target and reports success.
   *
   * <p>This check is defense in depth. The configuration load of the restore reads the same value
   * earlier. That load reports the named inconsistent-metadata result for a disagreement. A real
   * backup of another build therefore never reaches this check today. This check still guards the
   * case of a configuration load that stops comparing that value.
   */
  static void validateRestoredStorageLayoutVersion(String storageName, int restoredLayoutVersion) {
    if (restoredLayoutVersion != StorageConfiguration.CURRENT_VERSION) {
      throw new StorageException(
          storageName,
          "The restore of database '"
              + storageName
              + "' failed. The backup content carries storage layout version "
              + restoredLayoutVersion
              + ". This build supports storage layout version "
              + StorageConfiguration.CURRENT_VERSION
              + " only. Drop database '"
              + storageName
              + "' to remove the unusable restore target.");
    }
  }

  /**
   * Rejects restored content without a set genesis marker.
   *
   * <p>The genesis marker is the durable property that records a finished genesis. A backup of an
   * incomplete database carries no such marker. The refusal keeps the target in the
   * restore-in-progress lifecycle state.
   */
  static void validateRestoredGenesisMarker(String storageName, String markerValue) {
    if (!Boolean.parseBoolean(markerValue)) {
      throw new StorageException(
          storageName,
          "The restore of database '"
              + storageName
              + "' failed. The backup content carries no genesis completion marker. The backup"
              + " therefore holds an incomplete database. Drop database '"
              + storageName
              + "' to remove the unusable restore target.");
    }
  }

  private void postProcessIncrementalRestore(ContextConfiguration contextConfiguration)
      throws IOException {
    if (CollectionBasedStorageConfiguration.exists(writeCache)) {
      configuration = new CollectionBasedStorageConfiguration(this);
      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> ((CollectionBasedStorageConfiguration) configuration)
              .load(contextConfiguration, atomicOperation));
    } else {
      configuration = new CollectionBasedStorageConfiguration(this);
      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> ((CollectionBasedStorageConfiguration) configuration)
              .load(contextConfiguration, atomicOperation));
    }

    atomicOperationsManager.executeInsideAtomicOperation(this::openCollections);
    linkCollectionsBTreeManager.close();

    atomicOperationsManager.executeInsideAtomicOperation(linkCollectionsBTreeManager::load);
    atomicOperationsManager.executeInsideAtomicOperation(this::openIndexes);

    flushAllData();

    // Recovery-time orphan-truncation pass on the incremental-restore path. Runs
    // post-flush (mirrors the open() entry point): running the truncate after
    // flushAllData() ensures writeCachePages cannot re-extend any in-scope file past
    // targetBytes via a dirty WAL-replay entry past the logical horizon, silently
    // re-creating the orphan the pass exists to remove.
    atomicOperationsManager.executeInsideAtomicOperation(this::truncateOrphansAfterRecovery);

    atomicOperationsManager.executeInsideAtomicOperation(this::generateDatabaseInstanceId);
  }

  private void restoreFromIncrementalBackup(
      final String charset,
      final Locale locale,
      final ContextConfiguration contextConfiguration,
      final byte[] aesKey,
      final InputStream inputStream,
      final boolean isFull)
      throws IOException {
    final List<String> currentFiles = new ArrayList<>(writeCache.files().keySet());
    final var bufferedInputStream = new BufferedInputStream(inputStream);
    final var zipInputStream =
        new ZipInputStream(bufferedInputStream, Charset.forName(charset));
    final var pageSize = writeCache.pageSize();

    ZipEntry zipEntry;
    LogSequenceNumber maxLsn = null;

    List<String> processedFiles = new ArrayList<>();
    if (isFull) {
      final var files = writeCache.files();
      for (var entry : files.entrySet()) {
        final var fileId = writeCache.fileIdByName(entry.getKey());

        assert entry.getValue().equals(fileId);
        readCache.deleteFile(fileId, writeCache);
      }
    }

    final var walTempDir = createWalTempDirectory();

    byte[] encryptionIv = null;
    byte[] walIv = null;

    entryLoop : while ((zipEntry = zipInputStream.getNextEntry()) != null) {
      switch (zipEntry.getName()) {
        case IV_NAME -> {
          walIv = restoreIv(zipInputStream);
          continue;
        }
        case ENCRYPTION_IV -> {
          encryptionIv = restoreEncryptionIv(zipInputStream);
          continue;
        }
        case CONF_ENTRY_NAME -> {
          replaceConfiguration(zipInputStream);

          continue;
        }
      }

      if (zipEntry.getName().equalsIgnoreCase("database_instance.uuid")) {
        continue;
      }

      if (zipEntry.getName().equals(CONF_UTF_8_ENTRY_NAME)) {
        replaceConfiguration(zipInputStream);

        continue;
      }

      if (zipEntry
          .getName()
          .toLowerCase(locale)
          .endsWith(CASDiskWriteAheadLog.WAL_SEGMENT_EXTENSION)) {
        final var walName = zipEntry.getName();
        final var segmentIndex =
            walName.lastIndexOf(
                '.',
                walName.length() - CASDiskWriteAheadLog.WAL_SEGMENT_EXTENSION.length() - 1);
        if (segmentIndex < 0) {
          throw new IllegalStateException("Can not find index of WAL segment");
        }

        addFileToDirectory(
            contextConfiguration.getValueAsString(ContextConfiguration.WAL_BASE_NAME,
                ContextConfiguration.WAL_DEFAULT_NAME) + walName.substring(segmentIndex),
            zipInputStream, walTempDir);
        continue;
      }

      if (aesKey != null && encryptionIv == null) {
        throw new SecurityException(name, "IV can not be null if encryption key is provided");
      }

      final var binaryFileId = new byte[LongSerializer.LONG_SIZE];
      IOUtils.readFully(zipInputStream, binaryFileId, 0, binaryFileId.length);

      final var expectedFileId = LongSerializer.deserializeLiteral(binaryFileId, 0);
      long fileId;

      var rootDirectory = storagePath;
      var zipEntryPath = rootDirectory.resolve(zipEntry.getName()).normalize();

      if (!zipEntryPath.startsWith(rootDirectory)) {
        throw new IllegalStateException("Bad zip entry " + zipEntry.getName());
      }
      if (!zipEntryPath.getParent().equals(rootDirectory)) {
        throw new IllegalStateException("Bad zip entry " + zipEntry.getName());
      }

      var fileName = zipEntryPath.getFileName().toString();
      if (!writeCache.exists(fileName)) {
        fileId = readCache.addFile(fileName, expectedFileId, writeCache);
      } else {
        fileId = writeCache.fileIdByName(fileName);
      }

      if (!writeCache.fileIdsAreEqual(expectedFileId, fileId)) {
        throw new StorageException(name,
            "Can not restore database from backup because expected and actual file ids are not the"
                + " same");
      }

      while (true) {
        final var data = new byte[pageSize + LongSerializer.LONG_SIZE];

        var rb = 0;

        while (rb < data.length) {
          final var b = zipInputStream.read(data, rb, data.length - rb);

          if (b == -1) {
            if (rb > 0) {
              throw new StorageException(name, "Can not read data from file " + fileName);
            } else {
              processedFiles.add(fileName);
              continue entryLoop;
            }
          }

          rb += b;
        }

        final var pageIndex = LongSerializer.deserializeNative(data, 0);

        if (aesKey != null) {
          doEncryptionDecryption(
              Cipher.DECRYPT_MODE, aesKey, expectedFileId, pageIndex, data, encryptionIv);
        }

        // loadOrAddForWrite is total on disk (delegates to WriteCache.loadOrAdd which
        // gap-fills intermediate pages between currentSize and the recorded pageIndex);
        // incremental-backup restore only runs on the disk engine, so the disk-engine
        // totality is sufficient here.
        final var cacheEntry =
            readCache.loadOrAddForWrite(fileId, pageIndex, writeCache, true, null);
        // Incremental-backup restore is disk-only by construction, so an -ea assert
        // is sufficient here; AtomicOperationBinaryTracking.commitChanges documents
        // why the in-memory-reachable site throws instead.
        assert cacheEntry != null
            : "readCache.loadOrAddForWrite returned null during incremental backup restore"
                + " for fileId=" + fileId + " pageIndex=" + pageIndex
                + "; WriteCache.loadOrAdd totality contract violated";

        try {
          final var buffer = cacheEntry.getCachePointer().getBuffer();
          assert buffer != null;
          final var backedUpPageLsn =
              DurablePage.getLogSequenceNumber(LongSerializer.LONG_SIZE, data);
          if (isFull) {
            buffer.put(0, data, LongSerializer.LONG_SIZE,
                data.length - LongSerializer.LONG_SIZE);

            if (maxLsn == null || maxLsn.compareTo(backedUpPageLsn) < 0) {
              maxLsn = backedUpPageLsn;
            }
          } else {
            final var currentPageLsn =
                DurablePage.getLogSequenceNumberFromPage(buffer);
            if (backedUpPageLsn.compareTo(currentPageLsn) > 0) {
              buffer.put(
                  0, data, LongSerializer.LONG_SIZE, data.length - LongSerializer.LONG_SIZE);

              if (maxLsn == null || maxLsn.compareTo(backedUpPageLsn) < 0) {
                maxLsn = backedUpPageLsn;
              }
            }
          }

        } finally {
          readCache.releaseFromWrite(cacheEntry, writeCache, true);
        }
      }
    }

    currentFiles.removeAll(processedFiles);

    for (var file : currentFiles) {
      if (writeCache.exists(file)) {
        final var fileId = writeCache.fileIdByName(file);
        readCache.deleteFile(fileId, writeCache);
      }
    }

    try (final var restoreLog =
        createWalFromIBUFiles(walTempDir, contextConfiguration, locale, walIv)) {
      if (restoreLog != null) {
        final var beginLsn = restoreLog.begin();
        restoreFrom(restoreLog, beginLsn);
      }
    }

    if (maxLsn != null && writeAheadLog != null) {
      writeAheadLog.moveLsnAfter(maxLsn);
    }

    FileUtils.deleteRecursively(walTempDir);
  }

  private byte[] restoreEncryptionIv(final ZipInputStream zipInputStream) throws IOException {
    final var iv = new byte[16];
    var read = 0;
    while (read < iv.length) {
      final var localRead = zipInputStream.read(iv, read, iv.length - read);

      if (localRead < 0) {
        throw new StorageException(name,
            "End of stream is reached but IV data were not completely read");
      }

      read += localRead;
    }

    return iv;
  }

  @Override
  public int getAbsoluteLinkBagCounter(RID ownerId, String fieldName, RID key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AbsoluteChange getLinkBagCounter(DatabaseSessionEmbedded session,
      RecordIdInternal identity,
      String fieldName, RID rid) {
    throw new UnsupportedOperationException();
  }

  private static void replaceConfiguration(ZipInputStream zipInputStream) throws IOException {
    var buffer = new byte[1024];

    var rb = 0;
    while (true) {
      final var b = zipInputStream.read(buffer, rb, buffer.length - rb);

      if (b == -1) {
        break;
      }

      rb += b;

      if (rb == buffer.length) {
        var oldBuffer = buffer;

        buffer = new byte[buffer.length << 1];
        System.arraycopy(oldBuffer, 0, buffer, 0, oldBuffer.length);
      }
    }
  }

  private static Cipher getCipherInstance() {
    try {
      return Cipher.getInstance(TRANSFORMATION);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw BaseException.wrapException(
          new SecurityException((String) null,
              "Implementation of encryption " + TRANSFORMATION + " is absent"),
          e, (String) null);
    }
  }

  private record IncrementalRestorePreprocessingResult(
      ContextConfiguration contextConfiguration,
      String charset,
      Locale locale) {

  }

  protected record BackupMetadata(int backupFormatVersion,
      UUID databaseId,
      int sequenceNumber,
      LogSequenceNumber startLsn,
      LogSequenceNumber endLsn,
      long lastTxId,
      BackupSemanticIdentity semanticIdentity) {

  }

  /**
   * Names the semantic database format and the creation completion evidence of one backup unit.
   *
   * <p>The feature format names the storage features of the backed-up database. The storage layout
   * version names the on-disk layout of that database. The creation completion evidence states
   * whether the creation of that database finished.
   */
  protected record BackupSemanticIdentity(int featureFormatVersion,
      int storageLayoutVersion,
      int creationEvidence) {

  }

  /** Names the three outcomes of one backup unit inspection. */
  enum BackupUnitClassification {
    /** The unit passes every header check and every content check. */
    SUPPORTED,
    /**
     * The unit carries the complete supported header of this database and fails a content check.
     *
     * <p>Such a unit is incomplete output of an interrupted backup of this build. An incremental
     * backup removes such trailing output before the backup extends the chain.
     */
    RECOGNIZED_INCOMPLETE,
    /**
     * This build cannot classify the unit.
     *
     * <p>Five cases reach this outcome. An old header and a header of another build reach it.
     * A header without accepted creation completion evidence reaches it. A file name that
     * disagrees with the header and unreadable output reach it as well. Such a unit stays in
     * place forever, because the unit can hold a valuable backup of another build.
     */
    UNCLASSIFIABLE
  }

  /**
   * Reports the outcome of one backup unit inspection.
   *
   * @param metadata the metadata of one supported unit, and null for every rejected unit
   * @param contentCheckFailed true when the stored hash disagrees with the unit content
   * @param detail one sentence that names the outcome for a log entry or a failure message
   */
  record BackupUnitInspection(BackupUnitClassification classification,
      @Nullable BackupMetadata metadata,
      boolean contentCheckFailed,
      String detail) {

  }

  /**
   * Reports the trailing header bytes of one read backup unit.
   *
   * @param header the trailing header bytes, and null when the read found no header
   * @param failureDetail one sentence that names the missing header, and null for one header
   */
  private record BackupUnitTrailer(@Nullable byte[] header, @Nullable String failureDetail) {

  }

  /**
   * Reports the inspection result of one existing backup chain.
   *
   * @param chainHead the metadata of the newest supported unit, and null for an empty chain
   * @param removableFiles every recognized incomplete unit above that head, newest first. The
   *     inspection of the complete chain precedes every removal of these units.
   */
  private record ChainExtension(@Nullable BackupMetadata chainHead,
      List<String> removableFiles) {

  }

  private static final class XXHashOutputStream extends ProxyOutputStream {

    private final StreamingXXHash64 xxHash64 =
        XXHashFactory.fastestInstance().newStreamingHash64(XX_HASH_SEED);

    XXHashOutputStream(OutputStream out) {
      super(out);
    }

    @Override
    public void write(byte[] bts) throws IOException {
      xxHash64.update(bts, 0, bts.length);
      super.write(bts);
    }

    @Override
    public void write(byte[] bts, int st, int end) throws IOException {
      xxHash64.update(bts, st, end - st);
      super.write(bts, st, end);
    }

    @Override
    public void write(int b) throws IOException {
      xxHash64.update(new byte[] {(byte) b}, 0, 1);
      super.write(b);
    }

    @Override
    public void close() throws IOException {
      xxHash64.close();
      super.close();
    }
  }

  private static final class IBUFileNamesComparator implements Comparator<String> {

    @Override
    public int compare(String firstIbuName, String secondIbuName) {
      var firstNameLastDashIndex = firstIbuName.lastIndexOf('-');
      if (firstNameLastDashIndex == -1) {
        throw new IllegalArgumentException("Invalid backup unit file name: " + firstIbuName);
      }
      var firstNameWithoutDbName = firstIbuName.substring(0, firstNameLastDashIndex);

      var secondNameLastDashIndex = secondIbuName.lastIndexOf('-');
      if (secondNameLastDashIndex == -1) {
        throw new IllegalArgumentException("Invalid backup unit file name: " + secondIbuName);
      }

      var secondNameWithoutDbName = secondIbuName.substring(0, secondNameLastDashIndex);
      return firstNameWithoutDbName.compareTo(secondNameWithoutDbName);
    }
  }

  private record IBULocalFileNamesSupplier(Path backupDirectory, String databaseName,
      String dbUUID) implements Supplier<Iterator<String>> {

    @Override
    public Iterator<String> get() {
      try (var filesStream = Files.list(backupDirectory)) {
        return filesStream.filter(path -> {
          if (Files.isDirectory(path)) {
            return false;
          }

          var fileName = path.getFileName().toString();
          return fileName.endsWith(IBU_EXTENSION) && fileName.startsWith(dbUUID);
        }).map(path -> path.getFileName().toString()).toList().iterator();
      } catch (IOException e) {
        throw BaseException.wrapException(new DatabaseException(databaseName,
            "Can not list backup unit files in directory '" + backupDirectory + "'"), e,
            databaseName);
      }
    }
  }

  private record IBULocalFileInputStreamSupplier(Path backupDirectory,
      String databaseName) implements
      Function<String, InputStream> {

    @Override
    public InputStream apply(String ibuFileName) {
      var ibuPath = backupDirectory.resolve(ibuFileName);
      try {
        return new BufferedInputStream(
            Files.newInputStream(backupDirectory.resolve(ibuFileName)));
      } catch (IOException e) {
        throw BaseException.wrapException(new DatabaseException(databaseName,
            "Can open backup unit file " + ibuPath + " to read it."), e, databaseName);
      }
    }
  }

  private record IBULocalFileOutputStreamSupplier(Path backupDirectory,
      String databaseName) implements
      Function<String, OutputStream> {

    @Override
    public OutputStream apply(String ibuFileName) {
      var ibuPath = backupDirectory.resolve(ibuFileName);
      try {
        OutputStream os;
        var fileChannel = FileChannel.open(ibuPath, StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE);
        try {
          os = Channels.newOutputStream(fileChannel);
        } catch (Exception e) {
          fileChannel.close();
          throw e;
        }
        return new ProxyOutputStream(os) {
          @Override
          public void close() throws IOException {
            fileChannel.force(true);
            super.close();

            //trying to fsync directory changes making it visible to other processes
            try (var directoryChannel = FileChannel.open(backupDirectory)) {
              directoryChannel.force(true);
            } catch (IOException ignored) {
              //ignore if attempt is failed.
            }

          }
        };
      } catch (IOException e) {
        throw BaseException.wrapException(
            new DatabaseException(databaseName,
                "Can create new backup unit file " + ibuPath + " ."),
            e, databaseName);
      }
    }
  }

  private record IBULocalFileRemover(Path backupDirectory, String databaseName) implements
      Consumer<String> {

    @Override
    public void accept(String ibuFileName) {
      try {
        var ibuFilePath = backupDirectory.resolve(ibuFileName);
        Files.deleteIfExists(ibuFilePath);
        LogManager.instance().info(this, "Deleted backup unit file " + ibuFilePath);

        //trying to fsync directory changes making it visible to other processes
        try (var directoryChannel = FileChannel.open(backupDirectory)) {
          directoryChannel.force(true);
        } catch (IOException ignored) {
          //ignore if attempt is failed.
        }

      } catch (IOException e) {
        throw BaseException.wrapException(new DatabaseException(databaseName,
            "Can not delete backup unit file " + ibuFileName + " ."), e, databaseName);
      }
    }
  }
}
