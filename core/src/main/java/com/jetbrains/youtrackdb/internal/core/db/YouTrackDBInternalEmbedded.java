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

package com.jetbrains.youtrackdb.internal.core.db;

import static com.jetbrains.youtrackdb.api.config.GlobalConfiguration.WARNING_DEFAULT_USERS;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.internal.common.log.LogManager;
import com.jetbrains.youtrackdb.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrackdb.internal.core.command.script.ScriptManager;
import com.jetbrains.youtrackdb.internal.core.config.YouTrackDBConfig;
import com.jetbrains.youtrackdb.internal.core.engine.Engine;
import com.jetbrains.youtrackdb.internal.core.engine.MemoryAndLocalPaginatedEnginesInitializer;
import com.jetbrains.youtrackdb.internal.core.exception.BaseException;
import com.jetbrains.youtrackdb.internal.core.exception.CoreException;
import com.jetbrains.youtrackdb.internal.core.exception.DatabaseException;
import com.jetbrains.youtrackdb.internal.core.exception.GenesisIncompleteException;
import com.jetbrains.youtrackdb.internal.core.exception.InconsistentStorageMetadataException;
import com.jetbrains.youtrackdb.internal.core.exception.StorageException;
import com.jetbrains.youtrackdb.internal.core.gremlin.YTDBGraphFactory;
import com.jetbrains.youtrackdb.internal.core.metadata.security.auth.AuthenticationInfo;
import com.jetbrains.youtrackdb.internal.core.security.DefaultSecuritySystem;
import com.jetbrains.youtrackdb.internal.core.storage.Storage;
import com.jetbrains.youtrackdb.internal.core.storage.config.CollectionBasedStorageConfiguration;
import com.jetbrains.youtrackdb.internal.core.storage.disk.DiskStorage;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.AbstractStorage;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageAdmissionException;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageBootstrapMetadata;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class YouTrackDBInternalEmbedded implements YouTrackDBInternal {

  /**
   * Keeps track of next possible storage id.
   */
  private static final AtomicInteger nextStorageId = new AtomicInteger();

  /**
   * Storage IDs current assigned to the storage.
   */
  private static final Set<Integer> currentStorageIds =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final Map<String, AbstractStorage> storages = new ConcurrentHashMap<>();
  private final Map<String, SharedContext> sharedContexts = new ConcurrentHashMap<>();
  private final Set<DatabasePoolInternal> pools = Collections.newSetFromMap(
      new ConcurrentHashMap<>());
  private final YouTrackDBConfigImpl configuration;
  private final Path basePath;
  private final Engine memory;
  private final Engine disk;
  private final YouTrackDBEnginesManager youTrack;
  private final boolean serverMode;
  private final CachedDatabasePoolFactory cachedPoolFactory;
  private volatile boolean open = true;
  private volatile ScheduledFuture<?> autoCloseFuture = null;
  private final ScriptManager scriptManager = new ScriptManager();
  private final SystemDatabase systemDatabase;
  private final DefaultSecuritySystem securitySystem;
  private final CommandTimeoutChecker timeoutChecker;

  private volatile long maxWALSegmentSize = -1;
  private volatile long doubleWriteLogMaxSegSize = -1;

  @Nullable private Runnable restartAfterDeletionForTesting;
  @Nullable private UnaryOperator<OutputStream> preparedCopyOutputDecoratorForTesting;

  private final ReentrantLock fileMetadataLock = new ReentrantLock();

  public YouTrackDBInternalEmbedded(String directoryPath, YouTrackDBConfig configuration,
      YouTrackDBEnginesManager youTrack, boolean serverMode) {
    super();

    this.youTrack = youTrack;
    this.serverMode = serverMode;
    youTrack.onEmbeddedFactoryInit(this);
    memory = youTrack.getEngine("memory");
    disk = youTrack.getEngine("disk");
    basePath = Path.of(directoryPath.trim()).toAbsolutePath().normalize();

    this.configuration =
        (YouTrackDBConfigImpl) (configuration != null ? configuration
            : YouTrackDBConfig.defaultConfig());

    MemoryAndLocalPaginatedEnginesInitializer.INSTANCE.initialize();

    youTrack.addYouTrackDB(this);
    youTrack.createExecutor(this.configuration);
    youTrack.createIoExecutor(this.configuration);

    cachedPoolFactory = createCachedDatabasePoolFactory();

    initAutoClose();

    var timeout = getLongConfig(GlobalConfiguration.COMMAND_TIMEOUT);
    timeoutChecker = new CommandTimeoutChecker(timeout, this);
    systemDatabase = new SystemDatabase(this);
    securitySystem = new DefaultSecuritySystem();

    securitySystem.activate(this, this.configuration.getSecurityConfig());
  }

  private void initAutoClose() {

    var autoClose = getBoolConfig(GlobalConfiguration.AUTO_CLOSE_AFTER_DELAY);
    if (autoClose) {
      var autoCloseDelay = getIntConfig(GlobalConfiguration.AUTO_CLOSE_DELAY);
      final var delay = (long) autoCloseDelay * 60 * 1000;
      initAutoClose(delay);
    }
  }

  private boolean getBoolConfig(GlobalConfiguration config) {
    return this.configuration.getConfiguration().getValueAsBoolean(config);
  }

  private int getIntConfig(GlobalConfiguration config) {
    return this.configuration.getConfiguration().getValueAsInteger(config);
  }

  private long getLongConfig(GlobalConfiguration config) {
    return this.configuration.getConfiguration().getValueAsLong(config);
  }

  private CachedDatabasePoolFactory createCachedDatabasePoolFactory() {
    var capacity = getIntConfig(GlobalConfiguration.DB_CACHED_POOL_CAPACITY);
    long timeout = getIntConfig(GlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT);
    return new CachedDatabasePoolFactoryImpl(this, capacity, timeout);
  }

  public void initAutoClose(long delay) {
    final var scheduleTime = delay / 3;
    Runnable task = () -> YouTrackDBInternalEmbedded.this.execute(
        () -> checkAndCloseStorages(delay));
    autoCloseFuture = schedule(task, scheduleTime, scheduleTime);
  }

  private synchronized void checkAndCloseStorages(long delay) {
    Set<String> toClose = new HashSet<>();
    for (var storage : storages.values()) {
      if (storage.getType().equalsIgnoreCase(DatabaseType.DISK.name())
          && storage.getSessionsCount() == 0) {
        var currentTime = System.currentTimeMillis();
        if (currentTime > storage.getLastCloseTime() + delay) {
          toClose.add(storage.getName());
        }
      }
    }
    for (var storage : toClose) {
      forceDatabaseClose(storage);
    }
  }

  private long calculateInitialMaxWALSegSize() throws IOException {
    var walPath =
        configuration.getConfiguration().getValueAsString(GlobalConfiguration.WAL_LOCATION);

    if (walPath == null) {
      walPath = basePath.toString();
    }

    final var fileStore = Files.getFileStore(Paths.get(walPath));
    final var freeSpace = fileStore.getUsableSpace();

    long filesSize;
    try {
      filesSize =
          Files.walk(Paths.get(walPath))
              .mapToLong(
                  p -> {
                    try {
                      if (Files.isRegularFile(p)) {
                        return Files.size(p);
                      }

                      return 0;
                    } catch (IOException | UncheckedIOException e) {
                      LogManager.instance()
                          .error(this, "Error during calculation of free space for database", e);
                      return 0;
                    }
                  })
              .sum();
    } catch (IOException | UncheckedIOException e) {
      LogManager.instance().error(this, "Error during calculation of free space for database", e);

      filesSize = 0;
    }

    var maxSegSize = getLongConfig(GlobalConfiguration.WAL_MAX_SEGMENT_SIZE) * 1024 * 1024;

    if (maxSegSize <= 0) {
      var sizePercent = getIntConfig(GlobalConfiguration.WAL_MAX_SEGMENT_SIZE_PERCENT);
      if (sizePercent <= 0) {
        throw new DatabaseException(basePath.toString(),
            "Invalid configuration settings. Can not set maximum size of WAL segment");
      }

      maxSegSize = (freeSpace + filesSize) / 100 * sizePercent;
    }

    final var minSegSizeLimit = (long) (freeSpace * 0.25);

    var minSegSize = getLongConfig(GlobalConfiguration.WAL_MIN_SEG_SIZE) * 1024 * 1024;

    if (minSegSize > minSegSizeLimit) {
      minSegSize = minSegSizeLimit;
    }

    if (minSegSize > 0 && maxSegSize < minSegSize) {
      maxSegSize = minSegSize;
    }
    return maxSegSize;
  }

  private long calculateDoubleWriteLogMaxSegSize(Path storagePath) throws IOException {
    final var fileStore = Files.getFileStore(storagePath);
    final var freeSpace = fileStore.getUsableSpace();

    long filesSize;
    try {
      filesSize =
          Files.walk(storagePath)
              .mapToLong(
                  p -> {
                    try {
                      if (Files.isRegularFile(p)) {
                        return Files.size(p);
                      }

                      return 0;
                    } catch (IOException | UncheckedIOException e) {
                      LogManager.instance()
                          .error(this, "Error during calculation of free space for database", e);

                      return 0;
                    }
                  })
              .sum();
    } catch (IOException | UncheckedIOException e) {
      LogManager.instance().error(this, "Error during calculation of free space for database", e);

      filesSize = 0;
    }

    var maxSegSize =
        getLongConfig(GlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE) * 1024 * 1024;

    if (maxSegSize <= 0) {
      var sizePercent =
          getIntConfig(GlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE_PERCENT);

      if (sizePercent <= 0) {
        throw new DatabaseException(basePath.toString(),
            "Invalid configuration settings. Can not set maximum size of WAL segment");
      }

      maxSegSize = (freeSpace + filesSize) / 100 * sizePercent;
    }

    var minSegSize =
        getLongConfig(GlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MIN_SEG_SIZE) * 1024 * 1024;

    if (minSegSize > 0 && maxSegSize < minSegSize) {
      maxSegSize = minSegSize;
    }
    return maxSegSize;
  }

  @Override
  public YouTrackDBImpl newYouTrackDb() {
    return YTDBGraphFactory.ytdbInstance(basePath.toString(), () -> this);
  }

  @Override
  public DatabaseSessionEmbedded open(String name, String user, String password) {
    return open(name, user, password, null);
  }

  public DatabaseSessionEmbedded openNoAuthenticate(String name, String user) {
    checkDatabaseName(name);
    try {
      final DatabaseSessionEmbedded embedded;
      var config = solveConfig(null);
      synchronized (this) {
        checkOpen();
        var storage = getAndOpenStorage(name, config);
        embedded = newSessionInstance(storage, config);
      }
      embedded.rebuildIndexes();
      embedded.internalOpen(user, "nopwd", false);
      embedded.callOnOpenListeners();
      return embedded;
    } catch (Exception e) {
      throw BaseException.wrapException(
          new DatabaseException(basePath.toString(), "Cannot open database '" + name + "'"), e,
          basePath.toString());
    }
  }

  private DatabaseSessionEmbedded newSessionInstance(
      AbstractStorage storage, YouTrackDBConfigImpl config) {
    var embedded = new DatabaseSessionEmbedded(storage, serverMode);
    try {
      embedded.init(config, getOrCreateSharedContext(storage));
    } catch (RuntimeException e) {
      // A genesis-incomplete refusal (raised by SharedContext.load during the metadata load)
      // must not leave the corpse's storage open and registered — see the helper's javadoc.
      unregisterGenesisIncompleteCorpse(storage, e);
      throw e;
    }
    return embedded;
  }

  private static DatabaseSessionEmbedded newCreateSessionInstance(
      AbstractStorage storage, YouTrackDBConfigImpl config, SharedContext sharedContext) {
    var embedded = new DatabaseSessionEmbedded(storage, false);
    embedded.internalCreate(config, sharedContext);
    return embedded;
  }

  public DatabaseSessionEmbedded openNoAuthorization(String name) {
    checkDatabaseName(name);
    try {
      final DatabaseSessionEmbedded embedded;
      var config = solveConfig(null);
      synchronized (this) {
        checkOpen();
        var storage = getAndOpenStorage(name, config);
        embedded = newSessionInstance(storage, config);
      }
      embedded.rebuildIndexes();
      embedded.callOnOpenListeners();
      return embedded;
    } catch (Exception e) {
      throw BaseException.wrapException(
          new DatabaseException(basePath.toString(), "Cannot open database '" + name + "'"), e,
          basePath.toString());
    }
  }

  @Override
  public DatabaseSessionEmbedded open(
      String name, String user, String password, YouTrackDBConfig config) {
    checkDatabaseName(name);
    checkDefaultPassword(name, user, password);
    try {
      final DatabaseSessionEmbedded embedded;
      synchronized (this) {
        checkOpen();
        config = solveConfig((YouTrackDBConfigImpl) config);
        var storage = getAndOpenStorage(name, (YouTrackDBConfigImpl) config);

        embedded = newSessionInstance(storage, (YouTrackDBConfigImpl) config);
      }
      embedded.rebuildIndexes();
      embedded.internalOpen(user, password);
      embedded.callOnOpenListeners();
      return embedded;
    } catch (Exception e) {
      throw BaseException.wrapException(
          new DatabaseException(basePath.toString(), "Cannot open database '" + name + "'"), e,
          basePath.toString());
    }
  }

  @Override
  public DatabaseSessionEmbedded open(
      AuthenticationInfo authenticationInfo, YouTrackDBConfig config) {
    try {
      final DatabaseSessionEmbedded embedded;
      synchronized (this) {
        checkOpen();
        config = solveConfig((YouTrackDBConfigImpl) config);
        if (authenticationInfo.getDatabase().isEmpty()) {
          throw new SecurityException("Authentication info do not contain the database");
        }
        var database = authenticationInfo.getDatabase().get();
        var storage = getAndOpenStorage(database,
            (YouTrackDBConfigImpl) config);
        embedded = newSessionInstance(storage, (YouTrackDBConfigImpl) config);
      }
      embedded.rebuildIndexes();
      embedded.internalOpen(authenticationInfo);
      embedded.callOnOpenListeners();
      return embedded;
    } catch (Exception e) {
      throw BaseException.wrapException(
          new DatabaseException(basePath.toString(),
              "Cannot open database '" + authenticationInfo.getDatabase() + "'"),
          e, basePath.toString());
    }
  }

  private AbstractStorage getAndOpenStorage(String name, YouTrackDBConfigImpl config) {
    var registeredStorage = storages.get(name);
    var storage = getOrInitStorage(name);
    // THIS OPEN THE STORAGE ONLY THE FIRST TIME
    try {
      // THIS OPEN THE STORAGE ONLY THE FIRST TIME
      storage.open(config.getConfiguration());
    } catch (RuntimeException e) {
      storages.remove(storage.getName());
      if (registeredStorage != storage) {
        // This call created the storage, and the failed open leaves no usable storage behind.
        // The storage identifier therefore returns to the pool of free identifiers. A retained
        // identifier would leak one entry of the static identifier set on every failed open.
        currentStorageIds.remove(storage.getId());
      }

      throw e;
    }
    // The genesis-completion belt (design §A1) fires in SharedContext.load, AFTER the schema
    // version gate had its chance — not here (review CS52): an old-format database must get
    // the export/reimport redirect, never the discard-and-recreate refusal.
    // Wire the general-purpose executor into histogram managers so they can
    // schedule background rebalance work. We deliberately avoid ioExecutor
    // here because it is also used by AsynchronousFileChannel for I/O
    // completions — running blocking reads on it causes deadlocks.
    storage.setHistogramExecutor(youTrack.getExecutor());
    return storage;
  }

  /**
   * Closes and unregisters a corpse storage whose open was refused by the genesis-completion
   * belt (reviews BG14/CN55/CS55): keeping the refused storage open and registered would pin
   * its file locks and WAL buffers for the process lifetime and block the manual discard the
   * refusal message prescribes on locking platforms. Mirrors the {@code storage.open} failure
   * arm in {@code getAndOpenStorage}; {@code drop()} re-initializes the storage from disk for
   * the deletion. Other open failures keep today's behavior (the storage stays registered).
   * Cleanup failures never mask the refusal — they are attached as suppressed.
   */
  private synchronized void unregisterGenesisIncompleteCorpse(AbstractStorage storage,
      RuntimeException failure) {
    // Track 24 moved the genesis check of the shared context to the named inconsistent-metadata
    // result. Both shapes describe an image whose creation never ran to completion, so both
    // shapes unregister the storage and keep the storage closed for the caller.
    if (!isCausedByGenesisIncomplete(failure) && !isCausedByGenesisMarkerInconsistency(failure)) {
      return;
    }
    storages.remove(storage.getName());
    currentStorageIds.remove(storage.getId());
    var sharedContext = sharedContexts.remove(storage.getName());
    if (sharedContext != null) {
      try {
        sharedContext.close();
      } catch (RuntimeException cleanupFailure) {
        failure.addSuppressed(cleanupFailure);
      }
    }
    try {
      storage.shutdown();
    } catch (RuntimeException cleanupFailure) {
      failure.addSuppressed(cleanupFailure);
    }
  }

  /**
   * Whether the given open failure is (or wraps) the genesis-completion refusal. The open paths
   * wrap every failure into a generic {@code DatabaseException}, so the drop-path exemption
   * (CN54) walks the cause chain for the marker refusal.
   */
  private static boolean isCausedByGenesisIncomplete(Throwable failure) {
    for (var cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof GenesisIncompleteException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Reports whether the cause chain of one failure carries the genesis marker inconsistency.
   *
   * <p>A consistency check is a later check that compares two durable sources without deciding
   * admission. The genesis check of the shared context reports the named inconsistent-metadata
   * result when the accepted lifecycle state says active and the genesis marker says unfinished.
   * The drop path tolerates that result, so the prescribed discard always works.
   */
  private static boolean isCausedByGenesisMarkerInconsistency(Throwable failure) {
    for (var cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof InconsistentStorageMetadataException inconsistency
          && inconsistency.inconsistency()
              == InconsistentStorageMetadataException.Inconsistency.GENESIS_MARKER) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether the given failure is, or wraps, the interrupted-birth admission rejection.
   *
   * <p>Storage admission is the decision that accepts an existing storage image for use. Admission
   * reports the interrupted-birth reason for an image whose genesis never finished. Delayed
   * activation makes that image shape the ordinary residue of a crashed creation.
   */
  private static boolean isCausedByInterruptedBirth(Throwable failure) {
    return isCausedByAdmissionReason(
        failure, StorageAdmissionException.Reason.INTERRUPTED_BIRTH);
  }

  /**
   * Reports whether the cause chain of one failure carries the interrupted-restore reason.
   *
   * <p>An interrupted restore leaves the restore-in-progress lifecycle state. The drop tolerates
   * that reason, so an operator can always discard a restore target that no restore can finish.
   */
  private static boolean isCausedByInterruptedRestore(Throwable failure) {
    return isCausedByAdmissionReason(
        failure, StorageAdmissionException.Reason.INTERRUPTED_RESTORE);
  }

  /** Walks the cause chain of one failure and looks for one named admission reason. */
  private static boolean isCausedByAdmissionReason(
      Throwable failure, StorageAdmissionException.Reason reason) {
    for (var cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof StorageAdmissionException admission
          && admission.reason() == reason) {
        return true;
      }
    }
    return false;
  }

  private void checkDefaultPassword(String database, String user, String password) {
    if ((("admin".equals(user) && "admin".equals(password))
        || ("reader".equals(user) && "reader".equals(password))
        || ("writer".equals(user) && "writer".equals(password)))
        && WARNING_DEFAULT_USERS.getValueAsBoolean()) {
      LogManager.instance()
          .warn(
              this,
              String.format(
                  "IMPORTANT! Using default password is unsafe, please change password for user"
                      + " '%s' on database '%s'",
                  user, database));
    }
  }

  private YouTrackDBConfigImpl solveConfig(YouTrackDBConfigImpl config) {
    if (config != null) {
      config.setParent(this.configuration);
      return config;
    } else {
      var cfg = (YouTrackDBConfigImpl) YouTrackDBConfig.defaultConfig();
      cfg.setParent(this.configuration);
      return cfg;
    }
  }

  @Override
  public DatabaseSessionEmbedded poolOpen(
      String name, String user, String password, DatabasePoolInternal pool) {
    final DatabaseSessionEmbedded embedded;
    synchronized (this) {
      checkOpen();
      var storage = getAndOpenStorage(name, pool.getConfig());
      try {
        embedded = newPooledSessionInstance(pool, storage, getOrCreateSharedContext(storage),
            serverMode);
      } catch (RuntimeException e) {
        // Same corpse unregistration as the non-pooled open paths (BG14/CN55/CS55).
        unregisterGenesisIncompleteCorpse(storage, e);
        throw e;
      }
    }
    embedded.rebuildIndexes();
    embedded.internalOpen(user, password);
    embedded.callOnOpenListeners();
    return embedded;
  }

  public DatabaseSessionEmbedded poolOpenNoAuthenticate(String name, String user,
      DatabasePoolInternal pool) {
    final DatabaseSessionEmbedded embedded;
    synchronized (this) {
      checkOpen();
      var storage = getAndOpenStorage(name, pool.getConfig());
      try {
        embedded = newPooledSessionInstance(pool, storage, getOrCreateSharedContext(storage),
            serverMode);
      } catch (RuntimeException e) {
        // Same corpse unregistration as the non-pooled open paths (BG14/CN55/CS55).
        unregisterGenesisIncompleteCorpse(storage, e);
        throw e;
      }
    }

    embedded.rebuildIndexes();
    embedded.internalOpen(user, "nopwd", false);
    embedded.callOnOpenListeners();

    return embedded;
  }

  private static DatabaseSessionEmbedded newPooledSessionInstance(
      DatabasePoolInternal pool, AbstractStorage storage,
      SharedContext sharedContext, boolean serverMode) {
    var embedded = new DatabaseSessionEmbeddedPooled(pool, storage, serverMode);
    embedded.init(pool.getConfig(), sharedContext);
    return embedded;
  }

  private AbstractStorage getOrInitStorage(String name) {
    var storage = storages.get(name);
    if (storage == null) {
      if (basePath == null) {
        throw new DatabaseException(basePath.toString(),
            "Cannot open database '" + name + "' because it does not exists");
      }
      var storagePath = Paths.get(buildName(name));
      if (DiskStorage.exists(storagePath)) {
        name = storagePath.getFileName().toString();
      }

      storage = storages.get(name);
      if (storage == null) {
        storage =
            (AbstractStorage) disk.createStorage(
                buildName(name),
                getMaxWalSegSize(),
                getDoubleWriteLogMaxSegSize(),
                generateStorageId(),
                this);
        if (storage.exists()) {
          storages.put(name, storage);
        }
      }
    }
    return storage;
  }

  private static int generateStorageId() {
    var storageId = Math.abs(nextStorageId.getAndIncrement());
    while (!currentStorageIds.add(storageId)) {
      storageId = Math.abs(nextStorageId.getAndIncrement());
    }

    return storageId;
  }

  public synchronized AbstractStorage getStorage(String name) {
    return storages.get(name);
  }

  private String buildName(String name) {
    if (basePath == null) {
      throw new DatabaseException(basePath.toString(),
          "YouTrackDB instanced created without physical path, only memory databases are allowed");
    }
    return basePath + "/" + name;
  }

  private long getDoubleWriteLogMaxSegSize() {
    try {
      var currentSize = doubleWriteLogMaxSegSize;
      if (currentSize > 0) {
        return currentSize;
      }

      fileMetadataLock.lock();
      try {
        currentSize = doubleWriteLogMaxSegSize;
        if (currentSize > 0) {
          return currentSize;
        }

        if (!Files.exists(basePath)) {
          LogManager.instance()
              .info(this, "Directory " + basePath + " does not exist, try to create it.");

          Files.createDirectories(basePath);
        }

        doubleWriteLogMaxSegSize = calculateDoubleWriteLogMaxSegSize(basePath);
      } finally {
        fileMetadataLock.unlock();
      }

      return doubleWriteLogMaxSegSize;
    } catch (IOException e) {
      throw CoreException.wrapException(
          new DatabaseException(basePath.toString(),
              "Error during calculation of maximum of size of double write log segment."),
          e,
          basePath.toString());
    }
  }

  private long getMaxWalSegSize() {
    try {
      var currentSize = maxWALSegmentSize;
      if (currentSize > 0) {
        return currentSize;
      }

      fileMetadataLock.lock();
      try {
        currentSize = maxWALSegmentSize;
        if (currentSize > 0) {
          return currentSize;
        }

        if (!Files.exists(basePath)) {
          LogManager.instance()
              .info(this, "Directory " + basePath + " does not exist, try to create it.");

          Files.createDirectories(basePath);
        }

        var newSize = calculateInitialMaxWALSegSize();
        if (newSize <= 0) {
          throw new DatabaseException(basePath.toString(),
              "Invalid configuration settings. Can not set maximum size of WAL segment");
        }

        maxWALSegmentSize = newSize;
        LogManager.instance()
            .info(
                this, "WAL maximum segment size is set to %,d MB", newSize / 1024 / 1024);
      } finally {
        fileMetadataLock.unlock();
      }

      return maxWALSegmentSize;
    } catch (IOException e) {
      throw CoreException.wrapException(
          new DatabaseException(basePath.toString(),
              "Error during calculation of maximum of size of WAL segment."),
          e,
          basePath.toString());
    }
  }

  @Override
  public void create(String name, String user, String password, DatabaseType type) {
    create(name, user, password, type, null);
  }

  @Override
  public void create(
      String name, String user, String password, DatabaseType type, YouTrackDBConfig config) {
    create(name, user, password, type, config, true, null);
  }

  @Override
  public void create(
      String name,
      String user,
      String password,
      DatabaseType type,
      YouTrackDBConfig config,
      boolean failIfExists,
      DatabaseTask<Void> createOps) {
    if (createOps != null) {
      createStorage(name, type, (YouTrackDBConfigImpl) config, failIfExists,
          (storage, embedded) -> createOps.call(embedded));
    } else {
      createStorage(name, type, (YouTrackDBConfigImpl) config, failIfExists, null);
    }
  }

  @Override
  public void restore(
      String name,
      String path,
      YouTrackDBConfig config) {
    restore(name, path, null, config);
  }

  @Override
  public void restore(String name, String path,
      @Nullable String expectedUUID, YouTrackDBConfig config) {
    var backupDirectory = Path.of(path);
    restore(name,
        DiskStorage.localBackupFileNames(name, backupDirectory, expectedUUID),
        DiskStorage.localBackupFileStreams(name, backupDirectory),
        expectedUUID,
        config);
  }

  @Override
  public void restore(String name, Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, InputStream> ibuInputStreamSupplier, @Nullable String expectedUUID,
      YouTrackDBConfig config) {
    // The cheap checks of the request run before the chain-sized preparation. An expensive copy
    // therefore never precedes a known refusal of the target name.
    requireFreshRestoreAdmission(name);
    // The preparation runs outside the instance monitor, so a chain-sized copy blocks no other
    // operation of this manager. The preparation changes no file of the target.
    try (var prepared =
        DiskStorage.prepareBackupChain(name, ibuFilesSupplier, ibuInputStreamSupplier,
            expectedUUID, preparedCopyOutputDecoratorForTesting)) {
      restoreStorage(name, (YouTrackDBConfigImpl) config,
          storage -> storage.restoreFromPreparedBackupChain(prepared));
    }
  }

  /** Installs one request-scoped prepared-output decorator for deterministic failure tests. */
  void setPreparedCopyOutputDecoratorForTesting(
      @Nullable UnaryOperator<OutputStream> outputDecorator) {
    preparedCopyOutputDecoratorForTesting = outputDecorator;
  }

  /**
   * Runs every cheap nonmutating check of one fresh restore before the chain preparation.
   *
   * <p>The checks keep the order of the restore itself. The name check runs first, and the target
   * directory resolution runs next. The existence refusal follows, and the reserved-prefix check
   * ends the block. No check creates or updates any file, so a refusal leaves every directory
   * unchanged.
   *
   * <p>The restore repeats these checks inside the exclusion that covers the target creation. The
   * repetition closes the window between this block and that creation.
   */
  private void requireFreshRestoreAdmission(String name) {
    checkDatabaseName(name);
    resolveDatabaseDirectory(name);
    synchronized (this) {
      checkOpen();
      if (exists(name)) {
        throw new DatabaseException(basePath.toString(),
            "Cannot restore database '" + name + "' because it already exists");
      }
      checkReservedDatabaseNamePrefixes(name);
    }
  }

  @Override
  public void restartInterruptedRestore(String name, String path,
      @Nullable String expectedUUID, YouTrackDBConfig config) {
    var backupDirectory = Path.of(path);
    restartInterruptedRestore(name,
        DiskStorage.localBackupFileNames(name, backupDirectory, expectedUUID),
        DiskStorage.localBackupFileStreams(name, backupDirectory),
        expectedUUID,
        config);
  }

  /**
   * Restarts one interrupted restore from one prepared backup chain of any source form.
   *
   * <p>The cheap name checks and the first target-eligibility check precede the preparation. The
   * preparation then copies and validates the complete chain outside the instance monitor. No
   * chain-sized work therefore blocks another operation of this manager. No chain-sized work holds
   * the deletion lock of the target either.
   *
   * <p>The instance monitor then covers the destructive part of the restart. That part repeats the
   * target-eligibility check immediately before the first mutation. The deletion itself repeats
   * the same check inside the authority lock. The deletion also attempts the existing normal
   * database lock without waiting, immediately before the removal of the content files. The
   * in-memory discard of the target therefore stays between the repeated eligibility check and
   * that lock probe. That order is the existing lock order of the restart.
   *
   * <p>The monitor also covers the recreation, the replay of the prepared copies, the final
   * content checks, and the activation. A competing restart of this manager therefore never shuts
   * down the target of a running restart.
   */
  void restartInterruptedRestore(String name, Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, InputStream> ibuInputStreamSupplier, @Nullable String expectedUUID,
      YouTrackDBConfig config) {
    // Both name checks run before the restart changes anything, and both use the same
    // lower-case rule. A refused name therefore never reaches the deletion of the restart.
    checkDatabaseName(name);
    checkReservedDatabaseNamePrefixes(name);
    var targetPath = resolveDatabaseDirectory(name);
    var solvedConfig = solveConfig((YouTrackDBConfigImpl) config);
    synchronized (this) {
      checkOpen();
      // The acceptance check runs before the restart touches any in-memory state. A refused
      // restart therefore keeps every file and every live session of a healthy database. The
      // check creates no authority lock file of an unrelated directory.
      requireInterruptedRestoreTarget(name, targetPath);
    }
    try (var prepared =
        DiskStorage.prepareBackupChain(name, ibuFilesSupplier, ibuInputStreamSupplier,
            expectedUUID)) {
      // One instance monitor covers the acceptance check, the deletion, and the new restore. No
      // concurrent creation, open, or restart can occupy the name between the deletion and the
      // new restore. The name therefore never holds an empty active database.
      synchronized (this) {
        checkOpen();
        // The repeated check runs immediately before the first destructive step. A target that
        // became a healthy database during the preparation therefore survives the restart.
        requireInterruptedRestoreTarget(name, targetPath);
        // The registration of an earlier failed restore attempt of this process leaves now. The
        // registration would otherwise pin an open storage over the deleted files.
        discardRestoreTargetRegistration(name, targetPath);
        try {
          DiskStorage.deleteInterruptedRestoreTarget(targetPath);
        } catch (StorageAdmissionException rejection) {
          throw refusedRestart(name, rejection);
        } catch (IOException deletionFailure) {
          throw BaseException.wrapException(
              new DatabaseException(basePath.toString(),
                  "Cannot delete the interrupted restore target of database '"
                      + name
                      + "'"),
              deletionFailure,
              basePath.toString());
        }
        if (restartAfterDeletionForTesting != null) {
          restartAfterDeletionForTesting.run();
        }
        restoreStorage(name, solvedConfig,
            storage -> storage.restoreFromPreparedBackupChain(prepared), true);
      }
    }
  }

  /** Installs one deterministic test action after restart deletion and inside manager exclusion. */
  void setRestartAfterDeletionForTesting(@Nullable Runnable action) {
    restartAfterDeletionForTesting = action;
  }

  /**
   * Checks that one named database is a valid destructive restart target.
   *
   * <p>A destructive restart is the full deletion of an interrupted restore target and a fresh
   * restore. This check changes no file and no in-memory state. The deletion repeats the same
   * check inside its own exclusive unit, so the acceptance and the deletion stay one exclusive
   * unit.
   */
  private void requireInterruptedRestoreTarget(String name, Path targetPath) {
    try {
      DiskStorage.requireInterruptedRestoreTarget(targetPath);
    } catch (StorageAdmissionException rejection) {
      throw refusedRestart(name, rejection);
    } catch (IOException probeFailure) {
      throw BaseException.wrapException(
          new DatabaseException(basePath.toString(),
              "Cannot check the restore target of database '" + name + "'"),
          probeFailure,
          basePath.toString());
    }
  }

  /** Builds the failure of one refused destructive restart. The refusal changes no file. */
  private BaseException refusedRestart(String name, StorageAdmissionException rejection) {
    return BaseException.wrapException(
        new DatabaseException(basePath.toString(),
            "Cannot restart the interrupted restore of database '"
                + name
                + "' because the destructive restart refuses this target"),
        rejection,
        basePath.toString());
  }

  /**
   * Resolves the directory of one database inside the configured databases directory.
   *
   * <p>The resolved path must be one direct child of the databases directory. A name that escapes
   * the databases directory is refused. A path that exists and is no real directory is refused as
   * well, because a symbolic link would redirect a later deletion to another location.
   *
   * @return the resolved directory of the named database
   */
  private Path resolveDatabaseDirectory(String name) {
    if (basePath == null) {
      throw new DatabaseException("",
          "This manager holds no databases directory, so this manager allows a memory database"
              + " only");
    }
    var databasesDirectory = basePath.toAbsolutePath().normalize();
    var target = databasesDirectory.resolve(name).normalize();
    if (!databasesDirectory.equals(target.getParent())) {
      throw new DatabaseException(basePath.toString(),
          "Invalid database name '"
              + name
              + "'. A database name must resolve to one direct child of the databases directory");
    }
    try {
      var attributes =
          Files.readAttributes(target, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
      if (!attributes.isDirectory()) {
        throw new DatabaseException(basePath.toString(),
            "The path of database '" + name + "' is no real directory");
      }
    } catch (NoSuchFileException absentTarget) {
      // An absent directory needs no further check, because a later restore creates it.
    } catch (IOException readFailure) {
      throw BaseException.wrapException(
          new DatabaseException(basePath.toString(),
              "Cannot read the attributes of the directory of database '" + name + "'"),
          readFailure,
          basePath.toString());
    }
    return target;
  }

  /**
   * Creates one restore target without genesis and without activation, and then restores content.
   *
   * <p>Genesis is the creation of the initial database metadata. Genesis creates the schema, the
   * index manager, the index statistics, and the default users. The backup content replaces every
   * genesis artifact, so a restore target runs no genesis at all.
   *
   * <p>The order of this method has four steps. The first step creates the storage files, which
   * publishes the birth-in-progress lifecycle state. The second step publishes the
   * restore-in-progress lifecycle state directly from that birth-in-progress state. The third step
   * copies the backup content. The fourth step validates the restored content, passes a durability
   * barrier, and publishes the active lifecycle state inside the storage.
   *
   * <p>A failed restore keeps every file of the target, because the destructive restart entry and
   * the drop both need that evidence. A failed restore removes the in-memory registration only.
   */
  private void restoreStorage(String name, YouTrackDBConfigImpl config,
      Consumer<AbstractStorage> restoreContent) {
    restoreStorage(name, config, restoreContent, false);
  }

  /**
   * Restores one target, and skips the existence refusal after a destructive restart.
   *
   * @param afterDestructiveRestart true when the deletion of a destructive restart ran before
   */
  private void restoreStorage(String name, YouTrackDBConfigImpl config,
      Consumer<AbstractStorage> restoreContent, boolean afterDestructiveRestart) {
    checkDatabaseName(name);
    // Every restore entry resolves the target directory of the name. A name that escapes the
    // databases directory therefore never reaches the creation of a storage. The restart entry
    // resolved the same directory before the deletion, and one repeated resolution changes no file.
    resolveDatabaseDirectory(name);
    synchronized (this) {
      checkOpen();
      // The deletion of a destructive restart keeps the authority lock file of the target, so
      // the existence probe still reports one database. That single file is the expected state
      // after the deletion, and only the restart caller therefore skips the refusal below.
      if (!afterDestructiveRestart && exists(name)) {
        throw new DatabaseException(basePath.toString(),
            "Cannot restore database '" + name + "' because it already exists");
      }
      checkReservedDatabaseNamePrefixes(name);

      var solvedConfig = solveConfig(config);
      var storage =
          (AbstractStorage) disk.createStorage(
              buildName(name),
              getMaxWalSegSize(),
              getDoubleWriteLogMaxSegSize(),
              generateStorageId(),
              this);
      storages.put(name, storage);
      try {
        storage.create(solvedConfig.getConfiguration());
        storage.beginRestoreLifecycle();
        restoreContent.accept(storage);
      } catch (RuntimeException | Error restoreFailure) {
        // The on-disk target survives on purpose. The target carries the restore-in-progress
        // lifecycle state, so the destructive restart entry accepts the target, and a drop
        // discards the target. Only the in-memory registration leaves here.
        closeFailedRestoreTarget(name, storage, restoreFailure);
        throw restoreFailure;
      }
    }
  }

  /** Closes and unregisters the storage of a failed restore without any file deletion. */
  private void closeFailedRestoreTarget(
      String name, AbstractStorage storage, Throwable restoreFailure) {
    storages.remove(name);
    currentStorageIds.remove(storage.getId());
    var sharedContext = sharedContexts.remove(name);
    if (sharedContext != null) {
      try {
        sharedContext.close();
      } catch (RuntimeException cleanupFailure) {
        restoreFailure.addSuppressed(cleanupFailure);
      }
    }
    try {
      storage.shutdown();
    } catch (RuntimeException | Error cleanupFailure) {
      // A half-restored storage can refuse a clean shutdown. The restore failure stays primary.
      restoreFailure.addSuppressed(cleanupFailure);
    }
  }

  /**
   * Removes every in-memory registration of one restore target before a destructive restart.
   *
   * <p>The registration map holds one storage per database name. A memory database and a database
   * of a custom directory both use that same map. The restart therefore checks that the resolved
   * target directory backs the registered storage. A registered storage of another directory keeps
   * every registration, and the restart reports the mismatch to the caller.
   *
   * @param name the database name of the restore target
   * @param targetPath the resolved directory of the restore target
   */
  private void discardRestoreTargetRegistration(String name, Path targetPath) {
    requireRegisteredStorageOfTarget(name, targetPath);
    var sharedContext = sharedContexts.remove(name);
    if (sharedContext != null) {
      sharedContext.close();
    }
    var storage = storages.remove(name);
    if (storage != null) {
      currentStorageIds.remove(storage.getId());
      try {
        storage.shutdown();
      } catch (RuntimeException | Error cleanupFailure) {
        LogManager.instance()
            .warn(this,
                "Could not shut down the registered storage of restore target '%s'",
                cleanupFailure, name);
      }
    }
  }

  /**
   * Refuses a destructive restart whose registered storage sits in another directory.
   *
   * <p>A memory database keeps no directory at all, so a memory database never passes this check.
   * A database of a custom directory passes this check only when the custom directory equals the
   * resolved target directory. The check therefore stops the restart before the restart shuts down
   * a storage that the resolved target directory does not back.
   *
   * @param name the database name of the restore target
   * @param targetPath the resolved directory of the restore target
   */
  private void requireRegisteredStorageOfTarget(String name, Path targetPath) {
    var storage = storages.get(name);
    if (storage == null) {
      return;
    }
    if (storage instanceof DiskStorage diskStorage
        && diskStorage.getStoragePath().equals(targetPath)) {
      return;
    }
    throw new DatabaseException(basePath.toString(),
        "Cannot restart the interrupted restore of database '"
            + name
            + "' because the registered storage of that name does not sit in the directory "
            + targetPath);
  }

  /** Refuses a database name that carries one of the two reserved prefixes. */
  private static void checkReservedDatabaseNamePrefixes(String name) {
    var lowerCaseName = name.toLowerCase(Locale.ROOT);
    if (lowerCaseName.startsWith("ytdb")) {
      throw new IllegalArgumentException("Usage of database names started with 'ytdb'"
          + " is prohibited as it is used as prefix of the names of GraphTraversal instances,"
          + " provided name : "
          + name);
    }
    if (lowerCaseName.startsWith("server")) {
      throw new IllegalArgumentException(
          "Database name can not start with 'server' prefix, provided name is : " + name);
    }
  }

  private void createStorage(String name,
      DatabaseType type,
      YouTrackDBConfigImpl config, boolean failIfExists,
      BiConsumer<AbstractStorage, DatabaseSessionEmbedded> createOps) {
    checkDatabaseName(name);
    final DatabaseSessionEmbedded embedded;
    synchronized (this) {
      if (!exists(name)) {
        try {
          checkReservedDatabaseNamePrefixes(name);

          config = solveConfig(config);
          AbstractStorage storage;
          if (type == DatabaseType.MEMORY) {
            storage =
                (AbstractStorage) memory.createStorage(
                    name,
                    -1,
                    -1,
                    generateStorageId(),
                    this);
          } else {
            storage =
                (AbstractStorage) disk.createStorage(
                    buildName(name),
                    getMaxWalSegSize(),
                    getDoubleWriteLogMaxSegSize(),
                    generateStorageId(),
                    this);
          }
          storages.put(name, storage);
          embedded = internalCreate(config, storage);

          if (createOps != null) {
            createOps.accept(storage, embedded);
          }
        } catch (Exception e) {
          // Genesis-failure containment (design §A1, CS34+CN48): a failed create must leave NO
          // residue — otherwise exists() stays true, create(failIfExists=false) silently no-ops
          // on the corpse, and a create-retry is refused. The maps are purged, the storage
          // closed and its on-disk content deleted, restoring exists() == false. Cleanup
          // failures are attached as suppressed so the primary genesis failure propagates.
          cleanUpFailedCreate(name, e);
          throw BaseException.wrapException(
              new DatabaseException(basePath.toString(), "Cannot create database '" + name + "'"),
              e,
              basePath.toString());
        } catch (Error e) {
          // An Error (OOM, StackOverflow) mid-genesis must not skip the residue cleanup either
          // (review CN56); the cleanup is best-effort under an Error, and the Error itself is
          // rethrown unwrapped.
          cleanUpFailedCreate(name, e);
          throw e;
        }

        embedded.callOnCreateListeners();
      } else {
        if (failIfExists) {
          throw new DatabaseException(basePath.toString(),
              "Cannot create new database '" + name + "' because it already exists");
        } else {
          // Never silently no-op over a genesis-incomplete residue (§A1's letter, review
          // BG14): probe the pre-existing database's completion marker — opening the storage
          // exactly as a subsequent open would (a healthy database is left open and
          // registered, the same state an open() call produces). A marker-less residue is a
          // crash corpse (or an old-format/pre-marker database, which cannot be silently
          // adopted either); it is refused loudly instead of being reported as "nothing to
          // do". The message avoids unconditional discard advice: an old-format database's
          // migration guidance lives in the open path's schema-version gate.
          AbstractStorage existing;
          try {
            existing = getAndOpenStorage(name, solveConfig(config));
          } catch (RuntimeException openFailure) {
            // Track 24 delayed activation: a crashed creation now leaves an image in the
            // birth-in-progress lifecycle state, and admission rejects that image before the
            // marker branch below can read any property. The interrupted-birth reason therefore
            // gets the same tailored explanation as a marker-less image. The failed open of
            // getAndOpenStorage already removed the registration and freed the storage
            // identifier. Every other open failure keeps its present behavior and propagates.
            if (!isCausedByInterruptedBirth(openFailure)) {
              throw openFailure;
            }
            // The interrupted-birth reason covers two causes. The first cause is a creation that
            // never finished. The second cause is damage to every authority record of a complete
            // database. The message therefore states the observed condition and names both
            // causes. The wording lives in the reason description alone, so the create path and
            // the open path always tell the operator the same story.
            var interruptedBirthRefusal = new GenesisIncompleteException(name,
                "Cannot create database '"
                    + name
                    + "': a database with this name already exists, and storage admission"
                    + " rejected the image of that database with the interrupted-birth reason. "
                    + StorageAdmissionException.Reason.INTERRUPTED_BIRTH.description());
            interruptedBirthRefusal.addSuppressed(openFailure);
            throw interruptedBirthRefusal;
          }
          if (!Boolean.parseBoolean(
              existing.getProperty(SharedContext.GENESIS_COMPLETED_PROPERTY))) {
            var refusal = new GenesisIncompleteException(name,
                "Cannot create database '"
                    + name
                    + "': a database with this name already exists but does not carry the"
                    + " genesis-completion marker. If it is the residue of a crashed creation,"
                    + " drop it and re-create; if it was created by an older version, open it"
                    + " directly to get the migration guidance.");
            unregisterGenesisIncompleteCorpse(existing, refusal);
            throw refusal;
          }
          LogManager.instance()
              .info(this, "Database '%s' already exists, nothing to do", name);
          return;
        }

      }
    }
    embedded.callOnCreateListeners();
  }

  /**
   * Purges every trace of a failed database creation (design §A1, primary containment arm):
   * the storage is removed from {@link #storages} and its shared context from
   * {@link #sharedContexts}, the storage is closed and its on-disk residue deleted, so
   * {@link #exists} returns {@code false} again and both a create-retry and a
   * {@code create(failIfExists=false)} call re-create cleanly instead of silently adopting a
   * half-genesis corpse. Runs inside the caller's synchronized block. Cleanup failures never
   * mask the primary genesis failure — they are logged and attached as suppressed.
   */
  private void cleanUpFailedCreate(String name, Throwable primaryFailure) {
    var sharedContext = sharedContexts.remove(name);
    if (sharedContext != null) {
      try {
        sharedContext.close();
      } catch (RuntimeException cleanupFailure) {
        primaryFailure.addSuppressed(cleanupFailure);
      }
    }
    var storage = storages.remove(name);
    if (storage != null) {
      currentStorageIds.remove(storage.getId());
      try {
        // delete() closes the storage and removes its on-disk content (a no-op set of files
        // for the memory profile); it tolerates a storage whose create failed early.
        storage.delete();
      } catch (RuntimeException cleanupFailure) {
        LogManager.instance()
            .warn(this,
                "Could not delete the residue of the failed creation of database '%s'",
                cleanupFailure, name);
        primaryFailure.addSuppressed(cleanupFailure);
      }
    }
  }

  /**
   * Creates one storage image and runs genesis on that image.
   *
   * <p>Genesis is the creation of the initial database metadata. Genesis creates the schema, the
   * index manager, the index statistics, and the default users. Genesis completes inside
   * {@code newCreateSessionInstance}, which is the database session creation path.
   *
   * <p>This method is the single funnel of every production creation path. The ordinary create
   * path, the restore create path, and the server registration path all reach this method. The
   * publication of the active lifecycle state therefore happens here, after genesis finished and
   * after the durability barrier completed (Track 24 delayed activation).
   */
  private DatabaseSessionEmbedded internalCreate(
      YouTrackDBConfigImpl config, AbstractStorage storage) {
    storage.create(config.getConfiguration());
    var session = newCreateSessionInstance(storage, config, getOrCreateSharedContext(storage));
    // Genesis finished above. The barrier and the activation run only now, so a crash inside
    // genesis leaves an image that storage admission rejects with the interrupted-birth reason.
    storage.completeStorageBirth();
    return session;
  }

  private synchronized SharedContext getOrCreateSharedContext(
      AbstractStorage storage) {
    var result = sharedContexts.get(storage.getName());
    if (result == null) {
      result = createSharedContext(storage);
      sharedContexts.put(storage.getName(), result);
    }
    return result;
  }

  private SharedContext createSharedContext(AbstractStorage storage) {
    return new SharedContext(storage, this);
  }

  @Override
  public synchronized boolean exists(String name) {
    checkOpen();
    Storage storage = storages.get(name);
    if (storage == null) {
      if (basePath != null) {
        return DiskStorage.exists(Paths.get(buildName(name)));
      } else {
        return false;
      }
    }
    return storage.exists();
  }

  @Override
  public void drop(String name, String user, String password) {
    synchronized (this) {
      checkOpen();
    }
    checkDatabaseName(name);
    try {
      DatabaseSessionEmbedded db = null;
      try {
        db = openNoAuthenticate(name, user);
      } catch (RuntimeException openFailure) {
        // The genesis-completion check gates opens FOR USE; it must not block the very discard
        // it prescribes (design CN54). A genesis-incomplete corpse is deleted below without the
        // onDrop listeners — no usable session can be minted for it. Any other open failure
        // keeps today's behavior: the deletion in the finally still runs, and the failure
        // surfaces to the caller.
        // Track 24 adds the second tolerated cause. Delayed activation leaves the birth-in-progress
        // lifecycle state after a crashed creation, and admission then reports the
        // interrupted-birth reason. Drop deletes that incomplete image and reports success. Drop
        // stays loud for every other cause, so unrelated damage still reaches the operator.
        // Track 24 adds the third tolerated cause. An interrupted restore leaves the
        // restore-in-progress lifecycle state, and admission then reports the interrupted-restore
        // reason. A backup whose content fails the restore validation would otherwise leave a
        // target that no entry point can clear, so the drop deletes that target and reports
        // success.
        // Track 24 adds the fourth tolerated cause. The genesis check of the shared context now
        // reports the named inconsistent-metadata result with the genesis marker value. The drop
        // path tolerates that value and stays loud for the storage layout version value, because
        // an operator must see a layout version mismatch.
        if (!isCausedByGenesisIncomplete(openFailure)
            && !isCausedByGenesisMarkerInconsistency(openFailure)
            && !isCausedByInterruptedBirth(openFailure)
            && !isCausedByInterruptedRestore(openFailure)) {
          throw openFailure;
        }
        LogManager.instance()
            .info(this,
                "Dropping database '%s' whose creation never ran to completion; the onDrop"
                    + " lifecycle listeners are skipped because no session can be opened on it",
                name);
      }
      if (db != null) {
        for (var it = youTrack.getDbLifecycleListeners();
            it.hasNext();) {
          it.next().onDrop(db);
        }
        db.close();
      }
    } finally {
      synchronized (this) {
        if (exists(name)) {
          var storage = getOrInitStorage(name);
          var sharedContext = sharedContexts.get(name);
          if (sharedContext != null) {
            sharedContext.close();
          }
          final var storageId = storage.getId();
          try {
            storage.delete();
          } finally {
            // Always remove the storage from internal maps, even if delete() threw.
            // Leaving a partially-deleted storage in the maps would poison the
            // YouTrackDB instance for all subsequent operations on any database.
            storages.remove(name);
            currentStorageIds.remove(storageId);
            sharedContexts.remove(name);
          }
        }
      }
    }
  }

  private interface DatabaseFound {

    void found(String name);
  }

  @Override
  public synchronized Set<String> listDatabases(String user, String password) {
    checkOpen();
    // SEARCH IN CONFIGURED PATHS
    final Set<String> databases = new HashSet<>();
    // SEARCH IN DEFAULT DATABASE DIRECTORY
    if (basePath != null) {
      scanDatabaseDirectory(basePath.toFile(), databases::add);
    }
    databases.addAll(this.storages.keySet());
    // TODO: Verify validity this generic permission on guest
    if (!securitySystem.isAuthorized(null, "guest", "server.listDatabases.system")) {
      databases.remove(SystemDatabase.SYSTEM_DB_NAME);
    }
    return databases;
  }

  public synchronized void loadAllDatabases() {
    if (basePath != null) {
      scanDatabaseDirectory(
          basePath.toFile(),
          (name) -> {
            if (!storages.containsKey(name)) {
              var storage = getOrInitStorage(name);
              // THIS OPEN THE STORAGE ONLY THE FIRST TIME
              storage.open(configuration.getConfiguration());
              storage.setHistogramExecutor(youTrack.getExecutor());
            }
          });
    }
  }

  @Override
  public DatabasePoolInternal openPool(String name, String user, String password) {
    return openPool(name, user, password, null);
  }

  @Override
  public DatabasePoolInternal openPool(
      String name, String user, String password, YouTrackDBConfig config) {
    checkDatabaseName(name);
    checkOpen();
    var pool = new DatabasePoolImpl(this, name, user, password,
        solveConfig((YouTrackDBConfigImpl) config));
    pools.add(pool);
    return pool;
  }

  @Override
  public DatabasePoolInternal cachedPool(String database, String user,
      String password) {
    return cachedPool(database, user, password, null);
  }

  @Override
  public DatabasePoolInternal cachedPool(
      String database, String user, String password, YouTrackDBConfig config) {
    checkDatabaseName(database);
    checkOpen();
    var pool =
        cachedPoolFactory.getOrCreate(database, user, password,
            solveConfig((YouTrackDBConfigImpl) config));
    pools.add(pool);
    return pool;
  }

  @Override
  public DatabasePoolInternal cachedPoolNoAuthentication(String database,
      String user, YouTrackDBConfig config) {
    checkDatabaseName(database);
    checkOpen();
    var pool =
        cachedPoolFactory.getOrCreateNoAuthentication(database, user,
            solveConfig((YouTrackDBConfigImpl) config));
    pools.add(pool);
    return pool;
  }

  @Override
  public void close() {
    if (!open) {
      return;
    }
    timeoutChecker.close();
    securitySystem.shutdown();
    synchronized (this) {
      scriptManager.closeAll();
      internalClose();
      currentStorageIds.clear();
    }
    removeShutdownHook();
  }

  @Override
  public synchronized void internalClose() {
    if (!open) {
      return;
    }
    open = false;
    this.sharedContexts.values().forEach(SharedContext::close);
    final List<AbstractStorage> storagesCopy = new ArrayList<>(storages.values());

    Exception storageException = null;

    for (var stg : storagesCopy) {
      try {
        LogManager.instance().info(this, "- shutdown storage: %s ...", stg.getName());
        stg.shutdown();
      } catch (Exception e) {
        LogManager.instance().warn(this, "-- error on shutdown storage", e);
        storageException = e;
      } catch (Error e) {
        LogManager.instance().warn(this, "-- error on shutdown storage", e);
        throw e;
      }
    }
    this.sharedContexts.clear();
    storages.clear();
    youTrack.onEmbeddedFactoryClose(this);
    var acf = autoCloseFuture;
    if (acf != null) {
      acf.cancel(false);
    }

    if (storageException != null) {
      throw BaseException.wrapException(
          new StorageException(basePath.toString(), "Error during closing the storages"),
          storageException,
          basePath.toString());
    }
  }

  @Override
  public YouTrackDBConfigImpl getConfiguration() {
    return configuration;
  }

  @Override
  public void removePool(DatabasePoolInternal pool) {
    pools.remove(pool);
  }

  private static void scanDatabaseDirectory(final File directory, DatabaseFound found) {
    if (directory.exists() && directory.isDirectory()) {
      final var files = directory.listFiles();
      if (files != null) {
        for (var db : files) {
          if (db.isDirectory()) {
            for (var cf : db.listFiles()) {
              var fileName = cf.getName();
              // The scanner recognizes the same bootstrap authority artifact set as the existence
              // probe of the disk storage. A listing and an existence check therefore agree after
              // an interrupted storage birth.
              if (fileName.equals("database.ocf")
                  || (fileName.startsWith(CollectionBasedStorageConfiguration.COMPONENT_NAME)
                      && fileName.endsWith(
                          CollectionBasedStorageConfiguration.DATA_FILE_EXTENSION))
                  || StorageBootstrapMetadata.isBootstrapArtifactName(fileName)) {
                found.found(db.getName());
                break;
              }
            }
          }
        }
      }
    }
  }

  public synchronized void initCustomStorage(String name, String path) {
    DatabaseSessionEmbedded embedded = null;
    synchronized (this) {
      var exists = DiskStorage.exists(Paths.get(path));
      var storage =
          (AbstractStorage) disk.createStorage(
              path, getMaxWalSegSize(), getDoubleWriteLogMaxSegSize(), generateStorageId(),
              this);
      // TODO: Add Creation settings and parameters
      if (!exists) {
        try {
          embedded = internalCreate(configuration, storage);
        } catch (RuntimeException | Error e) {
          // §A1 parity for the server-configured create path (reviews CS53/CN58): a genesis
          // failure here must not leave residue the next boot silently adopts. The shared
          // context internalCreate registered is purged, the storage deleted (closing it and
          // removing the on-disk content); the storage was not yet in the storages map.
          var sharedContext = sharedContexts.remove(storage.getName());
          if (sharedContext != null) {
            try {
              sharedContext.close();
            } catch (RuntimeException cleanupFailure) {
              e.addSuppressed(cleanupFailure);
            }
          }
          currentStorageIds.remove(storage.getId());
          try {
            storage.delete();
          } catch (RuntimeException cleanupFailure) {
            e.addSuppressed(cleanupFailure);
          }
          throw e;
        }
      }
      storages.put(name, storage);
    }
    if (embedded != null) {
      embedded.callOnCreateListeners();
    }
  }

  @Override
  public void removeShutdownHook() {
    youTrack.removeYouTrackDB(this);
  }

  public synchronized Collection<Storage> getStorages() {
    return storages.values().stream().map((x) -> (Storage) x).collect(Collectors.toSet());
  }

  @Override
  public synchronized void forceDatabaseClose(String iDatabaseName) {
    var storage = storages.remove(iDatabaseName);
    if (storage != null) {
      var ctx = sharedContexts.remove(iDatabaseName);
      ctx.close();
      storage.shutdown();
    }
  }

  private void checkOpen() {
    if (!open) {
      throw new DatabaseException(basePath.toString(), "YouTrackDB Instance is closed");
    }
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean isEmbedded() {
    return true;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable task, long delay, long period) {
    // Wrap the task to catch exceptions and keep periodic tasks alive.
    // An uncaught exception in scheduleWithFixedDelay silently stops all future executions.
    Runnable safeTask = () -> {
      try {
        task.run();
      } catch (Exception e) {
        LogManager.instance().error(this,
            "Error during execution of periodic task " + task.getClass().getSimpleName(), e);
      }
    };
    return YouTrackDBEnginesManager.instance().getScheduledPool()
        .scheduleWithFixedDelay(safeTask, delay, period, TimeUnit.MILLISECONDS);
  }

  @Override
  public ScheduledFuture<?> scheduleOnce(Runnable task, long delay) {
    return YouTrackDBEnginesManager.instance().getScheduledPool()
        .schedule(task, delay, TimeUnit.MILLISECONDS);
  }

  public <X> Future<X> execute(String database, String user, DatabaseTask<X> task) {
    return youTrack.getExecutor().submit(
        () -> {
          try (var session = openNoAuthenticate(database, user)) {
            return task.call(session);
          }
        });
  }

  public Future<?> execute(Runnable task) {
    return youTrack.getExecutor().submit(task);
  }

  public <X> Future<X> execute(Callable<X> task) {
    return youTrack.getExecutor().submit(task);
  }

  public <X> Future<X> executeNoAuthorizationAsync(String database, DatabaseTask<X> task) {
    return youTrack.getExecutor().submit(
        () -> {
          if (open) {
            try (var session = openNoAuthorization(database)) {
              return task.call(session);
            }
          } else {
            LogManager.instance()
                .warn(this, " Cancelled execution of task, YouTrackDB instance is closed");
            return null;
          }
        });
  }

  public <X> X executeNoAuthorizationSync(
      DatabaseSessionEmbedded database, DatabaseTask<X> task) {
    var dbName = database.getDatabaseName();
    if (open) {
      try (var session = openNoAuthorization(dbName)) {
        return task.call(session);
      } finally {
        database.activateOnCurrentThread();
      }
    } else {
      throw new DatabaseException(basePath.toString(), "YouTrackDB instance is closed");
    }
  }

  public ScriptManager getScriptManager() {
    return scriptManager;
  }

  @Override
  public SystemDatabase getSystemDatabase() {
    return systemDatabase;
  }

  @Override
  public DefaultSecuritySystem getSecuritySystem() {
    return securitySystem;
  }

  @Override
  public String getBasePath() {
    return basePath.toString();
  }

  @Override
  public boolean isMemoryOnly() {
    return basePath == null;
  }

  /**
   * Refuses an invalid database name.
   *
   * <p>The two reserved prefixes use the same lower-case rule as
   * {@link #checkReservedDatabaseNamePrefixes}. One single letter-case rule keeps every caller of
   * the two checks consistent, so no caller passes the first check and fails the second check.
   */
  private void checkDatabaseName(String name) {
    Objects.requireNonNull(name, "Database name is required");
    if (name.contains("/") || name.contains(":")) {
      throw new DatabaseException(basePath.toString(),
          String.format("Invalid database name:'%s'", name));
    }
    var lowerCaseName = name.toLowerCase(Locale.ROOT);
    if (lowerCaseName.startsWith("ytdb")) {
      throw new DatabaseException(basePath.toString(),
          String.format("Invalid database name:'%s'. Database name cannot start with 'ytdb'",
              name));
    }
    if (lowerCaseName.startsWith("server")) {
      throw new DatabaseException(basePath.toString(),
          String.format("Invalid database name:'%s'. Database name cannot start with 'server'",
              name));
    }
  }

  public void startCommand(@Nullable Long timeout) {
    timeoutChecker.startCommand(timeout);
  }

  public void endCommand() {
    timeoutChecker.endCommand();
  }

  @Override
  public String getConnectionUrl() {
    var connectionUrl = "embedded:";
    if (basePath != null) {
      connectionUrl += basePath;
    }
    return connectionUrl;
  }

  public ExecutorService getIoExecutor() {
    return youTrack.getIoExecutor();
  }
}
