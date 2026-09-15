package com.jetbrains.youtrackdb.internal.core.storage.config;

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.internal.common.concur.lock.ScalableRWLock;
import com.jetbrains.youtrackdb.internal.common.log.LogManager;
import com.jetbrains.youtrackdb.internal.common.serialization.types.ByteSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.StringSerializer;
import com.jetbrains.youtrackdb.internal.common.util.RawPair;
import com.jetbrains.youtrackdb.internal.common.util.RawPairObjectInteger;
import com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrackdb.internal.core.config.IndexEngineData;
import com.jetbrains.youtrackdb.internal.core.config.StorageCollectionConfiguration;
import com.jetbrains.youtrackdb.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrackdb.internal.core.config.StorageConfigurationUpdateListener;
import com.jetbrains.youtrackdb.internal.core.config.StorageEntryConfiguration;
import com.jetbrains.youtrackdb.internal.core.config.StorageFileConfiguration;
import com.jetbrains.youtrackdb.internal.core.config.StoragePaginatedCollectionConfiguration;
import com.jetbrains.youtrackdb.internal.core.config.StorageSegmentConfiguration;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.exception.BaseException;
import com.jetbrains.youtrackdb.internal.core.exception.ConfigurationException;
import com.jetbrains.youtrackdb.internal.core.exception.SerializationException;
import com.jetbrains.youtrackdb.internal.core.exception.StorageException;
import com.jetbrains.youtrackdb.internal.core.id.RecordId;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.PropertyTypeInternal;
import com.jetbrains.youtrackdb.internal.core.storage.PhysicalPosition;
import com.jetbrains.youtrackdb.internal.core.storage.RawBuffer;
import com.jetbrains.youtrackdb.internal.core.storage.cache.WriteCache;
import com.jetbrains.youtrackdb.internal.core.storage.collection.PaginatedCollection;
import com.jetbrains.youtrackdb.internal.core.storage.disk.DiskStorage;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.AbstractStorage;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.StorageCollectionFactory;
import com.jetbrains.youtrackdb.internal.core.storage.index.sbtree.singlevalue.v3.BTree;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class CollectionBasedStorageConfiguration implements StorageConfiguration {

  public static final String MAP_FILE_EXTENSION = ".ccm";
  public static final String FREE_MAP_FILE_EXTENSION = ".fcm";
  // Dirty page bit set extension for the config collection (distinct from the default .dpb
  // used by regular data collections — see CollectionDirtyPageBitSet.DEF_EXTENSION).
  public static final String DIRTY_PAGE_FILE_EXTENSION = ".dcm";
  public static final String DATA_FILE_EXTENSION = ".cd";

  public static final String TREE_DATA_FILE_EXTENSION = ".bd";
  public static final String TREE_NULL_FILE_EXTENSION = ".nd";

  public static final String COMPONENT_NAME = "config";
  private static final String VERSION_PROPERTY = "version";
  private static final String SCHEMA_RECORD_ID_PROPERTY = "schemaRecordId";
  private static final String INDEX_MANAGER_RECORD_ID_PROPERTY = "indexManagerRecordId";

  private static final String LOCALE_LANGUAGE_PROPERTY = "localeLanguage";
  private static final String LOCALE_COUNTRY_PROPERTY = "localeCountry";
  private static final String LOCALE_PROPERTY_INSTANCE = "localeInstance";

  private static final String DATE_FORMAT_PROPERTY = "dateFormat";
  private static final String DATE_TIME_FORMAT_PROPERTY = "dateTimeFormat";

  private static final String TIME_ZONE_PROPERTY = "timeZone";
  private static final String CHARSET_PROPERTY = "charset";
  private static final String CONFLICT_STRATEGY_PROPERTY = "conflictStrategy";
  private static final String BINARY_FORMAT_VERSION_PROPERTY = "binaryFormatVersion";
  private static final String COLLECTION_SELECTION_PROPERTY = "collectionSelection";
  private static final String MINIMUM_COLLECTIONS_PROPERTY = "minimumCollections";
  private static final String RECORD_SERIALIZER_PROPERTY = "recordSerializer";
  private static final String RECORD_SERIALIZER_VERSION_PROPERTY = "recordSerializerVersion";
  private static final String CONFIGURATION_PROPERTY = "configuration";
  private static final int CONFIGURATION_PROPERTY_VERSION = 0;
  private static final String CREATED_AT_VERSION_PROPERTY = "createAtVersion";
  private static final String PAGE_SIZE_PROPERTY = "pageSize";
  private static final String FREE_LIST_BOUNDARY_PROPERTY = "freeListBoundary";
  private static final String MAX_KEY_SIZE_PROPERTY = "maxKeySize";

  private static final String COLLECTIONS_PREFIX_PROPERTY = "collection_";
  private static final int COLLECTIONS_PROPERTY_VERSION = 0;
  private static final String PROPERTY_PREFIX_PROPERTY = "property_";

  private static final String ENGINE_PREFIX_PROPERTY = "engine_";
  // 2: the entry carries the engine's stable fileBaseId after the indexId. Engine entries are
  // delete+add only — storeProperty never rewrites the version tag on an in-place update, so an
  // entry's tag always matches the format of its bytes (see the storeProperty update-branch
  // assert).
  private static final int INDEX_ENGINE_PROPERTY_VERSION = 2;

  /**
   * The persisted allocation floor of the index-engine file-base-id counter, written alongside
   * every allocation inside the allocating atomic operation (so a rolled-back engine create
   * reverts the floor with the files, while the in-process high-water mark keeps the burned value
   * from ever being handed out again). The key deliberately starts with none of the
   * {@code collection_} / {@code property_} / {@code engine_} scan prefixes, so the prefix
   * iterations ({@code preloadCollections}, {@code getProperties}, {@code indexEngines}) never
   * misparse it as one of theirs.
   */
  private static final String INDEX_ENGINE_FILE_BASE_ID_FLOOR_PROPERTY =
      "indexEngineFileBaseIdFloor";

  private static final String PROPERTIES = "properties";
  private static final String COLLECTIONS = "collections";
  private static final String UUID = "UUID";

  private static final String[] INT_PROPERTIES =
      new String[] {
          MINIMUM_COLLECTIONS_PROPERTY,
          VERSION_PROPERTY,
          BINARY_FORMAT_VERSION_PROPERTY,
          RECORD_SERIALIZER_VERSION_PROPERTY,
          PAGE_SIZE_PROPERTY,
          FREE_LIST_BOUNDARY_PROPERTY,
          MAX_KEY_SIZE_PROPERTY,
          INDEX_ENGINE_FILE_BASE_ID_FLOOR_PROPERTY
      };

  private static final String[] STRING_PROPERTIES =
      new String[] {
          SCHEMA_RECORD_ID_PROPERTY,
          INDEX_MANAGER_RECORD_ID_PROPERTY,
          LOCALE_LANGUAGE_PROPERTY,
          LOCALE_COUNTRY_PROPERTY,
          DATE_FORMAT_PROPERTY,
          DATE_TIME_FORMAT_PROPERTY,
          TIME_ZONE_PROPERTY,
          CHARSET_PROPERTY,
          CONFLICT_STRATEGY_PROPERTY,
          COLLECTION_SELECTION_PROPERTY,
          RECORD_SERIALIZER_PROPERTY,
          CREATED_AT_VERSION_PROPERTY,
          UUID
      };
  public static final String VALIDATION_PROPERTY = "validation";

  private ContextConfiguration configuration;
  private boolean validation;

  private final BTree<String> btree;
  private final PaginatedCollection collection;

  private final AbstractStorage storage;
  private final ScalableRWLock lock = new ScalableRWLock();

  private final HashMap<String, Object> cache = new HashMap<>();

  // Cache resolved TimeZone to avoid lock + property-map + TimeZone.getTimeZone
  // allocation on every date comparison. Hot path: SQL WHERE date predicates
  // hit this once per row via compareFromPageFrameOrdering → DateHelper.
  //
  // setTimeZone invalidates this field BEFORE rewriting the persistent property,
  // both inside the write lock. This ordering is what keeps the lock-bypassing
  // fast path in getTimeZone consistent: once a fast-path reader observes null
  // here it falls through to the slow path, which takes the read lock and
  // therefore blocks until the writer releases the write lock — at which point
  // the persistent state is fully published. Reversing the two writes would
  // expose a window where the persistent property is already NEW but
  // cachedTimeZone still serves the OLD value to fast-path readers.
  // volatile guarantees the null-write is visible to fast-path readers without
  // any lock handshake on their side.
  private volatile TimeZone cachedTimeZone;

  private StorageConfigurationUpdateListener updateListener;

  private final ThreadLocal<PausedNotificationsState> pauseNotifications =
      ThreadLocal.withInitial(PausedNotificationsState::new);

  public static boolean exists(final WriteCache writeCache) {
    return writeCache.exists(COMPONENT_NAME + DATA_FILE_EXTENSION);
  }

  public CollectionBasedStorageConfiguration(final AbstractStorage storage) {
    collection =
        StorageCollectionFactory.createCollection(
            COMPONENT_NAME,
            PaginatedCollection.getLatestBinaryVersion(),
            storage,
            DATA_FILE_EXTENSION,
            MAP_FILE_EXTENSION,
            FREE_MAP_FILE_EXTENSION,
            DIRTY_PAGE_FILE_EXTENSION);
    btree =
        new BTree<>(COMPONENT_NAME, TREE_DATA_FILE_EXTENSION, TREE_NULL_FILE_EXTENSION, storage,
            null);
    this.storage = storage;
  }

  public void create(
      final AtomicOperation atomicOperation, final ContextConfiguration contextConfiguration) {
    lock.writeLock().lock();
    try {
      collection.create(atomicOperation);
      btree.create(atomicOperation, StringSerializer.INSTANCE, null, 1);

      this.configuration = contextConfiguration;

      init(atomicOperation);

      preloadIntProperties(atomicOperation);
      preloadStringProperties(atomicOperation);
      preloadCollections(atomicOperation);
      preloadConfigurationProperties(atomicOperation);

      doSetProperty(atomicOperation, VALIDATION_PROPERTY,
          configuration.getValueAsBoolean(GlobalConfiguration.DB_VALIDATION) ? "true" : "false");
      recalculateLocale();
    } catch (Exception e) {
      cache.clear();
      throw BaseException.wrapException(new StorageException(storage.getName(),
          "Can not create storage configuration."), e, storage.getName());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void delete(AtomicOperation atomicOperation) {
    lock.writeLock().lock();
    try {
      updateListener = null;

      final var firstPosition = collection.getFirstPosition(atomicOperation);
      var positions =
          collection.ceilingPositions(new PhysicalPosition(firstPosition), Integer.MAX_VALUE,
              atomicOperation);
      while (positions.length > 0) {
        for (var position : positions) {
          collection.deleteRecord(atomicOperation, position.collectionPosition);
        }

        positions = collection.higherPositions(positions[positions.length - 1], Integer.MAX_VALUE,
            atomicOperation);
      }

      collection.delete(atomicOperation);

      try (var keyStream = btree.keyStream(atomicOperation)) {
        keyStream.forEach((key) -> btree.remove(atomicOperation, key));
      }

      btree.delete(atomicOperation);

      cache.clear();
    } catch (Exception e) {
      cache.clear();
      throw BaseException.wrapException(new StorageException(storage.getName(),
          "Can not delete collection " + collection.getName()), e, storage.getName());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void close(final AtomicOperation atomicOperation) {
    lock.writeLock().lock();
    try {
      updateListener = null;

      updateConfigurationProperty(atomicOperation);
      updateMinimumCollections(atomicOperation);

      cache.clear();
    } catch (Exception e) {
      cache.clear();
      throw BaseException.wrapException(new StorageException(storage.getName(),
          "Can not close collection " + collection.getName()), e, storage.getName());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void load(
      final ContextConfiguration configuration, final AtomicOperation atomicOperation)
      throws SerializationException {
    lock.writeLock().lock();
    try {
      this.configuration = configuration;

      collection.open(atomicOperation);
      btree.load(COMPONENT_NAME, 1, null, StringSerializer.INSTANCE, atomicOperation);

      preloadIntProperties(atomicOperation);
      // Reject-and-redirect gate for the storage format (the storage-format arm of the same
      // policy SchemaShared.fromStream applies to the schema record). It must run here — before
      // readConfiguration or any other property parse, and before any engine, index, or
      // collection component touches its files — because this load is the single choke point
      // every open path funnels through (AbstractStorage.open, DiskStorage.initConfiguration,
      // and the incremental-restore reopen). Gating first means a mismatched format always
      // surfaces as the clear redirect, never as a downstream parse error against a layout these
      // binaries cannot know (and never as a raw file-does-not-exist crash in openIndexes).
      validateStorageFormatVersion();

      readConfiguration(atomicOperation);

      preloadStringProperties(atomicOperation);
      preloadConfigurationProperties(atomicOperation);
      preloadCollections(atomicOperation);

      readMinimumCollections(atomicOperation);
      recalculateLocale();

      // Access the cache directly instead of going through getProperty() which
      // would try to acquire the read lock, causing a deadlock with the write lock
      // already held by this method (ScalableRWLock is non-reentrant).
      @SuppressWarnings("unchecked")
      final var properties = (Map<String, String>) cache.get(PROPERTIES);
      validation = properties != null
          && "true".equalsIgnoreCase(properties.get(VALIDATION_PROPERTY));
    } catch (ConfigurationException e) {
      // The format-version gate's rejection is the user-facing redirect; rethrow it unwrapped so
      // the export/import message is the primary error, not a cause buried under a generic
      // "can not load storage configuration".
      cache.clear();
      throw e;
    } catch (Exception e) {
      cache.clear();
      throw BaseException.wrapException(new StorageException(storage.getName(),
          "Can not load storage configuration."), e, storage.getName());
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Rejects a database whose persisted storage-format version does not exactly match
   * {@link #CURRENT_VERSION}. Both directions are rejected: an older format cannot be upgraded in
   * place (the engine-file-id format has no computable default for pre-24 entries), and a newer
   * format cannot be read by these binaries. Reads the just-preloaded cache directly instead of
   * {@code getVersion} because the caller holds the non-reentrant write lock.
   */
  private void validateStorageFormatVersion() {
    final var version = (Integer) cache.get(VERSION_PROPERTY);
    if (version == null || version < CURRENT_VERSION) {
      throw new ConfigurationException(storage.getName(),
          "Storage format version " + (version == null ? "<missing>" : version)
              + " of database '" + storage.getName() + "' predates the current format (version "
              + CURRENT_VERSION + "). Direct upgrade of the storage format is not supported:"
              + " please export your old database with the previous version of YouTrackDB and"
              + " reimport it using the current one.");
    }
    if (version > CURRENT_VERSION) {
      throw new ConfigurationException(storage.getName(),
          "Storage format version " + version + " of database '" + storage.getName()
              + "' is newer than the format this version of YouTrackDB supports (version "
              + CURRENT_VERSION + "). Please open the database with the YouTrackDB version that"
              + " created it.");
    }
  }

  /**
   * Rewrites the persisted storage-format version — FOR TESTS ONLY. Production code writes the
   * version once, at creation ({@code init()}); this seam lets the format-gate tests fabricate a
   * database that reads as older or newer than {@link #CURRENT_VERSION}.
   */
  public void updateVersionForTesting(AtomicOperation atomicOperation, int version) {
    lock.writeLock().lock();
    try {
      updateIntProperty(atomicOperation, VERSION_PROPERTY, version);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void pauseUpdateNotifications() {
    lock.writeLock().lock();
    try {
      final var pausedNotificationsState = pauseNotifications.get();
      pausedNotificationsState.notificationsPaused = true;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void fireUpdateNotifications() {
    lock.writeLock().lock();
    try {
      final var pausedNotificationsState = pauseNotifications.get();

      if (pausedNotificationsState.pendingChanges > 0 && updateListener != null) {
        updateListener.onUpdate(this);
        pausedNotificationsState.pendingChanges = 0;
      }

      pausedNotificationsState.notificationsPaused = false;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void setMinimumCollections(final int minimumCollections) {
    lock.writeLock().lock();
    try {
      getContextConfiguration()
          .setValue(GlobalConfiguration.CLASS_COLLECTIONS_COUNT, minimumCollections);
      autoInitCollections();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void updateMinimumCollections(AtomicOperation atomicOperation) {
    updateIntProperty(atomicOperation, MINIMUM_COLLECTIONS_PROPERTY, doGetMinimalCollections());
  }

  /// Reads the minimumCollections property from the cache and applies it.
  /// Called while the write lock is already held (from load()), so it accesses
  /// the configuration field directly instead of going through lock-acquiring
  /// methods (setMinimumCollections/getContextConfiguration) to avoid deadlock
  /// with the non-reentrant ScalableRWLock.
  private void readMinimumCollections(AtomicOperation atomicOperation) {
    if (containsProperty(MINIMUM_COLLECTIONS_PROPERTY, atomicOperation)) {
      var minimumCollections = readIntProperty(MINIMUM_COLLECTIONS_PROPERTY);
      configuration.setValue(GlobalConfiguration.CLASS_COLLECTIONS_COUNT, minimumCollections);
      autoInitCollections();
    }
  }

  @Override
  public int getMinimumCollections() {
    lock.readLock().lock();
    try {
      return doGetMinimalCollections();
    } finally {
      lock.readLock().unlock();
    }
  }

  private int doGetMinimalCollections() {
    final var mc =
        configuration.getValueAsInteger(GlobalConfiguration.CLASS_COLLECTIONS_COUNT);
    if (mc == 0) {
      autoInitCollections();
      return (Integer) configuration.getValue(GlobalConfiguration.CLASS_COLLECTIONS_COUNT);
    }
    return mc;
  }

  @Override
  public ContextConfiguration getContextConfiguration() {
    lock.readLock().lock();
    try {
      return configuration;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Added a version used for managed Network Versioning.
   */
  public byte[] toStream(final int iNetworkVersion, final Charset charset,
      AtomicOperation atomicOperation)
      throws SerializationException {
    lock.readLock().lock();
    try {
      final var buffer = new StringBuilder(8192);

      write(buffer, CURRENT_VERSION);
      write(buffer, null);

      write(buffer, getSchemaRecordId());
      write(buffer, "");
      write(buffer, getIndexMgrRecordId());

      write(buffer, getLocaleLanguage());
      write(buffer, getLocaleCountry());
      write(buffer, getDateFormat());
      write(buffer, getDateFormat());

      final var timeZone = getTimeZone();
      assert timeZone != null;

      write(buffer, timeZone);
      write(buffer, charset);
      if (iNetworkVersion > 24) {
        write(buffer, getConflictStrategy());
      }

      phySegmentToStream(buffer, new StorageSegmentConfiguration());

      final var collections = getCollections();
      write(buffer, collections.size());
      for (final var c : collections) {
        if (c == null) {
          write(buffer, -1);
          continue;
        }

        write(buffer, c.id());
        write(buffer, c.name());
        write(buffer, c.getDataSegmentId());

        if (c instanceof StoragePaginatedCollectionConfiguration paginatedCollectionConfiguration) {
          write(buffer, "d");

          write(buffer, paginatedCollectionConfiguration.useWal);
          write(buffer, paginatedCollectionConfiguration.recordOverflowGrowFactor);
          write(buffer, paginatedCollectionConfiguration.recordGrowFactor);
          write(buffer, paginatedCollectionConfiguration.compression);

          if (iNetworkVersion >= 31) {
            write(buffer, paginatedCollectionConfiguration.encryption);
          }
          if (iNetworkVersion > 24) {
            write(buffer, paginatedCollectionConfiguration.conflictStrategy);
          }

          if (iNetworkVersion == Integer.MAX_VALUE) {
            write(buffer, paginatedCollectionConfiguration.getBinaryVersion());
          }
        }
      }
      if (iNetworkVersion <= 25) {
        // dataSegment array
        write(buffer, 0);
        // tx Segment File
        write(buffer, "");
        write(buffer, "");
        write(buffer, 0);
        // tx segment flags
        write(buffer, false);
        write(buffer, false);
      }

      final var properties = getProperties();
      write(buffer, properties.size());
      for (final var e : properties) {
        entryToStream(buffer, e);
      }

      write(buffer, getBinaryFormatVersion(atomicOperation));
      write(buffer, getCollectionSelection());
      write(buffer, getMinimumCollections());

      if (iNetworkVersion > 24) {
        write(buffer, getRecordSerializer());
        write(buffer, getRecordSerializerVersion());

        // WRITE CONFIGURATION
        write(buffer, configuration.getContextSize());
        for (final var k : configuration.getContextKeys()) {
          final var cfg = GlobalConfiguration.findByKey(k);
          write(buffer, k);
          if (cfg != null) {
            write(buffer, cfg.isHidden() ? null : configuration.getValueAsString(cfg));
          } else {
            write(buffer, null);
            LogManager.instance()
                .warn(
                    this,
                    "Storing configuration for property:'"
                        + k
                        + "' not existing in current version");
          }
        }
      }

      final var engines = loadIndexEngines(atomicOperation);
      write(buffer, engines.size());
      for (final var engineData : engines) {
        write(buffer, engineData.getName());
        write(buffer, engineData.getAlgorithm());
        write(buffer, engineData.getIndexType() == null ? "" : engineData.getIndexType());

        write(buffer, engineData.getValueSerializerId());
        write(buffer, engineData.getKeySerializedId());

        write(buffer, engineData.isAutomatic());
        //Index is durable in a TX mode flag, kept for backward compatibility
        write(buffer, true);

        write(buffer, engineData.getVersion());
        write(buffer, engineData.isNullValuesSupport());
        write(buffer, engineData.getKeySize());
        write(buffer, engineData.getEncryption());
        write(buffer, engineData.getEncryptionOptions());

        if (engineData.getKeyTypes() != null) {
          write(buffer, engineData.getKeyTypes().length);
          for (final var type : engineData.getKeyTypes()) {
            write(buffer, type.name());
          }
        } else {
          write(buffer, 0);
        }

        if (engineData.getEngineProperties() == null) {
          write(buffer, 0);
        } else {
          write(buffer, engineData.getEngineProperties().size());
          for (final var property : engineData.getEngineProperties().entrySet()) {
            write(buffer, property.getKey());
            write(buffer, property.getValue());
          }
        }

        //index engine api version
        write(buffer, 1);
        write(buffer, engineData.isMultivalue());
      }

      write(buffer, getCreatedAtVersion());
      write(buffer, getPageSize(atomicOperation));
      write(buffer, getFreeListBoundary(atomicOperation));
      write(buffer, getMaxKeySize(atomicOperation));

      // PLAIN: ALLOCATE ENOUGH SPACE TO REUSE IT EVERY TIME
      buffer.append("|");

      return buffer.toString().getBytes(charset);
    } finally {
      lock.readLock().unlock();
    }
  }

  private static void entryToStream(
      final StringBuilder buffer, final StorageEntryConfiguration entry) {
    write(buffer, entry.name);
    write(buffer, entry.value);
  }

  private static void phySegmentToStream(
      final StringBuilder buffer, final StorageSegmentConfiguration segment) {
    write(buffer, segment.getLocation());
    write(buffer, segment.maxSize);
    write(buffer, segment.fileType);
    write(buffer, segment.fileStartSize);
    write(buffer, segment.fileMaxSize);
    write(buffer, segment.fileIncrementSize);
    write(buffer, segment.defrag);

    write(buffer, segment.infoFiles.length);
    for (final var f : segment.infoFiles) {
      fileToStream(buffer, f);
    }
  }

  private static void fileToStream(
      final StringBuilder iBuffer, final StorageFileConfiguration iFile) {
    write(iBuffer, iFile.path);
    write(iBuffer, iFile.type);
    write(iBuffer, iFile.maxSize);
  }

  private static void write(final StringBuilder buffer, final Object value) {
    if (!buffer.isEmpty()) {
      buffer.append('|');
    }

    buffer.append(value != null ? value.toString() : ' ');
  }

  private void updateVersion(final AtomicOperation atomicOperation) {
    updateIntProperty(atomicOperation, VERSION_PROPERTY, CURRENT_VERSION);
  }

  @Override
  public int getVersion(AtomicOperation atomicOperation) {
    lock.readLock().lock();
    try {
      return readIntProperty(VERSION_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  @Nullable public String getName() {
    return null;
  }

  public void setSchemaRecordId(AtomicOperation atomicOperation, final String schemaRecordId) {
    lock.writeLock().lock();
    try {
      updateStringProperty(atomicOperation, SCHEMA_RECORD_ID_PROPERTY, schemaRecordId, true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getSchemaRecordId() {
    lock.readLock().lock();
    try {
      return readStringProperty(SCHEMA_RECORD_ID_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setIndexMgrRecordId(AtomicOperation atomicOperation, final String indexMgrRecordId) {
    lock.writeLock().lock();
    try {
      updateStringProperty(
          atomicOperation, INDEX_MANAGER_RECORD_ID_PROPERTY, indexMgrRecordId, true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getIndexMgrRecordId() {
    lock.readLock().lock();
    try {
      return readStringProperty(INDEX_MANAGER_RECORD_ID_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setLocaleLanguage(final AtomicOperation atomicOperation, final String value) {
    lock.writeLock().lock();
    try {
      updateStringProperty(atomicOperation, LOCALE_LANGUAGE_PROPERTY, value, true);

      recalculateLocale();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getLocaleLanguage() {
    lock.readLock().lock();
    try {
      return readStringProperty(LOCALE_LANGUAGE_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setLocaleCountry(AtomicOperation atomicOperation, final String value) {
    lock.writeLock().lock();
    try {
      updateStringProperty(atomicOperation, LOCALE_COUNTRY_PROPERTY, value, true);

      recalculateLocale();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getLocaleCountry() {
    lock.readLock().lock();
    try {
      return readStringProperty(LOCALE_COUNTRY_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setDateFormat(final AtomicOperation atomicOperation, final String dateFormat) {
    lock.writeLock().lock();
    try {
      updateStringProperty(atomicOperation, DATE_FORMAT_PROPERTY, dateFormat, true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getDateFormat() {
    lock.readLock().lock();
    try {
      final var dateFormat = readStringProperty(DATE_FORMAT_PROPERTY);
      assert dateFormat != null;

      return dateFormat;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public SimpleDateFormat getDateFormatInstance() {
    lock.readLock().lock();
    try {
      final var dateFormatInstance = new SimpleDateFormat(getDateFormat());
      dateFormatInstance.setLenient(false);
      final var timeZone = getTimeZone();
      if (timeZone != null) {
        dateFormatInstance.setTimeZone(timeZone);
      }

      return dateFormatInstance;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public String getDateTimeFormat() {
    lock.readLock().lock();
    try {
      final var dateTimeFormat = readStringProperty(DATE_TIME_FORMAT_PROPERTY);
      assert dateTimeFormat != null;

      return dateTimeFormat;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setDateTimeFormat(
      final AtomicOperation atomicOperation, final String dateTimeFormat) {
    lock.writeLock().lock();
    try {
      updateStringProperty(atomicOperation, DATE_TIME_FORMAT_PROPERTY, dateTimeFormat, true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void setUuid(AtomicOperation atomicOperation, final String uuid) {
    lock.writeLock().lock();
    try {
      updateStringProperty(atomicOperation, UUID, uuid, true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getUuid() {
    lock.readLock().lock();
    try {
      return readStringProperty(UUID);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public SimpleDateFormat getDateTimeFormatInstance() {
    lock.readLock().lock();
    try {
      final var dateTimeFormatInstance = new SimpleDateFormat(getDateTimeFormat());
      dateTimeFormatInstance.setLenient(false);
      final var timeZone = getTimeZone();
      if (timeZone != null) {
        dateTimeFormatInstance.setTimeZone(timeZone);
      }

      return dateTimeFormatInstance;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setTimeZone(final AtomicOperation atomicOperation, final TimeZone timeZone) {
    lock.writeLock().lock();
    try {
      // Invalidate cache BEFORE rewriting the persistent property. Fast-path
      // readers in getTimeZone bypass the lock; if we updated the property
      // first, a concurrent reader could still see the OLD cached TimeZone
      // while the persistent state already holds NEW. Nulling first forces any
      // fast-path miss into the slow path, which blocks on the read lock until
      // this write lock is released — at which point the persistent state is
      // fully published.
      cachedTimeZone = null;
      updateStringProperty(atomicOperation, TIME_ZONE_PROPERTY, timeZone.getID(), true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  @Nullable public TimeZone getTimeZone() {
    var cached = cachedTimeZone;
    if (cached != null) {
      return cached;
    }
    lock.readLock().lock();
    try {
      cached = cachedTimeZone;
      if (cached != null) {
        return cached;
      }
      final var timeZone = readStringProperty(TIME_ZONE_PROPERTY);
      if (timeZone == null) {
        return null;
      }
      cached = TimeZone.getTimeZone(timeZone);
      cachedTimeZone = cached;
      return cached;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setCharset(final AtomicOperation atomicOperation, final String charset) {
    lock.writeLock().lock();
    try {
      updateStringProperty(atomicOperation, CHARSET_PROPERTY, charset, true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getCharset() {
    lock.readLock().lock();
    try {
      return readStringProperty(CHARSET_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setConflictStrategy(AtomicOperation atomicOperation, final String conflictStrategy) {
    lock.writeLock().lock();
    try {
      updateStringProperty(atomicOperation, CONFLICT_STRATEGY_PROPERTY, conflictStrategy, true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getConflictStrategy() {
    lock.readLock().lock();
    try {
      return readStringProperty(CONFLICT_STRATEGY_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  private void updateBinaryFormatVersion(final AtomicOperation atomicOperation) {
    updateIntProperty(
        atomicOperation, BINARY_FORMAT_VERSION_PROPERTY, CURRENT_BINARY_FORMAT_VERSION);
  }

  @Override
  public int getBinaryFormatVersion(AtomicOperation atomicOperation) {
    lock.readLock().lock();
    try {
      return readIntProperty(BINARY_FORMAT_VERSION_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setCollectionSelection(
      final AtomicOperation atomicOperation, final String collectionSelection) {
    lock.writeLock().lock();
    try {
      updateStringProperty(atomicOperation, COLLECTION_SELECTION_PROPERTY, collectionSelection,
          true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getCollectionSelection() {
    lock.readLock().lock();
    try {
      return readStringProperty(COLLECTION_SELECTION_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setRecordSerializer(
      final AtomicOperation atomicOperation, final String recordSerializer) {
    lock.writeLock().lock();
    try {
      updateStringProperty(atomicOperation, RECORD_SERIALIZER_PROPERTY, recordSerializer, true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getRecordSerializer() {
    lock.readLock().lock();
    try {
      return readStringProperty(RECORD_SERIALIZER_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setRecordSerializerVersion(
      final AtomicOperation atomicOperation, final int recordSerializerVersion) {
    lock.writeLock().lock();
    try {
      updateIntProperty(
          atomicOperation, RECORD_SERIALIZER_VERSION_PROPERTY, recordSerializerVersion);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int getRecordSerializerVersion() {
    lock.readLock().lock();
    try {
      return readIntProperty(RECORD_SERIALIZER_VERSION_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  private void updateConfigurationProperty(AtomicOperation atomicOperation) {
    final List<byte[]> entries = new ArrayList<>(8);
    var totalSize = 0;

    final var contextSize = new byte[IntegerSerializer.INT_SIZE];
    totalSize += contextSize.length;
    entries.add(contextSize);

    IntegerSerializer.serializeNative(configuration.getContextSize(), contextSize, 0);

    for (final var k : configuration.getContextKeys()) {
      final var cfg = GlobalConfiguration.findByKey(k);
      final var key = serializeStringValue(k);
      totalSize += key.length;
      entries.add(key);

      if (cfg != null) {
        final var value =
            serializeStringValue(cfg.isHidden() ? null : storedFormOf(cfg));
        totalSize += value.length;
        entries.add(value);
      } else {
        final var value = serializeStringValue(null);
        totalSize += value.length;
        entries.add(value);

        LogManager.instance()
            .warn(
                this,
                "Storing configuration for property:'" + k + "' not existing in current version");
      }
    }

    final var property = mergeBinaryEntries(totalSize, entries);
    storeProperty(
        atomicOperation, CONFIGURATION_PROPERTY, property, CONFIGURATION_PROPERTY_VERSION);
  }

  private void readConfiguration(AtomicOperation atomicOperation) {
    final var pair = readProperty(CONFIGURATION_PROPERTY, atomicOperation);
    if (pair == null) {
      return;
    }

    final var property = pair.first;

    var pos = 0;
    final var size = IntegerSerializer.deserializeNative(property, pos);
    pos += IntegerSerializer.INT_SIZE;

    for (var i = 0; i < size; i++) {
      final var key = deserializeStringValue(property, pos);
      pos += getSerializedStringSize(property, pos);

      final var value = deserializeStringValue(property, pos);
      pos += getSerializedStringSize(property, pos);

      final var cfg = GlobalConfiguration.findByKey(key);
      if (cfg != null) {
        if (value != null) {
          if (cfg.getType().isEnum()) {
            // Enum-typed keys are parsed here instead of by PropertyTypeInternal.convert, which has
            // no enum branch and would throw, failing the whole open. The shared helper stays
            // untouched because a query-level conversion method reaches it too.
            final var constant = parseEnumValue(cfg, value);
            if (constant == null) {
              // Tolerant only for enums. An unreadable constant name leaves the global default in
              // force instead of making the database unopenable. Every other type still fails the
              // open loudly, below. The value is skipped, so the next clean close writes only the
              // effective configuration and the stored text is gone from then on.
              LogManager.instance()
                  .warn(
                      this,
                      "Database '"
                          + storage.getName()
                          + "' stores the unreadable value '"
                          + value
                          + "' for the configuration key '"
                          + key
                          + "'. The global default applies instead. The next clean close of this"
                          + " database drops the stored value.");
            } else {
              configuration.setValue(key, constant);
            }
          } else {
            configuration.setValue(key, PropertyTypeInternal.convert(null, value, cfg.getType()));
          }
        }
      } else {
        LogManager.instance()
            .warn(this, "Ignored storage configuration because not supported: %s=%s", key, value);
      }
    }
  }

  /**
   * Resolves a stored string to a constant of an enum-typed configuration key. Names are matched
   * ignoring case, which is the rule {@link GlobalConfiguration#setValue} applies. Surrounding
   * whitespace is not tolerated, for the same reason. A value the setter accepts therefore reads
   * back, and a value the setter rejects is rejected here too.
   *
   * @return the matching constant, or {@code null} when the value names no constant
   */
  @Nullable private static Object parseEnumValue(final GlobalConfiguration cfg,
      final String value) {
    for (final var constant : cfg.getType().getEnumConstants()) {
      if (((Enum<?>) constant).name().equalsIgnoreCase(value)) {
        return constant;
      }
    }
    return null;
  }

  /**
   * Renders the effective value of one configuration key for storage.
   *
   * <p>An enum is written by constant name, because the load path matches constant names. Consider a
   * future constant whose text form differs from its name. Writing that text form would produce a
   * value the load path cannot match, and the setting would disappear on reopen.
   *
   * @return the text to persist, or {@code null} when the key holds no value
   */
  @Nullable private String storedFormOf(final GlobalConfiguration cfg) {
    final var value = configuration.getValue(cfg);
    if (value instanceof Enum<?> constant) {
      return constant.name();
    }
    return value == null ? null : value.toString();
  }

  public void setCreationVersion(final AtomicOperation atomicOperation, final String version) {
    lock.writeLock().lock();
    try {
      updateStringProperty(atomicOperation, CREATED_AT_VERSION_PROPERTY, version, true);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getCreatedAtVersion() {
    lock.readLock().lock();
    try {
      return readStringProperty(CREATED_AT_VERSION_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setPageSize(final AtomicOperation atomicOperation, final int pageSize) {
    lock.writeLock().lock();
    try {
      updateIntProperty(atomicOperation, PAGE_SIZE_PROPERTY, pageSize);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int getPageSize(AtomicOperation atomicOperation) {
    lock.readLock().lock();
    try {
      return readIntProperty(PAGE_SIZE_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setFreeListBoundary(
      final AtomicOperation atomicOperation, final int freeListBoundary) {
    lock.writeLock().lock();
    try {
      updateIntProperty(atomicOperation, FREE_LIST_BOUNDARY_PROPERTY, freeListBoundary);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int getFreeListBoundary(AtomicOperation atomicOperation) {
    lock.readLock().lock();
    try {
      return readIntProperty(FREE_LIST_BOUNDARY_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setMaxKeySize(final AtomicOperation atomicOperation, final int maxKeySize) {
    lock.writeLock().lock();
    try {
      updateIntProperty(atomicOperation, MAX_KEY_SIZE_PROPERTY, maxKeySize);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int getMaxKeySize(AtomicOperation atomicOperation) {
    lock.readLock().lock();
    try {
      return readIntProperty(MAX_KEY_SIZE_PROPERTY);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setProperty(
      final AtomicOperation atomicOperation, final String name, final String value) {
    lock.writeLock().lock();
    try {
      doSetProperty(atomicOperation, name, value);
    } catch (Exception e) {
      cache.clear();
      throw BaseException.wrapException(new StorageException(storage.getName(),
          "Can not set property " + name), e, storage.getName());
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void doSetProperty(AtomicOperation atomicOperation, String name, String value) {
    if (VALIDATION_PROPERTY.equalsIgnoreCase(name)) {
      validation = "true".equalsIgnoreCase(value);
    }

    final var key = PROPERTY_PREFIX_PROPERTY + name;
    updateStringProperty(atomicOperation, key, value, false);

    @SuppressWarnings("unchecked")
    final var properties = (Map<String, String>) cache.get(
        PROPERTIES);
    properties.put(name, value);
  }

  public void setValidation(final AtomicOperation atomicOperation, final boolean validation) {
    setProperty(atomicOperation, VALIDATION_PROPERTY, validation ? "true" : "false");
  }

  @Override
  public boolean isValidationEnabled() {
    lock.readLock().lock();
    try {
      return validation;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  @Nullable public String getDirectory() {
    if (storage instanceof DiskStorage disk) {
      return disk.getStoragePath().toString();
    } else {
      return null;
    }
  }

  @Override
  public String getProperty(final String name) {
    lock.readLock().lock();
    try {
      @SuppressWarnings("unchecked")
      final var properties = (Map<String, String>) cache.get(
          PROPERTIES);
      return properties.get(name);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<StorageEntryConfiguration> getProperties() {
    lock.readLock().lock();
    try {
      @SuppressWarnings("unchecked")
      final var properties = (Map<String, String>) cache.get(
          PROPERTIES);

      final List<StorageEntryConfiguration> result = new ArrayList<>(8);

      for (final var entry : properties.entrySet()) {
        result.add(new StorageEntryConfiguration(entry.getKey(), entry.getValue()));
      }

      return result;
    } finally {
      lock.readLock().unlock();
    }
  }

  private void preloadConfigurationProperties(AtomicOperation atomicOperation) {
    final Map<String, String> properties;
    try (var stream =
        btree.iterateEntriesMajor(PROPERTY_PREFIX_PROPERTY, false, true, atomicOperation)) {
      properties =
          stream
              .filter((pair) -> pair.first().startsWith(PROPERTY_PREFIX_PROPERTY))
              .map(
                  (entry) -> {
                    final RawBuffer buffer;
                    try {
                      buffer = collection.readRecord(
                          entry.second().getCollectionPosition(), atomicOperation)
                          .toRawBuffer();
                      return new RawPair<>(
                          entry.first().substring(PROPERTY_PREFIX_PROPERTY.length()),
                          deserializeStringValue(buffer.buffer(), 0));
                    } catch (IOException e) {
                      throw BaseException.wrapException(
                          new StorageException(storage.getName(),
                              "Can not preload configuration properties"),
                          e, storage.getName());
                    }
                  })
              .collect(Collectors.toMap(RawPair::first, RawPair::second));
    }

    cache.put(PROPERTIES, properties);
  }

  @Override
  public Locale getLocaleInstance() {
    lock.readLock().lock();
    try {
      var locale = (Locale) cache.get(LOCALE_PROPERTY_INSTANCE);
      if (locale == null) {
        locale = Locale.getDefault();
      }
      return locale;
    } finally {
      lock.readLock().unlock();
    }
  }

  private void recalculateLocale() {
    Locale locale;
    try {
      final var localeLanguage = readStringProperty(LOCALE_LANGUAGE_PROPERTY);
      final var localeCountry = readStringProperty(LOCALE_COUNTRY_PROPERTY);

      if (localeLanguage == null || localeCountry == null) {
        locale = Locale.getDefault();
      } else {
        locale = Locale.of(localeLanguage, localeCountry);
      }
    } catch (final RuntimeException e) {
      locale = Locale.getDefault();
    }

    cache.put(LOCALE_PROPERTY_INSTANCE, locale);
  }

  public void clearProperties(AtomicOperation atomicOperation) {
    lock.writeLock().lock();
    try {
      final List<String> keysToRemove;
      final List<RID> ridsToRemove;
      try (var stream =
          btree.iterateEntriesMajor(PROPERTY_PREFIX_PROPERTY, false, true, atomicOperation)) {

        keysToRemove = new ArrayList<>(8);
        ridsToRemove = new ArrayList<>(8);

        stream
            .filter((entry) -> entry.first().startsWith(PROPERTY_PREFIX_PROPERTY))
            .forEach(
                (entry) -> {
                  keysToRemove.add(entry.first());
                  ridsToRemove.add(entry.second());
                });
      }

      for (final var key : keysToRemove) {
        btree.remove(atomicOperation, key);
      }

      for (final var rid : ridsToRemove) {
        collection.deleteRecord(atomicOperation, rid.getCollectionPosition());
      }

      @SuppressWarnings("unchecked")
      final var properties = (Map<String, String>) cache.get(
          PROPERTIES);
      properties.clear();
    } catch (Exception e) {
      cache.clear();
      throw BaseException.wrapException(new StorageException(storage.getName(),
          "Can not clear properties"), e, storage.getName());
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void removeProperty(AtomicOperation atomicOperation, final String name) {
    lock.writeLock().lock();
    try {
      dropProperty(atomicOperation, PROPERTY_PREFIX_PROPERTY + name);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void addIndexEngine(
      final AtomicOperation atomicOperation, final String name, final IndexEngineData engineData) {
    lock.writeLock().lock();
    try {
      final var identifiable = btree.get(ENGINE_PREFIX_PROPERTY + name, atomicOperation);
      if (identifiable != null) {
        LogManager.instance()
            .warn(
                this,
                "Index engine with name '"
                    + engineData.getName()
                    + "' already contained in database configuration");
      } else {
        storeProperty(
            atomicOperation,
            ENGINE_PREFIX_PROPERTY + name,
            serializeIndexEngineProperty(engineData),
            INDEX_ENGINE_PROPERTY_VERSION);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void deleteIndexEngine(AtomicOperation atomicOperation, final String name) {
    lock.writeLock().lock();
    try {
      dropProperty(atomicOperation, ENGINE_PREFIX_PROPERTY + name);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * The persisted allocation floor of the index-engine file-base-id counter: the highest
   * {@code fileBaseId} any successfully committed engine create has allocated. The storage's
   * in-process high-water-mark allocator seeds from (among other inputs) this floor at open, so
   * a file base id is never reused across restarts.
   */
  public int getIndexEngineFileBaseIdFloor(AtomicOperation atomicOperation) {
    lock.readLock().lock();
    try {
      // Deliberately a direct b-tree read, not a cache lookup: the floor participates in the
      // high-water-mark allocator's seeding, and the in-memory cache map is not transactional (a
      // failed storeProperty clears it wholesale), so allocation state must only ever derive from
      // durable bytes.
      final var pair = readProperty(INDEX_ENGINE_FILE_BASE_ID_FLOOR_PROPERTY, atomicOperation);
      if (pair == null) {
        // init() seeds the floor on every v24 creation, so a missing property is corruption —
        // and answering a made-up floor (e.g. 0) could hand out an already-used file base id.
        throw new StorageException(storage.getName(),
            "The index-engine file-base-id floor is missing from the configuration of database '"
                + storage.getName() + "'; the configuration is corrupted");
      }
      return IntegerSerializer.deserializeNative(pair.first, 0);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Persists a new index-engine file-base-id floor. Called by the storage's high-water-mark
   * allocator with every allocation, inside the allocating atomic operation, so the floor commits
   * or reverts together with the engine files the allocation is for.
   */
  public void setIndexEngineFileBaseIdFloor(AtomicOperation atomicOperation, final int floor) {
    lock.writeLock().lock();
    try {
      updateIntProperty(atomicOperation, INDEX_ENGINE_FILE_BASE_ID_FLOOR_PROPERTY, floor);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Stores an index-engine entry under an explicit property version tag — FOR TESTS ONLY.
   * Production engine entries are always written at {@link #INDEX_ENGINE_PROPERTY_VERSION}; this
   * seam lets tests fabricate a stale-versioned entry to pin the deserializer's hard-fail gate.
   */
  public void storeIndexEngineEntryForTesting(
      final AtomicOperation atomicOperation,
      final String name,
      final IndexEngineData engineData,
      final int propertyVersion) {
    lock.writeLock().lock();
    try {
      storeProperty(
          atomicOperation,
          ENGINE_PREFIX_PROPERTY + name,
          serializeIndexEngineProperty(engineData),
          propertyVersion);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Set<String> indexEngines(AtomicOperation atomicOperation) {
    lock.readLock().lock();
    try {
      try (var stream =
          btree.iterateEntriesMajor(ENGINE_PREFIX_PROPERTY, false, true, atomicOperation)) {
        return stream
            .filter((entry) -> entry.first().startsWith(ENGINE_PREFIX_PROPERTY))
            .map((entry) -> entry.first().substring(ENGINE_PREFIX_PROPERTY.length()))
            .collect(Collectors.toSet());
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  private List<IndexEngineData> loadIndexEngines(AtomicOperation atomicOperation) {
    try (var stream =
        btree.iterateEntriesMajor(ENGINE_PREFIX_PROPERTY, false, true, atomicOperation)) {
      return stream
          .filter((entry) -> entry.first().startsWith(ENGINE_PREFIX_PROPERTY))
          .map(
              (entry) -> {
                String name = null;
                try {
                  name = entry.first().substring(ENGINE_PREFIX_PROPERTY.length());
                  final var buffer = collection.readRecord(
                      entry.second().getCollectionPosition(), atomicOperation)
                      .toRawBuffer();
                  return deserializeIndexEngineProperty(
                      name, buffer.buffer(), Integer.MIN_VALUE, entry.second().getCollectionId());
                } catch (IOException e) {
                  throw BaseException.wrapException(
                      new StorageException(storage.getName(),
                          "Can not load data for index "
                              + name
                              + " for storage "
                              + storage.getName()),
                      e, storage.getName());
                }
              })
          .collect(Collectors.toList());
    }
  }

  @Override
  @Nullable public IndexEngineData getIndexEngine(final String name, int defaultIndexId,
      AtomicOperation atomicOperation) {
    lock.readLock().lock();
    try {
      final var pair = readProperty(ENGINE_PREFIX_PROPERTY + name, atomicOperation);
      if (pair == null) {
        return null;
      }

      final var property = pair.first;
      return deserializeIndexEngineProperty(name, property, defaultIndexId, pair.second);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void updateCollection(
      AtomicOperation atomicOperation, final StorageCollectionConfiguration config) {
    lock.writeLock().lock();
    try {
      @SuppressWarnings("unchecked")
      final var collections =
          (List<StorageCollectionConfiguration>) cache.get(COLLECTIONS);
      if (config.id() < collections.size()) {
        collections.set(config.id(), config);
      } else {
        final var diff = config.id() - collections.size();
        for (var i = 0; i < diff; i++) {
          collections.add(null);
        }

        collections.add(config);
        assert collections.size() - 1 == config.id();
      }

      storeProperty(
          atomicOperation,
          COLLECTIONS_PREFIX_PROPERTY + config.id(),
          updateCollectionConfig(config),
          COLLECTIONS_PROPERTY_VERSION);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public List<StorageCollectionConfiguration> getCollections() {
    lock.readLock().lock();
    try {
      //noinspection unchecked
      return Collections.unmodifiableList(
          (List<StorageCollectionConfiguration>) cache.get(COLLECTIONS));
    } finally {
      lock.readLock().unlock();
    }
  }

  private void preloadCollections(AtomicOperation atomicOperation) {
    final List<StorageCollectionConfiguration> collections = new ArrayList<>(1024);
    try (var stream =
        btree.iterateEntriesMajor(COLLECTIONS_PREFIX_PROPERTY, false, true, atomicOperation)) {

      stream
          .filter((entry) -> entry.first().startsWith(COLLECTIONS_PREFIX_PROPERTY))
          .forEach(
              (entry) -> {
                final var id =
                    Integer.parseInt(entry.first().substring(COLLECTIONS_PREFIX_PROPERTY.length()));

                try {
                  final var buffer = collection.readRecord(
                      entry.second().getCollectionPosition(), atomicOperation)
                      .toRawBuffer();

                  if (collections.size() <= id) {
                    final var diff = id - collections.size();

                    for (var i = 0; i < diff; i++) {
                      collections.add(null);
                    }

                    collections.add(deserializeStorageCollectionConfig(id, buffer.buffer()));
                    assert id == collections.size() - 1;
                  } else {
                    collections.set(id, deserializeStorageCollectionConfig(id, buffer.buffer()));
                  }
                } catch (final IOException e) {
                  throw BaseException.wrapException(
                      new StorageException(storage.getName(),
                          "Can not load data for collection with id="
                              + id
                              + " for storage "
                              + storage.getName()),
                      e, storage.getName());
                }
              });
    }

    cache.put(COLLECTIONS, collections);
  }

  public void dropCollection(final AtomicOperation atomicOperation, final int collectionId) {
    lock.writeLock().lock();
    try {
      @SuppressWarnings("unchecked")
      final var collections =
          (List<StorageCollectionConfiguration>) cache.get(COLLECTIONS);
      if (collectionId < collections.size()) {
        collections.set(collectionId, null);
      }

      dropProperty(atomicOperation, COLLECTIONS_PREFIX_PROPERTY + collectionId);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void setConfigurationUpdateListener(
      final StorageConfigurationUpdateListener updateListener) {
    lock.writeLock().lock();
    try {
      this.updateListener = updateListener;
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static byte[] serializeIndexEngineProperty(final IndexEngineData indexEngineData) {
    var totalSize = 0;
    final List<byte[]> entries = new ArrayList<>(16);

    final var numericProperties =
        new byte[5 * IntegerSerializer.INT_SIZE + 5 * ByteSerializer.BYTE_SIZE];
    totalSize += numericProperties.length;
    entries.add(numericProperties);
    {
      var pos = 0;
      IntegerSerializer.serializeNative(
          indexEngineData.getVersion(), numericProperties, pos);
      pos += IntegerSerializer.INT_SIZE;
      //index engine api version, kept for compatibility
      IntegerSerializer.serializeNative(
          1, numericProperties, pos);
      pos += IntegerSerializer.INT_SIZE;

      numericProperties[pos] = indexEngineData.getValueSerializerId();
      pos++;
      numericProperties[pos] = indexEngineData.getKeySerializedId();
      pos++;
      numericProperties[pos] = indexEngineData.isAutomatic() ? (byte) 1 : 0;
      pos++;
      numericProperties[pos] = indexEngineData.isNullValuesSupport() ? (byte) 1 : 0;
      pos++;
      numericProperties[pos] = indexEngineData.isMultivalue() ? (byte) 1 : 0;
      pos++;

      IntegerSerializer.serializeNative(
          indexEngineData.getKeySize(), numericProperties, pos);
      pos += IntegerSerializer.INT_SIZE;

      IntegerSerializer.serializeNative(
          indexEngineData.getIndexId(), numericProperties, pos);
      pos += IntegerSerializer.INT_SIZE;

      // Engine-property binary version 2: the stable file base id follows the indexId.
      IntegerSerializer.serializeNative(
          indexEngineData.getFileBaseId(), numericProperties, pos);
    }

    final var algorithm = serializeStringValue(indexEngineData.getAlgorithm());
    totalSize += algorithm.length;
    entries.add(algorithm);

    final var indexType =
        serializeStringValue(
            indexEngineData.getIndexType() == null ? "" : indexEngineData.getIndexType());
    entries.add(indexType);
    totalSize += indexType.length;

    final var encryption = serializeStringValue(indexEngineData.getEncryption());
    totalSize += encryption.length;
    entries.add(encryption);

    final var keyTypesValue = indexEngineData.getKeyTypes();
    final var keyTypesSize = new byte[4];
    IntegerSerializer.serializeNative(keyTypesValue.length, keyTypesSize, 0);
    totalSize += keyTypesSize.length;
    entries.add(keyTypesSize);

    for (final var typeValue : keyTypesValue) {
      final var keyTypeName = serializeStringValue(typeValue.name());
      totalSize += keyTypeName.length;
      entries.add(keyTypeName);
    }

    final var engineProperties = indexEngineData.getEngineProperties();
    final var enginePropertiesSize = new byte[IntegerSerializer.INT_SIZE];
    totalSize += enginePropertiesSize.length;
    entries.add(enginePropertiesSize);

    if (engineProperties != null) {
      IntegerSerializer.serializeNative(engineProperties.size(), enginePropertiesSize, 0);

      for (final var engineProperty : engineProperties.entrySet()) {
        final var key = serializeStringValue(engineProperty.getKey());
        totalSize += key.length;
        entries.add(key);

        final var value = serializeStringValue(engineProperty.getValue());
        totalSize += value.length;
        entries.add(value);
      }
    }

    return mergeBinaryEntries(totalSize, entries);
  }

  private IndexEngineData deserializeIndexEngineProperty(
      final String name, final byte[] property, final int defaultIndexId,
      final int binaryVersion) {
    // Hard gate, no default and no sentinel: a pre-2 entry has no fileBaseId and there is no
    // computable fallback for one (file base ids must be unique for the storage's lifetime, so
    // fabricating one risks a file collision). The storage-format gate rejects every pre-24
    // database at load, so on a healthy database this branch is unreachable; reaching it means a
    // mixed-version write or corruption, and failing loudly beats silently mis-keying engine
    // files.
    // ConfigurationException (a HighLevelException) deliberately: this is a controlled
    // format rejection with a user-facing redirect, like the load-time version gate — it must
    // not flip the storage into the restart-requiring error state.
    if (binaryVersion < INDEX_ENGINE_PROPERTY_VERSION) {
      throw new ConfigurationException(storage.getName(),
          "Index engine entry '" + name + "' of database '" + storage.getName()
              + "' is stored under engine-property version " + binaryVersion
              + ", which predates the engine-file-id format (version "
              + INDEX_ENGINE_PROPERTY_VERSION + ") and carries no file base id. The database"
              + " must be exported with the previous version of YouTrackDB and reimported using"
              + " the current one.");
    }
    var pos = 0;

    final var version = IntegerSerializer.deserializeNative(property, pos);
    pos += IntegerSerializer.INT_SIZE;

    final var apiVersion = IntegerSerializer.deserializeNative(property, pos);
    pos += IntegerSerializer.INT_SIZE;

    final var valueSerializerId = property[pos];
    pos++;

    final var keySerializerId = property[pos];
    pos++;

    final var isAutomatic = property[pos] == 1;
    pos++;

    final var isNullValueSupport = property[pos] == 1;
    pos++;

    final var isMultiValue = property[pos] == 1;
    pos++;

    final var keySize = IntegerSerializer.deserializeNative(property, pos);
    pos += IntegerSerializer.INT_SIZE;

    // The entry is version >= 2 (gated above), so the indexId and fileBaseId are always present
    // — the historical "config version >= 23 or entry version >= 1" disjunct that guarded the
    // indexId read collapsed to constant-true once pre-24 databases were rejected at load.
    final int indexId;
    {
      final var iid = IntegerSerializer.deserializeNative(property, pos);
      if (iid == Integer.MIN_VALUE) {
        indexId = defaultIndexId;
      } else {
        indexId = iid;
      }

      pos += IntegerSerializer.INT_SIZE;
    }

    final var fileBaseId = IntegerSerializer.deserializeNative(property, pos);
    pos += IntegerSerializer.INT_SIZE;

    final var algorithm = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final var indexType = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final var encryption = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final var keyTypesSize = IntegerSerializer.deserializeNative(property, pos);
    pos += IntegerSerializer.INT_SIZE;

    final var keyTypes = new PropertyTypeInternal[keyTypesSize];
    for (var i = 0; i < keyTypesSize; i++) {
      final var typeName = deserializeStringValue(property, pos);
      pos += getSerializedStringSize(property, pos);

      keyTypes[i] = PropertyTypeInternal.valueOf(typeName);
    }

    final Map<String, String> engineProperties = new HashMap<>(8);
    final var enginePropertiesSize = IntegerSerializer.deserializeNative(property, pos);
    pos += IntegerSerializer.INT_SIZE;

    for (var i = 0; i < enginePropertiesSize; i++) {
      final var key = deserializeStringValue(property, pos);
      pos += getSerializedStringSize(property, pos);

      final var value = deserializeStringValue(property, pos);
      pos += getSerializedStringSize(property, pos);

      engineProperties.put(key, value);
    }

    return new IndexEngineData(
        indexId,
        fileBaseId,
        name,
        algorithm,
        indexType,
        true,
        version,
        apiVersion,
        isMultiValue,
        valueSerializerId,
        keySerializerId,
        isAutomatic,
        keyTypes,
        isNullValueSupport,
        keySize,
        encryption,
        configuration.getValueAsString(GlobalConfiguration.STORAGE_ENCRYPTION_KEY),
        engineProperties);
  }

  private static byte[] mergeBinaryEntries(final int totalSize, final List<byte[]> entries) {
    final var property = new byte[totalSize];
    var pos = 0;
    for (final var entry : entries) {
      System.arraycopy(entry, 0, property, pos, entry.length);
      pos += entry.length;
    }

    assert pos == property.length;
    return property;
  }

  private static byte[] updateCollectionConfig(final StorageCollectionConfiguration collection) {
    var totalSize = 0;
    final List<byte[]> entries = new ArrayList<>(8);

    final var name = serializeStringValue(collection.name());
    totalSize += name.length;
    entries.add(name);

    final var paginatedCollectionConfiguration =
        (StoragePaginatedCollectionConfiguration) collection;
    final var numericData = new byte[IntegerSerializer.INT_SIZE + ByteSerializer.BYTE_SIZE];
    totalSize += numericData.length;
    entries.add(numericData);

    numericData[0] = paginatedCollectionConfiguration.useWal ? (byte) 1 : 0;

    IntegerSerializer.serializeNative(
        paginatedCollectionConfiguration.getBinaryVersion(), numericData, 1);

    final var encryption = serializeStringValue(paginatedCollectionConfiguration.encryption);
    totalSize += encryption.length;
    entries.add(encryption);

    final var conflictStrategy =
        serializeStringValue(paginatedCollectionConfiguration.conflictStrategy);
    totalSize += conflictStrategy.length;
    entries.add(conflictStrategy);

    final var compression = serializeStringValue(paginatedCollectionConfiguration.compression);
    entries.add(compression);
    totalSize += compression.length;

    return mergeBinaryEntries(totalSize, entries);
  }

  private StorageCollectionConfiguration deserializeStorageCollectionConfig(
      final int id, final byte[] property) {
    var pos = 0;

    final var name = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final var useWal = (property[pos] == 1);
    pos++;

    final var binaryVersion = IntegerSerializer.deserializeNative(property, pos);
    pos += IntegerSerializer.INT_SIZE;

    final var encryption = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final var conflictStrategy = deserializeStringValue(property, pos);
    pos += getSerializedStringSize(property, pos);

    final var compression = deserializeStringValue(property, pos);

    return new StoragePaginatedCollectionConfiguration(
        id,
        name,
        null,
        useWal,
        0,
        0,
        compression,
        encryption,
        configuration.getValueAsString(GlobalConfiguration.STORAGE_ENCRYPTION_KEY),
        conflictStrategy,
        binaryVersion);
  }

  private void dropProperty(final AtomicOperation atomicOperation, final String name) {
    final var identifiable = btree.remove(atomicOperation, name);

    if (identifiable != null) {
      collection.deleteRecord(atomicOperation, identifiable.getCollectionPosition());
    }

    final var pausedNotificationsState = pauseNotifications.get();
    if (updateListener != null) {
      if (!pausedNotificationsState.notificationsPaused) {
        updateListener.onUpdate(this);
        pausedNotificationsState.pendingChanges = 0;
      } else {
        pausedNotificationsState.pendingChanges++;
      }
    }
  }

  private void updateStringProperty(
      final AtomicOperation atomicOperation,
      final String name,
      final String value,
      final boolean useCache) {
    if (useCache) {
      cache.put(name, value);
    }

    final var property = serializeStringValue(value);

    storeProperty(atomicOperation, name, property, 0);
  }

  private static byte[] serializeStringValue(final String value) {
    final byte[] property;
    if (value == null) {
      property = new byte[1];
    } else {
      final var rawString = value.getBytes(StandardCharsets.UTF_16);
      property = new byte[rawString.length + 1 + IntegerSerializer.INT_SIZE];
      property[0] = 1;

      IntegerSerializer.serializeNative(rawString.length, property, 1);

      System.arraycopy(rawString, 0, property, 5, rawString.length);
    }

    return property;
  }

  @Nullable private static String deserializeStringValue(final byte[] raw, final int start) {
    if (raw[start] == 0) {
      return null;
    }

    final var stringSize = IntegerSerializer.deserializeNative(raw, start + 1);
    return new String(raw, start + 5, stringSize, StandardCharsets.UTF_16);
  }

  private static int getSerializedStringSize(final byte[] raw, final int start) {
    if (raw[start] == 0) {
      return 1;
    }

    return IntegerSerializer.deserializeNative(raw, start + 1) + 5;
  }

  private void updateIntProperty(
      AtomicOperation atomicOperation, final String name, final int value) {
    // PINNED: the cache map is NOT rollback-aware — this put survives a rolled-back
    // atomicOperation while the storeProperty write below reverts, leaving the cached value
    // permanently ahead of the durable bytes. Any property that can be written inside a
    // caller-owned (rollback-able) operation must therefore be read btree-direct via
    // readProperty, never through the cache; getIndexEngineFileBaseIdFloor already does
    // exactly that. Properties written only in self-contained operations (create-time init,
    // close) may keep using the cache-backed readIntProperty.
    cache.put(name, value);

    final var property = new byte[IntegerSerializer.INT_SIZE];
    IntegerSerializer.serializeNative(value, property, 0);

    storeProperty(atomicOperation, name, property, 0);
  }

  private void storeProperty(
      AtomicOperation atomicOperation,
      final String name,
      final byte[] property,
      final int propertyBinaryVersion) {
    try {
      var identity = btree.get(name, atomicOperation);

      if (identity == null) {
        final var position =
            collection.createRecord(property, (byte) 0, null, atomicOperation);
        identity = new RecordId(propertyBinaryVersion, position.collectionPosition);
        btree.put(atomicOperation, name, identity);
      } else {
        // The property's version tag rides the b-tree value's collection-id component and is
        // written only on create — this update branch keeps the existing tag. A caller that
        // bumps a property's format version must therefore delete+add, never update in place
        // (the discipline engine entries rely on for INDEX_ENGINE_PROPERTY_VERSION); assert the
        // tags agree so a version-crossing in-place update fails loudly under -ea instead of
        // persisting bytes that disagree with their tag.
        assert identity.getCollectionId() == propertyBinaryVersion
            : "property '" + name + "' is stored under version tag " + identity.getCollectionId()
                + " but an in-place update was attempted with version " + propertyBinaryVersion
                + "; version-bumped properties must be deleted and re-added";
        collection.updateRecord(identity.getCollectionPosition(), property, (byte) 0,
            atomicOperation);
      }

      final var pausedNotificationsState = pauseNotifications.get();
      if (updateListener != null) {
        if (!pausedNotificationsState.notificationsPaused) {
          pausedNotificationsState.pendingChanges = 0;
          updateListener.onUpdate(this);
        } else {
          pausedNotificationsState.pendingChanges++;
        }
      }
    } catch (Exception e) {
      cache.clear();
      throw BaseException.wrapException(
          new StorageException(storage.getName(), "Can not store property " + name), e,
          storage.getName());
    }
  }

  @Nullable private RawPairObjectInteger<byte[]> readProperty(final String name,
      @Nonnull AtomicOperation atomicOperation) {
    try {
      final var rid = btree.get(name, atomicOperation);
      if (rid == null) {
        return null;
      }

      final var buffer =
          collection.readRecord(rid.getCollectionPosition(), atomicOperation)
              .toRawBuffer();
      return new RawPairObjectInteger<>(buffer.buffer(), rid.getCollectionId());
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new StorageException(storage.getName(),
              "Error during read of configuration property " + name),
          e, storage.getName());
    }
  }

  private boolean containsProperty(@SuppressWarnings("SameParameterValue") final String name,
      AtomicOperation atomicOperation) {
    return btree.get(name, atomicOperation) != null;
  }

  private String readStringProperty(final String name) {
    return (String) cache.get(name);
  }

  private int readIntProperty(final String name) {
    final var cachedValue = cache.get(name);
    return (int) cachedValue;
  }

  private void preloadIntProperties(AtomicOperation atomicOperation) {
    for (final var name : INT_PROPERTIES) {
      final var pair = readProperty(name, atomicOperation);

      if (pair != null) {
        cache.put(name, IntegerSerializer.deserializeNative(pair.first, 0));
      }
    }
  }

  private void preloadStringProperties(AtomicOperation atomicOperation) {
    for (final var name : STRING_PROPERTIES) {
      final var property = readProperty(name, atomicOperation);
      if (property != null) {
        cache.put(name, deserializeStringValue(property.first, 0));
      }
    }
  }

  private void init(final AtomicOperation atomicOperation) {
    updateVersion(atomicOperation);
    updateBinaryFormatVersion(atomicOperation);

    updateStringProperty(atomicOperation, CHARSET_PROPERTY, DEFAULT_CHARSET, true);
    updateStringProperty(atomicOperation, DATE_FORMAT_PROPERTY, DEFAULT_DATE_FORMAT, true);
    updateStringProperty(atomicOperation, DATE_TIME_FORMAT_PROPERTY, DEFAULT_DATETIME_FORMAT, true);

    updateStringProperty(atomicOperation, LOCALE_LANGUAGE_PROPERTY,
        Locale.getDefault().getLanguage(), true);
    updateStringProperty(atomicOperation, LOCALE_COUNTRY_PROPERTY,
        Locale.getDefault().getCountry(), true);

    recalculateLocale();

    updateStringProperty(atomicOperation, TIME_ZONE_PROPERTY, TimeZone.getDefault().getID(), true);

    updateIntProperty(atomicOperation, PAGE_SIZE_PROPERTY, -1);
    updateIntProperty(atomicOperation, FREE_LIST_BOUNDARY_PROPERTY, -1);
    updateIntProperty(atomicOperation, MAX_KEY_SIZE_PROPERTY, -1);

    if (!configuration
        .getContextKeys()
        .contains(GlobalConfiguration.CLASS_COLLECTIONS_COUNT.getKey())) {
      configuration.setValue(
          GlobalConfiguration.CLASS_COLLECTIONS_COUNT,
          GlobalConfiguration.CLASS_COLLECTIONS_COUNT.getValueAsInteger()); // 0 = AUTOMATIC
    }
    autoInitCollections();

    updateMinimumCollections(atomicOperation); // store inside of configuration

    updateIntProperty(
        atomicOperation, RECORD_SERIALIZER_VERSION_PROPERTY, 0);

    // Seed the index-engine file-base-id floor so the very first read (the allocator seeding at
    // open, or a genesis-bootstrap allocation on this virgin configuration) finds a durable
    // value; a missing floor on a v24 database reads as corruption.
    updateIntProperty(atomicOperation, INDEX_ENGINE_FILE_BASE_ID_FLOOR_PROPERTY, 0);
  }

  private void autoInitCollections() {
    if (configuration.getValueAsInteger(GlobalConfiguration.CLASS_COLLECTIONS_COUNT)
        == 0) {
      configuration.setValue(GlobalConfiguration.CLASS_COLLECTIONS_COUNT, 8);
    }
  }

  private static final class PausedNotificationsState {

    private boolean notificationsPaused;
    private long pendingChanges;
  }
}
