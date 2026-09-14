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

package com.jetbrains.youtrackdb.internal.core.storage.impl.local;

import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException;
import com.jetbrains.youtrackdb.api.exception.HighLevelException;
import com.jetbrains.youtrackdb.api.exception.RecordNotFoundException;
import com.jetbrains.youtrackdb.internal.common.comparator.DefaultComparator;
import com.jetbrains.youtrackdb.internal.common.concur.NeedRetryException;
import com.jetbrains.youtrackdb.internal.common.concur.lock.ScalableRWLock;
import com.jetbrains.youtrackdb.internal.common.concur.lock.ThreadInterruptedException;
import com.jetbrains.youtrackdb.internal.common.io.YTIOException;
import com.jetbrains.youtrackdb.internal.common.log.LogManager;
import com.jetbrains.youtrackdb.internal.common.profiler.metrics.CoreMetrics;
import com.jetbrains.youtrackdb.internal.common.profiler.metrics.Stopwatch;
import com.jetbrains.youtrackdb.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrackdb.internal.common.serialization.types.UTF8Serializer;
import com.jetbrains.youtrackdb.internal.common.types.ModifiableBoolean;
import com.jetbrains.youtrackdb.internal.common.util.RawPair;
import com.jetbrains.youtrackdb.internal.core.YouTrackDBConstants;
import com.jetbrains.youtrackdb.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrackdb.internal.core.config.IndexEngineData;
import com.jetbrains.youtrackdb.internal.core.config.StorageCollectionConfiguration;
import com.jetbrains.youtrackdb.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrackdb.internal.core.config.StorageConfigurationUpdateListener;
import com.jetbrains.youtrackdb.internal.core.conflict.RecordConflictStrategy;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.YouTrackDBInternalEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.record.CurrentStorageComponentsFactory;
import com.jetbrains.youtrackdb.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.db.record.ridbag.LinkBagDeleter;
import com.jetbrains.youtrackdb.internal.core.exception.BaseException;
import com.jetbrains.youtrackdb.internal.core.exception.CollectionDoesNotExistException;
import com.jetbrains.youtrackdb.internal.core.exception.CommitSerializationException;
import com.jetbrains.youtrackdb.internal.core.exception.ConcurrentCreateException;
import com.jetbrains.youtrackdb.internal.core.exception.ConfigurationException;
import com.jetbrains.youtrackdb.internal.core.exception.DatabaseException;
import com.jetbrains.youtrackdb.internal.core.exception.IndexEngineReplacedException;
import com.jetbrains.youtrackdb.internal.core.exception.InternalErrorException;
import com.jetbrains.youtrackdb.internal.core.exception.InvalidDatabaseNameException;
import com.jetbrains.youtrackdb.internal.core.exception.InvalidIndexEngineIdException;
import com.jetbrains.youtrackdb.internal.core.exception.InvalidInstanceIdException;
import com.jetbrains.youtrackdb.internal.core.exception.ModificationOperationProhibitedException;
import com.jetbrains.youtrackdb.internal.core.exception.StaleIndexEngineException;
import com.jetbrains.youtrackdb.internal.core.exception.StorageDoesNotExistException;
import com.jetbrains.youtrackdb.internal.core.exception.StorageException;
import com.jetbrains.youtrackdb.internal.core.exception.StorageExistsException;
import com.jetbrains.youtrackdb.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrackdb.internal.core.id.RecordId;
import com.jetbrains.youtrackdb.internal.core.id.RecordIdInternal;
import com.jetbrains.youtrackdb.internal.core.index.CompositeKey;
import com.jetbrains.youtrackdb.internal.core.index.Index;
import com.jetbrains.youtrackdb.internal.core.index.IndexAbstract;
import com.jetbrains.youtrackdb.internal.core.index.IndexDefinition;
import com.jetbrains.youtrackdb.internal.core.index.IndexException;
import com.jetbrains.youtrackdb.internal.core.index.IndexManagerEmbedded;
import com.jetbrains.youtrackdb.internal.core.index.IndexMetadata;
import com.jetbrains.youtrackdb.internal.core.index.Indexes;
import com.jetbrains.youtrackdb.internal.core.index.IndexesSnapshot;
import com.jetbrains.youtrackdb.internal.core.index.engine.BaseIndexEngine;
import com.jetbrains.youtrackdb.internal.core.index.engine.HistogramSnapshot;
import com.jetbrains.youtrackdb.internal.core.index.engine.IndexEngine;
import com.jetbrains.youtrackdb.internal.core.index.engine.IndexEngineReference;
import com.jetbrains.youtrackdb.internal.core.index.engine.IndexEngineValidator;
import com.jetbrains.youtrackdb.internal.core.index.engine.IndexEngineValuesTransformer;
import com.jetbrains.youtrackdb.internal.core.index.engine.IndexHistogramManager;
import com.jetbrains.youtrackdb.internal.core.index.engine.MultiValueIndexEngine;
import com.jetbrains.youtrackdb.internal.core.index.engine.SingleValueIndexEngine;
import com.jetbrains.youtrackdb.internal.core.index.engine.V1IndexEngine;
import com.jetbrains.youtrackdb.internal.core.index.engine.v1.BTreeIndexEngine;
import com.jetbrains.youtrackdb.internal.core.index.engine.v1.BTreeMultiValueIndexEngine;
import com.jetbrains.youtrackdb.internal.core.index.engine.v1.BTreeSingleValueIndexEngine;
import com.jetbrains.youtrackdb.internal.core.index.lifecycle.IndexBuildState;
import com.jetbrains.youtrackdb.internal.core.index.lifecycle.IndexBuildStateStore;
import com.jetbrains.youtrackdb.internal.core.index.lifecycle.IndexLifecycleCell;
import com.jetbrains.youtrackdb.internal.core.index.lifecycle.IndexLifecycleRegistry;
import com.jetbrains.youtrackdb.internal.core.index.lifecycle.IndexLifecycleSnapshot;
import com.jetbrains.youtrackdb.internal.core.metadata.MetadataDefault;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.PropertyTypeInternal;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.SchemaShared;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.TxSchemaState;
import com.jetbrains.youtrackdb.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrackdb.internal.core.serialization.serializer.binary.impl.index.CompositeKeySerializer;
import com.jetbrains.youtrackdb.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrackdb.internal.core.serialization.serializer.stream.StreamSerializerRID;
import com.jetbrains.youtrackdb.internal.core.storage.IdentifiableStorage;
import com.jetbrains.youtrackdb.internal.core.storage.PhysicalPosition;
import com.jetbrains.youtrackdb.internal.core.storage.RawBuffer;
import com.jetbrains.youtrackdb.internal.core.storage.RawPageBuffer;
import com.jetbrains.youtrackdb.internal.core.storage.RecordMetadata;
import com.jetbrains.youtrackdb.internal.core.storage.Storage;
import com.jetbrains.youtrackdb.internal.core.storage.StorageCollection;
import com.jetbrains.youtrackdb.internal.core.storage.StorageCollection.ATTRIBUTES;
import com.jetbrains.youtrackdb.internal.core.storage.StorageReadResult;
import com.jetbrains.youtrackdb.internal.core.storage.cache.ReadCache;
import com.jetbrains.youtrackdb.internal.core.storage.cache.WriteCache;
import com.jetbrains.youtrackdb.internal.core.storage.cache.local.BackgroundExceptionListener;
import com.jetbrains.youtrackdb.internal.core.storage.collection.CollectionPositionMapBucket.PositionEntry;
import com.jetbrains.youtrackdb.internal.core.storage.collection.PaginatedCollection;
import com.jetbrains.youtrackdb.internal.core.storage.collection.PaginatedCollection.RECORD_STATUS;
import com.jetbrains.youtrackdb.internal.core.storage.collection.SnapshotKey;
import com.jetbrains.youtrackdb.internal.core.storage.collection.VisibilityKey;
import com.jetbrains.youtrackdb.internal.core.storage.collection.v2.PaginatedCollectionV2;
import com.jetbrains.youtrackdb.internal.core.storage.config.CollectionBasedStorageConfiguration;
import com.jetbrains.youtrackdb.internal.core.storage.disk.PreparedBackupChain;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageIdentity;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.StorageLineageIdentity;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsManager;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsTable;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.atomicoperations.operationsfreezer.FreezeKind;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.base.DurablePage;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.AtomicUnitEndRecord;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.AtomicUnitStartRecord;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.FileCreatedWALRecord;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.FileDeletedWALRecord;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.HighLevelTransactionChangeRecord;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.LogSequenceNumber;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.NonTxOperationPerformedWALRecord;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.OperationUnitRecord;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.PageOperation;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.PageOperationRegistry;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.StorageCollectionFactory;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.UpdatePageRecord;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.WALPageBrokenException;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.WALRecord;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.WALRecordsFactory;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.WriteAheadLog;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.wal.common.EmptyWALRecord;
import com.jetbrains.youtrackdb.internal.core.storage.ridbag.BTreeBasedLinkBag;
import com.jetbrains.youtrackdb.internal.core.storage.ridbag.LinkCollectionsBTreeManager;
import com.jetbrains.youtrackdb.internal.core.storage.ridbag.LinkCollectionsBTreeManagerShared;
import com.jetbrains.youtrackdb.internal.core.storage.ridbag.ridbagbtree.EdgeSnapshotKey;
import com.jetbrains.youtrackdb.internal.core.storage.ridbag.ridbagbtree.EdgeVisibilityKey;
import com.jetbrains.youtrackdb.internal.core.storage.ridbag.ridbagbtree.LinkBagValue;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransaction;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransactionImpl;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransactionIndexChanges;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransactionIndexChangesPerKey;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation for paginated storage engines, providing common storage operations.
 *
 * @since 28.03.13
 */
public abstract class AbstractStorage
    implements CheckpointRequestListener,
    IdentifiableStorage,
    BackgroundExceptionListener,
    FreezableStorageComponent,
    PageIsBrokenListener {

  private static final Logger logger = LoggerFactory.getLogger(AbstractStorage.class);
  private static final int WAL_RESTORE_REPORT_INTERVAL = 30 * 1000; // milliseconds

  private static final Comparator<RecordOperation> COMMIT_RECORD_OPERATION_COMPARATOR =
      Comparator.comparing(
          o -> o.record.getIdentity());

  /** Suffix appended to index engine names for null-key trees. */
  public static final String NULL_TREE_SUFFIX = "$null";

  /**
   * The stem prefix of file-base-id-keyed index-engine files ({@code ie_<fileBaseId>} plus the
   * usual extensions and the {@link #NULL_TREE_SUFFIX} variant). The open-time high-water-mark
   * seeding sweeps the write cache for stems of this shape so an engine file that survived a
   * rolled-back create on the in-memory profile (whose eager cache install does not revert) can
   * never have its file base id handed out again.
   */
  public static final String INDEX_ENGINE_FILE_STEM_PREFIX = "ie_";

  /**
   * The storage-component name stem of the engine with the given stable file base id — the
   * single derivation site for the {@code ie_<fileBaseId>} shape. Every file of the engine's
   * family carries this stem (the multi-value null tree appends {@link #NULL_TREE_SUFFIX}), so
   * neither the index name nor a class name ever keys a storage file.
   */
  public static String indexEngineFileStem(final int fileBaseId) {
    return INDEX_ENGINE_FILE_STEM_PREFIX + fileBaseId;
  }

  /**
   * Version comparator for index snapshot visibility-index maps: orders by the
   * last key element (the committing TX's version = newVersion), falling back
   * to full element-wise comparison for uniqueness. Enables efficient
   * headMap(lwm) eviction.
   *
   * <p>Hand-written to avoid per-comparison allocations from
   * Comparator.comparingLong (unboxing) and thenComparing(identity)
   * (iterator allocation in CompositeKey.compareTo).
   */
  public static final Comparator<CompositeKey> INDEX_SNAPSHOT_VERSION_COMPARATOR =
      (a, b) -> {
        var aKeys = a.getKeys();
        var bKeys = b.getKeys();
        // Primary: compare last element (version) as long
        int cmp = Long.compare(
            (Long) aKeys.getLast(),
            (Long) bKeys.getLast());
        if (cmp != 0) {
          return cmp;
        }
        // Tiebreaker: element-wise comparison without iterator allocation.
        // Uses DefaultComparator for null-safe comparison (null keys are valid
        // in snapshot visibility maps for null-indexed entries).
        int aSize = aKeys.size();
        int bSize = bKeys.size();
        int minSize = Math.min(aSize, bSize);
        for (int i = 0; i < minSize; i++) {
          cmp = DefaultComparator.INSTANCE.compare(aKeys.get(i), bKeys.get(i));
          if (cmp != 0) {
            return cmp;
          }
        }
        return Integer.compare(aSize, bSize);
      };

  protected volatile LinkCollectionsBTreeManagerShared linkCollectionsBTreeManager;

  private final Map<String, StorageCollection> collectionMap = new HashMap<>();
  private final List<StorageCollection> collections = new CopyOnWriteArrayList<>();

  private final AtomicBoolean walVacuumInProgress = new AtomicBoolean();

  protected volatile WriteAheadLog writeAheadLog;
  @Nullable private StorageRecoverListener recoverListener;

  protected volatile ReadCache readCache;
  protected volatile WriteCache writeCache;

  private volatile RecordConflictStrategy recordConflictStrategy =
      YouTrackDBEnginesManager.instance().getRecordConflictStrategy().getDefaultImplementation();

  protected volatile AtomicOperationsManager atomicOperationsManager;

  /**
   * The freeze ids of this storage's currently engaged operator freezes: {@link #freeze} pushes
   * the id its registration returned and {@link #release} pops one and releases by that real id,
   * so the freezer removes the freeze's id&rarr;kind record instead of stranding it (the {@code
   * -1} sentinel remains only as a guarded fallback for a {@code release()} without a matching
   * {@code freeze()}). LIFO pairing across concurrent freeze/release cycles is sound: every
   * retained id resolves to the same OPERATOR kind, so which paired id a release pops does not
   * change any counter movement.
   */
  private final ConcurrentLinkedDeque<Long> operatorFreezeIds = new ConcurrentLinkedDeque<>();

  private volatile boolean wereNonTxOperationsPerformedInPreviousOpen;

  /**
   * Internal file IDs of non-durable files deleted during crash recovery, used by WAL replay
   * ({@link #restoreAtomicUnit}) to skip records referencing these files. Populated by
   * {@code writeCache.deleteNonDurableFilesOnRecovery()} before WAL replay, cleared after
   * replay completes. Plain (non-volatile) field — recovery is single-threaded.
   */
  private IntOpenHashSet deletedNonDurableFileIds = new IntOpenHashSet();

  private final int id;

  private final Map<String, BaseIndexEngine> indexEngineNameMap = new HashMap<>();
  private final List<BaseIndexEngine> indexEngines = new ArrayList<>();
  private final StorageIdentityHolder storageIdentityHolder;
  private final IndexLifecycleRegistry indexLifecycleRegistry;
  private static final String TOP_LEVEL_INDEX_LIFECYCLES =
      AbstractStorage.class.getName() + ".topLevelIndexLifecycles";

  private final IndexBuildStateStorageBackend indexBuildStateStorageBackend;
  private final IndexBuildStateStore indexBuildStateStore;
  private long indexEngineGeneration;
  private final AtomicOperationIdGen idGen = new AtomicOperationIdGen();

  /**
   * In-process high-water mark of the index-engine file-base-id allocator. Strictly monotonic for
   * the storage instance's lifetime and deliberately NOT reverted by a rolled-back engine create:
   * the persisted floor (written alongside every allocation inside the allocating atomic
   * operation) reverts with the files, but on the in-memory profile a rolled-back create's eager
   * file install survives, so a reverting allocator would re-issue the burned id and wedge every
   * subsequent create against the surviving orphan. Burning the value instead keeps allocation
   * collision-free on both profiles. Seeded at {@link #openIndexes} from
   * max(persisted floor, max persisted engine {@code fileBaseId}, max swept
   * {@code ie_<n>} file stem) — see {@link #seedIndexEngineFileBaseIdHwm}.
   */
  private final AtomicLong indexEngineFileBaseIdHwm = new AtomicLong();

  /**
   * Storage-level shared cache for histogram snapshots, keyed by engine ID.
   * Shared across all {@link IndexHistogramManager} instances in this storage.
   */
  private final ConcurrentHashMap<Integer, HistogramSnapshot> histogramSnapshotCache =
      new ConcurrentHashMap<>();

  /**
   * Executor reference for histogram managers. Must NOT be the ioExecutor
   * (used by AsynchronousFileChannel for I/O completions) — blocking reads
   * on the ioExecutor cause deadlocks. Stored so that newly created indexes
   * (after database open) can receive the executor immediately.
   * Set by {@link #setHistogramExecutor}.
   */
  @Nullable private volatile ExecutorService histogramExecutor;

  /**
   * Storage-level semaphore limiting concurrent histogram rebalance tasks.
   * Permit count is read from
   * {@link GlobalConfiguration#QUERY_STATS_MAX_CONCURRENT_REBALANCES} at
   * construction time ({@code -1} = auto: {@code max(2, processors / 4)}).
   * Propagated to each histogram manager via
   * {@link IndexHistogramManager#setRebalanceSemaphore}.
   */
  private final Semaphore histogramRebalanceSemaphore;

  private boolean wereDataRestoredAfterOpen;

  // Test-observability only: counts how many times the recovery-time orphan-truncation
  // pass (truncateOrphansAfterRecovery) is dispatched on this storage instance. Production
  // code never reads it. The open-time dispatch is gated so that a cleanly-closed disk
  // database that replays no WAL skips the pass entirely (YTDB-1039); a regression test
  // reads this counter to prove the dispatch did NOT run on a clean reopen and DID run on a
  // crash (WAL-replay) reopen. File size alone cannot distinguish "pass skipped" from "pass
  // ran and truncated nothing", so an explicit dispatch counter is the observation hook.
  // Package-private so only same-package tests can read it; never reset.
  final AtomicInteger orphanTruncationDispatchCountForTests = new AtomicInteger(0);

  protected UUID uuid;

  private final AtomicInteger sessionCount = new AtomicInteger(0);
  private volatile long lastCloseTime = System.currentTimeMillis();

  protected static final String DATABASE_INSTANCE_ID = "databaseInstenceId";

  protected AtomicOperationsTable atomicOperationsTable;
  protected final String url;
  protected final ScalableRWLock stateLock;

  protected volatile StorageConfiguration configuration;
  protected volatile CurrentStorageComponentsFactory componentsFactory;
  protected final String name;
  private final AtomicLong version = new AtomicLong();

  protected volatile STATUS status = STATUS.CLOSED;

  protected final AtomicReference<Throwable> error = new AtomicReference<>(null);
  protected YouTrackDBInternalEmbedded context;
  private volatile CountDownLatch migration = new CountDownLatch(1);

  protected final ReentrantLock backupLock = new ReentrantLock();

  // Non-blocking lock for snapshot index cleanup. Only one thread performs cleanup at a time;
  // other committing threads skip cleanup if the lock is already held (tryLock pattern).
  // Package-private for testability (SnapshotIndexCleanupTest).
  final ReentrantLock snapshotCleanupLock = new ReentrantLock();

  private final Stopwatch dropDuration;
  private final Stopwatch synchDuration;
  private final Stopwatch shutdownDuration;

  // Per-thread holder for the minimum active operation timestamp (tsMin).
  // Used by the low-water-mark GC to determine which snapshot index entries are safe to evict.
  protected final ThreadLocal<TsMinHolder> tsMinThreadLocal =
      ThreadLocal.withInitial(TsMinHolder::new);

  // Set of all TsMinHolders across threads. Backed by Guava's ConcurrentHashMap with weak
  // keys so that entries are automatically removed when the owning thread's TsMinHolder
  // becomes unreachable (after thread death releases the ThreadLocal's strong reference).
  // Uses identity equality (MapMaker.weakKeys() default), matching TsMinHolder's inherited
  // Object.equals/hashCode.
  protected final Set<TsMinHolder> tsMins = newTsMinsSet();

  // Commit-window re-entry counter, per thread. A schema-carrying commit takes
  // stateLock.writeLock() from the start and then, while still holding it,
  // reads records to serialize and re-parse the schema (txLocalSchema.toStream,
  // committedSchema.fromStream reached through session.load). Those reads route
  // back into this storage's stateLock.readLock()-taking methods. The
  // ScalableRWLock is non-reentrant, so a read re-acquire from the write-lock
  // holder busy-spins forever. While this counter is positive on the current
  // thread, the readLock-taking record-read methods skip the readLock: the held
  // write lock already excludes every concurrent registrar and supplies the
  // happens-before edge, so the read is safe and lock-free. The counter is a
  // depth, not a flag, so nested enter/exit pairs compose. It is set only by the
  // commit window via enterCommitWindow()/exitCommitWindow(); a thread that does
  // not hold the write lock never enters the window, so the pure-data read-lock
  // fast path is unchanged. exitCommitWindow() remove()s this ThreadLocal once the
  // depth returns to zero, so a reused pooled worker never observes leftover window
  // state from an earlier task.
  private final ThreadLocal<int[]> commitWindowDepth =
      ThreadLocal.withInitial(() -> new int[1]);

  static Set<TsMinHolder> newTsMinsSet() {
    return Sets.newSetFromMap(new MapMaker().weakKeys().makeMap());
  }

  // Shared snapshot index: maps (componentId, collectionPosition, recordVersion) → PositionEntry.
  // Replaces per-collection snapshotIndex fields in PaginatedCollectionV2, enabling centralized
  // low-water-mark GC across all collections.
  protected final ConcurrentSkipListMap<SnapshotKey, PositionEntry> sharedSnapshotIndex =
      new ConcurrentSkipListMap<>();

  // Visibility index: maps (recordTs, componentId, collectionPosition) → SnapshotKey.
  // Ordering by recordTs first enables efficient range-scan eviction via headMap(lowWaterMark).
  protected final ConcurrentSkipListMap<VisibilityKey, SnapshotKey> visibilityIndex =
      new ConcurrentSkipListMap<>();

  // Approximate count of entries in sharedSnapshotIndex, used for O(1) cleanup threshold checks.
  // ConcurrentSkipListMap.size() is O(n) and calling it on every commit causes resource
  // exhaustion under sustained heavy concurrent load (e.g., 30-minute soak tests).
  // Incremented during flushSnapshotBuffers(), decremented during evictStaleSnapshotEntries().
  protected final AtomicLong snapshotIndexSize = new AtomicLong();

  // Indexes snapshot: maps CompositeKey(indexId, userKey..., version) → RID (TombstoneRID or plain).
  private final ConcurrentSkipListMap<CompositeKey, RID> sharedIndexesSnapshot =
      new ConcurrentSkipListMap<>();
  private final ConcurrentSkipListMap<CompositeKey, RID> sharedNullIndexesSnapshot =
      new ConcurrentSkipListMap<>();

  // Indexes snapshot visibility index: maps removedKey (newVersion) → addedKey (oldVersion),
  // ordered by newVersion (last key element). Enables efficient range-scan eviction via
  // headMap(lowWaterMark), matching the collection/edge eviction pattern.
  private final ConcurrentSkipListMap<CompositeKey, CompositeKey> indexesSnapshotVisibilityIndex =
      new ConcurrentSkipListMap<>(INDEX_SNAPSHOT_VERSION_COMPARATOR);
  private final ConcurrentSkipListMap<CompositeKey, CompositeKey> nullIndexSnapshotVisibilityIndex =
      new ConcurrentSkipListMap<>(INDEX_SNAPSHOT_VERSION_COMPARATOR);

  // Approximate count of entries in sharedIndexesSnapshotData + sharedNullIndexesSnapshotData,
  // used for O(1) cleanup threshold checks.
  // Incremented by IndexesSnapshot.addSnapshotPair() (2 per pair), decremented by
  // evictStaleIndexesSnapshotEntries().
  protected final AtomicLong indexesSnapshotEntriesCount = new AtomicLong();

  // Edge snapshot index: maps (componentId, ridBagId, targetCollection, targetPosition, version)
  // → LinkBagValue. Stores old versions of link bag entries for snapshot isolation on edges.
  // Parallel to sharedSnapshotIndex but with edge-specific key types and full value storage
  // (B-tree entries don't have stable page positions across splits/merges).
  protected final ConcurrentSkipListMap<EdgeSnapshotKey, LinkBagValue> sharedEdgeSnapshotIndex =
      new ConcurrentSkipListMap<>();

  // Edge visibility index: maps (recordTs, componentId, ridBagId, targetCollection,
  // targetPosition) → EdgeSnapshotKey. Ordering by recordTs first enables efficient
  // range-scan eviction via headMap(lowWaterMark), same pattern as visibilityIndex.
  protected final ConcurrentSkipListMap<EdgeVisibilityKey, EdgeSnapshotKey> edgeVisibilityIndex =
      new ConcurrentSkipListMap<>();

  // Approximate count of entries in sharedEdgeSnapshotIndex, used for O(1) cleanup threshold
  // checks. Same rationale as snapshotIndexSize — ConcurrentSkipListMap.size() is O(n).
  protected final AtomicLong edgeSnapshotIndexSize = new AtomicLong();

  // Stale transaction monitor (YTDB-550): periodically scans tsMins to detect long-running
  // transactions and logs warnings. Initialized at storage open, stopped at shutdown.
  @Nullable private volatile StaleTransactionMonitor staleTransactionMonitor;

  public AbstractStorage(
      final String name, final String filePath, final int id,
      YouTrackDBInternalEmbedded context) {
    this(name, filePath, id, context, true);
  }

  protected AbstractStorage(
      final String name,
      final String filePath,
      final int id,
      YouTrackDBInternalEmbedded context,
      boolean initializeVolatileIdentity) {
    this.context = context;
    this.name = checkName(name);

    url = filePath;

    stateLock = new ScalableRWLock();

    this.id = id;

    storageIdentityHolder = initializeVolatileIdentity
        ? new StorageIdentityHolder(StorageIdentity.random(), StorageLineageIdentity.random())
        : new StorageIdentityHolder();
    indexLifecycleRegistry = new IndexLifecycleRegistry(storageIdentityHolder::lineageIdentity);
    indexBuildStateStorageBackend = new IndexBuildStateStorageBackend(this);
    indexBuildStateStore = new IndexBuildStateStore(indexBuildStateStorageBackend);

    int permits = GlobalConfiguration.QUERY_STATS_MAX_CONCURRENT_REBALANCES
        .getValueAsInteger();
    if (permits <= 0) {
      permits = Math.max(2,
          Runtime.getRuntime().availableProcessors() / 4);
    }
    this.histogramRebalanceSemaphore = new Semaphore(permits);

    linkCollectionsBTreeManager = new LinkCollectionsBTreeManagerShared(this);
    dropDuration = YouTrackDBEnginesManager.instance()
        .getMetricsRegistry()
        .databaseMetric(CoreMetrics.DATABASE_DROP_DURATION, this.name);
    synchDuration = YouTrackDBEnginesManager.instance()
        .getMetricsRegistry()
        .databaseMetric(CoreMetrics.DATABASE_SYNCH_DURATION, this.name);
    shutdownDuration = YouTrackDBEnginesManager.instance()
        .getMetricsRegistry()
        .databaseMetric(CoreMetrics.DATABASE_SHUTDOWN_DURATION, this.name);
  }

  protected static String normalizeName(String name) {
    final var firstIndexOf = name.lastIndexOf('/');
    final var secondIndexOf = name.lastIndexOf(File.separator);

    if (firstIndexOf >= 0 || secondIndexOf >= 0) {
      return name.substring(Math.max(firstIndexOf, secondIndexOf) + 1);
    } else {
      return name;
    }
  }

  public static String checkName(String name) {
    name = normalizeName(name);

    var pattern = Pattern.compile("^\\p{L}[\\p{L}\\d_$-]*$");
    var matcher = pattern.matcher(name);
    var isValid = matcher.matches();
    if (!isValid) {
      throw new InvalidDatabaseNameException(
          "Invalid name for database. ("
              + name
              + ") Name can contain only letters, numbers, underscores and dashes. "
              + "Name should start with letter.");
    }

    return name;
  }

  @Override
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public Storage getUnderlying() {
    return this;
  }

  @Override
  public String getName() {
    return name;
  }

  public String getURL() {
    return url;
  }

  public ContextConfiguration getContextConfiguration() {
    return configuration.getContextConfiguration();
  }

  public String getSchemaRecordId() {
    return configuration.getSchemaRecordId();
  }

  public String getCharset() {
    return configuration.getCharset();
  }

  public String getIndexMgrRecordId() {
    return configuration.getIndexMgrRecordId();
  }

  @Nullable public TimeZone getTimeZone() {
    return configuration.getTimeZone();
  }

  public SimpleDateFormat getDateFormatInstance() {
    return configuration.getDateFormatInstance();
  }

  public String getDateFormat() {
    return configuration.getDateFormat();
  }

  public String getDateTimeFormat() {
    return configuration.getDateTimeFormat();
  }

  public SimpleDateFormat getDateTimeFormatInstance() {
    return configuration.getDateTimeFormatInstance();
  }

  public String getRecordSerializer() {
    return configuration.getRecordSerializer();
  }

  public int getRecordSerializerVersion() {
    return configuration.getRecordSerializerVersion();
  }

  public String getLocaleCountry() {
    return configuration.getLocaleCountry();
  }

  public String getLocaleLanguage() {
    return configuration.getLocaleLanguage();
  }

  public int getMinimumCollections() {
    return configuration.getMinimumCollections();
  }

  public boolean isValidationEnabled() {
    return configuration.isValidationEnabled();
  }

  @Override
  public void close(DatabaseSessionEmbedded session) {
    var sessions = sessionCount.decrementAndGet();

    if (sessions < 0) {
      throw new StorageException(name,
          "Amount of closed sessions in storage "
              + name
              + " is bigger than amount of open sessions");
    }
    lastCloseTime = System.currentTimeMillis();
  }

  private void closeIfPossible() {
    while (true) {
      var sessions = sessionCount.get();
      if (sessions < 0) {
        return;
      }
      if (sessionCount.compareAndSet(sessions, sessions - 1)) {
        lastCloseTime = System.currentTimeMillis();
        return;
      }
    }
  }

  public long getSessionsCount() {
    return sessionCount.get();
  }

  public long getLastCloseTime() {
    return lastCloseTime;
  }

  @Override
  public boolean dropCollection(DatabaseSessionEmbedded session, final String iCollectionName) {
    return dropCollection(session, getCollectionIdByName(iCollectionName));
  }

  @Override
  public long countRecords(DatabaseSessionEmbedded session) {
    long tot = 0;
    for (var c : getCollectionInstances()) {
      if (c != null) {
        tot += c.getApproximateRecordsCount();
      }
    }
    return tot;
  }

  @Override
  public String toString() {
    return url != null ? url : "?";
  }

  @Override
  public STATUS getStatus() {
    return status;
  }

  @Override
  public boolean isAssigningCollectionIds() {
    return true;
  }

  @Override
  public CurrentStorageComponentsFactory getComponentsFactory() {
    return componentsFactory;
  }

  @Override
  public long getVersion() {
    return version.get();
  }

  @Override
  public void shutdown() {
    stateLock.writeLock().lock();
    try {
      doShutdown();
    } catch (final IOException e) {
      final var message = "Error on closing of storage '" + name;
      LogManager.instance().error(this, message, e);

      throw BaseException.wrapException(new StorageException(name, message), e, name);
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  private static void checkPageSizeAndRelatedParametersInGlobalConfiguration(String dbName) {
    final var pageSize = GlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() << 10;
    var maxKeySize = GlobalConfiguration.BTREE_MAX_KEY_SIZE.getValueAsInteger();
    var bTreeMaxKeySize = (int) (pageSize * 0.3);

    if (maxKeySize <= 0) {
      maxKeySize = bTreeMaxKeySize;
      GlobalConfiguration.BTREE_MAX_KEY_SIZE.setValue(maxKeySize);
    }

    if (maxKeySize > bTreeMaxKeySize) {
      throw new StorageException(dbName,
          "Value of parameter "
              + GlobalConfiguration.DISK_CACHE_PAGE_SIZE.getKey()
              + " should be at least 4 times bigger than value of parameter "
              + GlobalConfiguration.BTREE_MAX_KEY_SIZE.getKey()
              + " but real values are :"
              + GlobalConfiguration.DISK_CACHE_PAGE_SIZE.getKey()
              + " = "
              + pageSize
              + " , "
              + GlobalConfiguration.BTREE_MAX_KEY_SIZE.getKey()
              + " = "
              + maxKeySize);
    }
  }

  private static SortedMap<String, FrontendTransactionIndexChanges> getSortedIndexOperations(
      final FrontendTransaction clientTx) {
    return new TreeMap<>(clientTx.getIndexOperations());
  }

  @Override
  public final void open(
      DatabaseSessionEmbedded remote, final String iUserName,
      final String iUserPassword,
      final ContextConfiguration contextConfiguration) {
    open(contextConfiguration);
  }

  public final void open(final ContextConfiguration contextConfiguration) {
    try {
      stateLock.readLock().lock();
      try {
        if (status == STATUS.OPEN || isInError()) {
          // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
          // REUSED

          sessionCount.incrementAndGet();
          return;
        }

      } finally {
        stateLock.readLock().unlock();
      }

      checkPageSizeAndRelatedParametersInGlobalConfiguration(name);
      try {
        stateLock.writeLock().lock();
        try {
          if (status == STATUS.MIGRATION) {
            try {
              // Yes this look inverted but is correct.
              stateLock.writeLock().unlock();
              migration.await();
            } finally {
              stateLock.writeLock().lock();
            }
          }

          if (status == STATUS.OPEN || isInError())
          // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
          // REUSED
          {
            return;
          }

          if (status != STATUS.CLOSED) {
            throw new StorageException(name,
                "Storage " + name + " is in wrong state " + status + " and can not be opened.");
          }

          if (!exists()) {
            throw new StorageDoesNotExistException(name,
                "Cannot open the storage '" + name + "' because it does not exist in path: " + url);
          }

          // This call is the single admission decision of storage open. Admission runs before
          // write-ahead log initialization and before recovery, so a rejected image never reaches
          // recovery. A write-ahead log records a change before that change reaches its final file.
          readStorageIdentity();
          readIv();

          initWalAndDiskCache(contextConfiguration);

          // Register all PageOperation types so recovery can deserialize logical WAL records.
          // Must happen after WAL initialization (above) and before recoverIfNeeded() (below).
          PageOperationRegistry.registerAll(WALRecordsFactory.INSTANCE);

          final var startupMetadata = checkIfStorageDirty();
          final var lastTxId = startupMetadata.lastTxId;
          if (lastTxId > 0) {
            idGen.setStartId(lastTxId + 1);
          } else {
            idGen.setStartId(0);
          }

          atomicOperationsTable =
              new AtomicOperationsTable(
                  contextConfiguration.getValueAsInteger(
                      GlobalConfiguration.STORAGE_ATOMIC_OPERATIONS_TABLE_COMPACTION_LIMIT),
                  idGen.getLastId() + 1);
          atomicOperationsManager = new AtomicOperationsManager(this, atomicOperationsTable);

          recoverIfNeeded();

          atomicOperationsManager.executeInsideAtomicOperation(
              atomicOperation -> {
                if (CollectionBasedStorageConfiguration.exists(writeCache)) {
                  configuration = new CollectionBasedStorageConfiguration(this);
                  ((CollectionBasedStorageConfiguration) configuration)
                      .load(contextConfiguration, atomicOperation);

                  // otherwise delayed to disk based storage to convert old format to new format.
                }

                initConfiguration(contextConfiguration, atomicOperation);
              });

          atomicOperationsManager.executeInsideAtomicOperation(
              (atomicOperation) -> {
                var uuid = configuration.getUuid();
                if (uuid == null) {
                  uuid = UUID.randomUUID().toString();
                  configuration.setUuid(atomicOperation, uuid);
                }
                this.uuid = UUID.fromString(uuid);
              });

          var binaryFormatVersion =
              atomicOperationsManager.calculateInsideAtomicOperation(
                  configuration::getBinaryFormatVersion);
          componentsFactory = new CurrentStorageComponentsFactory(binaryFormatVersion);

          atomicOperationsManager.executeInsideAtomicOperation(
              this::checkPageSizeAndRelatedParameters);

          atomicOperationsManager.executeInsideAtomicOperation(linkCollectionsBTreeManager::load);

          atomicOperationsManager.executeInsideAtomicOperation(this::openCollections);
          atomicOperationsManager.executeInsideAtomicOperation(this::openIndexes);

          // Recovery-time orphan-truncation pass. Restores the per-component
          // logical <= physical invariant before any non-recovery transaction runs.
          // Gated on wereDataRestoredAfterOpen: on the disk engine an orphan (physical
          // pages past the logical horizon) is crash-only. A rolled-back transaction
          // leaves zero physical footprint (the physical apply runs only inside
          // commitChanges, which endAtomicOperation skips on rollback), and no correct
          // production read extends a file outside crash recovery (this read-extend
          // invariant is established in the read-cache-concurrency-bug design:
          // WOWCache.loadOrAdd's extend branches are unreachable from loadForRead because
          // no component reads past its own logical horizon), so a cleanly-closed
          // database is orphan-free. An orphan can therefore arise only from a crash,
          // and a crash always leaves isDirty() == true at the next open, which sets
          // wereDataRestoredAfterOpen during recoverIfNeeded(). The pass runs whenever
          // WAL replay happened. We gate on the field rather than a re-read isDirty()
          // because recoverIfNeeded -> flushAllData -> clearStorageDirty has already
          // cleared the dirty flag by the time we reach here, so isDirty() would read
          // false even on a crash reopen; the field is the surviving "did this open
          // replay WAL" signal. The field is shared with
          // IndexManagerEmbedded.autoRecreateIndexesAfterCrash and is set once and never
          // reset, so it is left alone on close.
          //
          // Trade-off: the pass is best-effort: truncateOrphansAfterRecovery wraps each
          // per-component verifyAndTruncateOrphans dispatch in a try/catch that logs a
          // truncate IOException as a WARN and continues. Skipping the pass on a clean
          // reopen drops the cross-clean-cycle retry of a transient truncate failure on
          // an otherwise readable component. That is bounded-acceptable: such a failure
          // is loud (it logs a WARN at the reopen where it occurred) and is re-armed by
          // any later crash, which sets the field again and re-runs the pass.
          if (wereDataRestoredAfterOpen) {
            atomicOperationsManager.executeInsideAtomicOperation(
                this::truncateOrphansAfterRecovery);
          }

          atomicOperationsManager.executeInsideAtomicOperation(
              (atomicOperation) -> {
                final var cs = configuration.getConflictStrategy();
                if (cs != null) {
                  // SET THE CONFLICT STORAGE STRATEGY FROM THE LOADED CONFIGURATION
                  doSetConflictStrategy(
                      YouTrackDBEnginesManager.instance().getRecordConflictStrategy()
                          .getStrategy(cs),
                      atomicOperation);
                }
              });

          status = STATUS.MIGRATION;
        } finally {
          stateLock.writeLock().unlock();
        }

        // we need to use read lock to allow for example correctly truncate WAL during data
        // processing
        // all operations are prohibited on storage because of usage of special status.
        stateLock.readLock().lock();
        try {
          if (status != STATUS.MIGRATION) {
            LogManager.instance()
                .error(
                    this,
                    "Unexpected storage status %s, process of creation of storage is aborted",
                    null,
                    status.name());
            return;
          }

          //migration goes here, for future use
        } finally {
          stateLock.readLock().unlock();
        }

        stateLock.writeLock().lock();
        try {
          if (status != STATUS.MIGRATION) {
            LogManager.instance()
                .error(
                    this,
                    "Unexpected storage status %s, process of creation of storage is aborted",
                    null,
                    status.name());
            return;
          }

          atomicOperationsManager.executeInsideAtomicOperation(this::checkRidBagsPresence);
          status = STATUS.OPEN;
          startStaleTransactionMonitor();
          migration.countDown();
        } finally {
          stateLock.writeLock().unlock();
        }

      } catch (final RuntimeException e) {
        try {
          if (writeCache != null) {
            readCache.closeStorage(writeCache);
          }
        } catch (final Exception ee) {
          // ignore
        }

        try {
          if (writeAheadLog != null) {
            writeAheadLog.close();
          }
        } catch (final Exception ee) {
          // ignore
        }

        try {
          postCloseSteps(false, true, idGen.getLastId());
        } catch (final Exception ee) {
          // ignore
        }

        status = STATUS.CLOSED;
        throw e;
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      if (status == STATUS.OPEN) {
        sessionCount.incrementAndGet();
      }
    }

    final var additionalArgs = new Object[] {getURL(), YouTrackDBConstants.getVersion()};
    LogManager.instance()
        .info(this, "Storage '%s' is opened under YouTrackDB distribution : %s", additionalArgs);
  }

  protected abstract void readIv() throws IOException;

  @SuppressWarnings("unused")
  protected abstract byte[] getIv();

  /** {@inheritDoc} */
  @Override
  public final String getCreatedAtVersion() {
    return configuration.getCreatedAtVersion();
  }

  protected final void openIndexes(AtomicOperation atomicOperation) {
    final var cf = componentsFactory;
    if (cf == null) {
      throw new StorageException(name, "Storage '" + name + "' is not properly initialized");
    }

    // Seed the file-base-id allocator before any engine loads: openIndexes is on every path that
    // brings persisted engines back to life (normal open and the incremental-restore reopen), so
    // seeding here guarantees no later allocation can collide with a persisted or orphaned file
    // base id.
    seedIndexEngineFileBaseIdHwm(atomicOperation);

    final var indexNames = configuration.indexEngines(atomicOperation);
    var counter = 0;

    // avoid duplication of index engine ids
    for (final var indexName : indexNames) {
      final var engineData = configuration.getIndexEngine(indexName, -1, atomicOperation);
      if (counter <= engineData.getIndexId()) {
        counter = engineData.getIndexId() + 1;
      }
    }

    for (final var indexName : indexNames) {
      final var engineData = configuration.getIndexEngine(indexName, counter, atomicOperation);

      final var engine = Indexes.createIndexEngine(this, engineData);

      engine.load(engineData, atomicOperation);

      // Wire histogram manager for B-tree engines
      if (engine instanceof BTreeIndexEngine btreeEngine) {
        wireHistogramManagerOnLoad(btreeEngine, engineData, atomicOperation);
      }

      indexEngineNameMap.put(engineData.getName(), engine);
      while (engineData.getIndexId() >= indexEngines.size()) {
        indexEngines.add(null);
      }
      indexEngines.set(engineData.getIndexId(), engine);
      counter++;
    }
  }

  /**
   * Seeds the in-process file-base-id high-water mark from every durable trace an allocation can
   * leave, taking the max of three inputs:
   *
   * <ol>
   *   <li>the persisted floor — the last successfully committed allocation;
   *   <li>the max {@code fileBaseId} across the persisted engine entries — defends against a
   *       floor that lags the entries (e.g. a partially restored configuration);
   *   <li>the max {@code ie_<n>} stem among the write cache's files — defends against an orphaned
   *       engine file that survived a rolled-back create on the in-memory profile (the eager
   *       cache install does not revert) and would otherwise collide with a re-issued id.
   * </ol>
   *
   * <p>Never lowers the mark: the update takes {@code max(current, seed)}, so a re-seed (an
   * incremental restore into an already-open storage) keeps the monotonicity guarantee.
   */
  private void seedIndexEngineFileBaseIdHwm(final AtomicOperation atomicOperation) {
    long seed = ((CollectionBasedStorageConfiguration) configuration)
        .getIndexEngineFileBaseIdFloor(atomicOperation);

    for (final var engineName : configuration.indexEngines(atomicOperation)) {
      final var engineData = configuration.getIndexEngine(engineName, -1, atomicOperation);
      if (engineData != null && engineData.getFileBaseId() > seed) {
        seed = engineData.getFileBaseId();
      }
    }

    final var wc = writeCache;
    if (wc != null) {
      for (final var fileName : wc.files().keySet()) {
        final var swept = parseIndexEngineFileBaseId(fileName);
        if (swept > seed) {
          seed = swept;
        }
      }
    }

    final var finalSeed = seed;
    indexEngineFileBaseIdHwm.updateAndGet(current -> Math.max(current, finalSeed));
  }

  /**
   * Extracts the file base id from an engine file name of the shape
   * {@code ie_<n>[$null].<engine extension>}, or {@code -1} when the name cannot be an engine
   * file. Three rejection filters keep foreign artifacts from polluting the HWM seed:
   *
   * <ul>
   *   <li>the extension must belong to the engine family ({@code .cbt}/{@code .nbt}/{@code .ixs})
   *       — a user collection legally named {@code ie_123} produces files with collection
   *       extensions and is ignored;
   *   <li>the id must be pure digits — names with signs or other characters are not ours;
   *   <li>the id must fit the allocator's {@code (0, Integer.MAX_VALUE]} range — an
   *       out-of-range value cannot have been allocated, and accepting it would push the
   *       high-water mark past the persistable ceiling and permanently brick index creation.
   * </ul>
   *
   * <p>Accepted matches can still include a crash-window orphan: a kill between the physical
   * file creation inside {@code commitChanges} and the WAL end-record becoming durable leaves a
   * registered empty {@code ie_*} file whose atomic unit is discarded on replay. That file leaks
   * (bounded, one family per crashed create, never collected) and its id is burned by this sweep
   * — accepted behavior: burning the id is exactly what keeps the leaked file from ever
   * colliding with a future engine's family.
   */
  private static long parseIndexEngineFileBaseId(final String fileName) {
    final var extensionStart = fileName.lastIndexOf('.');
    if (extensionStart <= 0) {
      return -1;
    }
    if (!isIndexEngineFileExtension(fileName.substring(extensionStart))) {
      return -1;
    }
    var stem = fileName.substring(0, extensionStart);
    if (stem.endsWith(NULL_TREE_SUFFIX)) {
      stem = stem.substring(0, stem.length() - NULL_TREE_SUFFIX.length());
    }
    if (!stem.startsWith(INDEX_ENGINE_FILE_STEM_PREFIX)) {
      return -1;
    }
    final var digits = stem.substring(INDEX_ENGINE_FILE_STEM_PREFIX.length());
    if (digits.isEmpty()) {
      return -1;
    }
    for (var i = 0; i < digits.length(); i++) {
      final var c = digits.charAt(i);
      if (c < '0' || c > '9') {
        return -1;
      }
    }
    final long parsed;
    try {
      parsed = Long.parseLong(digits);
    } catch (final NumberFormatException ignore) {
      // More digits than a long can carry — certainly not an allocated id.
      return -1;
    }
    if (parsed <= 0 || parsed > Integer.MAX_VALUE) {
      return -1;
    }
    return parsed;
  }

  /**
   * Allocates the next index-engine file base id and persists the new floor inside
   * {@code atomicOperation}. The returned value is unique for the storage's whole lifetime: the
   * in-process high-water mark only ever grows (a rolled-back allocation burns its value — see
   * {@link #indexEngineFileBaseIdHwm}), and the floor write makes the allocation durable together
   * with the engine files it is for.
   *
   * <p>Must be called with {@code stateLock.writeLock()} held (both engine-create paths — the
   * public {@link #addIndexEngine(IndexMetadata, Map)} and the commit-window
   * {@link #createIndexEngineInCommitWindow}) — hold it): the exclusive lock serializes the
   * increment-then-persist pair against concurrent creates. The lock carries no owner, so the
   * assert can only check that the exclusive window is held by somebody; a caller that must
   * itself hold the lock cannot race another holder anyway.
   *
   * <p>Package-private for tests.
   */
  int allocateIndexEngineFileBaseId(final AtomicOperation atomicOperation) {
    assert stateLock.isWriteLocked()
        : "index-engine file-base-id allocation requires stateLock.writeLock(): the high-water"
            + " mark increment and the floor write must be serialized against concurrent engine"
            + " creates";
    final var allocated = indexEngineFileBaseIdHwm.incrementAndGet();
    if (allocated > Integer.MAX_VALUE) {
      throw new StorageException(name,
          "The index-engine file-base-id space of storage '" + name + "' is exhausted ("
              + allocated + " exceeds the persistable ceiling " + Integer.MAX_VALUE + ")");
    }
    ((CollectionBasedStorageConfiguration) configuration)
        .setIndexEngineFileBaseIdFloor(atomicOperation, (int) allocated);
    return (int) allocated;
  }

  /**
   * The current value of the index-engine file-base-id high-water mark — FOR TESTS ONLY (seeding
   * assertions).
   */
  long indexEngineFileBaseIdHwmForTesting() {
    return indexEngineFileBaseIdHwm.get();
  }

  protected final void openCollections(final AtomicOperation atomicOperation) throws IOException {
    // OPEN BASIC SEGMENTS
    int pos;

    // REGISTER COLLECTION
    final var configurationCollections = configuration.getCollections();
    for (var i = 0; i < configurationCollections.size(); ++i) {
      final var collectionConfig = configurationCollections.get(i);

      if (collectionConfig != null) {
        pos = createCollectionFromConfig(collectionConfig, atomicOperation);
        try {
          if (pos == -1) {
            collections.get(i).open(atomicOperation);
          } else {
            collections.get(pos).open(atomicOperation);
          }
        } catch (final FileNotFoundException e) {
          LogManager.instance()
              .warn(
                  this,
                  "Error on loading collection '"
                      + configurationCollections.get(i).name()
                      + "' ("
                      + i
                      + "): file not found. It will be excluded from current database '"
                      + name
                      + "'.",
                  e);

          collectionMap.remove(configurationCollections.get(i).name().toLowerCase(Locale.ROOT));

          setCollection(i, null);
        }
      } else {
        setCollection(i, null);
      }
    }
  }

  private void checkRidBagsPresence(final AtomicOperation operation) {
    for (final var collection : collections) {
      if (collection != null) {
        final var collectionId = collection.getId();

        if (!LinkCollectionsBTreeManagerShared.isComponentPresent(operation, collectionId)) {
          LogManager.instance()
              .info(
                  this,
                  "Collection with id %d does not have associated rid bag, fixing ...",
                  collectionId);
          linkCollectionsBTreeManager.createComponent(operation, collectionId);
        }
      }
    }
  }

  /**
   * Recovery-time orphan-truncation pass. Walks the four entry-point-equipped storage
   * component groups, reads each component's persisted logical-page counter, and dispatches
   * the layered {@code ReadCache.shrinkFile} primitive to drop any physical pages beyond the
   * logical horizon. The pass restores the {@code logical <= physical} invariant before any
   * non-recovery transaction runs.
   *
   * <p>The orchestrator iterates three groups:
   *
   * <ul>
   *   <li>{@code collections} — each non-null {@link PaginatedCollectionV2} entry triggers a
   *       single {@code verifyAndTruncateOrphans} call on the collection itself; the
   *       PCV2 helper's siblings-hook then delegates the embedded
   *       {@link CollectionPositionMapV2} truncate (the {@code .cpm} position-map file)
   *       internally, so the orchestrator never reaches across PCV2's encapsulation.</li>
   *   <li>{@code indexEngines} — each engine that is a {@link BTreeSingleValueIndexEngine}
   *       or {@link BTreeMultiValueIndexEngine} routes through its engine-side wrapper,
   *       which keeps the {@code sbTree}/{@code svTree}/{@code nullTree} fields
   *       {@code private final} on the engine.</li>
   *   <li>{@code linkCollectionsBTreeManager} — a single call to
   *       {@link LinkCollectionsBTreeManagerShared#verifyAndTruncateAllOrphans}, which fans
   *       out internally over its private {@code fileIdBTreeMap}. The manager intentionally
   *       exposes no public iteration accessor.</li>
   * </ul>
   *
   * <p>The pass is wrapped in {@code executeInsideAtomicOperation} to match the
   * catalogue-load idiom used by the surrounding {@code open()} sites. Each per-component
   * helper is silent on the clean case (it dispatches {@code shrinkFile} unconditionally;
   * the cache layer's pre-flight no-ops when {@code physical <= target}) and emits a
   * one-line WARN log per affected file from inside the {@code WOWCache.shrinkFile} body
   * when an actual truncate fires.
   *
   * <p>Per-component failure handling: each per-component dispatch is wrapped in a
   * try/catch that absorbs {@link StorageException} and {@link IOException}, logs a
   * one-line WARN naming the component and fileId, and continues with the next component.
   * A failure in one component (e.g., a corrupted EP page surfaced by
   * {@code checksumMode=StoreAndThrow}, or a missing-file entry that
   * {@link com.jetbrains.youtrackdb.internal.core.storage.cache.local.WOWCache#shrinkFile
   * WOWCache.shrinkFile} now signals loudly) must not poison recovery for the other
   * components — the orphan-truncation pass is best-effort and the affected component
   * remains recoverable via the existing storage-corruption runbook. The pass therefore
   * does not throw out of any per-component group; only programming errors
   * ({@link RuntimeException} subclasses that are not {@code StorageException}) escape.
   *
   * <p>The orchestrator runs after {@code recoverIfNeeded()} (which drains the flush executor
   * via {@code flushAllData()} on the dirty-reopen path) so dirty WAL-replay pages have
   * settled before any truncate fires. It also runs strictly AFTER {@code openCollections}
   * and {@code openIndexes} populate the {@code collections} and {@code indexEngines}
   * catalogues that Groups 1 and 2 iterate — running the pass earlier would observe empty
   * catalogues and silently skip every per-component dispatch. EP-less components
   * ({@code FreeSpaceMap}, {@code CollectionDirtyPageBitSet}) and
   * {@code IndexHistogramManager} are deliberately out of scope — their growth loops
   * compute physical horizons from the allocator, so physical-orphan pages past their
   * logical horizon are structurally invisible to them.
   *
   * <p>Each per-component helper reads only the EP page (already checksum-valid from the
   * production write path) and dispatches the truncate through the cache layer; the orphan
   * pages themselves are never read during the pass, so a checksum-corrupted orphan body
   * cannot abort recovery.
   *
   * @param atomicOperation the enclosing recovery-pass atomic operation supplied by
   *     {@code executeInsideAtomicOperation}
   * @throws IOException only on unrecoverable infrastructure failures outside any
   *     per-component scope (today: never).
   */
  protected void truncateOrphansAfterRecovery(final AtomicOperation atomicOperation)
      throws IOException {
    // Test-observability only: record that the pass was dispatched (see the field's Javadoc).
    // Null-guarded because a Mockito CALLS_REAL_METHODS mock of this class skips field
    // initializers, leaving the counter null; on a real storage instance it is always set.
    if (orphanTruncationDispatchCountForTests != null) {
      orphanTruncationDispatchCountForTests.incrementAndGet();
    }

    // Group 1: paginated collections. Each non-null entry truncates the collection's own
    // .pcl data file; the PCV2 helper's siblings hook (verifyAndTruncateOrphansSiblings)
    // then internally truncates the embedded position map's .cpm file.
    for (final var collection : collections) {
      if (collection instanceof PaginatedCollectionV2 pcv2) {
        try {
          pcv2.verifyAndTruncateOrphans(atomicOperation, readCache, writeCache);
        } catch (final StorageException | IOException e) {
          // Best-effort recovery: log and continue so a single corrupted component does
          // not block orphan truncation for the rest of the storage. The affected
          // component is left in whatever shape WAL replay produced; operators handle
          // it via the storage-corruption runbook.
          LogManager.instance()
              .warn(
                  this,
                  String.format(
                      "Orphan-truncation skipped for PaginatedCollectionV2 '%s'"
                          + " (fileId=%d): %s",
                      pcv2.getName(), pcv2.getFileId(), e.getMessage()));
        }
      }
    }

    // Group 2: B-tree index engines. Filter to the engine classes whose underlying
    // CellBTreeSingleValue trees carry the EP shape this pass targets; other engine
    // classes (e.g., hash-index, legacy variants) either have no EP-vs-physical drift
    // or no public orphan-truncation hook.
    for (final var engine : indexEngines) {
      if (engine instanceof BTreeSingleValueIndexEngine svEngine) {
        try {
          svEngine.verifyAndTruncateOrphans(atomicOperation, readCache, writeCache);
        } catch (final StorageException | IOException e) {
          LogManager.instance()
              .warn(
                  this,
                  String.format(
                      "Orphan-truncation skipped for BTreeSingleValueIndexEngine: %s",
                      e.getMessage()));
        }
      } else if (engine instanceof BTreeMultiValueIndexEngine mvEngine) {
        try {
          mvEngine.verifyAndTruncateOrphans(atomicOperation, readCache, writeCache);
        } catch (final StorageException | IOException e) {
          LogManager.instance()
              .warn(
                  this,
                  String.format(
                      "Orphan-truncation skipped for BTreeMultiValueIndexEngine: %s",
                      e.getMessage()));
        }
      }
    }

    // Group 3: shared link-bag B-trees. Iteration is internal to the manager so the
    // manager's private fileIdBTreeMap stays encapsulated; per-SLBB best-effort handling
    // (try/catch + WARN log + continue) lives inside the manager's iteration so the call
    // here neither throws nor needs an enclosing catch.
    linkCollectionsBTreeManager.verifyAndTruncateAllOrphans(
        atomicOperation, readCache, writeCache);
  }

  @Override
  public void create(final ContextConfiguration contextConfiguration) {

    try {
      stateLock.writeLock().lock();
      try {
        doCreate(contextConfiguration);
      } catch (final java.lang.InterruptedException e) {
        throw BaseException.wrapException(
            new StorageException(name, "Storage creation was interrupted"), e, name);
      } catch (final StorageException e) {
        closeIfPossible();
        throw e;
      } catch (final IOException e) {
        closeIfPossible();
        throw BaseException.wrapException(
            new StorageException(name, "Error on creation of storage '" + name + "'"), e, name);
      } finally {
        stateLock.writeLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
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

  protected void doCreate(ContextConfiguration contextConfiguration)
      throws IOException, java.lang.InterruptedException {
    checkPageSizeAndRelatedParametersInGlobalConfiguration(name);

    if (name == null) {
      throw new InvalidDatabaseNameException("Database name can not be null");
    }

    if (name.isEmpty()) {
      throw new InvalidDatabaseNameException("Database name can not be empty");
    }

    final var namePattern = Pattern.compile("[^\\w$_-]+");
    final var matcher = namePattern.matcher(name);
    if (matcher.find()) {
      throw new InvalidDatabaseNameException(
          "Only letters, numbers, `$`, `_` and `-` are allowed in database name. Provided name :`"
              + name
              + "`");
    }

    if (status != STATUS.CLOSED) {
      throw new StorageExistsException(name,
          "Cannot create new storage '" + getURL() + "' because it is not closed");
    }

    if (existsBeforeCreation()) {
      throw new StorageExistsException(name,
          "Cannot create new storage '" + getURL() + "' because it already exists");
    }

    prepareStorageBirth();
    uuid = UUID.randomUUID();
    initIv();

    initWalAndDiskCache(contextConfiguration);

    // Register all PageOperation types — symmetric with open() path.
    PageOperationRegistry.registerAll(WALRecordsFactory.INSTANCE);

    atomicOperationsTable =
        new AtomicOperationsTable(
            contextConfiguration.getValueAsInteger(
                GlobalConfiguration.STORAGE_ATOMIC_OPERATIONS_TABLE_COMPACTION_LIMIT),
            idGen.getLastId() + 1);
    atomicOperationsManager = new AtomicOperationsManager(this, atomicOperationsTable);

    preCreateSteps();
    makeStorageDirty();

    atomicOperationsManager.executeInsideAtomicOperation(
        (atomicOperation) -> {
          configuration = new CollectionBasedStorageConfiguration(this);
          ((CollectionBasedStorageConfiguration) configuration)
              .create(atomicOperation, contextConfiguration);
          configuration.setUuid(atomicOperation, uuid.toString());

          var binaryFormatVersion = atomicOperationsManager.calculateInsideAtomicOperation(
              configuration::getBinaryFormatVersion);
          componentsFactory = new CurrentStorageComponentsFactory(binaryFormatVersion);

          linkCollectionsBTreeManager.load(atomicOperation);

          status = STATUS.OPEN;
          startStaleTransactionMonitor();

          linkCollectionsBTreeManager = new LinkCollectionsBTreeManagerShared(this);

          // ADD THE METADATA COLLECTION TO STORE INTERNAL STUFF
          doAddCollection(atomicOperation, MetadataDefault.COLLECTION_INTERNAL_NAME);

          // The $blob<i> collections are storage-birth system collections: they are created
          // here, inside the same WAL atomic operation as the `internal` collection, so they
          // are atomic with storage creation (a crashed create WAL-reverts them together with
          // everything else). The count is read exactly ONCE, from the create-time context
          // configuration — it is a storage-birth property frozen at create; a later re-read
          // could observe a different process-global value and diverge from what was actually
          // created. SharedContext.create() registers the resulting collections in the schema
          // by enumerating the actual $blob* collections by name, never re-reading this value.
          var blobCollectionsCount =
              contextConfiguration.getValueAsInteger(
                  GlobalConfiguration.STORAGE_BLOB_COLLECTIONS_COUNT);
          // A negative count is a misconfiguration that would otherwise silently produce a
          // database that can never store blobs (failing only at the first blob save with a
          // message that never names this knob) — and the value is frozen for the database's
          // lifetime, so it must be rejected loudly here at create time. Zero stays allowed:
          // a deliberately blob-less database is a valid configuration. The throw propagates
          // out of the atomic operation, so the whole create rolls back.
          if (blobCollectionsCount < 0) {
            throw new StorageException(name,
                "Invalid value "
                    + blobCollectionsCount
                    + " of configuration parameter '"
                    + GlobalConfiguration.STORAGE_BLOB_COLLECTIONS_COUNT.getKey()
                    + "': the blob-collections count of a storage can not be negative");
          }
          for (var i = 0; i < blobCollectionsCount; i++) {
            doAddCollection(atomicOperation, MetadataDefault.BLOB_COLLECTION_NAME_PREFIX + i);
          }

          // Preserve established blob collection identifiers. Storage creation still completes
          // before genesis creates security indexes, so the build state collection is ready.
          doAddCollection(atomicOperation, MetadataDefault.INDEX_BUILD_STATE_COLLECTION_NAME);

          ((CollectionBasedStorageConfiguration) configuration)
              .setCreationVersion(atomicOperation, YouTrackDBConstants.getVersion());
          ((CollectionBasedStorageConfiguration) configuration)
              .setPageSize(
                  atomicOperation,
                  GlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() << 10);
          ((CollectionBasedStorageConfiguration) configuration)
              .setMaxKeySize(
                  atomicOperation, GlobalConfiguration.BTREE_MAX_KEY_SIZE.getValueAsInteger());

          generateDatabaseInstanceId(atomicOperation);
          clearStorageDirty();
          postCreateSteps();
        });

    // Track 24 delays the publication of the active lifecycle state. Storage creation builds the
    // storage files only. Genesis creates the initial schema, the index manager, the index
    // statistics, and the default users after this method returns. The creation path calls
    // completeStorageBirth after genesis, so a crash inside genesis leaves an inadmissible image.
  }

  /**
   * Completes storage birth after genesis finished.
   *
   * <p>Genesis is the creation of the initial database metadata. Genesis creates the schema, the
   * index manager, the index statistics, and the default users. Genesis completes inside the
   * database session creation path of the embedded factory.
   *
   * <p>This method runs the durability barrier over every genesis artifact first. This method
   * publishes the active lifecycle state afterwards. A crash before this method therefore leaves
   * an image that storage admission rejects with the interrupted-birth reason.
   */
  public final void completeStorageBirth() {
    final var observer = SCOPED_BIRTH_COMPLETION_OBSERVER.get();
    if (observer != null) {
      observer.afterGenesisBeforeBarrier(this);
    }
    barrierOverGenesisArtifacts();
    if (observer != null) {
      observer.afterBarrierBeforeActivation(this);
    }
    activateStorageBirth();
  }

  /**
   * Observes the two boundaries inside {@link #completeStorageBirth()}.
   *
   * <p>This interface is a test seam. A test uses this seam to build the durable image of a crash
   * after genesis and before the durability barrier. A test also uses this seam to build the
   * durable image of a crash after the durability barrier and before the activation. Production
   * code installs no observer, so production code runs two null checks per creation.
   */
  interface BirthCompletionObserver {

    /** Runs after genesis finished and before the durability barrier starts. */
    void afterGenesisBeforeBarrier(AbstractStorage storage);

    /** Runs after the durability barrier finished and before the activation starts. */
    void afterBarrierBeforeActivation(AbstractStorage storage);
  }

  private static final ThreadLocal<BirthCompletionObserver> SCOPED_BIRTH_COMPLETION_OBSERVER =
      new ThreadLocal<>();

  /** Installs one birth completion observer on the current thread until the scope closes. */
  static AutoCloseable useBirthCompletionObserverForCurrentThread(
      final BirthCompletionObserver observer) {
    final var previous = SCOPED_BIRTH_COMPLETION_OBSERVER.get();
    SCOPED_BIRTH_COMPLETION_OBSERVER.set(Objects.requireNonNull(observer, "observer"));
    return () -> {
      if (previous == null) {
        SCOPED_BIRTH_COMPLETION_OBSERVER.remove();
      } else {
        SCOPED_BIRTH_COMPLETION_OBSERVER.set(previous);
      }
    };
  }

  /**
   * Forces every genesis artifact to durable media.
   *
   * <p>The default implementation performs no work. Memory storage keeps that default, because
   * Track 24 changes disk storage admission only. Memory storage therefore pays no extra flush
   * during creation.
   */
  protected void barrierOverGenesisArtifacts() {
  }

  /**
   * Publishes the restore-in-progress lifecycle state of a fresh restore target.
   *
   * <p>Restore creates the restore target without genesis and without activation. Genesis is the
   * creation of the initial database metadata. The restore path calls this method directly after
   * the creation of the storage files, so a restore target never appears as an empty active
   * database.
   *
   * <p>The default implementation performs no work, because Track 24 changes disk storage
   * admission only. Memory storage supports no restore at all.
   */
  public void beginRestoreLifecycle() {
  }

  /**
   * Forces every durable artifact of this storage to durable media.
   *
   * <p>This method runs the same two steps as the public synchronization entry point. The first
   * step flushes dirty index histogram data. An index histogram is index statistics data that the
   * index engine keeps in a separate file. The second step flushes every index engine, the
   * write-ahead log, and the write cache.
   *
   * <p>This method takes no state lock, so the caller must already own the storage state lock. The
   * state lock of this storage is not reentrant, so the public entry point would deadlock a caller
   * that already owns the write lock.
   *
   * <p>This method fails closed. The public synchronization entry point skips every flush while
   * the storage sits in the internal error state. That entry point also swallows a failure of one
   * index histogram flush and a failure of one index engine flush. This method reports every such
   * case to the caller instead. The caller therefore never publishes the active lifecycle state
   * over content that never reached durable media.
   */
  protected final void barrierWhileCallerOwnsStateLock() {
    requireNoErrorStateForBarrier();
    flushDirtyHistograms(true);
    doSynch(true);
    // A flush can move this storage into the error state without throwing. One example is a page
    // checksum failure under the read-only error mode. The second check therefore repeats.
    requireNoErrorStateForBarrier();
  }

  /**
   * Runs the durability barrier of this storage under the state read lock of this storage.
   *
   * <p>The creation path owns no state lock when the path completes storage birth, so the barrier
   * of the creation path takes the read lock itself. The barrier fails closed, so a failed flush
   * reaches the creation path and the creation path publishes no active lifecycle state.
   */
  protected final void barrierWithOwnStateLock() {
    stateLock.readLock().lock();
    try {
      barrierWhileCallerOwnsStateLock();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Fails the durability barrier when this storage sits in the internal error state.
   *
   * <p>A storage in the internal error state flushes nothing. A barrier over such a storage would
   * therefore report success without any durable write. The barrier reports the error state to the
   * caller instead, and the caller keeps the storage out of the active lifecycle state.
   */
  private void requireNoErrorStateForBarrier() {
    final var storageError = error.get();
    if (storageError == null) {
      return;
    }
    throw BaseException.wrapException(
        new StorageException(
            name,
            "The durability barrier of database '"
                + name
                + "' failed, because the storage reached an internal error state"),
        storageError,
        name);
  }

  /** Reads durable identity before write-ahead log processing starts. */
  protected void readStorageIdentity() {
  }

  /** Publishes durable birth before write-ahead log initialization starts. */
  protected void prepareStorageBirth() {
  }

  /**
   * Activates durable birth after genesis finished and after the durability barrier completed.
   *
   * <p>Only {@link #completeStorageBirth()} calls this method on the creation path.
   */
  protected void activateStorageBirth() {
  }

  protected final void updateStorageIdentity(
      StorageIdentity storageIdentity, StorageLineageIdentity lineageIdentity) {
    storageIdentityHolder.update(storageIdentity, lineageIdentity);
  }

  public final StorageIdentity getStorageIdentity() {
    return storageIdentityHolder.storageIdentity();
  }

  public final StorageLineageIdentity getStorageLineageIdentity() {
    return storageIdentityHolder.lineageIdentity();
  }

  protected final void dropStaleIndexLifecycles() {
    indexLifecycleRegistry.dropStale();
  }

  protected void generateDatabaseInstanceId(AtomicOperation atomicOperation) {
    ((CollectionBasedStorageConfiguration) configuration)
        .setProperty(atomicOperation, DATABASE_INSTANCE_ID, UUID.randomUUID().toString());
  }

  @Nullable protected UUID readDatabaseInstanceId() {
    var id = configuration.getProperty(DATABASE_INSTANCE_ID);
    if (id != null) {
      return UUID.fromString(id);
    } else {
      return null;
    }
  }

  @SuppressWarnings("unused")
  protected void checkDatabaseInstanceId(UUID backupUUID) {
    var dbUUID = readDatabaseInstanceId();
    if (backupUUID == null) {
      throw new InvalidInstanceIdException(name,
          "The Database Instance Id do not mach, backup UUID is null");
    }
    if (dbUUID != null) {
      if (!dbUUID.equals(backupUUID)) {
        throw new InvalidInstanceIdException(name,
            String.format(
                "The Database Instance Id do not mach, database: '%s' backup: '%s'",
                dbUUID, backupUUID));
      }
    }
  }

  protected abstract void initIv() throws IOException;

  private void checkPageSizeAndRelatedParameters(AtomicOperation atomicOperation) {
    final var pageSize = GlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() << 10;
    final var maxKeySize = GlobalConfiguration.BTREE_MAX_KEY_SIZE.getValueAsInteger();

    if (configuration.getPageSize(atomicOperation) != -1
        && configuration.getPageSize(atomicOperation) != pageSize) {
      throw new StorageException(name,
          "Storage is created with value of "
              + configuration.getPageSize(atomicOperation)
              + " parameter equal to "
              + GlobalConfiguration.DISK_CACHE_PAGE_SIZE.getKey()
              + " but current value is "
              + pageSize);
    }

    if (configuration.getMaxKeySize(atomicOperation) != -1
        && configuration.getMaxKeySize(atomicOperation) != maxKeySize) {
      throw new StorageException(name,
          "Storage is created with value of "
              + configuration.getMaxKeySize(atomicOperation)
              + " parameter equal to "
              + GlobalConfiguration.BTREE_MAX_KEY_SIZE.getKey()
              + " but current value is "
              + maxKeySize);
    }
  }

  @Override
  public final boolean isClosed(DatabaseSessionEmbedded database) {
    try {
      // The schema-carry commit reaches this through the tx-local schema serialization: deleting a
      // dropped class's record runs the link-consistency pre-update path, which logs via
      // EntityImpl.toString() -> session.isClosed(). That runs while the commit holds
      // stateLock.writeLock(), so re-acquiring the read lock busy-spins forever on the non-reentrant
      // ScalableRWLock. The commit window self-routes to the plain status read; the held write lock
      // already proves the storage is open and supplies the visibility edge.
      final boolean lockFree = isCommitWindowActive();
      if (!lockFree) {
        stateLock.readLock().lock();
      }
      try {
        return isClosedInternal();
      } finally {
        if (!lockFree) {
          stateLock.readLock().unlock();
        }
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  protected final boolean isClosedInternal() {
    return status == STATUS.CLOSED;
  }

  @Override
  public final void close(DatabaseSessionEmbedded database, final boolean force) {
    try {
      if (!force) {
        close(database);
        return;
      }

      doShutdown();
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final void delete() {
    try {
      dropDuration.timed(() -> {
        stateLock.writeLock().lock();
        try {
          doDelete();
        } finally {
          stateLock.writeLock().unlock();
        }
      });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void doDelete() throws IOException {
    // A never-opened storage (a crash corpse whose open fails, re-initialized by drop() purely
    // for deletion) has no startup-metadata channel to write the dirty flag to — and needs
    // none: the flag exists to force recovery on a crashed WRITER, and deletion removes the
    // files wholesale. Skipping it makes drop() the universal discard for W1/W2-class corpses
    // instead of failing on the missing channel before any file is removed (review CS54).
    if (status != STATUS.CLOSED) {
      makeStorageDirty();
    }

    // CLOSE THE DATABASE BY REMOVING THE CURRENT USER
    doShutdownOnDelete();
    postDeleteSteps();
  }

  @Override
  public final int addCollection(DatabaseSessionEmbedded database, final String collectionName,
      final Object... parameters) {
    try {
      stateLock.writeLock().lock();
      try {
        if (collectionMap.containsKey(collectionName)) {
          throw new ConfigurationException(
              database.getDatabaseName(),
              String.format("Collection with name:'%s' already exists", collectionName));
        }
        checkOpennessAndMigration();

        makeStorageDirty();
        return atomicOperationsManager.calculateInsideAtomicOperation(
            (atomicOperation) -> doAddCollection(atomicOperation, collectionName));
      } catch (final IOException e) {
        throw BaseException.wrapException(
            new StorageException(name, "Error in creation of new collection '" + collectionName), e,
            name);
      } finally {
        stateLock.writeLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final int addCollection(DatabaseSessionEmbedded database, final String collectionName,
      final int requestedId) {
    try {
      stateLock.writeLock().lock();
      try {
        checkOpennessAndMigration();

        if (requestedId < 0) {
          throw new ConfigurationException(database.getDatabaseName(),
              "Collection id must be positive!");
        }
        if (requestedId < collections.size() && collections.get(requestedId) != null) {
          throw new ConfigurationException(
              database.getDatabaseName(), "Requested collection ID ["
                  + requestedId
                  + "] is occupied by collection with name ["
                  + collections.get(requestedId).getName()
                  + "]");
        }
        if (collectionMap.containsKey(collectionName)) {
          throw new ConfigurationException(
              database.getDatabaseName(),
              String.format("Collection with name:'%s' already exists", collectionName));
        }

        makeStorageDirty();
        return atomicOperationsManager.calculateInsideAtomicOperation(
            atomicOperation -> doAddCollection(atomicOperation, collectionName, requestedId));

      } catch (final IOException e) {
        throw BaseException.wrapException(
            new StorageException(name,
                "Error in creation of new collection '" + collectionName + "'"),
            e,
            name);
      } finally {
        stateLock.writeLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final boolean dropCollection(DatabaseSessionEmbedded database, final int collectionId) {
    try {
      stateLock.writeLock().lock();
      try {
        checkOpennessAndMigration();
        if (collectionId < 0 || collectionId >= collections.size()) {
          throw new IllegalArgumentException(
              "Collection id '"
                  + collectionId
                  + "' is outside the of range of configured collections (0-"
                  + (collections.size() - 1)
                  + ") in database '"
                  + name
                  + "'");
        }

        makeStorageDirty();

        return atomicOperationsManager.calculateInsideAtomicOperation(
            atomicOperation -> {
              if (dropCollectionInternal(atomicOperation, collectionId)) {
                return false;
              }

              ((CollectionBasedStorageConfiguration) configuration)
                  .dropCollection(atomicOperation, collectionId);
              linkCollectionsBTreeManager.deleteComponentByCollectionId(atomicOperation,
                  collectionId);

              return true;
            });
      } catch (final Exception e) {
        throw BaseException.wrapException(
            new StorageException(name, "Error while removing collection '" + collectionId + "'"), e,
            name);

      } finally {
        stateLock.writeLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void checkCollectionId(int collectionId) {
    if (collectionId < 0 || collectionId >= collections.size()) {
      throw new CollectionDoesNotExistException(name,
          "Collection id '"
              + collectionId
              + "' is outside the of range of configured collections (0-"
              + (collections.size() - 1)
              + ") in database '"
              + name
              + "'");
    }
  }

  @Override
  public String getCollectionNameById(int collectionId) {
    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();

        checkCollectionId(collectionId);
        final var collection = collections.get(collectionId);
        if (collection == null) {
          throwCollectionDoesNotExist(collectionId);
        }

        return collection.getName();
      } finally {
        stateLock.readLock().unlock();
      }

    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public String getCollectionRecordConflictStrategy(int collectionId) {
    try {
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        checkCollectionId(collectionId);
        final var collection = collections.get(collectionId);
        if (collection == null) {
          throwCollectionDoesNotExist(collectionId);
        }

        return Optional.ofNullable(collection.getRecordConflictStrategy())
            .map(RecordConflictStrategy::getName)
            .orElse(null);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public boolean isSystemCollection(int collectionId) {
    try {
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        checkCollectionId(collectionId);
        final var collection = collections.get(collectionId);
        if (collection == null) {
          throwCollectionDoesNotExist(collectionId);
        }

        return collection.isSystemCollection();
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private void throwCollectionDoesNotExist(int collectionId) {
    throw new CollectionDoesNotExistException(name,
        "Collection with id " + collectionId + " does not exist inside of storage " + name);
  }

  @Override
  public final int getId() {
    return id;
  }

  public UUID getUuid() {
    return uuid;
  }

  @Override
  public final LinkCollectionsBTreeManager getLinkCollectionsBtreeCollectionManager() {
    return linkCollectionsBTreeManager;
  }

  public ReadCache getReadCache() {
    return readCache;
  }

  public WriteCache getWriteCache() {
    return writeCache;
  }

  @Override
  public final long count(DatabaseSessionEmbedded session, final int iCollectionId) {
    return count(session, iCollectionId, false);
  }

  @Override
  public final long count(DatabaseSessionEmbedded session, final int collectionId,
      final boolean countTombstones) {
    try {
      if (collectionId == -1) {
        throw new StorageException(name,
            "Collection Id " + collectionId + " is invalid in database '" + name + "'");
      }

      var transaction = session.getActiveTransaction();
      var atomicOperation = transaction.getAtomicOperation();
      // COUNT PHYSICAL COLLECTION IF ANY
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        final var collection = collections.get(collectionId);
        if (collection == null) {
          return 0;
        }

        if (countTombstones) {
          return collection.getEntries(atomicOperation);
        }

        return collection.getEntries(atomicOperation) - collection.getTombstonesCount();
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public final long getApproximateRecordsCount(final int collectionId) {
    try {
      if (collectionId < 0) {
        throw new StorageException(name,
            "Collection Id " + collectionId + " is invalid in database '" + name + "'");
      }

      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();

        if (collectionId >= collections.size() || collections.get(collectionId) == null) {
          throw new StorageException(name,
              "Collection with id " + collectionId + " does not exist in database '"
                  + name + "'");
        }

        return collections.get(collectionId).getApproximateRecordsCount();
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  /**
   * The lock-free commit-window analogue of {@link #getApproximateRecordsCount(int)}: reads a
   * collection's approximate record count without taking {@code stateLock}, because the schema-carry
   * commit already holds {@code stateLock.writeLock()} and re-acquiring the non-reentrant read lock
   * would busy-spin. The commit-time index build uses it to enforce the v1 empty-source-collection
   * bound. Mirrors the lock-free {@link #getIndexEngineWithStateLock(int)} precondition: the caller
   * MUST hold
   * {@code stateLock.writeLock()} with the commit window open, which supplies the exclusion and the
   * happens-before edge for the plain-list read.
   *
   * @param collectionId a real (>= 0) collection id.
   * @return the collection's approximate record count, or 0 when the id names no live collection.
   */
  public final long getApproximateRecordsCountInCommitWindow(final int collectionId) {
    assert isCommitWindowActive()
        : "commit-window primitive called outside the commit window (stateLock.writeLock() not"
            + " held)";
    if (collectionId < 0
        || collectionId >= collections.size()
        || collections.get(collectionId) == null) {
      return 0;
    }
    return collections.get(collectionId).getApproximateRecordsCount();
  }

  /**
   * The lock-free commit-window analogue of {@link StorageCollection#getEntries(AtomicOperation)}:
   * reads a collection's <em>exact</em> record count without taking {@code stateLock}, because the
   * schema-carry commit already holds {@code stateLock.writeLock()} and re-acquiring the non-reentrant
   * read lock would busy-spin. The commit-time index build uses it to confirm an approximate-zero
   * count before it relies on the v1 empty-source-collection bound, closing the under-report hole
   * where a stale approximate zero would let the build skip committed rows. Exact but cheap on the
   * empty collection the caller has already pre-checked as approximately empty. Mirrors the lock-free
   * {@link #getIndexEngineWithStateLock(int)} precondition: the caller MUST hold
   * {@code stateLock.writeLock()} with
   * the commit window open, which supplies the exclusion and the happens-before edge.
   *
   * @param collectionId    a real (>= 0) collection id.
   * @param atomicOperation the in-flight commit atomic operation the count reads through.
   * @return the collection's exact record count, or 0 when the id names no live collection.
   */
  public final long getExactRecordsCountInCommitWindow(
      final int collectionId, final AtomicOperation atomicOperation) {
    assert isCommitWindowActive()
        : "commit-window primitive called outside the commit window (stateLock.writeLock() not"
            + " held)";
    if (collectionId < 0
        || collectionId >= collections.size()
        || collections.get(collectionId) == null) {
      return 0;
    }
    return collections.get(collectionId).getEntries(atomicOperation);
  }

  @Override
  public final long count(DatabaseSessionEmbedded session, final int[] iCollectionIds) {
    return count(session, iCollectionIds, false);
  }

  @Override
  public final void onException(final Throwable e) {

    LogManager.instance()
        .error(
            this,
            "Error in data flush background thread, for storage %s ,"
                + "please restart database and send full stack trace inside of bug report",
            e,
            name);

    if (status == STATUS.CLOSED) {
      return;
    }

    if (!(e instanceof InternalErrorException)) {
      setInError(e);
    }

    try {
      makeStorageDirty();
    } catch (IOException ioException) {
      // ignore
    }
  }

  private void setInError(final Throwable e) {
    // Defense-in-depth: skip AssertionError so a stray dev/test invariant violation
    // does not flip the storage into permanent error state. Safe because (i) the JVM
    // default is -ea OFF, so production paths never throw asserts; (ii) the
    // broadened catch at commit() and the broadened catches in the four
    // AtomicOperationsManager wrappers convert lambda-body and inner-try
    // AssertionErrors into StorageException at the source; (iii) the
    // clamp+error rewrite of the four engine-level addToApproximate{Entries,Null}Count
    // mutators prevents the in-memory underflow from throwing at all. Any AssertionError
    // that still reaches this setter will be logged and rethrown by the surrounding
    // logAndPrepareForRethrow(...) call; the guard only suppresses the read-only-mode
    // flip. Other Error subclasses (OutOfMemoryError, StackOverflowError, LinkageError)
    // still trigger error state.
    if (e instanceof AssertionError) {
      return;
    }
    error.set(e);
    // Postcondition: the AssertionError guard above is the only path that can
    // leave error.get() != e for a non-null AssertionError argument. A future
    // refactor that reordered or removed the guard would surface here (with
    // -ea on, i.e. the test JVM default). Production has -ea off, so this is
    // free in shipped binaries.
    assert !(e instanceof AssertionError) || error.get() != e
        : "setInError must skip AssertionError; reached the setter with " + e;
  }

  @Override
  public final long count(DatabaseSessionEmbedded session, final int[] iCollectionIds,
      final boolean countTombstones) {
    try {
      long tot = 0;

      var transaction = session.getActiveTransaction();
      var atomicOperation = transaction.getAtomicOperation();
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        for (final var iCollectionId : iCollectionIds) {
          if (iCollectionId >= collections.size()) {
            throw new ConfigurationException(
                session.getDatabaseName(),
                "Collection id " + iCollectionId + " was not found in database '" + name + "'");
          }

          if (iCollectionId > -1) {
            final var c = collections.get(iCollectionId);
            if (c != null) {
              tot +=
                  c.getEntries(atomicOperation) - (countTombstones ? 0L : c.getTombstonesCount());
            }
          }
        }

        return tot;
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Nullable @Override
  public final RecordMetadata getRecordMetadata(DatabaseSessionEmbedded session,
      final RID rid) {
    try {
      if (rid.isNew()) {
        throw new StorageException(name,
            "Passed record with id " + rid + " is new and cannot be stored.");
      }

      stateLock.readLock().lock();
      try {

        final var collection = doGetAndCheckCollection(rid.getCollectionId());
        checkOpennessAndMigration();

        var atomicOperation = session.getActiveTransaction().getAtomicOperation();
        final var ppos =
            collection.getPhysicalPosition(new PhysicalPosition(rid.getCollectionPosition()),
                atomicOperation);
        if (ppos == null) {
          return null;
        }

        return new RecordMetadata(rid, ppos.recordVersion);
      } catch (final IOException ioe) {
        LogManager.instance()
            .error(this, "Retrieval of record  '" + rid + "' cause: " + ioe.getMessage(), ioe);
      } finally {
        stateLock.readLock().unlock();
      }

      return null;
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  public Iterator<CollectionBrowsePage> browseCollection(
      final int collectionId,
      final boolean forward,
      AtomicOperation atomicOperation) {
    return browseCollection(collectionId, forward, () -> atomicOperation);
  }

  public Iterator<CollectionBrowsePage> browseCollection(
      final int collectionId,
      final boolean forward,
      Supplier<AtomicOperation> atomicOperationSupplier) {
    try {
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        if (collectionId == RID.COLLECTION_ID_INVALID) {
          // GET THE DEFAULT COLLECTION
          throw new StorageException(name, "Collection Id " + collectionId + " is invalid");
        }
        return new Iterator<>() {
          @Nullable private CollectionBrowsePage page;
          private long lastPos = forward ? -1 : Long.MAX_VALUE;

          @Override
          public boolean hasNext() {
            if (page == null) {
              page = nextPage(collectionId, lastPos, forward,
                  atomicOperationSupplier.get());
              if (page != null) {
                lastPos = page.getLastPosition();
              }
            }
            return page != null;
          }

          @Override
          public CollectionBrowsePage next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            final var curPage = page;
            page = null;
            return curPage;
          }
        };
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private CollectionBrowsePage nextPage(
      final int collectionId,
      final long lastPosition,
      final boolean forward,
      AtomicOperation atomicOperation) {
    try {
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        final var collection = doGetAndCheckCollection(collectionId);
        return collection.nextPage(lastPosition, forward, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private StorageCollection doGetAndCheckCollection(final int collectionId) {
    checkCollectionSegmentIndexRange(collectionId);

    final var collection = collections.get(collectionId);
    if (collection == null) {
      throw new IllegalArgumentException("Collection " + collectionId + " is null");
    }
    return collection;
  }

  @Override
  public @Nonnull StorageReadResult readRecord(final RecordIdInternal rid,
      @Nonnull AtomicOperation atomicOperation) {
    try {
      return readRecordInternal(rid, atomicOperation);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  public final AtomicOperationsManager getAtomicOperationsManager() {
    return atomicOperationsManager;
  }

  @Nonnull
  public WriteAheadLog getWALInstance() {
    return writeAheadLog;
  }

  public AtomicOperationIdGen getIdGen() {
    return idGen;
  }

  @Override
  public final Set<String> getCollectionNames() {
    try {
      // The schema-carry commit can reach this through the tx-local schema fromStream promotion
      // (the collection-counter init fallback enumerates existing collection names) while holding
      // stateLock.writeLock(). The commit window self-routes to the lock-free read; the held write
      // lock supplies the exclusion and the visibility edge for collectionMap.
      final boolean lockFree = isCommitWindowActive();
      if (!lockFree) {
        stateLock.readLock().lock();
      }
      try {

        checkOpennessAndMigration();

        // CN60 (Track 8 cumulative review): COPY under the lock — the previously returned
        // live keySet view escaped the lock, and callers (the exporter above all) iterated
        // it unlocked while concurrent DDL commits mutate the backing map under the write
        // lock: JMM-undefined — a CME at best, silently skipped entries at worst (an exit-0
        // export missing whole collections). The copy iterates while the lock (or the
        // commit-window write lock) is held.
        return Set.copyOf(collectionMap.keySet());
      } finally {
        if (!lockFree) {
          stateLock.readLock().unlock();
        }
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public final int getCollectionIdByName(final String collectionName) {
    return getCollectionIdByName(collectionName, isCommitWindowActive());
  }

  /** Resolves a collection without reacquiring state protection already held by this thread. */
  final int getCollectionIdByNameWithStateLock(final String collectionName) {
    return getCollectionIdByName(collectionName, callerOwnsStateProtection());
  }

  private int getCollectionIdByName(final String collectionName, final boolean lockFree) {
    try {
      if (collectionName == null) {
        throw new IllegalArgumentException("Collection name is null");
      }

      if (collectionName.isEmpty()) {
        throw new IllegalArgumentException("Collection name is empty");
      }

      // Protected callers already have the exclusion and visibility edge for collectionMap.
      // Other callers acquire read mode around the lookup.
      if (!lockFree) {
        stateLock.readLock().lock();
      }
      try {
        checkOpennessAndMigration();

        final var segment = collectionMap.get(collectionName.toLowerCase(Locale.ROOT));
        if (segment != null) {
          return segment.getId();
        }

        return -1;
      } finally {
        if (!lockFree) {
          stateLock.readLock().unlock();
        }
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  /**
   * Scan the given transaction for new record and allocate a record id for them, the relative
   * record id is inserted inside the transaction for future use.
   *
   * @param clientTx the transaction of witch allocate rids
   */
  public void preallocateRids(final FrontendTransaction clientTx) {
    try {
      final Iterable<RecordOperation> entries = clientTx.getRecordOperationsInternal();
      final var collectionsToLock = new TreeMap<Integer, StorageCollection>();

      final Set<RecordOperation> newRecords = new TreeSet<>(COMMIT_RECORD_OPERATION_COMPARATOR);

      for (final var txEntry : entries) {

        if (txEntry.type == RecordOperation.CREATED) {
          newRecords.add(txEntry);
          final var collectionId = txEntry.getRecordId().getCollectionId();
          collectionsToLock.put(collectionId, doGetAndCheckCollection(collectionId));
        }
      }
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        makeStorageDirty();
        atomicOperationsManager.executeInsideAtomicOperation(
            atomicOperation -> {
              lockCollections(collectionsToLock, atomicOperation);

              var session = clientTx.getDatabaseSession();
              for (final var txEntry : newRecords) {
                final var rec = txEntry.record;
                if (!rec.getIdentity().isPersistent()) {
                  if (rec.isDirty()) {
                    // This allocate a position for a new record
                    final var rid = rec.getIdentity();
                    final var oldRID = rid.copy();
                    final var collection = doGetAndCheckCollection(rid.getCollectionId());
                    final var ppos =
                        collection.allocatePosition(
                            rec.getRecordType(), atomicOperation);
                    if (rid instanceof ChangeableRecordId changeableRecordId) {
                      changeableRecordId.setCollectionPosition(ppos.collectionPosition);
                    } else {
                      throw new DatabaseException(name,
                          "Provided record is not new and its position cannot be changed");
                    }

                    assert clientTx.assertIdentityChangedAfterCommit(oldRID, rid);
                  }
                } else {
                  // This allocate position starting from a valid rid, used in distributed for
                  // allocate the same position on other nodes
                  final var rid = rec.getIdentity();
                  final var collection =
                      (PaginatedCollection) doGetAndCheckCollection(rid.getCollectionId());
                  var recordStatus = collection.getRecordStatus(rid.getCollectionPosition(),
                      atomicOperation);
                  if (recordStatus == RECORD_STATUS.NOT_EXISTENT) {
                    var ppos =
                        collection.allocatePosition(
                            rec.getRecordType(), atomicOperation);
                    while (ppos.collectionPosition < rid.getCollectionPosition()) {
                      ppos =
                          collection.allocatePosition(
                              rec.getRecordType(), atomicOperation);
                    }
                    if (ppos.collectionPosition != rid.getCollectionPosition()) {
                      throw new ConcurrentCreateException(session.getDatabaseName(),
                          rid,
                          new RecordId(rid.getCollectionId(), ppos.collectionPosition));
                    }
                  } else if (recordStatus == RECORD_STATUS.PRESENT
                      || recordStatus == RECORD_STATUS.REMOVED) {
                    final var ppos =
                        collection.allocatePosition(
                            rec.getRecordType(), atomicOperation);
                    throw new ConcurrentCreateException(session.getDatabaseName(),
                        rid, new RecordId(rid.getCollectionId(), ppos.collectionPosition));
                  }
                }
              }
            });
      } catch (final IOException | RuntimeException ioe) {
        throw BaseException.wrapException(new StorageException(name, "Could not preallocate RIDs"),
            ioe, name);
      } finally {
        stateLock.readLock().unlock();
      }

    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Traditional commit that support already temporary rid and already assigned rids
   *
   * @param clientTx the transaction to commit
   */
  @Override
  public void commit(final FrontendTransactionImpl clientTx) {
    commit(clientTx, false);
  }

  /**
   * Commit a transaction where the rid where pre-allocated in a previous phase
   *
   * @param clientTx the pre-allocated transaction to commit
   * @return The list of operations applied by the transaction
   */
  @SuppressWarnings("UnusedReturnValue")
  public List<RecordOperation> commitPreAllocated(final FrontendTransactionImpl clientTx) {
    return commit(clientTx, true);
  }

  /**
   * The commit operation can be run in 3 different conditions, embedded commit, pre-allocated
   * commit, other node commit. <bold>Embedded commit</bold> is the basic commit where the operation
   * is run in embedded or server side, the transaction arrive with invalid rids that get allocated
   * and committed. <bold>pre-allocated commit</bold> is the commit that happen after an
   * preAllocateRids call is done, this is usually run by the coordinator of a tx in distributed.
   * <bold>other node commit</bold> is the commit that happen when a node execute a transaction of
   * another node where all the rids are already allocated in the other node.
   *
   * @param frontendTransaction the transaction to commit
   * @param allocated           true if the operation is pre-allocated commit
   * @return The list of operations applied by the transaction
   */
  @SuppressWarnings("IdentityHashMapUsage")
  protected List<RecordOperation> commit(
      final FrontendTransactionImpl frontendTransaction, final boolean allocated) {
    // XXX: At this moment, there are two implementations of the commit method. One for regular
    // client transactions and one for
    // implicit micro-transactions. The implementations are quite identical, but operate on slightly
    // different data. If you change
    // this method don't forget to change its counterpart:
    //
    //
    try {
      final var session = frontendTransaction.getDatabaseSession();

      // The unified schema-or-index signal: a tx-local schema state means the transaction
      // changed the schema (or, once the index work lands, an index), so the commit takes the write
      // lock from the start and reconciles structure inside it. A pure-data commit keeps today's
      // read-lock fast path and its concurrency. Both branches share the inner apply body; only the
      // lock taken, the pre-apply reconciliation, and the post-apply promotion differ.
      final var txSchemaState = session.getTxSchemaState();
      final boolean schemaCarry = txSchemaState != null;

      // Freezer-gate checkpoint (1): the best-effort entry probe, hoisted ABOVE the snapshot pin
      // so its throw has genuinely zero side effects — no pin taken, no lock held, no WAL or
      // table state. It catches the common pre-engaged operator freeze at the cheapest point;
      // the later checkpoints stay the authoritative backstop for a freeze engaging after this
      // probe passed. Data commits are not probed — their freeze semantics are byte-for-byte
      // unchanged.
      if (schemaCarry && isOperatorFreezeActive()) {
        throw operatorFreezeGateException();
      }

      session.getMetadata().makeThreadLocalSchemaSnapshot();
      // Single-owner pin/clear pairing: this NESTED try opens immediately after the pin above and
      // its finally performs the SOLE clear — lexically paired with the pin, deliberately NOT the
      // method's outermost try (which precedes the pin and the hoisted probe: an outermost-finally
      // clear would fire on pre-pin throws, including probe rejections, and drive the pin count
      // negative). Every escape path after the pin — the write-lock abort throw, the in-freezer
      // gate throws, any applyCommitOperations exception (including ordinary version-conflict
      // aborts), and success — funnels through this one clear exactly once; applyCommitOperations
      // itself no longer clears. This pairing also covers throws from the index-operation sort
      // and atomic-operation reads below, which previously sat between the pin and the old clear.
      try {
        final var indexOperations = getSortedIndexOperations(frontendTransaction);
        final var atomicOperation = frontendTransaction.getAtomicOperation();
        final List<RecordOperation> result = new ArrayList<>(8);

        if (schemaCarry) {
          commitSchemaCarry(frontendTransaction, session, txSchemaState, indexOperations,
              atomicOperation, allocated, result);
        } else {
          stateLock.readLock().lock();
          try {
            applyCommitOperations(frontendTransaction, session, indexOperations, atomicOperation,
                allocated, null, result);
          } finally {
            stateLock.readLock().unlock();
          }
        }

        if (logger.isDebugEnabled()) {
          // Whole entities can resolve links after the atomic operation has ended. Log only values
          // that are already available on each committed record operation.
          final var committedRecords =
              result.stream()
                  .map(
                      operation -> "%s %s %s"
                          .formatted(
                              operation.getRecordId(),
                              RecordOperation.getName(operation.type),
                              operation.record == null
                                  ? "<null>"
                                  : "v.%d".formatted(operation.record.getVersion())))
                  .toList();
          LogManager.instance()
              .debug(
                  this,
                  "%d Committed transaction %d on database '%s' (records=%s)",
                  logger, Thread.currentThread().threadId(),
                  frontendTransaction.getId(),
                  session.getDatabaseName(),
                  committedRecords);
        }
        return result;
      } finally {
        session.getMetadata().clearThreadLocalSchemaSnapshot();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * The per-commit record working set computed up front: the record operations to apply, the
   * collections each touched record lives in (so they are locked before any write), the new-record
   * subset, and the per-operation collection-id overrides the default-collection fix produced. Both
   * the pure-data and the schema-carry commit branches gather this the same way; the schema-carry
   * branch gathers it only after the tx-local schema serializes its new per-class records, so they
   * join the working set.
   *
   * @param recordOperations all record operations in commit order.
   * @param collectionsToLock collection id to its storage collection, for every touched collection.
   * @param newRecords the created-record subset, in the commit comparator's order.
   * @param collectionOverrides per-operation collection-id override from the default-collection fix.
   */
  private record CommitWorkingSet(
      Iterable<RecordOperation> recordOperations,
      TreeMap<Integer, StorageCollection> collectionsToLock,
      Set<RecordOperation> newRecords,
      Map<RecordOperation, Integer> collectionOverrides) {

  }

  /**
   * The schema-carry inputs threaded into {@link #applyCommitOperations}. Non-null only for a
   * schema-carrying commit; the pure-data commit passes {@code null} and the apply body runs exactly
   * its legacy sequence. Carries the committed shared schema (the promotion target), the tx-local
   * schema (the serialization source), the tx-schema state (the provisional-id carrier and the index
   * overlay), and the index manager (the target for the commit-time index publication).
   */
  private record SchemaCommitContext(
      SchemaShared committedSchema, SchemaShared txLocalSchema, TxSchemaState txSchemaState,
      IndexManagerEmbedded indexManager) {

  }

  /**
   * Gathers the record working set the commit applies: validates created/updated records, resolves
   * each touched collection (so {@link #lockCollections} can lock them), and applies the
   * default-collection fix for a new record that arrived without a collection id. Must run under a
   * held {@code stateLock} (read lock for a pure-data commit, write lock for a schema-carry commit
   * with the commit window open), because it calls the lock-free {@link #doGetAndCheckCollection}.
   */
  @SuppressWarnings("IdentityHashMapUsage")
  private CommitWorkingSet computeCommitWorkingSet(
      final FrontendTransactionImpl frontendTransaction,
      final DatabaseSessionEmbedded session) {
    final var recordOperations = frontendTransaction.getRecordOperationsInternal();
    final var collectionsToLock = new TreeMap<Integer, StorageCollection>();
    final Map<RecordOperation, Integer> collectionOverrides = new IdentityHashMap<>(8);
    final Set<RecordOperation> newRecords = new TreeSet<>(COMMIT_RECORD_OPERATION_COMPARATOR);

    for (final var recordOperation : recordOperations) {
      var record = recordOperation.record;
      // A provisional collection id (<= -2) must never reach the working set: the schema-carry
      // commit rewrites every provisional record-collection id to its reconciled real id before
      // gathering the working set, and a pure-data commit cannot produce one (a tx-created
      // class exists only inside a schema-changing transaction).
      assert !SchemaShared.isProvisionalCollectionId(record.getIdentity().getCollectionId())
          : "provisional collection id reached the commit working set: " + record.getIdentity();
      if (recordOperation.type == RecordOperation.CREATED
          || recordOperation.type == RecordOperation.UPDATED) {

        if (record.isUnloaded()) {
          throw new IllegalStateException(
              "Unloaded record " + record.getIdentity() + " cannot be committed");
        }

        if (record instanceof EntityImpl entity) {
          entity.validate();
        }
      }

      if (recordOperation.type == RecordOperation.UPDATED
          || recordOperation.type == RecordOperation.DELETED) {
        final var collectionId = recordOperation.record.getIdentity().getCollectionId();
        collectionsToLock.put(collectionId, doGetAndCheckCollection(collectionId));
      } else if (recordOperation.type == RecordOperation.CREATED) {
        newRecords.add(recordOperation);

        final RID rid = record.getIdentity();

        var collectionId = rid.getCollectionId();

        if (record.isDirty()
            && collectionId == RID.COLLECTION_ID_INVALID
            && record instanceof EntityImpl entity) {
          // TRY TO FIX COLLECTION ID TO THE DEFAULT COLLECTION ID DEFINED IN SCHEMA CLASS

          var cls = entity.getImmutableSchemaClass(session);
          if (cls != null) {
            collectionId = cls.getCollectionForNewInstance(entity);
            collectionOverrides.put(recordOperation, collectionId);
          }
        }
        collectionsToLock.put(collectionId, doGetAndCheckCollection(collectionId));
      }
    }

    return new CommitWorkingSet(recordOperations, collectionsToLock, newRecords,
        collectionOverrides);
  }

  /**
   * Rewrites each record id that carries a provisional collection id ({@code <= -2}, allocated
   * for a class created inside the still-open transaction) to the real collection id the
   * commit's reconciliation assigned. New records of a tx-created class carry the provisional
   * id from creation so a same-transaction scan finds them under it; by this point every
   * provisional id must have a recorded resolution, because the id participates in collection
   * locking, position allocation, and record serialization right after. The rewrite goes
   * through {@link ChangeableRecordId#setCollectionAndPosition} so the transaction's rid-keyed
   * maps re-key through the identity-change listeners, exactly like the temp-position rewrite
   * the allocation loop applies later.
   */
  private void rewriteProvisionalRecordCollectionIds(
      final FrontendTransactionImpl frontendTransaction, final TxSchemaState txSchemaState) {
    // Copy first: the identity-change listeners re-key the transaction's record-operation map
    // mid-iteration, and getRecordOperationsInternal() is a live view over that map.
    final var recordOperations =
        new ArrayList<>(frontendTransaction.getRecordOperationsInternal());
    for (final var recordOperation : recordOperations) {
      final var rid = recordOperation.record.getIdentity();
      final var collectionId = rid.getCollectionId();
      if (!SchemaShared.isProvisionalCollectionId(collectionId)) {
        continue;
      }
      final var realCollectionId = txSchemaState.getResolvedCollectionId(collectionId);
      if (realCollectionId == TxSchemaState.NO_RESOLUTION) {
        // Reconciliation records a resolution for every provisional collection the tx-local
        // schema still owns at commit, so a miss means the record's class stopped owning it
        // later in the same transaction (the class was dropped, or altered back to abstract,
        // after the record was written). Failing loudly is deliberate: silently discarding the
        // record operation would hide a data-losing commit, and letting the provisional id
        // continue would corrupt durable bytes.
        throw new StorageException(name,
            "Record " + rid + " references provisional collection " + collectionId
                + " (collection name '"
                + txSchemaState.getProvisionalCollectionNames().get(collectionId)
                + "'), but its class was dropped or made abstract later in the same"
                + " transaction, so the commit reconciliation resolved no real collection for"
                + " it. Delete the class's records before dropping the class in the same"
                + " transaction");
      }
      // Reconciliation must resolve a provisional id to a real (non-negative) collection: -1 is
      // the abstract/invalid sentinel, and a -1 that slipped through here would be silently
      // re-resolved by the working-set CREATED-branch collection fix-up, masking the
      // reconciliation bug and possibly landing the record in a different collection than the
      // one reconciliation built.
      assert realCollectionId >= 0
          : "reconciliation resolved provisional collection " + collectionId
              + " to non-real id " + realCollectionId + " for record " + rid;
      if (rid instanceof ChangeableRecordId changeableRecordId) {
        changeableRecordId.setCollectionAndPosition(realCollectionId,
            rid.getCollectionPosition());
      } else {
        throw new StorageException(name,
            "Record " + rid + " carries a provisional collection id but its identity cannot be"
                + " rewritten");
      }
    }
  }

  /**
   * Applies a commit inside the held {@code stateLock}, shared by the pure-data and schema-carry
   * branches. The schema-carry steps fire only when {@code schemaContext} is non-null and run inside
   * the atomic-operation window so their structural file writes are buffered as WAL-reverted intent:
   * after {@code startTxCommit} opens the apply window it reconciles collections (set difference over
   * committed versus tx-local real ids), resolves every provisional id, and serializes the tx-local
   * schema, so the changed per-class records join the working set gathered next. It then locks
   * collections, link bags, and indexes; allocates positions for new records and rewrites their temp
   * RIDs in place; writes every record; runs the link-bag B-tree operations; and commits the index
   * changes. On error it rolls back the atomic operation (and, for a schema-carry commit, undoes the
   * in-memory registry publication so no phantom registration survives); on success it ends the
   * atomic operation, cleans the snapshot index, and (for a schema-carry commit) promotes the
   * tx-local schema into the committed shared instances with one trailing {@code forceSnapshot}. The
   * {@code stateLock} (read lock for pure-data, write lock plus the open commit window for
   * schema-carry) is acquired and released by the caller, not here.
   */
  @SuppressWarnings("IdentityHashMapUsage")
  private void applyCommitOperations(
      final FrontendTransactionImpl frontendTransaction,
      final DatabaseSessionEmbedded session,
      final SortedMap<String, FrontendTransactionIndexChanges> indexOperations,
      final AtomicOperation atomicOperation,
      final boolean allocated,
      final SchemaCommitContext schemaContext,
      final List<RecordOperation> result) throws IOException {
    try {
      checkOpennessAndMigration();

      makeStorageDirty();

      Throwable error = null;
      boolean structurePublished = false;
      // Collections dropped by reconciliation, captured for the failure-path restore arm. A drop
      // removes the collection from the in-memory registries eagerly inside reconcileCollections, so
      // a failed commit must re-register them (rollback reverts only the on-disk files). Stays empty
      // on a pure-data commit and on a schema-carry commit that drops nothing.
      List<DroppedCollection> droppedCollections = List.of();
      // The transaction's index deltas (created / dropped / membership), reconciled at commit across
      // three phases: enroll the changed per-index records before the working set, build/drop and
      // populate the engines after the record apply, publish into the shared index maps after
      // commitChanges. Null on a pure-data commit and on a schema-carry commit with no index delta.
      IndexManagerEmbedded.ReconciledIndexPlan indexPlan = null;
      var topLevelIndexLifecycles = pendingTopLevelIndexLifecycles(frontendTransaction);
      startTxCommit(atomicOperation, schemaContext != null);
      try {
        if (schemaContext != null) {
          // The reconciliation publishes created collections into the live registries inside its own
          // loop and removes dropped ones eagerly. Set the published flag before the call (not after)
          // so a throw partway through reconciliation still routes the failure finally to the undo:
          // any collection already created/dropped in that loop must be reverted in memory.
          structurePublished = true;
          // Reconcile structure inside the atomic-operation window, before any record serializes:
          // create the tx-local-only collections and drop the committed-only ones, recording each
          // provisional->real id. The structural file writes are WAL-reverted on a rollback.
          droppedCollections =
              reconcileCollections(schemaContext.committedSchema(), schemaContext.txLocalSchema(),
                  schemaContext.txSchemaState(), atomicOperation);

          // Test-only seam: fire any installed in-window hook now that structure is published but
          // before the record apply. Tests use it to inject a fault (verifying the failure-path undo
          // leaves no phantom/lost registration) or to latch the commit thread inside the held write
          // lock (verifying a concurrent data commit proceeds on the read-lock path). Null and free in
          // production. Fired inside the open commit window so a hook that reads records routes through
          // the lock-free substrate rather than re-entering the read lock.
          final var hook = commitWindowTestHook;
          if (hook != null) {
            hook.run();
          }

          // Patch every provisional id in the tx-local schema to its real id, then serialize the
          // tx-local schema so its changed per-class records and root enrol into the transaction.
          schemaContext.txLocalSchema()
              .resolveProvisionalCollectionIds(
                  schemaContext.txSchemaState().getResolvedCollectionIds());
          schemaContext.txLocalSchema().acquireSchemaWriteLock(session);
          // Suppress the bidirectional-link-consistency tracker around schema serialization AND the
          // index-record enrollment below. The honest justification is a mixed-tracking asymmetry,
          // not a blanket "structural records carry no back-reference bag" (legacy-created index
          // entities DO carry bags from their historical tracked adds): the tracker stays
          // self-consistent only when both halves of a link edit run tracked (a tracked add
          // auto-creates the back-reference bag that the tracked deletion arm later consumes),
          // which is how the legacy committed create/drop path never threw. The tx commit path
          // breaks that symmetry — the enroll phase edits the root's "classes" LINKSET and the
          // index-manager's CONFIG_INDEXES LINKSET manually (indexLinkSet.remove +
          // deleteRecordAtCommit, in reversed order), and its own creates run inside this
          // suppressed window and are therefore bag-less — so an unsuppressed tracker throws on
          // either arm (a missing bag on decrement, or a linkset entry the enroll already
          // removed). Empirically forced, not preemptive: deleting a dropped class's per-class
          // record during toStream tripped LinksConsistencyException on the root's classes
          // linkset when the commit-time schema promotion first landed. The window is minimal —
          // user-record link checks complete at commit
          // entry, before this window opens.
          //
          // Capture and restore the prior flag rather than forcing it back on: a schema-carry
          // commit can run inside an outer scope that already disabled the check (for example an
          // import), and an unconditional re-enable would clobber that outer disable for the rest
          // of the outer operation.
          final var priorLinkConsistency = session.isLinkConsistencyEnabled();
          session.disableLinkConsistencyCheck();
          try {
            // The selective write keys on the transaction's changed-class set so only changed classes'
            // per-class records enter the working set (the write-amplification win), and on whether the
            // root non-link payload differs from the committed schema's so the root record is rewritten
            // exactly when a property-create, counter advance, or blob-collection change touched it.
            // The committed schema still carries its pre-commit payload here; promotion runs later on
            // the success path, so the comparison sees the true before-state.
            final var changedClasses = schemaContext.txSchemaState().getChangedClasses();
            final var writeRootPayload =
                schemaContext.txLocalSchema()
                    .rootPayloadDiffersFrom(schemaContext.committedSchema());
            // The serialization loads the root and per-class records through a fresh-committed-read
            // scope. The seed refreshed the session's local record cache, but that cache is
            // weak-referenced: a GC between the seed and this point can evict the fresh instances,
            // and a plain cache-miss reload here would ride the transaction's begin-time snapshot —
            // resurrecting exactly the stale-seed version conflict the seed isolation exists to
            // prevent. The scope makes any such miss (and any page-frame fallback re-read raised by
            // property access inside toStream) read the latest committed state instead; a cache hit
            // is refreshed in place, which no-ops on the version equality the held metadata-write
            // mutex guarantees since the seed.
            try {
              session.computeWithFreshCommittedReads(() -> {
                schemaContext.txLocalSchema().toStream(session, changedClasses, writeRootPayload);
                return null;
              });
            } finally {
              // Release without the save side effect: the records are enrolled in the user
              // transaction and persist through the apply below, not through saveInternal.
              schemaContext.txLocalSchema().releaseSchemaWriteLock(session, false);
            }

            // Index reconciliation, phase 1: enroll the changed per-index records into the transaction
            // (the tx-created index entities and the index-manager link-set delta) so they join the
            // working set gathered next, and enforce the v1 empty-source-collection build bound. The
            // engine build and the shared-map publication run in later phases. Reconciliation resolves
            // provisional collection ids on the tx-local schema above, so a deferred handle indexing a
            // same-tx class now names the real collection. No overlay -> null plan -> the phases below
            // are no-ops. The plan is created and assigned BEFORE enrollment runs: enrollment
            // applies eager membership mutations to shared committed indexes as it goes, so a
            // throw mid-enrollment must still leave the failure path a non-null plan whose
            // recorded mutations it can revert.
            final var indexOverlay = schemaContext.txSchemaState().getIndexOverlay();
            indexPlan = schemaContext.indexManager().newReconciledIndexPlan(indexOverlay);
            if (indexPlan != null) {
              // Same fresh-committed-read reasoning as the toStream scope above: enrollment loads
              // the changed per-index records and the index-manager record, whose cached instances
              // the weak local cache may have dropped since they were last read; a miss must not
              // fall back to the begin-time snapshot. The plan capture keeps indexPlan itself
              // assigned BEFORE enrollment runs, so a throw mid-enrollment still leaves the
              // failure path a non-null plan whose recorded eager membership mutations it reverts.
              final var plan = indexPlan;
              session.computeWithFreshCommittedReads(() -> {
                schemaContext.indexManager()
                    .enrollReconciledIndexRecords(
                        session, frontendTransaction, indexOverlay, atomicOperation, plan);
                return null;
              });
            }
          } finally {
            if (priorLinkConsistency) {
              session.enableLinkConsistencyCheck();
            }
          }
          if (indexPlan != null) {
            // Drop the tx-created indexes' tracked key changes from both the commit's local
            // index-apply set and the transaction's own index-changes map. A same-tx insert into the
            // indexed class routed addIndexEntry to the deferred handle (indexId = -1, absent from the
            // shared registry), so those tracked changes would (a) make lockIndexes/commitIndexes try
            // to lock and write an unbuilt engine and (b) make assertIdentityChangedAfterCommit fail
            // resolving the not-yet-published index by name. The commit-time population scan re-derives
            // the tx-created index from the transaction's final record state instead, so those tracked
            // changes are redundant. Committed indexes' tracked changes stay unchanged.
            final var txIndexChanges = frontendTransaction.getIndexOperations();
            for (final var created : indexPlan.created()) {
              indexOperations.remove(created.getName());
              txIndexChanges.remove(created.getName());
            }
          }

          if (!schemaContext.txSchemaState().getResolvedCollectionIds().isEmpty()) {
            // The snapshot pinned at commit entry was built from the tx-local schema while it
            // still carried provisional collection ids. resolveProvisionalCollectionIds patched
            // the schema itself above, but the pinned snapshot is a separate immutable copy, and
            // the session-private memo still serves the stale build. Invalidate the memo first
            // (or the rebuild below would just return it), then rebuild the pin in place, so the
            // working-set read below resolves each tx-created class to its reconciled real
            // collection id and never hands doGetAndCheckCollection a provisional one.
            schemaContext.txSchemaState().invalidateOverlaySnapshot();
            session.getMetadata().rebuildThreadLocalSchemaSnapshot();
          }

          // Rewrite every record id that still carries a provisional collection id (<= -2) to
          // the real id reconciliation assigned above, so the working set gathered below locks,
          // allocates, and serializes against real collections only and no provisional id can
          // reach durable bytes. Runs after the pinned-snapshot rebuild so the record ids and
          // the snapshot agree on the reconciled ids. Guarded on the allocation map (not the
          // resolution map): only a class create allocates a provisional id, so a schema commit
          // that created no class skips the O(record-operations) copy-and-scan entirely, while
          // a create-then-drop-with-rows transaction (allocated but unresolved) still reaches
          // the rewrite's loud data-loss failure.
          if (!schemaContext.txSchemaState().getProvisionalCollectionNames().isEmpty()) {
            rewriteProvisionalRecordCollectionIds(frontendTransaction,
                schemaContext.txSchemaState());
          }
        }

        // Gather the working set after any schema serialization so the new schema records join it.
        final var workingSet = computeCommitWorkingSet(frontendTransaction, session);

        lockCollections(workingSet.collectionsToLock(), atomicOperation);
        lockLinkBags(workingSet.collectionsToLock(), atomicOperation);
        lockIndexes(indexOperations, atomicOperation);

        final Map<RecordOperation, PhysicalPosition> positions = new IdentityHashMap<>(8);
        for (final var recordOperation : workingSet.newRecords()) {
          final var rec = recordOperation.record;

          if (allocated) {
            if (rec.getIdentity().isPersistent()) {
              positions.put(
                  recordOperation,
                  new PhysicalPosition(rec.getIdentity().getCollectionPosition()));
            } else {
              throw new StorageException(name,
                  "Impossible to commit a transaction with not valid rid in pre-allocated"
                      + " commit");
            }
          } else if (rec.isDirty() && !rec.getIdentity().isPersistent()) {
            final var rid = rec.getIdentity();
            final var oldRID = rid.copy();

            final var collectionOverride = workingSet.collectionOverrides().get(recordOperation);
            final int collectionId =
                Optional.ofNullable(collectionOverride).orElseGet(rid::getCollectionId);

            final var collection = doGetAndCheckCollection(collectionId);

            var physicalPosition =
                collection.allocatePosition(
                    rec.getRecordType(),
                    atomicOperation);

            if (rid.getCollectionPosition() > -1) {
              // CREATE EMPTY RECORDS UNTIL THE POSITION IS REACHED. THIS IS THE CASE WHEN A
              // SERVER IS OUT OF SYNC
              // BECAUSE A TRANSACTION HAS BEEN ROLLED BACK BEFORE TO SEND THE REMOTE CREATES.
              // SO THE OWNER NODE DELETED
              // RECORD HAVING A HIGHER COLLECTION POSITION
              while (rid.getCollectionPosition() > physicalPosition.collectionPosition) {
                physicalPosition =
                    collection.allocatePosition(
                        rec.getRecordType(),
                        atomicOperation);
              }

              if (rid.getCollectionPosition() != physicalPosition.collectionPosition) {
                throw new ConcurrentCreateException(name,
                    rid, new RecordId(collection.getId(),
                        physicalPosition.collectionPosition));
              }
            }
            positions.put(recordOperation, physicalPosition);

            if (rid instanceof ChangeableRecordId changeableRecordId) {
              changeableRecordId.setCollectionAndPosition(collection.getId(),

                  physicalPosition.collectionPosition);
            } else {
              throw new DatabaseException(name,
                  "Provided record is not new and its identity cannot be changed");
            }
            assert frontendTransaction.assertIdentityChangedAfterCommit(oldRID, rid);
          }
        }

        if (indexPlan != null) {
          // Descriptor identities are persistent now. Create the lifecycle record and set the
          // descriptor link before record serialization. Every write remains in this atomic unit.
          schemaContext.indexManager()
              .createReconciledIndexLifecycles(
                  frontendTransaction, atomicOperation, indexPlan);
        }
        createTopLevelIndexLifecycles(
            frontendTransaction, atomicOperation, topLevelIndexLifecycles);

        for (final var recordOperation : workingSet.recordOperations()) {
          commitEntry(
              frontendTransaction,
              atomicOperation,
              recordOperation,
              positions.get(recordOperation),
              session.getSerializer());
          result.add(recordOperation);
        }

        //update of b-tree based link bags
        var recordSerializationContext = frontendTransaction.getRecordSerializationContext();
        recordSerializationContext.executeOperations(atomicOperation, this);

        commitIndexes(frontendTransaction.getDatabaseSession(), atomicOperation,
            indexOperations);

        if (indexPlan != null) {
          // Index reconciliation, phase 2: now that the record apply has assigned the transaction's
          // records persistent RIDs, build each tx-created index's engine and populate it from the
          // transaction's final record state, and delete each tx-dropped index's engine. The engine
          // files are buffered in this same atomic operation, so they revert with a rollback; the
          // engine registry publication is eager and reverted by the failure-path undo below.
          schemaContext.indexManager()
              .buildAndDropReconciledEngines(session, frontendTransaction, atomicOperation,
                  indexPlan);

          // Test-only seam: fire any installed post-build hook now that engines are built/published
          // and dropped but before the record apply commits. Tests use it to inject a fault at the
          // engines-published-but-not-durable point so the failure-path engine undo/restore arms
          // actually run (the pre-record-apply commitWindowTestHook fires before any engine exists, so
          // it cannot exercise them). Null and free in production. Fired inside the open commit window.
          final var postBuildHook = postEngineBuildTestHook;
          if (postBuildHook != null) {
            postBuildHook.run();
          }
        }
      } catch (final IOException | RuntimeException | AssertionError e) {
        // AssertionError is caught so a persisted-side underflow from
        // BTree.addToApproximateEntriesCount (raised inside commitIndexes
        // or the lifecycle persist hook later in endAtomicOperation)
        // routes through the rollback path. Without this, the assert
        // would escape the outer catch (Error), call setInError, and
        // put the storage in permanent error state. Persisted-side
        // underflow signals a structural inconsistency on the
        // entry-point page; rolling back the WAL atomic operation
        // reverts the offending writes and leaves the storage usable
        // for subsequent commits.
        error = e;
        if (e instanceof RuntimeException runtimeException) {
          throw runtimeException;
        }
        // IOException or AssertionError — wrap as StorageException so
        // the API caller's contract stays uniform (no Error escapes).
        throw BaseException.wrapException(
            new StorageException(name, "Error during transaction commit"), e, name);
      } finally {
        if (error != null) {
          rollback(error, atomicOperation);
          if (schemaContext != null && structurePublished) {
            undoSchemaCarryRegistryPublication(schemaContext, indexPlan, droppedCollections);
          }
        } else {
          // endTxCommit invokes AtomicOperationsManager.endAtomicOperation,
          // the single lifecycle gate that now owns persist (before
          // commitChanges) and apply (after commitChanges, before the
          // inner-finally releaseLocks). The pre-endTxCommit catch above
          // still owns commitIndexes failures; the lifecycle apply hook
          // logs-and-swallows its own failures. endTxCommit itself can
          // still fail, in two distinct shapes that must be told apart by
          // the operation's rollback flag:
          //
          // (1) A lifecycle persist-hook failure: endAtomicOperation
          //     converts it to a rollback internally (rollbackInProgress
          //     set, commitChanges skipped, table entry rolled back) and
          //     rethrows after its freezer/lock teardown. Nothing is
          //     durable, so the failure routes through the same in-memory
          //     registry undo/restore arms as the pre-endTxCommit failures
          //     — propagating it uncaught would leave phantom created
          //     collections/engines registered and dropped ones missing.
          //     No second rollback(...) call is made here — the operation
          //     is already ended — only the registry undo.
          //
          // (2) A commitChanges throw: no rollback happened
          //     (rollbackInProgress stays false) and durability is
          //     IN-DOUBT — the WAL atomic-unit end record may or may not
          //     have been flushed before the throw (e.g. a failure mid
          //     shared-cache apply happens after durability). Undoing the
          //     registry here could de-register a durably committed
          //     collection (and the undo's structural cleanup could then
          //     durably delete its files); keeping the storage live could
          //     advertise a reverted commit. Neither is safe, so the
          //     registry publication is left standing and the storage is
          //     moved to error state. Containment scope, stated honestly:
          //     the error state gates component-WRITE operations only
          //     (checkErrorState is consulted at the component-lock
          //     acquisition), so writes fail fast while reads and registry
          //     queries keep serving — in the durably-committed sub-shape
          //     (a post-commitChanges throw) other sessions can read the
          //     new durable rows under the still-unpromoted, stale schema
          //     until the reopen. That divergence window is accepted: the
          //     dirty flag is preserved by the error state, so the next
          //     open replays the WAL and rebuilds registries and schema
          //     from the durable truth, whichever way the WAL landed.
          //     AssertionError is wrapped before setInError below because
          //     the setter's stray-dev-assert skip would otherwise leave
          //     this arm's containment disabled under -ea — here the
          //     poison is load-bearing (an in-doubt commit with its
          //     publication standing must not stay writable), not a
          //     defensive courtesy.
          try {
            endTxCommit(atomicOperation);
          } catch (final IOException | RuntimeException | AssertionError e) {
            if (schemaContext != null && structurePublished) {
              if (atomicOperation.isRollbackInProgress()) {
                undoSchemaCarryRegistryPublication(schemaContext, indexPlan, droppedCollections);
              } else {
                setInError(e instanceof AssertionError
                    ? BaseException.wrapException(
                        new StorageException(name,
                            "endTxCommit failed without an internal rollback"),
                        e, name)
                    : e);
                LogManager.instance()
                    .error(this,
                        "endTxCommit failed after the schema-carry reconcile without an internal"
                            + " rollback; durability is in-doubt, so the in-memory registry"
                            + " publication is left standing and the storage is moved to error"
                            + " state. Re-open the storage to restore consistency from the durable"
                            + " state.",
                        e);
              }
            }
            throw e;
          }
          publishTopLevelIndexLifecycles(topLevelIndexLifecycles);
          try {
            cleanupSnapshotIndex();
          } catch (final RuntimeException | AssertionError e) {
            // Cleanup is best-effort — its failure must never mask a successful commit.
            // The commit is already durable (WAL flushed, pages applied). If cleanup
            // throws, stale snapshot entries simply accumulate until the next successful
            // cleanup pass. AssertionError caught here for symmetry with the
            // pre-endTxCommit catch above: an `assert` regression along the cleanup
            // path must not escape and re-open the cascade.
            LogManager.instance()
                .warn(this, "Snapshot index cleanup failed after successful commit", e);
          }

          if (schemaContext != null) {
            if (indexPlan != null) {
              // Index reconciliation, phase 3: the records are durable and the engines built, so
              // publish the transaction's index deltas into the shared lookup maps (register the
              // tx-created indexes, remove the tx-dropped ones, apply the membership deltas). Deferring
              // this past commitChanges keeps the committed index view unchanged on a failed commit,
              // mirroring the schema promotion just below. Runs under the held index-manager write
              // lock (taken at commit entry) before the single forceSnapshot the promotion fires.
              schemaContext.indexManager()
                  .publishReconciledIndexes(frontendTransaction, indexPlan);
            }
            // Promote: the records are now durable, so re-parse the just-committed root and
            // per-class records into the committed shared instances and invalidate the shared
            // snapshot exactly once. fromStream binds new classes to the committed owner; a dropped
            // class drops out. Promotion runs after a successful apply only, still under the held
            // write lock and the open commit window.
            // The commit is already durable here (endTxCommit applied the WAL). A throw during
            // promotion (a cache-miss load, a fromStream parse failure, a forceSnapshot assert)
            // must not both mask the successful commit and leave the in-memory committed schema
            // half-parsed against correct durable bytes. fromStream clears and rebuilds the in-
            // memory schema, so a mid-parse throw corrupts it, and rebuilding the snapshot would
            // only reflect that corruption (makeSnapshot reads the in-memory class graph, not the
            // disk). Reloading from disk inline is unsafe (it would begin a nested transaction
            // while this commit is still active), so on failure drop any stale snapshot and move
            // the storage to error state: the divergence then self-corrects on the next reopen,
            // which re-parses the schema from the durable records. The durable commit still
            // succeeds; the failure is logged rather than rethrown.
            try {
              // The promotion re-parse loads the just-committed root and per-class records through
              // a fresh-committed-read scope. The tx-written records resolve through the
              // transaction's own record set, but an unchanged per-class record's cached instance
              // may have been evicted (weak local cache) since toStream warmed it; a bare
              // cache-miss load here would ride the transaction's begin-time atomic operation —
              // stale under contention, and already ENDED by endTxCommit on the disk profile
              // ("atomic operation is not active"). The scope's dedicated read-only operation is
              // active and its snapshot post-dates the just-applied commit, so the promotion reads
              // exactly the durable state it must re-parse.
              session.computeWithFreshCommittedReads(() -> {
                final EntityImpl committedRoot =
                    session.load(schemaContext.committedSchema().getIdentity());
                schemaContext.committedSchema().fromStream(session, committedRoot);
                return null;
              });
              schemaContext.committedSchema().forceSnapshot();
            } catch (final RuntimeException | AssertionError e) {
              LogManager.instance()
                  .error(this,
                      "Schema promotion failed after a durable schema-carry commit; the in-memory"
                          + " schema is now untrusted and the storage will reload it from disk on"
                          + " reopen",
                      e);
              try {
                schemaContext.committedSchema().forceSnapshot();
              } catch (final RuntimeException | AssertionError ignored) {
                // Best-effort: a forceSnapshot failure here must not mask the durable commit.
              }
              setInError(e);
            }
          }
        }
      }
    } finally {
      atomicOperationsManager.ensureThatComponentsUnlocked(atomicOperation);
      // The thread-local schema snapshot pinned at commit entry is deliberately NOT cleared here:
      // commit() owns the pin/clear pairing as a single owner (the nested finally right after its
      // pin), so every escape path clears exactly once — a second clear here would drive the pin
      // count negative on every failed commit.
    }
  }

  /**
   * Commits a schema-carrying transaction: the dependency inversion's commit half. Takes the four
   * locks in the acyclic order (the metadata-write mutex is already engaged from the first schema
   * write; here the order is {@code SchemaShared.lock} &rarr; index-manager lock &rarr;
   * {@code stateLock.writeLock}), opens the lock-free commit window, and delegates the apply body to
   * {@link #applyCommitOperations} with a non-null {@link SchemaCommitContext}. That method, inside
   * the atomic-operation window, reconciles collections, resolves provisional ids, serializes the
   * tx-local schema, applies the working set, and on success promotes the committed schema (on
   * failure it undoes the in-memory registry publication so no phantom registration survives).
   * Splitting the structural reconciliation into the apply body keeps the structural file writes
   * inside the {@code startTxCommit}/{@code endTxCommit} window, so they are buffered as WAL-reverted
   * intent and revert atomically with the record writes on a rollback.
   */
  private void commitSchemaCarry(
      final FrontendTransactionImpl frontendTransaction,
      final DatabaseSessionEmbedded session,
      final TxSchemaState txSchemaState,
      final SortedMap<String, FrontendTransactionIndexChanges> indexOperations,
      final AtomicOperation atomicOperation,
      final boolean allocated,
      final List<RecordOperation> result) throws IOException {
    final var committedSchema = session.getSharedContext().getSchema();
    final var indexManager = session.getSharedContext().getIndexManager();
    final var schemaContext =
        new SchemaCommitContext(committedSchema, txSchemaState.getTxLocalSchema(), txSchemaState,
            indexManager);

    // Four-lock order: the mutex is already engaged (first schema write). Take the two shared
    // metadata write locks before stateLock so the order stays acyclic, then the storage write lock.
    committedSchema.acquireSchemaWriteLock(session);
    try {
      indexManager.acquireExclusiveLockForCommit();
      try {
        // Freezer-gate checkpoint (2): acquire the storage write lock through the
        // abort-predicate single-acquisition primitive. A plain lock() here could block
        // indefinitely behind readers while an operator freeze engages — with the two metadata
        // locks held, that converts the freeze into a schema-read outage; and a queued plain
        // writer would stall every new reader behind it (writer preference). The primitive
        // acquires the write bit once (bounded by the residual reader residence — no
        // release-on-timeout retry window for readers to slip through) and aborts within one
        // poll granularity when the operator-kind predicate turns true; on abort nothing is
        // held and no reader is queued behind a gave-up writer, and the two metadata locks
        // unwind through the enclosing finallys.
        if (!stateLock.exclusiveLockWithAbort(
            this::isOperatorFreezeActive, OPERATOR_FREEZE_ABORT_POLL_NANOS)) {
          throw operatorFreezeGateException();
        }
        try {
          // Open the lock-free commit window: schema toStream/fromStream and the record-read path
          // re-enter stateLock.readLock() through session.load, which would busy-spin forever on the
          // non-reentrant lock the commit already holds for writing. The window makes those reads
          // skip the read lock; the held write lock supplies the exclusion and the visibility edge.
          enterCommitWindow();
          try {
            applyCommitOperations(frontendTransaction, session, indexOperations, atomicOperation,
                allocated, schemaContext, result);
          } finally {
            exitCommitWindow();
          }
        } finally {
          stateLock.writeLock().unlock();
        }
      } finally {
        indexManager.releaseExclusiveLockForCommit();
      }
    } finally {
      committedSchema.releaseSchemaWriteLock(session, false);
    }
  }

  /**
   * Reconciles collections as the set difference over committed versus tx-local real collection
   * ids, inside the held {@code stateLock.writeLock()} with the commit window open. A collection id
   * present in the tx-local schema but absent from the committed schema is a create; the reverse is
   * a drop. Creates use the lock-free {@link #doCreateCollection} at a commit-local first-null-slot
   * id (so a failed commit's ids free for reuse), publish into the registries, and record the
   * provisional&rarr;real resolution on {@code txSchemaState}. Drops use {@link
   * #dropCollectionInternal}. Diffing by collection id rather than class name keeps a rename
   * structurally inert.
   */
  private List<DroppedCollection> reconcileCollections(
      final SchemaShared committedSchema,
      final SchemaShared txLocalSchema,
      final TxSchemaState txSchemaState,
      final AtomicOperation atomicOperation) {
    try {
      final var committedIds = committedSchema.getRealCollectionIds();
      final var txProvisionalNames = txSchemaState.getProvisionalCollectionNames();

      // Drops: a real collection id in the committed schema absent from the tx-local schema. The
      // tx-local schema's real-id set is the committed reals minus any dropped class's ids; a
      // provisional id (<= -2) is excluded from getRealCollectionIds, so it never reads as a drop.
      final var txRealIds = txLocalSchema.getRealCollectionIds();
      final List<DroppedCollection> dropped = new ArrayList<>();
      for (final var committedId : committedIds) {
        if (!txRealIds.contains(committedId)) {
          // Capture the collection before it is nulled, so the failure path can re-register it.
          final var collection =
              committedId < collections.size() ? collections.get(committedId) : null;
          if (!dropCollectionInternal(atomicOperation, committedId)) {
            // A real drop happened (the collection existed). Record the dropped collection for the
            // failure-path undo BEFORE the two structural drops below: dropCollectionInternal has
            // already nulled the registry slot and deleted the data file, so if dropCollection or
            // deleteComponentByCollectionId throws, the failure-path undo still needs the captured
            // collection to re-register it. The captured `collection` is the pre-null live object, so
            // the record is complete even when a later structural drop throws.
            dropped.add(new DroppedCollection(committedId, collection));
            // Mirror the public dropCollection(int): dropCollectionInternal deletes only the data
            // files and the in-memory entry, so also drop the storage-configuration entry and the
            // link-bag B-tree component on the same atomic operation. All three structural deletions
            // then revert together on a rollback and become durable together on commit, keeping the
            // drop symmetric with doCreateCollection (which writes the data file, the config entry,
            // and the component together).
            ((CollectionBasedStorageConfiguration) configuration)
                .dropCollection(atomicOperation, committedId);
            linkCollectionsBTreeManager.deleteComponentByCollectionId(atomicOperation, committedId);
          }
        }
      }

      // Creates: each provisional id the transaction allocated becomes a real collection created
      // under the carried counter-only (c_<counter>) name at a commit-local first-null-slot id.
      // Record the
      // resolution so the patch list can rewrite every provisional reference before serialization.
      final var ownedProvisionalIds = txLocalSchema.getOwnedProvisionalCollectionIds();
      for (final var entry : txProvisionalNames.int2ObjectEntrySet()) {
        final int provisionalId = entry.getIntKey();
        if (!ownedProvisionalIds.contains(provisionalId)) {
          // The tx-local schema no longer owns this provisional collection at commit: the class
          // that allocated it was dropped (or made abstract again) later in the same transaction.
          // Creating its collection would publish an orphan no schema path can ever reach, so it
          // is skipped and no resolution is recorded. A record operation still carrying the id
          // then fails loudly in the provisional-id rewrite, which keeps the drop from silently
          // committing the class's rows into an unreachable collection.
          continue;
        }
        final String collectionName = entry.getValue();
        final int realId = nextFreeCollectionId();
        // The allocator must hand back a genuinely free slot (an append at the list end or a null
        // slot). undoReconciledCollections relies on this: a resolved id that collided with a
        // committed live slot would make the undo skip a real cleanup and doCreateCollection would
        // overwrite a live collection. Zero production cost; catches a slot-bookkeeping regression at
        // create time during testing.
        assert realId >= collections.size() || collections.get(realId) == null
            : "commit-local allocator returned an occupied collection slot " + realId;
        final var collection = doCreateCollection(atomicOperation, collectionName, realId);
        registerCollection(collection);
        txSchemaState.recordResolvedCollectionId(provisionalId, realId);
      }
      return dropped;
    } catch (final IOException e) {
      // Wrap as an unchecked StorageException so the schema-carry commit's RuntimeException/Error
      // catch reaches the registry-undo path, mirroring how the public structural methods wrap
      // their atomic-operation IOExceptions.
      throw BaseException.wrapException(
          new StorageException(name, "Error reconciling collections during schema commit"), e,
          name);
    }
  }

  /**
   * The first null slot in {@code collections}, or the list size when none is free — the
   * commit-local collection-id allocator's next id. Seeded by a direct read of the shared
   * {@code collections} list, which is safe because the caller holds {@code stateLock.writeLock()}
   * (the seed read sits inside the write-lock window so it excludes the non-commit
   * engine registrars that run under {@code stateLock.write} alone). Reused between successive
   * creates within one reconciliation because each create publishes its collection into the slot
   * before the next call scans.
   */
  private int nextFreeCollectionId() {
    for (var i = 0; i < collections.size(); i++) {
      if (collections.get(i) == null) {
        return i;
      }
    }
    return collections.size();
  }

  /**
   * A collection {@link #reconcileCollections} dropped, captured so a failed schema-carry commit can
   * restore its in-memory registration. The id is the dropped collection's slot; the collection is
   * the {@link StorageCollection} object removed from the registries (its on-disk files are reverted
   * by the rolled-back atomic operation, so re-registering the same object restores a consistent
   * view).
   */
  private record DroppedCollection(int id, StorageCollection collection) {

  }

  /**
   * Runs every in-memory registry undo/restore arm for a failed schema-carry commit, in the pinned
   * order. A failed commit must leave the in-memory registries exactly as they were before the
   * commit: the created and dropped structural files are WAL-reverted by the rolled-back atomic
   * operation, but {@link #reconcileCollections} and the index build/drop phases mutated the
   * in-memory registries synchronously, so both sides are undone here. Called from the commit
   * finally's failure branch (after {@code rollback}) and from the {@code endTxCommit} failure
   * catch (where the failed {@code endAtomicOperation} already performed its own rollback
   * bookkeeping, so no second rollback call precedes this undo).
   *
   * <p>Arm order: the collection undo runs first (phantom creates dropped, then dropped
   * registrations restored — see {@link #undoReconciledCollections}); then, when an index plan
   * exists, the engine create-undo removes the phantom engine registrations (the engine files
   * revert with the rolled-back atomic operation on the disk profile; the in-memory profile leaves
   * them, so the undo also drops the surviving files, freeing the failed commit's engine ids for
   * reuse), the engine drop-restore reconstructs each dropped engine from its captured durable data
   * (deleteIndexEngineInCommitWindow tore it out of the registry synchronously; without the
   * reconstruction the surviving committed index would point at a nulled engine slot and throw on
   * the next read — running after the create-undo so a freed slot cannot collide with a restore),
   * and finally the eager in-memory membership mutations the enroll phase applied are reverted (the
   * record writes revert with the atomic operation, but the in-memory collectionsToIndex sets were
   * mutated synchronously).
   */
  private void undoSchemaCarryRegistryPublication(
      final SchemaCommitContext schemaContext,
      @Nullable final IndexManagerEmbedded.ReconciledIndexPlan indexPlan,
      final List<DroppedCollection> droppedCollections) {
    undoReconciledCollections(schemaContext.committedSchema(),
        schemaContext.txSchemaState(), droppedCollections);
    if (indexPlan != null) {
      undoReconciledIndexEngines(indexPlan.createdEngineExternalIds());
      restoreReconciledDroppedIndexEngines(indexPlan.droppedEngines());
      schemaContext.indexManager().undoAppliedMembership(indexPlan);
    }
  }

  /**
   * Restores the in-memory collection registry to its pre-commit state on a failed schema-carry
   * commit. The rolled-back atomic operation already reverts every structural file (created and
   * dropped), but {@link #reconcileCollections} mutated the in-memory registries synchronously, so
   * this undoes both sides: it removes the phantom registration of the collections the commit
   * created, and re-registers the collections the commit dropped. Runs under the held write lock.
   * Idempotent against a never-published id (the map lookup misses).
   *
   * <p>The create side also reverts the created collection's <em>structure</em>, not just its
   * in-memory registry entry. The disk engine's {@code readCache.addFile} buffers the file create as
   * WAL-reverted intent, so on a rollback the created collection's data file, config entry, and
   * link-bag B-tree component vanish with the atomic operation and only the in-memory registry needs
   * undoing. The in-memory engine eagerly installs every new file into its cache and does <em>not</em>
   * revert that install on rollback (see {@code AtomicOperationBinaryTracking.addFile}), so the
   * created collection's data file and its id-named {@code global_collection_<id>.grb} component
   * survive the rollback as orphans; the surviving component then blocks the next id-reusing create
   * with a "file already exists" error. This arm drops that orphaned structure in a fresh atomic
   * operation (the failed commit's operation is already closed by {@code rollback}), guarded on the
   * orphaned component still being present so it is a no-op on the disk engine (where rollback already
   * removed it) and the real cleanup on the in-memory engine. The fresh atomic operation commits
   * normally, so the orphan removal is itself durable.
   *
   * @param dropped the collections reconciliation dropped, in drop order; restored in memory here.
   */
  private void undoReconciledCollections(
      final SchemaShared committedSchema, final TxSchemaState txSchemaState,
      final List<DroppedCollection> dropped) {
    final var committedIds = committedSchema.getRealCollectionIds();
    // The slots this same reconciliation dropped. A commit that drops a committed collection frees
    // its slot for the commit-local allocator, so a created collection's resolved real id CAN
    // coincide with a committed id — exactly when that id is in this set (the committed schema is
    // promoted only on the success path, so on this failure path it still lists the dropped id as
    // committed).
    final var droppedIds = new IntOpenHashSet(dropped.size());
    for (final var entry : dropped) {
      droppedIds.add(entry.id());
    }
    for (final var realId : txSchemaState.getResolvedCollectionIds().values()) {
      // Only drop ids the commit created (resolved from a provisional), never a pre-existing
      // committed id. A resolved id that reads as committed but sits in droppedIds reused a slot
      // this same reconciliation freed: it is commit-created and must be undone here, so the
      // drop-restore loop below finds the slot null again for the original occupant.
      if (committedIds.contains(realId) && !droppedIds.contains((int) realId)) {
        continue;
      }
      final var collection = realId < collections.size() ? collections.get(realId) : null;
      if (collection != null) {
        collectionMap.remove(collection.getName().toLowerCase(Locale.ROOT));
        collections.set(realId, null);
        // Drop the created collection's surviving structure (in-memory engine only — see the method
        // Javadoc). The collection was just removed from the registry above, so pass the captured
        // object to the structural cleanup rather than re-reading the now-null slot. The cleanup
        // must know whether this create reused a slot this same reconciliation dropped: on that
        // path the slot-keyed structures (config entry, link-bag component) present after the
        // rollback belong to the restored dropped collection, and deleting them would durably
        // destroy the survivor's registration — see the slot-reuse branch inside.
        revertCreatedCollectionStructure(realId, collection, droppedIds.contains((int) realId));
      }
    }
    // Re-register each dropped collection: rollback restored its files, so the captured object is
    // consistent again. This is the mirror image of the create-undo arm above; without it a failed
    // commit that dropped a collection would leave the registry reporting it absent while it is
    // fully present on disk and in the storage configuration. The slot guard keeps the restore
    // idempotent if a later create somehow reused the freed slot.
    for (final var entry : dropped) {
      final var collection = entry.collection();
      if (collection == null) {
        continue;
      }
      if (entry.id() < collections.size() && collections.get(entry.id()) == null) {
        registerCollection(collection);
        // The drop also removed the collection's SharedLinkBagBTree from the link-bag manager's
        // in-memory map EAGERLY (deleteComponentByCollectionId is not rollback-aware; only the
        // file delete was buffered and reverted), and every link-bag access resolves through that
        // map with no reload branch — without this restore the re-registered collection's first
        // link-bag operation fails until a reopen rebuilds the map. Best-effort like the
        // surrounding cleanup: a failure here is logged, never thrown over the propagating commit
        // failure. The read-only operation is the lightweight snapshot-only kind (started and
        // deactivated, never applied), so it works even when the storage is already in error
        // state (the error gate covers component-write operations only).
        try {
          final var restoreReadOperation = atomicOperationsManager.startAtomicOperation();
          try {
            linkCollectionsBTreeManager.restoreComponentByCollectionId(
                restoreReadOperation, entry.id());
          } finally {
            restoreReadOperation.deactivate();
          }
        } catch (final RuntimeException | AssertionError linkBagRestoreFailure) {
          LogManager.instance()
              .error(this,
                  "Failed to restore the link-bag component registration of collection '%s'"
                      + " (slot %d) during failed-commit undo; link-bag operations on the restored"
                      + " collection will fail until the storage reopens",
                  linkBagRestoreFailure, collection.getName(), entry.id());
        }
      } else {
        // Unreachable by construction: the create-undo arm above ran first (freeing every id this
        // commit created) and the whole undo holds stateLock.writeLock(), so a dropped collection's
        // original slot is in range and null here. The assert pins that invariant at zero production
        // cost during testing. If it is somehow violated in production, do NOT register over the slot
        // (that would leak the occupant) and do NOT throw: undoReconciledCollections runs in the
        // failure-path finally while the original commit exception propagates, so a throw would mask
        // the real failure. Log loudly so the in-memory-registry inconsistency is diagnosable, then
        // leave the slot alone, mirroring the log-and-swallow stance of the surrounding cleanup.
        assert entry.id() < collections.size() && collections.get(entry.id()) == null
            : "drop-restore slot " + entry.id() + " is out of range or occupied";
        LogManager.instance()
            .error(this,
                "Cannot re-register dropped collection '%s' on slot %d during failed-commit undo:"
                    + " the slot is out of range or occupied; the collection is present on disk but"
                    + " absent from the in-memory registry",
                null, collection.getName(), entry.id());
      }
    }
  }

  /**
   * Drops the surviving structure of a collection that a failed schema-carry commit created: the
   * link-bag B-tree component, the storage-config entry, and the collection's own data file. The
   * mirror of {@link #doCreateCollection}, which writes exactly those three structures. Runs in a
   * fresh atomic operation because the failed commit's operation is already closed by {@code
   * rollback}. Guarded on the orphaned component still being present so it is a no-op on the disk
   * engine (where rollback reverted every structure already) and the real cleanup on the in-memory
   * engine (where the eager cache install survived the rollback). See {@link
   * #undoReconciledCollections} for why only the in-memory engine leaves an orphan.
   *
   * <p>Best-effort and self-contained: this runs from the failure-path {@code finally} while the
   * original commit exception is propagating, so any failure here is logged and swallowed rather than
   * thrown, exactly as the success-path snapshot cleanup is. A leaked orphan is a bounded
   * single-collection leak, not a correctness break; masking the real commit failure would be worse.
   *
   * <p><b>Slot-reuse path ({@code slotReused}).</b> When the create reused a slot this same
   * reconciliation dropped, the id-keyed discriminator and the id-keyed deletes below are all
   * WRONG: after the rollback the {@code global_collection_<id>.grb} component present under this
   * id is the restored dropped collection's own file (the failed operation's component "create"
   * merely resurrected the buffered-deleted file id — {@code addFile} un-deletes rather than
   * eagerly installing — so it reverted with the rollback on both engines), and the slot's config
   * entry is the restored collection's registration. Deleting them here would durably destroy the
   * surviving collection's config entry and link-bag B-tree while the drop-restore arm re-registers
   * it in memory — permanent damage, not cleanup. On this path only the created collection's own
   * name-keyed data files can survive the rollback (the in-memory engine's eager install), so the
   * cleanup reduces to deleting those, guarded by their presence ({@code exists} is name-keyed, so
   * it answers false on the disk engine where the rollback reverted the create).
   *
   * @param realId     the created collection's resolved real id (its slot, already nulled by the
   *                   caller).
   * @param collection the captured collection object removed from the registry, used to delete its
   *                   data file.
   * @param slotReused whether {@code realId} is a slot this same reconciliation dropped (the
   *                   created collection reused the freed slot).
   */
  private void revertCreatedCollectionStructure(final int realId,
      final StorageCollection collection, final boolean slotReused) {
    // Enters the freezer UNARMED (the internal atomic-operation wrapper below) while holding the
    // metadata-write mutex, the schema and index-manager write locks, and stateLock.writeLock.
    // Never parks under an operator freeze: an operator freeze can only arm under
    // stateLock.readLock (see freeze()), which the held write lock excludes for this whole
    // window. If that premise ever changes, this undo could park unbounded here with no gate.
    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> {
            if (slotReused) {
              // Slot-reuse: every id-keyed structure at this slot now belongs to the restored
              // dropped collection (see Javadoc). Only the created collection's own name-keyed
              // data files can be orphaned, and only on the in-memory engine; the name-keyed
              // exists() probe is false on the disk engine, where the rollback reverted the
              // create entirely.
              if (collection.exists(atomicOperation)) {
                collection.delete(atomicOperation);
              }
              return;
            }
            // The component file (global_collection_<id>.grb) is the only structure keyed by id and
            // the one that blocks an id-reusing create, so its presence is the in-memory-vs-disk
            // discriminator: present means the rollback did not revert the eager cache install, so
            // this collection's structure is orphaned and must be dropped; absent means the disk
            // engine already reverted everything and there is nothing to do. (This discriminator is
            // sound only for a fresh slot: the slot-reuse path above never reaches it, because there
            // the id-named component present after rollback is the restored dropped collection's.)
            if (!LinkCollectionsBTreeManagerShared.isComponentPresent(atomicOperation, realId)) {
              return;
            }
            // Delete the data file, the config entry, and the component on this fresh atomic
            // operation, mirroring doCreateCollection in reverse. The collection was already removed
            // from the in-memory registry by the caller, so this only reclaims on-disk/in-cache
            // structure.
            collection.delete(atomicOperation);
            ((CollectionBasedStorageConfiguration) configuration)
                .dropCollection(atomicOperation, realId);
            linkCollectionsBTreeManager.deleteComponentByCollectionId(atomicOperation, realId);
          });
    } catch (final IOException | RuntimeException | AssertionError e) {
      // Best-effort: never let a cleanup failure mask the original commit exception that is
      // propagating through the failure-path finally. AssertionError is caught for symmetry with the
      // success-path snapshot cleanup, so a -ea-only assert along the cleanup path cannot escape and
      // replace the real error.
      LogManager.instance()
          .warn(this,
              "Failed to revert orphaned structure of collection id " + realId
                  + " after a failed schema commit; a bounded single-collection leak may remain",
              e);
    }
  }

  /**
   * The first null slot in {@code indexEngines}, or the list size when none is free — the
   * commit-local index-engine-id allocator's next id. The index analogue of {@link
   * #nextFreeCollectionId()}: it lets a failed engine-creating commit free its id for reuse on the
   * next commit, so the failed-commit registry-cleanliness guarantee extends from collections to
   * engines. Seeded by a direct read of the shared {@code indexEngines} list, which is safe because
   * the caller holds {@code stateLock.writeLock()} inside the commit window. Reused between successive
   * creates within one reconciliation because each create publishes its engine into the slot before
   * the next call scans.
   */
  private int nextFreeIndexEngineId() {
    for (var i = 0; i < indexEngines.size(); i++) {
      if (indexEngines.get(i) == null) {
        return i;
      }
    }
    return indexEngines.size();
  }

  /**
   * Creates an index engine inside the schema-carry commit window at a commit-local first-null-slot
   * id, publishes it into the in-memory registries, and returns its external id. The commit-window
   * analogue of {@link #addIndexEngine(IndexMetadata, Map)}: it takes no {@code stateLock} (the
   * caller already holds {@code stateLock.writeLock()}, and re-acquiring the non-reentrant lock would
   * busy-spin), runs inside the commit's own atomic operation so the engine files are buffered as
   * WAL-reverted intent, and allocates from a freed slot so a failed commit reuses the id. The
   * registry publish is eager (mirroring how {@link #reconcileCollections} publishes a created
   * collection), and a failed commit reverts it through {@link #undoReconciledIndexEngines}.
   *
   * <p>Must be called with {@code stateLock.writeLock()} held and the commit window open.
   *
   * @param indexMetadata   the engine's metadata (name, definition, collections, type, algorithm).
   * @param engineProperties per-engine properties passed to the engine factory.
   * @param atomicOperation the in-flight commit atomic operation that buffers the file creates.
   * @return the external (API-version-tagged) index id the built engine registered under.
   */
  public int createIndexEngineInCommitWindow(
      final IndexMetadata indexMetadata,
      final Map<String, String> engineProperties,
      final AtomicOperation atomicOperation)
      throws IOException {
    assert isCommitWindowActive()
        : "commit-window primitive called outside the commit window (stateLock.writeLock() not"
            + " held)";
    final var indexDefinition = indexMetadata.getIndexDefinition();
    if (indexDefinition == null) {
      throw new IndexException(name, "Index definition has to be provided");
    }
    final var keyTypes = indexDefinition.getTypes();
    if (keyTypes == null) {
      throw new IndexException(name, "Types of indexed keys have to be provided");
    }
    if (indexEngineNameMap.containsKey(indexMetadata.getName())) {
      throw new IndexException(name,
          "Index with name " + indexMetadata.getName() + " already exists");
    }
    makeStorageDirty();

    final var keySerializer = determineKeySerializer(indexDefinition, atomicOperation);
    if (keySerializer == null) {
      throw new IndexException(name, "Can not determine key serializer");
    }
    final var keySize = determineKeySize(indexDefinition);
    final var ctxCfg = configuration.getContextConfiguration();
    final var cfgEncryptionKey =
        ctxCfg.getValueAsString(GlobalConfiguration.STORAGE_ENCRYPTION_KEY);
    // A commit-local first-null-slot id, so a failed commit's engine id frees for reuse.
    final var generatedId = nextFreeIndexEngineId();
    assert generatedId >= indexEngines.size() || indexEngines.get(generatedId) == null
        : "commit-local index-engine allocator returned an occupied slot " + generatedId;
    discardIndexDeltas(atomicOperation, generatedId);
    // Unlike the reusable slot id above, the file base id is never reused: allocated from the
    // monotonic high-water mark inside this commit's atomic operation (the floor write reverts
    // with a failed commit; the mark itself burns the value).
    final var fileBaseId = allocateIndexEngineFileBaseId(atomicOperation);
    final var engineData =
        new IndexEngineData(
            generatedId,
            fileBaseId,
            indexMetadata,
            true,
            StreamSerializerRID.INSTANCE.getId(),
            keySerializer.getId(),
            keyTypes,
            keySize,
            null,
            cfgEncryptionKey,
            engineProperties);

    final var engine = doAddIndexEngine(atomicOperation, engineData);
    publishIndexEngine(engineData.getIndexId(), engine);
    return generateIndexId(engineData.getIndexId(), engine);
  }

  /**
   * A dropped engine captured by {@link #deleteIndexEngineInCommitWindow} at drop time, so a failed
   * schema-carry commit can reconstruct it. The id is the dropped engine's slot; the data is the
   * engine's durable {@link IndexEngineData} read from the storage configuration <em>before</em> the
   * delete's irreversible in-memory teardown, so a fresh engine can be rebuilt from it on the failure
   * path (the delete's own {@code IndexHistogramManager} teardown makes the live engine object
   * unusable, so the failure path never re-publishes the torn object).
   */
  public record DroppedIndexEngine(
      int internalId,
      IndexEngineData data,
      @Nullable RID ownerDescriptorIdentity) {

  }

  /**
   * Deletes an index engine inside the schema-carry commit window: runs the WAL-reverted file delete
   * on the commit's atomic operation and removes the engine from the in-memory registries. The
   * commit-window analogue of {@link #deleteIndexEngine(int)}: it takes no {@code stateLock} (the
   * caller holds {@code stateLock.writeLock()}). Unlike the public method, the map mutation runs
   * synchronously here — the whole commit rolls back atomically on failure. The registry removal is
   * therefore restored by {@link #restoreReconciledDroppedIndexEngines}: the caller records the
   * returned {@link DroppedIndexEngine} on the plan and, on a failed commit, hands the captured list
   * to that arm, which reconstructs a fresh engine from the captured durable data (the engine files
   * revert with the rolled-back atomic operation on the disk profile, so the durable state the
   * reconstruction reads is the pre-commit state again).
   *
   * <p>The durable {@link IndexEngineData} is captured before {@link #doDeleteIndexEngine} because the
   * delete removes the storage-configuration entry the reconstruction reads, and the live engine
   * object's histogram-manager teardown makes it unusable for re-publication.
   *
   * <p>Must be called with {@code stateLock.writeLock()} held and the commit window open.
   *
   * @param externalIndexId the external (API-version-tagged) id of the engine to delete.
   * @param atomicOperation the in-flight commit atomic operation that buffers the file deletes.
   * @return the captured dropped engine (slot + durable data) for the failure-path reconstruction.
   */
  public DroppedIndexEngine deleteIndexEngineInCommitWindow(
      final int externalIndexId, @Nullable IndexEngineReference expectedReference,
      final AtomicOperation atomicOperation)
      throws IOException, InvalidIndexEngineIdException {
    assert isCommitWindowActive()
        : "commit-window primitive called outside the commit window";
    final var internalIndexId = extractInternalId(externalIndexId);
    checkOpennessAndMigration();
    makeStorageDirty();
    return deleteIndexEngineInCommitWindow(
        internalIndexId, engineForDereference(internalIndexId, expectedReference), atomicOperation);
  }

  public DroppedIndexEngine deleteIndexEngineInCommitWindow(
      final int externalIndexId, final AtomicOperation atomicOperation)
      throws IOException, InvalidIndexEngineIdException {
    assert isCommitWindowActive()
        : "commit-window primitive called outside the commit window (stateLock.writeLock() not"
            + " held)";
    final var internalIndexId = extractInternalId(externalIndexId);
    checkOpennessAndMigration();
    makeStorageDirty();
    return deleteIndexEngineInCommitWindow(
        internalIndexId, engineByIdentifier(internalIndexId), atomicOperation);
  }

  private DroppedIndexEngine deleteIndexEngineInCommitWindow(
      final int internalIndexId, final BaseIndexEngine engine,
      final AtomicOperation atomicOperation) throws IOException {
    assert internalIndexId == engine.getId();

    // Capture the durable engine data before the delete removes the config entry it reads. The
    // captured data (not the torn live engine) drives the failure-path reconstruction. Copy the
    // owner because commit apply can mutate its ChangeableRecordId in place after this capture.
    final var ownerDescriptorIdentity = engine.getEngineReference() == null
        ? null
        : engine.getEngineReference().ownerDescriptorIdentity();
    final var copiedOwnerDescriptorIdentity = ownerDescriptorIdentity == null
        ? null
        : new RecordId(ownerDescriptorIdentity.getCollectionId(),
            ownerDescriptorIdentity.getCollectionPosition());
    final var capturedData =
        configuration.getIndexEngine(engine.getName(), internalIndexId, atomicOperation);

    discardIndexDeltas(atomicOperation, internalIndexId);
    doDeleteIndexEngine(atomicOperation, engine);
    detachHistogramManager(engine);
    indexEngines.set(internalIndexId, null);
    indexEngineNameMap.remove(engine.getName());
    return new DroppedIndexEngine(internalIndexId, capturedData, copiedOwnerDescriptorIdentity);
  }

  /**
   * Restores the in-memory engine registries to their pre-commit state on a failed schema-carry
   * commit that created engines. The rolled-back atomic operation reverts every engine file, but
   * {@link #createIndexEngineInCommitWindow} published the engine synchronously, so this removes each
   * phantom registration. The mirror of {@link #undoReconciledCollections}'s create-undo arm for
   * engines. Runs under the held write lock. Idempotent against a never-published id (the slot read
   * misses).
   *
   * <p>Like the collection arm, the create side also reverts the engine's <em>files</em> on the
   * in-memory profile, which does not revert an eager engine-file {@code addFile} on rollback: the
   * disk engine's rollback already removed the files, but the in-memory engine leaves them, so the
   * surviving files would block the next id-reusing create. This arm drops the surviving engine files
   * in a fresh atomic operation, guarded on the engine still being present, so it is a no-op on the
   * disk engine and the real cleanup on the in-memory engine.
   *
   * @param createdEngineExternalIds the external ids of the engines the failed commit created.
   */
  private void undoReconciledIndexEngines(final List<Integer> createdEngineExternalIds) {
    for (final var externalId : createdEngineExternalIds) {
      final var internalId = extractInternalId(externalId);
      final var engine =
          internalId >= 0 && internalId < indexEngines.size()
              ? indexEngines.get(internalId)
              : null;
      if (engine == null) {
        continue;
      }
      detachHistogramManager(engine);
      indexEngines.set(internalId, null);
      indexEngineNameMap.remove(engine.getName());
      revertCreatedIndexEngineStructure(engine);
    }
  }

  void bindOwnerAndPublishRestoredIndexEngine(
      int indexId, BaseIndexEngine engine, @Nullable RID ownerDescriptorIdentity) {
    if (ownerDescriptorIdentity != null) {
      final var engineReference = engine.getEngineReference();
      if (engineReference != null) {
        engineReference.bindOwner(ownerDescriptorIdentity);
      } else {
        // Publish reference-less engines because preserving the surviving committed index is safer
        // than turning optional process-local ownership metadata into a restore failure.
        LogManager.instance().warn(
            this,
            "Restored index engine '" + engine.getName()
                + "' exposes no process-local reference. Owner " + ownerDescriptorIdentity
                + " was not bound");
      }
    }
    publishIndexEngine(indexId, engine);
  }

  /**
   * Restores the in-memory engine registry for every engine a failed schema-carry commit dropped, the
   * drop-side mirror of {@link #undoReconciledCollections}'s re-register arm. {@link
   * #deleteIndexEngineInCommitWindow} tore the engine out of {@code indexEngines} /
   * {@code indexEngineNameMap} synchronously, so a failed commit must put it back or the surviving
   * committed index would point at a nulled engine slot and throw on the next read. The rolled-back
   * atomic operation reverts the engine's files on the disk profile, so the durable state this arm
   * reads is the pre-commit state again; a fresh engine is reconstructed from the captured durable
   * {@link IndexEngineData} through the same {@code Indexes.createIndexEngine} + {@code engine.load}
   * path {@link #openIndexes} uses at startup, and re-registered at the original slot.
   *
   * <p>A fresh engine object is rebuilt rather than re-publishing the torn one: {@code
   * deleteIndexEngineInCommitWindow} ran the engine's own {@code delete}, which for a B-tree engine
   * permanently marks its histogram manager's rebalance flag and clears its caches, so the torn object
   * would spin forever in the histogram rebalance CAS loop if re-published.
   *
   * <p>Deterministic invariant, not best-effort: after this arm runs the surviving committed index
   * MUST be fully usable again. The only best-effort part is failure containment — this runs in the
   * failure-path {@code finally} while the original commit exception propagates, so a reconstruction
   * that itself hits an unexpected error is logged loudly and never thrown (a throw — including an
   * {@code -ea} assert — would mask the primary commit failure and skip the remaining undo arms).
   * When the storage is already in error state (a non-retry-family primary failure poisoned it
   * before this undo ran), the reconstruction's component operations fail fast by design and the
   * forced reopen is the healer. Runs under the held write lock.
   *
   * @param droppedEngines the engines the failed commit dropped (slot + captured durable data).
   */
  private void restoreReconciledDroppedIndexEngines(final List<DroppedIndexEngine> droppedEngines) {
    // Enters the freezer UNARMED while holding the metadata-write mutex, both metadata write
    // locks, and stateLock.writeLock; safe from an operator-freeze park because a freeze arms
    // only under stateLock.readLock (see freeze()), excluded here by the held write lock.
    for (final var dropped : droppedEngines) {
      final var internalId = dropped.internalId();
      final var engineData = dropped.data();
      if (engineData == null) {
        // No captured durable data means there was nothing to reconstruct (a drop whose config read
        // returned null before the delete). Nothing to restore; the divergence self-corrects on the
        // next reopen if it ever occurs. Logged, NOT assert-thrown: this loop runs while the
        // primary commit exception propagates, and an -ea AssertionError here would replace that
        // primary failure and skip the remaining restore arms plus the membership undo.
        LogManager.instance()
            .error(this,
                "Dropped index engine at slot %d had no captured durable data during"
                    + " failed-commit undo; nothing to reconstruct",
                null, internalId);
        continue;
      }
      // Idempotent against a slot a later create-undo already left null-and-reusable: only reconstruct
      // when the slot is actually empty, so a restore never overwrites a live registration.
      if (internalId >= 0 && internalId < indexEngines.size()
          && indexEngines.get(internalId) != null) {
        continue;
      }
      try {
        atomicOperationsManager.executeInsideAtomicOperation(
            atomicOperation -> {
              final var engine = Indexes.createIndexEngine(this, engineData);
              engine.load(engineData, atomicOperation);
              if (engine instanceof BTreeIndexEngine btreeEngine) {
                wireHistogramManagerOnLoad(btreeEngine, engineData, atomicOperation);
              }
              bindOwnerAndPublishRestoredIndexEngine(
                  engineData.getIndexId(), engine, dropped.ownerDescriptorIdentity());
            });
      } catch (final IOException | RuntimeException | AssertionError e) {
        // Loud failure containment: the reconstruction runs while the primary commit exception is
        // propagating, so it must not throw a secondary exception (including an -ea
        // AssertionError) that masks the primary and skips the remaining restore arms plus the
        // caller's trailing membership undo. Log at error and continue; the log is the surfacing.
        // A failure here is EXPECTED when the primary failure already moved the storage to error
        // state (a non-retry-family lifecycle failure poisons before this undo runs, and the
        // reconstruction's component operations then fail fast on checkErrorState) — in that case
        // the reopen the poison forces is the designated healer. Outside the poisoned case a
        // reconstruction failure is a real defect (the surviving committed index is unusable
        // until the next reopen), diagnosed from this log rather than a masking throw.
        LogManager.instance()
            .error(this,
                "Failed to reconstruct dropped index engine '"
                    + engineData.getName() + "' (slot " + internalId + ") after a failed schema"
                    + " commit; the surviving committed index is unusable until the storage"
                    + " reopens"
                    + (isInError()
                        ? " (storage already in error state; the forced reopen will heal it)"
                        : ""),
                e);
      }
    }
  }

  /**
   * Drops the surviving files of an engine a failed schema-carry commit created, mirroring {@link
   * #revertCreatedCollectionStructure}. Runs in a fresh atomic operation because the failed commit's
   * operation is already closed by {@code rollback}. Guarded on the engine name still being present
   * in the storage configuration so it is a no-op on the disk engine (where rollback reverted the
   * files) and the real cleanup on the in-memory engine (where the eager cache install survived).
   * Best-effort: a failure here is logged and swallowed rather than thrown, so it never masks the
   * original commit exception propagating through the failure-path finally.
   *
   * @param engine the captured engine removed from the registry, used to delete its files.
   */
  // Package-private for tests: the guarded config-delete and the non-B-tree skip are
  // failure-path arms that production only reaches through an injected commit fault.
  void revertCreatedIndexEngineStructure(final BaseIndexEngine engine) {
    // Enters the freezer UNARMED while holding the metadata-write mutex, both metadata write
    // locks, and stateLock.writeLock; safe from an operator-freeze park because a freeze arms
    // only under stateLock.readLock (see freeze()), excluded here by the held write lock.
    // The file family is keyed by the engine's stable file base id, so only an engine that
    // carries one (every local B-tree engine) can be swept for survivors. A non-B-tree engine
    // cannot be created by the local commit window, so the conservative skip is unreachable in
    // production. Deliberately a logged warning, NOT an assert: this runs on the failure path
    // while the primary commit exception propagates, and an -ea AssertionError thrown here would
    // mask that primary failure.
    if (!(engine instanceof BTreeIndexEngine btreeEngine)) {
      LogManager.instance()
          .warn(this,
              "Failed-commit engine cleanup reached a non-B-tree engine '" + engine.getName()
                  + "'; its surviving files (if any) cannot be swept and may leak",
              null);
      return;
    }
    try {
      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> {
            // The surviving engine data file is the in-memory-vs-disk discriminator, mirroring the
            // collection arm's isComponentPresent check. The old guard tested
            // configuration.indexEngines(...).contains(name), the storage-configuration entry — but
            // that entry reverts with the rolled-back atomic operation on BOTH profiles (the in-memory
            // config cache reverts its eager put too), so on the in-memory profile it returned false
            // while the eager-installed engine files survived, and the guard skipped the cleanup that
            // was its whole reason to exist. A physical-file-presence check is true only when the
            // files actually survived: false on the disk profile (rollback removed them) and true on
            // the in-memory profile (the eager cache install survived), so the cleanup runs exactly
            // where an orphan exists.
            if (!engineFilesPresent(btreeEngine.getFileBaseId())) {
              return;
            }
            // Ownership asymmetry drives the two deletes below — and this fresh cleanup
            // operation COMMITS, so a wrong delete here is durable. The engine FILES are
            // fileBaseId-keyed and file base ids are never reused, so surviving ie_* files
            // provably belong to the reverted engine: delete them unconditionally. The config
            // ENTRY is name-keyed, and after the failed commit's rollback the entry under this
            // engine's name may not be this engine's at all: it is either absent (pure failed
            // create — the entry write reverted with the rolled-back operation) or the restored
            // OLD engine's entry (a failed commit that dropped and re-created the same index
            // name). An unconditional name-keyed delete would durably clobber that restored
            // entry while the drop-restore arm re-publishes the old engine in memory —
            // config/registry divergence that loses the committed index's engine at the next
            // reopen. Hence: unconditional file delete, fileBaseId-gated entry delete (the entry
            // is removed only when its stored fileBaseId proves it belongs to the reverted
            // engine).
            engine.delete(atomicOperation);
            final var storedEntry =
                configuration.getIndexEngine(engine.getName(), -1, atomicOperation);
            if (storedEntry != null
                && storedEntry.getFileBaseId() == btreeEngine.getFileBaseId()) {
              ((CollectionBasedStorageConfiguration) configuration)
                  .deleteIndexEngine(atomicOperation, engine.getName());
            }
          });
    } catch (final IOException | RuntimeException | AssertionError e) {
      LogManager.instance()
          .warn(this,
              "Failed to revert orphaned files of index engine '" + engine.getName()
                  + "' after a failed schema commit; a bounded single-engine leak may remain",
              e);
    }
  }

  /**
   * Whether any physical file of the engine with the given file base id survives in the write
   * cache — the in-memory-vs-disk discriminator for the failed-commit engine cleanup arms, the
   * engine analogue of the collection arm's
   * {@code LinkCollectionsBTreeManagerShared.isComponentPresent}. Every file of an engine's
   * family carries the {@code ie_<fileBaseId>} stem (the multi-value null tree appends
   * {@link #NULL_TREE_SUFFIX}), so the check strips each file name's extension and matches the
   * remaining stem exactly — keyed by the stable id, never by parsing an index name out of a
   * file name. The write cache reflects the committed physical files: the eager cache install
   * survives a rollback on the in-memory profile and is reverted on the disk profile, so a
   * surviving stem match is exactly an orphan to clean up.
   *
   * <p>Matching is restricted to the engine file-extension family (see
   * {@link #isIndexEngineFileExtension}): a user artifact whose name happens to share the stem
   * shape (e.g. a collection named {@code ie_7}, whose files carry collection extensions) must
   * never false-positive the cleanup arm into a spurious delete attempt.
   *
   * <p>Package-private for tests.
   *
   * @param fileBaseId the dropped/created engine's stable file base id.
   * @return {@code true} when at least one file of the engine's family survives.
   */
  boolean engineFilesPresent(final int fileBaseId) {
    final var wc = writeCache;
    if (wc == null) {
      return false;
    }
    final var stem = indexEngineFileStem(fileBaseId);
    final var nullTreeStem = stem + NULL_TREE_SUFFIX;
    for (final var fileName : wc.files().keySet()) {
      final var extensionStart = fileName.lastIndexOf('.');
      if (extensionStart <= 0) {
        continue;
      }
      if (!isIndexEngineFileExtension(fileName.substring(extensionStart))) {
        continue;
      }
      final var fileStem = fileName.substring(0, extensionStart);
      if (fileStem.equals(stem) || fileStem.equals(nullTreeStem)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether {@code extension} (with the leading dot) is one an {@code ie_<fileBaseId>}-stemmed
   * engine file can carry: the B-tree data file, its null bucket, or the histogram stats file.
   * Both the open-time HWM sweep and the failed-commit file-presence check are restricted to
   * this family so files of other components (or user artifacts) that merely share the stem
   * shape are never mistaken for engine files.
   */
  private static boolean isIndexEngineFileExtension(final String extension) {
    return BTreeMultiValueIndexEngine.DATA_FILE_EXTENSION.equals(extension)
        || BTreeMultiValueIndexEngine.NULL_BUCKET_FILE_EXTENSION.equals(extension)
        || IndexHistogramManager.IXS_EXTENSION.equals(extension);
  }

  /**
   * A test-only hook fired inside the schema-carry commit window, after structure is published and
   * before the record apply, while {@code stateLock.writeLock()} is held and the commit window is
   * open. Null in production (no production caller sets it). Tests install a hook to inject a fault
   * at the published-but-not-applied point (so the failure-path undo runs and its registry
   * cleanliness can be asserted) or to latch the commit thread inside the held write lock (so a
   * concurrent pure-data commit can be observed proceeding on the read-lock path). {@code volatile}
   * because the installing thread and the committing thread may differ in a concurrent test.
   */
  private volatile Runnable commitWindowTestHook;

  /**
   * Installs (or clears, with {@code null}) the test-only in-window commit hook. Test-only seam — no
   * production code sets a hook, so production behaviour is unchanged. See {@link
   * #commitWindowTestHook}.
   */
  public void setCommitWindowTestHook(final Runnable hook) {
    this.commitWindowTestHook = hook;
  }

  /**
   * A test-only hook fired inside the schema-carry commit window <em>after</em> the engine build/drop
   * phase (engines built, published, and dropped) and before the record apply commits, while {@code
   * stateLock.writeLock()} is held and the commit window is open. Null in production. Unlike {@link
   * #commitWindowTestHook} (which fires before any engine is created), this hook fires at the
   * engines-published-but-not-durable point, so a fault injected here drives the failure-path engine
   * undo and drop-restore arms over a non-empty {@code createdEngineExternalIds} / {@code
   * droppedEngines} — the only point where those arms have work to do. {@code volatile} because the
   * installing and committing threads may differ in a concurrent test.
   */
  private volatile Runnable postEngineBuildTestHook;

  /**
   * Installs (or clears, with {@code null}) the test-only post-engine-build commit hook. Test-only
   * seam — no production code sets a hook. See {@link #postEngineBuildTestHook}.
   */
  public void setPostEngineBuildTestHook(final Runnable hook) {
    this.postEngineBuildTestHook = hook;
  }

  /**
   * A test-only hook fired at the top of {@code endTxCommit}, i.e. after every reconcile phase and
   * the record apply, at the point where the commit's {@code endAtomicOperation} lifecycle call can
   * still fail (a lifecycle persist-hook failure, a {@code commitChanges} throw). A hook that
   * throws a {@link RuntimeException} is routed through {@code endAtomicOperation(operation,
   * error)} first — rolling the operation back with the normal freezer/lock teardown — and then
   * rethrown, reproducing exactly the state a real endTxCommit failure leaves behind. Tests use it
   * to verify the commit finally's endTxCommit catch runs the in-memory registry undo/restore arms
   * instead of propagating the failure uncaught. Null in production. {@code volatile} because the
   * installing and committing threads may differ in a concurrent test.
   */
  private volatile Runnable endTxCommitFailureTestHook;

  /**
   * Installs (or clears, with {@code null}) the test-only endTxCommit failure hook. Test-only seam
   * — no production code sets a hook. See {@link #endTxCommitFailureTestHook}.
   */
  public void setEndTxCommitFailureTestHook(final Runnable hook) {
    this.endTxCommitFailureTestHook = hook;
  }

  /**
   * A test-only hook fired inside {@code endTxCommit} <em>after</em> a successful
   * {@code endAtomicOperation}. A hook that throws simulates the commitChanges-throw failure
   * shape: the failure escapes {@code endTxCommit} with the operation fully ended and
   * {@code isRollbackInProgress()} {@code false} — the no-internal-rollback state whose durability
   * the commit finally's catch must treat as in-doubt (registry publication left standing, storage
   * moved to error state) instead of undoing. The only delta from the real shape is that here the
   * commit is certainly durable, which is the conservative half of in-doubt. Null in production.
   * {@code volatile} because the installing and committing threads may differ in a concurrent
   * test.
   */
  private volatile Runnable endTxCommitPostDurabilityFailureTestHook;

  /**
   * Installs (or clears, with {@code null}) the test-only post-durability endTxCommit failure
   * hook. Test-only seam — no production code sets a hook. See {@link
   * #endTxCommitPostDurabilityFailureTestHook}.
   */
  public void setEndTxCommitPostDurabilityFailureTestHook(final Runnable hook) {
    this.endTxCommitPostDurabilityFailureTestHook = hook;
  }

  /**
   * Whether the named engine is registered in both {@code indexEngineNameMap} and {@code
   * indexEngines}, read lock-free for a caller already inside the commit window. The public {@link
   * #loadIndexEngine(String)} takes {@code stateLock.readLock()}, which busy-spins forever when the
   * schema-carry commit already holds the non-reentrant write lock, so a test hook firing inside the
   * commit window must use this window-aware probe instead. Test-only: it is the assertion seam that
   * proves an engine actually exists at the fault point, so a failed-commit test cannot pass
   * vacuously. Must be called with {@code stateLock.writeLock()} held and the commit window open.
   *
   * @param engineName the engine name to probe.
   * @return {@code true} when a registered engine of that name exists.
   */
  public boolean isIndexEngineRegisteredInCommitWindow(final String engineName) {
    assert isCommitWindowActive()
        : "commit-window primitive called outside the commit window (stateLock.writeLock() not"
            + " held)";
    return indexEngineNameMap.containsKey(engineName);
  }

  private void commitIndexes(DatabaseSessionEmbedded db, AtomicOperation atomicOperation,
      final Map<String, FrontendTransactionIndexChanges> indexesToCommit) {
    for (final var changes : indexesToCommit.values()) {
      var index = changes.getIndex();
      try {
        if (changes.cleared) {
          if (index instanceof IndexAbstract handle) {
            final var snapshot = handle.engineSnapshot();
            if (snapshot.engineIdentifier() < 0) {
              throw new StaleIndexEngineException(
                  name, "Index engine became unavailable before index clear");
            }
            final var internalIndexId = extractInternalId(snapshot.engineIdentifier());
            doClearIndex(
                atomicOperation, internalIndexId,
                engineForDereference(internalIndexId, snapshot.engineReference()));
          } else {
            doClearIndex(atomicOperation, extractInternalId(index.getIndexId()));
          }
        }

        for (final var changesPerKey : changes.changesPerKey.values()) {
          applyTxChanges(db, changesPerKey, index);
        }

        applyTxChanges(db, changes.nullKeyChanges, index);
      } catch (final IndexEngineReplacedException exception) {
        final var stale = new StaleIndexEngineException(
            name, "Index engine changed during index commit");
        stale.initCause(exception);
        throw stale;
      } catch (final InvalidIndexEngineIdException exception) {
        final var stale = new StaleIndexEngineException(
            name, "Index engine became unavailable during index commit");
        stale.initCause(exception);
        throw stale;
      }
    }
  }

  private void applyTxChanges(DatabaseSessionEmbedded session,
      FrontendTransactionIndexChangesPerKey changes, Index index)
      throws InvalidIndexEngineIdException {
    assert !(changes.key instanceof RID orid) || orid.isPersistent();
    for (var op : index.interpretTxKeyChanges(changes)) {
      switch (op.getOperation()) {
        case PUT -> index.doPut(session, this, changes.key, op.getValue().getIdentity());
        case REMOVE -> {
          if (op.getValue() != null) {
            index.doRemove(session, this, changes.key, op.getValue().getIdentity());
          } else {
            index.doRemove(this, changes.key, session);
          }
        }
        case CLEAR -> {
          // SHOULD NEVER BE THE CASE HANDLE BY cleared FLAG
        }
      }
    }
  }

  /**
   * Persists accumulated index count deltas to the BTree entry point pages
   * within the current WAL atomic operation. Called between
   * {@code commitIndexes()} and the catch clause in the commit flow, so any
   * failure triggers the existing rollback path.
   *
   * <p>Mirrors the defensive checks of {@link #applyIndexCountDeltas}: skips
   * engines with out-of-bounds IDs, null entries, or non-{@link BTreeIndexEngine}
   * instances (e.g., if an engine was concurrently dropped, leaving a stale delta).
   */
  public void persistIndexCountDeltas(AtomicOperation atomicOperation) {
    var holder = atomicOperation.getIndexCountDeltas();
    if (holder == null) {
      return;
    }
    for (var entry : holder.getDeltas().int2ObjectEntrySet()) {
      int engineId = entry.getIntKey();
      var delta = entry.getValue();
      if (engineId >= 0 && engineId < indexEngines.size()) {
        var engine = indexEngines.get(engineId);
        if (engine instanceof BTreeIndexEngine btreeEngine) {
          btreeEngine.persistCountDelta(
              atomicOperation, delta.getTotalDelta(), delta.getNullDelta());
        }
      }
    }
    // Latch the holder so the lifecycle persist hook in
    // AtomicOperationsManager.endAtomicOperation short-circuits the second
    // pass on the same atomic operation. Defensive belt against any future
    // re-entry into persist within the same atomic operation, for example a
    // nested or mistakenly-replayed lifecycle pass.
    holder.setPersisted();
  }

  /**
   * Applies index entry count deltas accumulated during the transaction to
   * the engines' in-memory {@code AtomicLong} counters. Called after
   * {@code endTxCommit()} succeeds so that counters always reflect committed
   * state only. On rollback, the delta holder is discarded with the
   * operation.
   */
  public void applyIndexCountDeltas(AtomicOperation atomicOperation) {
    var holder = atomicOperation.getIndexCountDeltas();
    if (holder == null) {
      return;
    }
    // Idempotency latch: serves as a defensive belt against any future
    // re-entry into apply on the same atomic operation (for example, a
    // nested or mistakenly-replayed lifecycle pass). The lifecycle apply
    // hook reads the latch in its gate and short-circuits the call when
    // the holder is already applied.
    if (holder.isApplied()) {
      return;
    }
    // Latch the holder up front so a partial-loop throw caught by the
    // lifecycle hook's RuntimeException | AssertionError swallow still
    // latches the holder. The latch closes the partial-loop window: any
    // subsequent re-entry on the same holder sees isApplied() and skips
    // the per-engine loop, preventing a double-apply on engines processed
    // before the throw. Mirrors the setPersisted() latch at the top of
    // persistIndexCountDeltas above.
    holder.setApplied();
    for (var entry : holder.getDeltas().int2ObjectEntrySet()) {
      int engineId = entry.getIntKey();
      var delta = entry.getValue();
      // indexEngines is a plain ArrayList. Ordinary transaction commits retain the state read lock
      // and index tree component lock through Hook B. The rebuild tail reaches Hook B after
      // releasing both locks. That pre-existing unlocked lookup is deferred to YTDB-1268.
      if (engineId >= 0 && engineId < indexEngines.size()) {
        var engine = indexEngines.get(engineId);
        if (engine instanceof BTreeIndexEngine btreeEngine) {
          // Sum the per-put delta and the in-mem-only recalibration adjustment.
          // The two accumulators land at the same point on the in-mem side so
          // a recalibration and per-put activity in the same atomic operation
          // compose into a single addAndGet on each counter. The persisted
          // EP-page side is fed only by getTotalDelta()/getNullDelta() via
          // Hook A (persistCountDelta); the inMemAdjust* fields are NOT
          // persisted because buildInitialHistogram already lands its
          // persisted-side write inline via setApproximateEntriesCount(op,
          // target), which is WAL-tracked and reverts on rollback.
          btreeEngine.addToApproximateEntriesCount(
              delta.getTotalDelta() + delta.getInMemAdjustTotal());
          btreeEngine.addToApproximateNullCount(
              delta.getNullDelta() + delta.getInMemAdjustNull());
        }
      }
    }
  }

  /**
   * Removes transaction-local work keyed by a slot whose engine lifetime has ended. Commit-window
   * drops run after ordinary index writes, while creates run before replacement population. Thus,
   * the removed work can only belong to the old owner. Work for the replacement accumulates later.
   * Holders attached to sibling atomic operations remain untouched.
   */
  private static void discardIndexDeltas(
      final AtomicOperation atomicOperation, final int engineId) {
    final var countDeltas = atomicOperation.getIndexCountDeltas();
    if (countDeltas != null) {
      countDeltas.discard(engineId);
    }

    final var histogramDeltas = atomicOperation.getHistogramDeltas();
    if (histogramDeltas != null) {
      histogramDeltas.discard(engineId);
    }
  }

  /**
   * Applies histogram deltas accumulated during the transaction to the in-memory CHM cache. Called
   * after {@code endTxCommit()} succeeds so that the cache always reflects committed state only.
   */
  public void applyHistogramDeltas(AtomicOperation atomicOperation) {
    var holder = atomicOperation.getHistogramDeltas();
    if (holder == null) {
      return;
    }
    // Idempotency latch: same role as the IndexCountDeltaHolder.applied
    // latch read above. A re-entry on the same atomic operation (for
    // example, a nested or mistakenly-replayed lifecycle pass) must
    // short-circuit so the CHM cache is not re-mutated within a single
    // transaction.
    if (holder.isApplied()) {
      return;
    }
    // Latch the holder up front so a partial-loop throw caught by the
    // lifecycle hook's RuntimeException | AssertionError swallow still
    // latches the holder. The latch closes the partial-loop window: any
    // subsequent re-entry on the same holder sees isApplied() and skips
    // the per-engine loop, preventing a double-apply on engines processed
    // before the throw. Mirrors the setApplied() latch hoisted to the top
    // of applyIndexCountDeltas above.
    holder.setApplied();
    for (var entry : holder.getDeltas().entrySet()) {
      var engineId = entry.getKey();
      var delta = entry.getValue();
      // indexEngines is a plain ArrayList. Ordinary transaction commits retain the state read lock
      // and index tree component lock through Hook B. The rebuild tail reaches Hook B after
      // releasing both locks. That pre-existing unlocked lookup is deferred to YTDB-1268.
      if (engineId < indexEngines.size()) {
        var engine = indexEngines.get(engineId);
        if (engine instanceof BTreeIndexEngine btreeEngine) {
          var mgr = btreeEngine.getHistogramManager();
          if (mgr != null) {
            mgr.applyDelta(delta);
          }
        }
      }
    }
  }

  /** Allocates process-local identity when a local engine object is constructed. */
  public synchronized IndexEngineReference allocateIndexEngineReference(int slot, int apiVersion) {
    if (indexEngineGeneration == Long.MAX_VALUE) {
      throw new StorageException(name, "Index engine generation space is exhausted");
    }

    return new IndexEngineReference(slot, apiVersion, ++indexEngineGeneration);
  }

  private static final class TopLevelIndexLifecycle {

    private final RID descriptorIdentity;
    private IndexBuildStateStore.CreatedLifecycleRecord created;

    private TopLevelIndexLifecycle(RID descriptorIdentity) {
      this.descriptorIdentity = descriptorIdentity;
    }
  }

  /** Registers lifecycle creation for a top-level index descriptor in the current commit. */
  public void registerTopLevelIndexLifecycle(
      FrontendTransaction transaction, RID descriptorIdentity) {
    var descriptor = transaction.loadEntity(descriptorIdentity);
    if (descriptor.getLink(Index.LIFECYCLE_RECORD) != null) {
      throw new IllegalStateException("Index descriptor already has a lifecycle record link");
    }

    var pending = pendingTopLevelIndexLifecycles(transaction);
    if (pending.isEmpty()) {
      pending = new ArrayList<>();
      transaction.setCustomData(TOP_LEVEL_INDEX_LIFECYCLES, pending);
    }
    if (pending.stream()
        .anyMatch(candidate -> candidate.descriptorIdentity.equals(descriptorIdentity))) {
      throw new IllegalStateException("Index descriptor already awaits a lifecycle record");
    }
    pending.add(new TopLevelIndexLifecycle(descriptorIdentity));
  }

  @SuppressWarnings("unchecked")
  private List<TopLevelIndexLifecycle> pendingTopLevelIndexLifecycles(
      FrontendTransaction transaction) {
    var pending =
        (List<TopLevelIndexLifecycle>) transaction.getCustomData(TOP_LEVEL_INDEX_LIFECYCLES);
    return pending == null ? List.of() : pending;
  }

  private void createTopLevelIndexLifecycles(
      FrontendTransaction transaction,
      AtomicOperation atomicOperation,
      List<TopLevelIndexLifecycle> pending) {
    for (var lifecycle : pending) {
      var descriptor = transaction.loadEntity(lifecycle.descriptorIdentity);
      var existing = descriptor.getLink(Index.LIFECYCLE_RECORD);
      lifecycle.created =
          createInitialIndexLifecycle(
              lifecycle.descriptorIdentity, existing, atomicOperation);
      descriptor.setLink(Index.LIFECYCLE_RECORD, lifecycle.created.identity());
    }
  }

  private void publishTopLevelIndexLifecycles(List<TopLevelIndexLifecycle> pending) {
    for (var lifecycle : pending) {
      publishInitialIndexLifecycle(
          lifecycle.descriptorIdentity, lifecycle.created.snapshot());
    }
  }

  /** Fails metadata loading when the lifecycle storage area is absent. */
  public void validateIndexBuildStateCollection() {
    indexBuildStateStorageBackend.validateBuildStateCollection();
  }

  /** Loads durable lifecycle state or publishes stable missing-state quarantine. */
  public IndexLifecycleCell recoverIndexLifecycle(
      RID descriptorIdentity, @Nullable RID lifecycleIdentity) {
    final IndexLifecycleSnapshot recovered;
    if (lifecycleIdentity == null) {
      recovered = quarantineMissingLifecycle(
          descriptorIdentity, "Index descriptor has no lifecycle record link");
    } else {
      IndexLifecycleSnapshot durable;
      try {
        durable = indexBuildStateStore.read(descriptorIdentity, lifecycleIdentity);
      } catch (IndexBuildStateStore.MissingLinkedRecordException exception) {
        durable = null;
      }
      if (durable == null) {
        recovered = quarantineMissingLifecycle(
            descriptorIdentity, "Linked index build state record is missing");
      } else if (durable.buildState().ownerEpoch() != null) {
        recovered =
            new IndexLifecycleSnapshot(
                durable.buildState().withoutProcessOwnership(), durable.recordVersion());
      } else {
        recovered = durable;
      }
    }
    return indexLifecycleRegistry.recover(descriptorIdentity, recovered);
  }

  private static IndexLifecycleSnapshot quarantineMissingLifecycle(
      RID descriptorIdentity, String diagnosticReason) {
    return new IndexLifecycleSnapshot(
        IndexBuildState.quarantineMissing(descriptorIdentity, diagnosticReason), 0);
  }

  /** Returns the durable lifecycle read and write path for local schema indexes. */
  public IndexBuildStateStore getIndexBuildStateStore() {
    return indexBuildStateStore;
  }

  /** Creates initial lifecycle state inside an enclosing index creation atomic unit. */
  public IndexBuildStateStore.CreatedLifecycleRecord createInitialIndexLifecycle(
      RID descriptorIdentity,
      @Nullable RID existingLifecycleIdentity,
      AtomicOperation atomicOperation) {
    if (!callerOwnsStateProtection()) {
      throw new StorageStateContextException(
          "Initial lifecycle creation requires the storage state lock or an active commit window");
    }
    return indexBuildStateStore.createInitial(
        descriptorIdentity,
        existingLifecycleIdentity,
        value -> indexBuildStateStorageBackend.createInsideAtomicOperation(atomicOperation, value));
  }

  /** Publishes a committed lifecycle snapshot into the storage-scoped registry. */
  public IndexLifecycleCell publishInitialIndexLifecycle(
      RID descriptorIdentity, IndexLifecycleSnapshot snapshot) {
    return indexLifecycleRegistry.getOrCreate(descriptorIdentity, snapshot);
  }

  /** Returns the stable lifecycle carrier for a durable index descriptor. */
  public IndexLifecycleCell getOrCreateIndexLifecycle(RID descriptorIdentity) {
    var cell = indexLifecycleRegistry.get(descriptorIdentity);
    if (cell == null) {
      throw new IllegalStateException("The index lifecycle snapshot is not loaded");
    }
    return cell;
  }

  public IndexLifecycleCell getIndexLifecycle(RID descriptorIdentity) {
    return indexLifecycleRegistry.get(descriptorIdentity);
  }

  public void removeIndexLifecycle(RID descriptorIdentity) {
    indexLifecycleRegistry.remove(descriptorIdentity);
  }

  @Nullable public IndexEngineReference getIndexEngineReference(int externalIndexId) {
    if (isCommitWindowActive()) {
      return getIndexEngineReferenceWithStateLock(externalIndexId);
    }

    stateLock.readLock().lock();
    try {
      return getIndexEngineReferenceWithStateLock(externalIndexId);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Nullable private IndexEngineReference getIndexEngineReferenceWithStateLock(int externalIndexId) {
    final var internalIndexId = extractInternalId(externalIndexId);
    try {
      checkIndexId(internalIndexId);
    } catch (InvalidIndexEngineIdException e) {
      throw new IllegalStateException("Cannot resolve an unregistered index engine", e);
    }

    return indexEngines.get(internalIndexId).getEngineReference();
  }

  /**
   * Binds a local engine to its durable descriptor after that descriptor obtains or loads its RID.
   * A differing reference is accepted only for a newer engine already bound to the same owner.
   */
  @Nullable public IndexEngineReference attachIndexEngineOwner(
      int externalIndexId, RID descriptorIdentity,
      @Nullable IndexEngineReference carrierReference) {
    if (isCommitWindowActive()) {
      return attachIndexEngineOwnerWithStateLock(
          externalIndexId, descriptorIdentity, carrierReference);
    }

    stateLock.readLock().lock();
    try {
      return attachIndexEngineOwnerWithStateLock(
          externalIndexId, descriptorIdentity, carrierReference);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Nullable private IndexEngineReference attachIndexEngineOwnerWithStateLock(
      int externalIndexId, RID descriptorIdentity,
      @Nullable IndexEngineReference carrierReference) {
    final var liveReference = getIndexEngineReferenceWithStateLock(externalIndexId);
    if (liveReference == null) {
      if (carrierReference != null) {
        throw new IllegalStateException("Index engine lost its process-local reference");
      }
      return null;
    }
    if (carrierReference == null) {
      throw new IllegalStateException("Index engine unexpectedly gained a process-local reference");
    }
    if (liveReference != carrierReference) {
      final var liveOwner = liveReference.ownerDescriptorIdentity();
      final var isOwnedMonotonicReplacement =
          liveReference.slot() == carrierReference.slot()
              && liveReference.generation() > carrierReference.generation()
              && descriptorIdentity.equals(liveOwner);
      if (!isOwnedMonotonicReplacement) {
        throw new IllegalStateException(
            "Index engine reference changed from generation " + carrierReference.generation()
                + " to " + liveReference.generation() + " for slot " + liveReference.slot());
      }
    }

    liveReference.bindOwner(descriptorIdentity);
    return liveReference;
  }

  /**
   * Resolves the packed identifier of the engine owned by a durable index descriptor.
   *
   * <p>The commit-window branch avoids reacquiring the non-reentrant storage state lock.
   */
  public ResolvedIndexEngine resolveIndexEngineByOwner(final RID descriptorIdentity) {
    try {
      if (isCommitWindowActive() || stateLock.isReadLockedByCurrentThread()) {
        return resolveIndexEngineByOwnerWithStateLock(descriptorIdentity);
      }

      stateLock.readLock().lock();
      try {
        return resolveIndexEngineByOwnerWithStateLock(descriptorIdentity);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error, false);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable, false);
    }
  }

  /**
   * Resolves an engine by descriptor owner while the caller retains the storage state lock.
   *
   * @throws StaleIndexEngineException when no registered engine has the requested owner
   * @throws StorageException when registry ownership is ambiguous
   */
  public ResolvedIndexEngine resolveIndexEngineByOwnerWithStateLock(
      final RID descriptorIdentity) {
    if (!stateLock.isReadLockedByCurrentThread() && !isCommitWindowActive()) {
      throw new IllegalStateException(
          "Index engine owner resolution requires the storage state lock or an active commit"
              + " window");
    }

    checkOpennessAndMigration();
    int resolvedIdentifier = -1;
    for (var slot = 0; slot < indexEngines.size(); slot++) {
      final var engine = indexEngines.get(slot);
      if (engine == null) {
        continue;
      }

      final var reference = engine.getEngineReference();
      if (reference == null) {
        // An engine without a reference cannot be owned by any descriptor. Skipping it cannot hide
        // a possible owner-resolution target.
        continue;
      }

      if (descriptorIdentity.equals(reference.ownerDescriptorIdentity())) {
        if (resolvedIdentifier >= 0) {
          throw new StorageException(
              name,
              "Multiple registered index engines are owned by descriptor "
                  + descriptorIdentity);
        }
        resolvedIdentifier = generateIndexId(slot, engine);
      }
    }

    if (resolvedIdentifier < 0) {
      throw new StaleIndexEngineException(
          name,
          "Cannot resolve index engine for descriptor " + descriptorIdentity
              + ": no registered engine has that owner");
    }
    final var internalIdentifier = extractInternalId(resolvedIdentifier);
    return new ResolvedIndexEngine(
        resolvedIdentifier, indexEngines.get(internalIdentifier).getEngineReference());
  }

  /** One coherent result from an owner scan. */
  public record ResolvedIndexEngine(
      int engineIdentifier, @Nullable IndexEngineReference engineReference) {
  }

  public int loadIndexEngine(final String name) {
    try {
      if (isCommitWindowActive()) {
        return loadIndexEngineWithStateLock(name);
      }

      stateLock.readLock().lock();
      try {
        return loadIndexEngineWithStateLock(name);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  /** Resolves an engine identifier by name while the caller retains the storage state lock. */
  public int loadIndexEngineWithStateLock(final String name) {
    if (!stateLock.isReadLockedByCurrentThread() && !isCommitWindowActive()) {
      throw new IllegalStateException(
          "Index engine reload requires the storage state lock or an active commit window");
    }

    checkOpennessAndMigration();
    final var engine = indexEngineNameMap.get(name);
    if (engine == null) {
      return -1;
    }
    final var indexId = indexEngines.indexOf(engine);
    assert indexId == engine.getId();
    return generateIndexId(indexId, engine);
  }

  public int loadExternalIndexEngine(
      final IndexMetadata indexMetadata, final Map<String, String> engineProperties,
      AtomicOperation atomicOperation) {
    // Legacy re-attach of an externally created engine (binary format <= 15). The storage-format
    // gate rejects every pre-24 database at open, so a database that legitimately needs this
    // path can no longer be opened; reaching it on a current database means the index-manager
    // record references an engine the storage configuration does not know (corruption or a
    // partially deleted index). The pre-gate code would fabricate an engine entry without a
    // stable file base id and try to load name-stemmed files, failing later with a cryptic
    // file-does-not-exist error; fail loudly and explainably here instead.
    throw new IndexException(name,
        "Index '" + indexMetadata.getName() + "' has no registered engine in storage '" + name
            + "'. Re-attaching an externally created legacy index engine is no longer supported:"
            + " if this database was created by a previous version of YouTrackDB, export it with"
            + " that version and reimport it using the current one; otherwise drop and recreate"
            + " the index.");
  }

  /** Test-only failure seam fired immediately before non-commit-window engine construction. */
  private volatile Runnable indexEngineCreationTestHook;

  /** Installs a test-only failure seam for public engine creation and rebuild replacement. */
  public void setIndexEngineCreationTestHook(final Runnable hook) {
    indexEngineCreationTestHook = hook;
  }

  public int addIndexEngine(
      final IndexMetadata indexMetadata,
      final Map<String, String> engineProperties) {
    final var indexDefinition = indexMetadata.getIndexDefinition();

    try {
      if (indexDefinition == null) {
        throw new IndexException(name, "Index definition has to be provided");
      }
      final var keyTypes = indexDefinition.getTypes();
      if (keyTypes == null) {
        throw new IndexException(name, "Types of indexed keys have to be provided");
      }

      stateLock.writeLock().lock();
      try {
        checkOpennessAndMigration();

        makeStorageDirty();
        return atomicOperationsManager.calculateInsideAtomicOperation(
            atomicOperation -> {
              final var keySerializer = determineKeySerializer(indexDefinition, atomicOperation);
              if (keySerializer == null) {
                throw new IndexException(name, "Can not determine key serializer");
              }

              final var keySize = determineKeySize(indexDefinition);
              if (indexEngineNameMap.containsKey(indexMetadata.getName())) {
                // OLD INDEX FILE ARE PRESENT: THIS IS THE CASE OF PARTIAL/BROKEN INDEX
                LogManager.instance()
                    .warn(
                        this,
                        "Index with name '%s' already exists, removing it and re-create the index",
                        indexMetadata.getName());
                final var engine = indexEngineNameMap.remove(indexMetadata.getName());
                if (engine != null) {
                  detachHistogramManager(engine);
                  indexEngines.set(engine.getId(), null);

                  engine.delete(atomicOperation);
                  ((CollectionBasedStorageConfiguration) configuration)
                      .deleteIndexEngine(atomicOperation, indexMetadata.getName());
                }
              }
              final var valueSerializerId =
                  StreamSerializerRID.INSTANCE.getId();
              final var ctxCfg = configuration.getContextConfiguration();
              final var cfgEncryptionKey =
                  ctxCfg.getValueAsString(GlobalConfiguration.STORAGE_ENCRYPTION_KEY);
              // The public method appends at the live registry size; the commit
              // window passes a commit-local-allocated id instead.
              final var generatedId = indexEngines.size();
              // The never-reused stable file base id, allocated inside this same atomic
              // operation (the held stateLock.writeLock() serializes it against concurrent
              // creates).
              final var fileBaseId = allocateIndexEngineFileBaseId(atomicOperation);
              final var engineData =
                  new IndexEngineData(
                      generatedId,
                      fileBaseId,
                      indexMetadata,
                      true,
                      valueSerializerId,
                      keySerializer.getId(),
                      keyTypes,
                      keySize,
                      null,
                      cfgEncryptionKey,
                      engineProperties);

              final var creationHook = indexEngineCreationTestHook;
              if (creationHook != null) {
                creationHook.run();
              }
              final var engine = doAddIndexEngine(atomicOperation, engineData);

              publishIndexEngine(engineData.getIndexId(), engine);

              return generateIndexId(engineData.getIndexId(), engine);
            });
      } catch (final IOException e) {
        throw BaseException.wrapException(
            new StorageException(name,
                "Cannot add index engine " + indexMetadata.getName() + " in storage."),
            e, name);
      } finally {
        stateLock.writeLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Creates the index engine, its files, and its storage-configuration entry inside the
   * given atomic operation, without publishing the engine into the in-memory registries
   * ({@code indexEngineNameMap} / {@code indexEngines}). Everything this method touches is
   * buffered as WAL-reverted intent in {@code atomicOperation}, so a rolled-back or
   * crashed-before-commit operation leaves no engine files behind. Splitting creation from
   * registry publication ({@link #publishIndexEngine(int, BaseIndexEngine)}) lets a caller
   * defer the in-memory publish past commit so a failed commit leaves no phantom
   * registration; the public {@link #addIndexEngine(IndexMetadata, Map)} wrapper still
   * publishes inside the same atomic operation (its existing crash-safe behavior is
   * unchanged), and the commit window that defers the publish is wired separately. This
   * mirrors the existing {@link #deleteIndexEngine(int)} discipline, which already defers
   * its in-memory map mutation to after the atomic operation.
   *
   * <p>The caller is responsible for id allocation: {@code engineData.getIndexId()} is the
   * caller-chosen internal id. The public {@link #addIndexEngine(IndexMetadata, Map)} uses
   * {@code indexEngines.size()}; the commit window uses a commit-local allocator seeded
   * inside the write lock.
   *
   * @param atomicOperation the in-flight atomic operation that buffers the file creates.
   * @param engineData      the engine metadata, carrying the caller-allocated internal id.
   * @return the created engine, not yet published into the in-memory registries.
   */
  private BaseIndexEngine doAddIndexEngine(
      final AtomicOperation atomicOperation, final IndexEngineData engineData)
      throws IOException {
    final var engine = Indexes.createIndexEngine(this, engineData);

    engine.create(atomicOperation, engineData);

    // Create and wire histogram manager for B-tree engines
    if (engine instanceof BTreeIndexEngine btreeEngine) {
      var mgr = createAndWireHistogramManager(btreeEngine, engineData, atomicOperation);
      mgr.createStatsFile(atomicOperation);
    }

    ((CollectionBasedStorageConfiguration) configuration)
        .addIndexEngine(atomicOperation, engineData.getName(), engineData);

    return engine;
  }

  /**
   * Publishes a created engine into the in-memory registries ({@code indexEngineNameMap}
   * and {@code indexEngines}). This is the registry-publication half of the create seam,
   * split from {@link #doAddIndexEngine(AtomicOperation, IndexEngineData)} so the commit
   * window can defer it past {@code commitChanges}: a failed commit must leave no phantom
   * in-memory registration. Grows the {@code indexEngines} list to the
   * internal id and sets the slot, so it works for both an append at the live size (the
   * public path) and a reused hole below the size (the commit-local allocator).
   *
   * @param internalId the engine's internal id (its slot in {@code indexEngines}).
   * @param engine     the engine to publish.
   */
  private void publishIndexEngine(final int internalId, final BaseIndexEngine engine) {
    indexEngineNameMap.put(engine.getName(), engine);
    setIndexEngine(internalId, engine);
  }

  /**
   * Detaches histogram publication from an engine that is leaving the registry. This must run
   * before its slot is released, so removal linearizes before a replacement publishes there.
   */
  private static void detachHistogramManager(final BaseIndexEngine engine) {
    if (engine instanceof BTreeIndexEngine btreeEngine) {
      final var manager = btreeEngine.getHistogramManager();
      if (manager != null) {
        manager.detach();
      }
    }
  }

  /**
   * Sets {@code indexEngines.get(id) == engine}, growing the list with null padding when
   * {@code id} is at or past the current size, mirroring {@link #setCollection(int,
   * StorageCollection)}. The public append path uses {@code id == indexEngines.size()};
   * the commit-local allocator may reuse a hole at {@code id < indexEngines.size()}.
   */
  private void setIndexEngine(final int id, final BaseIndexEngine engine) {
    if (indexEngines.size() <= id) {
      while (indexEngines.size() < id) {
        indexEngines.add(null);
      }
      indexEngines.add(engine);
    } else {
      indexEngines.set(id, engine);
    }
  }

  public BinarySerializer<?> resolveObjectSerializer(final byte serializerId) {
    return componentsFactory.binarySerializerFactory.getObjectSerializer(serializerId);
  }

  /**
   * Wires a histogram manager into a B-tree engine during database open.
   * If the .ixs file exists, opens it; otherwise creates a new one with
   * initial counters from the B-tree (migration path).
   */
  private void wireHistogramManagerOnLoad(
      BTreeIndexEngine btreeEngine,
      IndexEngineData engineData,
      AtomicOperation atomicOperation) {
    try {
      var mgr = createAndWireHistogramManager(
          btreeEngine, engineData, atomicOperation);
      if (mgr.statsFileExists(atomicOperation)) {
        // Normal path: .ixs file exists, load from it
        mgr.openStatsFile(atomicOperation);
      } else {
        // Migration path: no .ixs file, create with counters from B-tree
        long totalCount = btreeEngine.getTotalCount(atomicOperation);
        long nullCount = btreeEngine.getNullCount(atomicOperation);
        // For single-value: distinctCount == non-null count (each key is unique)
        // For multi-value: distinctCount == non-null count (overestimates
        // because the same key can map to multiple RIDs; corrected on first
        // rebalance when exact NDV is computed from the key scan)
        long distinctCount = totalCount - nullCount;
        mgr.createStatsFileWithCounters(
            atomicOperation, totalCount, distinctCount, nullCount);
      }
    } catch (IOException e) {
      // Histogram manager failure must not prevent database open.
      // Clean up any partially-populated cache entry and null out the
      // partially-wired manager to avoid broken state (fileId == -1, no
      // snapshot in cache) causing issues on onPut/onRemove.
      histogramSnapshotCache.remove(engineData.getIndexId());
      btreeEngine.setHistogramManager(null);
      LogManager.instance().warn(
          this,
          "Failed to wire histogram manager for index '%s': %s",
          engineData.getName(), e.getMessage());
    }
  }

  /**
   * Creates an {@link IndexHistogramManager}, wires it into the given
   * B-tree engine, and returns it. The caller is responsible for calling
   * {@code createStatsFile()}, {@code openStatsFile()}, or
   * {@code createStatsFileWithCounters()} after this method returns.
   *
   * @param engine          the B-tree engine to wire the manager into
   * @param engineData      engine metadata for serializer resolution
   * @param atomicOperation current atomic operation (for binary format check)
   */
  private IndexHistogramManager createAndWireHistogramManager(
      BTreeIndexEngine engine,
      IndexEngineData engineData,
      AtomicOperation atomicOperation) {
    boolean isSingleValue = engine instanceof BTreeSingleValueIndexEngine;

    // Determine the histogram key serializer: for composite indexes,
    // boundaries are leading-field values, so we need the leading field's
    // type serializer rather than the composite key serializer.
    var keyTypes = engineData.getKeyTypes();
    BinarySerializer<?> histogramKeySerializer;
    byte histogramSerializerId;
    if (keyTypes != null && keyTypes.length > 1) {
      var leadingType = keyTypes[0];
      if (leadingType == PropertyTypeInternal.STRING
          && configuration.getBinaryFormatVersion(atomicOperation) >= 13) {
        histogramKeySerializer = UTF8Serializer.INSTANCE;
      } else {
        histogramKeySerializer =
            componentsFactory.binarySerializerFactory
                .getObjectSerializer(leadingType);
      }
      histogramSerializerId = histogramKeySerializer.getId();
    } else {
      histogramSerializerId = engineData.getKeySerializedId();
      histogramKeySerializer =
          resolveObjectSerializer(histogramSerializerId);
    }

    var mgr = new IndexHistogramManager(
        this,
        // The stats file is part of the engine's file family and is keyed by the stable file
        // base id like every other member — the index name keys no file.
        indexEngineFileStem(engineData.getFileBaseId()),
        engineData.getIndexId(),
        isSingleValue,
        histogramSnapshotCache,
        histogramKeySerializer,
        componentsFactory.binarySerializerFactory,
        histogramSerializerId);

    // User-facing diagnostics report the index's logical name, never the ie_<n> file stem.
    mgr.setDisplayName(engineData.getName());

    int keyFieldCount = (keyTypes != null) ? keyTypes.length : 1;
    mgr.setKeyFieldCount(keyFieldCount);

    // Wire the sorted key stream function for background rebalance.
    // Uses the raw (non-SI-filtered) key stream: SI filtering is unnecessary
    // for histogram rebalance because the histogram tolerates the tiny error
    // from uncommitted/phantom entries (< 0.01% of index size), and skipping
    // it avoids drift between scanned counts and scalar counters.
    if (isSingleValue) {
      mgr.setKeyStreamSupplier(
          atomicOp -> ((BTreeSingleValueIndexEngine) engine)
              .rawKeyStreamForHistogram(atomicOp));
    } else {
      mgr.setKeyStreamSupplier(
          atomicOp -> ((BTreeMultiValueIndexEngine) engine)
              .rawKeyStreamForHistogram(atomicOp));
    }
    engine.setHistogramManager(mgr);

    // Propagate the rebalance semaphore to limit concurrent rebalance tasks.
    mgr.setRebalanceSemaphore(histogramRebalanceSemaphore);

    // Propagate the executor if already available (index created after
    // database open). Before database open, histogramExecutor is null
    // and setHistogramExecutor() will wire it for all engines.
    var exec = histogramExecutor;
    if (exec != null) {
      mgr.setBackgroundExecutor(exec);
    }

    return mgr;
  }

  private static int generateIndexId(final int internalId, final BaseIndexEngine indexEngine) {
    return indexEngine.getEngineAPIVersion() << ((IntegerSerializer.INT_SIZE << 3) - 5)
        | internalId;
  }

  private static int extractInternalId(final int externalId) {
    if (externalId < 0) {
      throw new IllegalStateException("Index id has to be positive");
    }

    return externalId & 0x7_FF_FF_FF;
  }

  public static int extractEngineAPIVersion(final int externalId) {
    return externalId >>> ((IntegerSerializer.INT_SIZE << 3) - 5);
  }

  private static int determineKeySize(final IndexDefinition indexDefinition) {
    if (indexDefinition == null) {
      return 1;
    } else {
      return indexDefinition.getTypes().length;
    }
  }

  private BinarySerializer<?> determineKeySerializer(final IndexDefinition indexDefinition,
      AtomicOperation atomicOperation) {
    if (indexDefinition == null) {
      throw new StorageException(name, "Index definition has to be provided");
    }

    final var keyTypes = indexDefinition.getTypes();
    if (keyTypes == null || keyTypes.length == 0) {
      throw new StorageException(name, "Types of index keys has to be defined");
    }
    if (keyTypes.length < indexDefinition.getProperties().size()) {
      throw new StorageException(name,
          "Types are provided only for "
              + keyTypes.length
              + " fields. But index definition has "
              + indexDefinition.getProperties().size()
              + " fields.");
    }

    final BinarySerializer<?> keySerializer;
    if (indexDefinition.getTypes().length > 1) {
      keySerializer = CompositeKeySerializer.INSTANCE;
    } else {
      final var keyType = indexDefinition.getTypes()[0];

      if (keyType == PropertyTypeInternal.STRING
          && configuration.getBinaryFormatVersion(atomicOperation) >= 13) {
        return UTF8Serializer.INSTANCE;
      }

      final var currentStorageComponentsFactory = componentsFactory;
      if (currentStorageComponentsFactory != null) {
        keySerializer =
            currentStorageComponentsFactory.binarySerializerFactory.getObjectSerializer(keyType);
      } else {
        throw new IllegalStateException(
            "Cannot load binary serializer, storage is not properly initialized");
      }
    }

    return keySerializer;
  }

  public void deleteIndexEngine(
      int indexId, @Nullable IndexEngineReference expectedReference)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);
    try {
      stateLock.writeLock().lock();
      try {
        checkOpennessAndMigration();
        final var engine = engineForDereference(internalIndexId, expectedReference);
        makeStorageDirty();
        atomicOperationsManager.executeInsideAtomicOperation(
            atomicOperation -> doDeleteIndexEngine(atomicOperation, engine));
        detachHistogramManager(engine);
        indexEngines.set(internalIndexId, null);
        indexEngineNameMap.remove(engine.getName());
      } catch (final IOException exception) {
        throw BaseException.wrapException(
            new StorageException(name, "Error on index deletion"), exception, name);
      } finally {
        stateLock.writeLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable);
    }
  }

  public void deleteIndexEngine(int indexId)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);

    try {
      stateLock.writeLock().lock();
      try {
        checkOpennessAndMigration();
        checkIndexId(internalIndexId);

        makeStorageDirty();

        final var engine = indexEngines.get(internalIndexId);
        assert internalIndexId == engine.getId();

        atomicOperationsManager.executeInsideAtomicOperation(
            atomicOperation -> doDeleteIndexEngine(atomicOperation, engine));

        // Update in-memory maps only AFTER the atomic operation commits
        // successfully. If the atomic operation rolls back, the maps remain
        // consistent so that addIndexEngine()'s "OLD INDEX FILE" recovery
        // branch can find and clean up the stale engine.
        detachHistogramManager(engine);
        indexEngines.set(internalIndexId, null);
        indexEngineNameMap.remove(engine.getName());

      } catch (final IOException e) {
        throw BaseException.wrapException(new StorageException(name, "Error on index deletion"), e,
            name);
      } finally {
        stateLock.writeLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Deletes the engine's files and its storage-configuration entry inside the given atomic
   * operation, without mutating the in-memory registries ({@code indexEngines} /
   * {@code indexEngineNameMap}). This is the atomic-op half of the delete seam, split from
   * {@link #deleteIndexEngine(int)} so the commit window can run the WAL-reverted delete
   * under the held write lock and defer (or revert) the in-memory map mutation itself. The
   * public {@code deleteIndexEngine} performs the map mutation after the atomic operation
   * commits, as it always has.
   *
   * @param atomicOperation the in-flight atomic operation that buffers the file deletes.
   * @param engine          the already-resolved engine to delete.
   */
  private void doDeleteIndexEngine(
      final AtomicOperation atomicOperation, final BaseIndexEngine engine) throws IOException {
    engine.delete(atomicOperation);
    ((CollectionBasedStorageConfiguration) configuration)
        .deleteIndexEngine(atomicOperation, engine.getName());
  }

  private void checkIndexId(final int indexId) throws InvalidIndexEngineIdException {
    if (indexId < 0 || indexId >= indexEngines.size() || indexEngines.get(indexId) == null) {
      throw new InvalidIndexEngineIdException(
          "Engine with id " + indexId + " is not registered inside of storage");
    }
  }

  public boolean removeKeyFromIndex(
      final int indexId, @Nullable IndexEngineReference expectedReference,
      final Object key, @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    try {
      return removeKeyFromIndexInternal(
          atomicOperation, extractInternalId(indexId), expectedReference, key);
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable);
    }
  }

  public boolean removeKeyFromIndex(final int indexId, final Object key,
      @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);
    try {
      return removeKeyFromIndexInternal(atomicOperation, internalIndexId, key);
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean removeKeyFromIndexInternal(
      final AtomicOperation atomicOperation, final int indexId, final Object key)
      throws InvalidIndexEngineIdException {
    return removeKeyFromIndexInternal(
        atomicOperation, engineByIdentifier(indexId), key);
  }

  private boolean removeKeyFromIndexInternal(
      final AtomicOperation atomicOperation, final int indexId,
      @Nullable IndexEngineReference expectedReference, final Object key)
      throws InvalidIndexEngineIdException {
    return removeKeyFromIndexInternal(
        atomicOperation, engineForDereference(indexId, expectedReference), key);
  }

  private boolean removeKeyFromIndexInternal(
      final AtomicOperation atomicOperation, final BaseIndexEngine engine, final Object key) {
    try {
      if (engine.getEngineAPIVersion() == IndexEngine.VERSION) {
        return ((IndexEngine) engine).remove(this, atomicOperation, key);
      } else {
        final var v1IndexEngine = (V1IndexEngine) engine;
        if (!v1IndexEngine.isMultiValue()) {
          return ((SingleValueIndexEngine) engine).remove(atomicOperation, key);
        } else {
          throw new StorageException(name,
              "To remove entry from multi-value index not only key but value also should be"
                  + " provided");
        }
      }
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new StorageException(name,
              "Error during removal of entry with key " + key + " from index "),
          e, name);
    }
  }

  public void clearIndex(
      final int indexId, @Nullable IndexEngineReference expectedReference) {
    try {
      final var internalIndexId = extractInternalId(indexId);
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();
        makeStorageDirty();
        final var engine = engineForDereference(internalIndexId, expectedReference);
        atomicOperationsManager.executeInsideAtomicOperation(
            atomicOperation -> doClearIndex(atomicOperation, internalIndexId, engine));
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      final var stale = new StaleIndexEngineException(
          name, "Index engine changed before clear");
      stale.initCause(exception);
      throw stale;
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable);
    }
  }

  public void clearIndex(final int indexId) {
    try {
      final var internalIndexId = extractInternalId(indexId);
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        makeStorageDirty();

        atomicOperationsManager.executeInsideAtomicOperation(
            atomicOperation -> doClearIndex(atomicOperation, internalIndexId));
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void doClearIndex(final AtomicOperation atomicOperation,
      final int indexId)
      throws InvalidIndexEngineIdException {
    doClearIndex(atomicOperation, indexId, engineByIdentifier(indexId));
  }

  private void doClearIndex(
      final AtomicOperation atomicOperation, final int indexId, final BaseIndexEngine engine) {
    try {
      assert indexId == engine.getId();
      engine.clear(this, atomicOperation);
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new StorageException(name, "Error during clearing of index"),
          e, name);
    }
  }

  public Stream<RID> getIndexValues(
      int indexId, final Object key, AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var engineAPIVersion = extractEngineAPIVersion(indexId);
    if (engineAPIVersion != 1) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 1 but found " + engineAPIVersion);
    }

    final var internalIndexId = extractInternalId(indexId);
    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();
        return ((V1IndexEngine) engineByIdentifier(internalIndexId)).get(key, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error, false);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable, false);
    }
  }

  public Stream<RID> getIndexValues(
      int indexId, @Nullable IndexEngineReference expectedReference,
      final Object key, AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var engineAPIVersion = extractEngineAPIVersion(indexId);
    if (engineAPIVersion != 1) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 1 but found " + engineAPIVersion);
    }

    indexId = extractInternalId(indexId);

    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();

        return doGetIndexValues(indexId, expectedReference, key, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private Stream<RID> doGetIndexValues(
      final int indexId, @Nullable IndexEngineReference expectedReference,
      final Object key, AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var engine = engineForDereference(indexId, expectedReference);
    assert indexId == engine.getId();

    return ((V1IndexEngine) engine).get(key, atomicOperation);
  }

  public BaseIndexEngine getIndexEngine(final int indexId)
      throws InvalidIndexEngineIdException {
    try {
      if (isCommitWindowActive()) {
        return getIndexEngineWithStateLock(indexId);
      }

      stateLock.readLock().lock();
      try {
        return getIndexEngineWithStateLock(indexId);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error, false);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable, false);
    }
  }

  public BaseIndexEngine getIndexEngine(
      final int indexId, @Nullable final IndexEngineReference expectedReference)
      throws InvalidIndexEngineIdException {
    try {
      // A schema-carrying commit already holds stateLock.writeLock(). The active commit window
      // proves that ownership and avoids a non-reentrant read-lock acquisition.
      if (isCommitWindowActive()) {
        return getIndexEngineWithStateLock(indexId, expectedReference);
      }

      stateLock.readLock().lock();
      try {
        return getIndexEngineWithStateLock(indexId, expectedReference);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  /**
   * Resolves an engine without acquiring {@code stateLock}. The caller must already hold the
   * state read lock, or run inside the commit window while holding the state write lock.
   */
  public BaseIndexEngine getIndexEngineWithStateLock(final int indexId)
      throws InvalidIndexEngineIdException {
    if (!stateLock.isReadLockedByCurrentThread() && !isCommitWindowActive()) {
      throw new IllegalStateException(
          "Index engine resolution requires the storage state lock or an active commit window");
    }

    checkOpennessAndMigration();
    return engineByIdentifier(extractInternalId(indexId));
  }

  public BaseIndexEngine getIndexEngineWithStateLock(
      final int indexId, @Nullable final IndexEngineReference expectedReference)
      throws InvalidIndexEngineIdException {
    if (!stateLock.isReadLockedByCurrentThread() && !isCommitWindowActive()) {
      throw new IllegalStateException(
          "Index engine resolution requires the storage state lock or an active commit window");
    }

    checkOpennessAndMigration();
    final var internalId = extractInternalId(indexId);
    return engineForDereference(internalId, expectedReference);
  }

  private BaseIndexEngine engineByIdentifier(final int internalId)
      throws InvalidIndexEngineIdException {
    checkIndexId(internalId);
    final var engine = indexEngines.get(internalId);
    assert internalId == engine.getId();
    return engine;
  }

  private BaseIndexEngine engineForDereference(
      final int internalId, @Nullable final IndexEngineReference expectedReference)
      throws InvalidIndexEngineIdException {
    final var engine = engineByIdentifier(internalId);
    if (engine.getEngineReference() != expectedReference) {
      throw new IndexEngineReplacedException(
          "Engine at slot " + internalId + " no longer matches the index handle");
    }
    assert internalId == engine.getId();
    return engine;
  }

  /**
   * Opens the lock-free commit-window read path on the current thread. The
   * schema-carrying commit calls this immediately after taking {@code
   * stateLock.writeLock()} and before any record read it performs while still
   * holding that write lock (schema serialization and promotion read records via
   * {@code session.load}, which routes back into this storage's
   * readLock-taking record-read methods). While the window is open, {@link
   * #getPhysicalCollectionNameById(int)} and {@link #readRecordInternal} skip
   * {@code stateLock.readLock()} so they do not deadlock re-acquiring the
   * non-reentrant {@link ScalableRWLock} the commit already holds for writing.
   *
   * <p><b>Precondition the caller MUST satisfy:</b> the current thread holds
   * {@code stateLock.writeLock()} for the duration of the window. That held
   * write lock is the only thing that excludes a concurrent registrar and
   * supplies the happens-before edge that makes the lock-free reads of {@code
   * collections} / the collection stores visibility-safe. Opening the window
   * without the write lock is a data race and is unsupported.
   *
   * <p>Re-entrant: nested enter/exit pairs compose via a depth counter, so the
   * window stays open until the outermost {@link #exitCommitWindow()} runs. Every
   * {@code enterCommitWindow()} MUST be balanced by an {@code exitCommitWindow()}
   * in a {@code finally}, or later reads on the same (pooled) thread would wrongly
   * skip the read lock.
   */
  void enterCommitWindow() {
    commitWindowDepth.get()[0]++;
  }

  /**
   * Closes one nesting level of the lock-free commit-window read path opened by
   * {@link #enterCommitWindow()}. The window stays open until the depth returns to
   * zero. MUST be called in a {@code finally} balancing each {@code
   * enterCommitWindow()}; a leaked open window would make later reads on the same
   * pooled thread skip the read lock unsafely.
   *
   * <p>Two defenses keep the per-thread state from poisoning a reused pooled
   * worker. The decrement is clamped at zero so a stray exit without a matching
   * enter cannot drive the depth negative — a negative depth would let a later
   * legitimate window read as closed ({@link #isCommitWindowActive()} tests
   * {@code > 0}), silently re-introducing the read-lock busy-spin the window
   * exists to avoid. Java {@code assert}s are disabled in production, so the
   * clamp, not the assert, is the production guard; the assert still surfaces the
   * unbalanced call loudly under {@code -ea} in tests. When the depth returns to
   * zero the {@link ThreadLocal} is {@code remove()}'d so the next unrelated read
   * on the same pooled thread starts from a fresh zero-depth cell rather than
   * observing leftover window state.
   */
  void exitCommitWindow() {
    final var depth = commitWindowDepth.get();
    assert depth[0] > 0 : "exitCommitWindow without a matching enterCommitWindow";
    if (depth[0] > 0) {
      depth[0]--;
    }
    if (depth[0] == 0) {
      commitWindowDepth.remove();
    }
  }

  /**
   * True when the current thread is inside a commit window opened by {@link
   * #enterCommitWindow()} and so holds {@code stateLock.writeLock()}. The
   * readLock-taking record-read methods consult this to decide whether to take
   * the read lock (normal callers) or run lock-free (commit-window callers).
   */
  private boolean isCommitWindowActive() {
    return commitWindowDepth.get()[0] > 0;
  }

  /** Returns whether this thread owns any supported storage state protection. */
  final boolean callerOwnsStateProtection() {
    return stateLock.isReadLockedByCurrentThread()
        || stateLock.isWriteLockedByCurrentThread()
        || isCommitWindowActive();
  }

  public <T> void callIndexEngine(
      final boolean readOperation, int indexId,
      @Nullable IndexEngineReference expectedReference, final IndexEngineCallback<T> callback)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);
    try {
      if (isCommitWindowActive()) {
        checkOpennessAndMigration();
        if (readOperation) {
          makeStorageDirty();
        }
        callback.callEngine(engineForDereference(internalIndexId, expectedReference));
        return;
      }

      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();
        if (readOperation) {
          makeStorageDirty();
        }
        callback.callEngine(engineForDereference(internalIndexId, expectedReference));
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error, false);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable, false);
    }
  }

  public <T> void callIndexEngine(
      final boolean readOperation, int indexId, final IndexEngineCallback<T> callback)
      throws InvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      // The commit-time engine build reaches this through IndexAbstract.onIndexEngineChange
      // (engine.init) while the schema-carrying commit already holds stateLock.writeLock() with the
      // commit window open. Re-acquiring the read lock there would busy-spin forever on the
      // non-reentrant ScalableRWLock, so the window self-routes to the lock-free body, the same seam
      // getIndexEngine uses. The held write lock supplies the exclusion and the happens-before edge.
      if (isCommitWindowActive()) {
        checkOpennessAndMigration();
        if (readOperation) {
          makeStorageDirty();
        }
        doCallIndexEngine(indexId, callback);
        return;
      }

      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();

        if (readOperation) {
          makeStorageDirty();
        }

        doCallIndexEngine(indexId, callback);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private <T> void doCallIndexEngine(final int indexId, final IndexEngineCallback<T> callback)
      throws InvalidIndexEngineIdException {
    checkIndexId(indexId);

    final var engine = indexEngines.get(indexId);

    callback.callEngine(engine);
  }

  public void putRidIndexEntry(
      int indexId, @Nullable IndexEngineReference expectedReference,
      final Object key, final RID value, @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var engineAPIVersion = extractEngineAPIVersion(indexId);
    final var internalIndexId = extractInternalId(indexId);
    if (engineAPIVersion != 1) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 1 but found " + engineAPIVersion);
    }

    try {
      final var engine = engineForDereference(internalIndexId, expectedReference);
      ((V1IndexEngine) engine).put(atomicOperation, key, value);
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable);
    }
  }

  public void putRidIndexEntry(int indexId, final Object key, final RID value,
      @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var engineAPIVersion = extractEngineAPIVersion(indexId);
    final var internalIndexId = extractInternalId(indexId);

    if (engineAPIVersion != 1) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 1 but found " + engineAPIVersion);
    }

    try {
      putRidIndexEntryInternal(atomicOperation, internalIndexId, key, value);
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void putRidIndexEntryInternal(
      final AtomicOperation atomicOperation, final int indexId, final Object key,
      final RID value)
      throws InvalidIndexEngineIdException {
    checkIndexId(indexId);

    final var engine = indexEngines.get(indexId);
    assert engine.getId() == indexId;

    ((V1IndexEngine) engine).put(atomicOperation, key, value);
  }

  public boolean removeRidIndexEntry(
      int indexId, @Nullable IndexEngineReference expectedReference,
      final Object key, final RID value, @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var engineAPIVersion = extractEngineAPIVersion(indexId);
    final var internalIndexId = extractInternalId(indexId);
    if (engineAPIVersion != 1) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 1 but found " + engineAPIVersion);
    }

    try {
      final var engine = engineForDereference(internalIndexId, expectedReference);
      return ((MultiValueIndexEngine) engine).remove(atomicOperation, key, value);
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable);
    }
  }

  public boolean removeRidIndexEntry(int indexId, final Object key, final RID value,
      @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var engineAPIVersion = extractEngineAPIVersion(indexId);
    final var internalIndexId = extractInternalId(indexId);

    if (engineAPIVersion != 1) {
      throw new IllegalStateException(
          "Unsupported version of index engine API. Required 1 but found " + engineAPIVersion);
    }

    try {
      return removeRidIndexEntryInternal(atomicOperation, internalIndexId, key, value);
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean removeRidIndexEntryInternal(
      final AtomicOperation atomicOperation, final int indexId, final Object key,
      final RID value)
      throws InvalidIndexEngineIdException {
    checkIndexId(indexId);

    final var engine = indexEngines.get(indexId);
    assert engine.getId() == indexId;

    return ((MultiValueIndexEngine) engine).remove(atomicOperation, key, value);
  }

  /**
   * Puts the given value under the given key into this storage for the index with the given index
   * id. Validates the operation using the provided validator.
   *
   * @param indexId   the index id of the index to put the value into.
   * @param key       the key to put the value under.
   * @param value     the value to put.
   * @param validator the operation validator.
   * @return {@code true} if a new key was inserted, {@code false} if an existing key was updated
   *     in-place or the validator rejected the operation (IGNORE).
   * @see IndexEngineValidator#validate(Object, Object, Object)
   */
  @SuppressWarnings("UnusedReturnValue")
  public boolean validatedPutIndexValue(
      final int indexId,
      @Nullable IndexEngineReference expectedReference,
      final Object key,
      final RID value,
      final IndexEngineValidator<Object, RID> validator,
      @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);
    try {
      return doValidatedPutIndexValue(
          atomicOperation, internalIndexId,
          engineForDereference(internalIndexId, expectedReference), key, value, validator);
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable);
    }
  }

  @SuppressWarnings("UnusedReturnValue")
  public boolean validatedPutIndexValue(
      final int indexId,
      final Object key,
      final RID value,
      final IndexEngineValidator<Object, RID> validator, @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);

    try {
      return doValidatedPutIndexValue(atomicOperation, internalIndexId, key, value, validator);
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private boolean doValidatedPutIndexValue(
      AtomicOperation atomicOperation,
      final int indexId,
      final Object key,
      final RID value,
      final IndexEngineValidator<Object, RID> validator)
      throws InvalidIndexEngineIdException {
    return doValidatedPutIndexValue(
        atomicOperation, indexId, engineByIdentifier(indexId), key, value, validator);
  }

  private boolean doValidatedPutIndexValue(
      AtomicOperation atomicOperation,
      final int indexId,
      final BaseIndexEngine engine,
      final Object key,
      final RID value,
      final IndexEngineValidator<Object, RID> validator) {
    try {
      assert indexId == engine.getId();

      if (engine instanceof IndexEngine indexEngine) {
        return indexEngine.validatedPut(atomicOperation, key, value, validator);
      }

      if (engine instanceof SingleValueIndexEngine singleValueIndexEngine) {
        return singleValueIndexEngine
            .validatedPut(atomicOperation, key, value.getIdentity(), validator);
      }

      throw new IllegalStateException(
          "Invalid type of index engine " + engine.getClass().getName());
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new StorageException(name,
              "Cannot put key " + key + " value " + value + " entry to the index"),
          e, name);
    }
  }

  public Stream<RawPair<Object, RID>> iterateIndexEntriesBetween(
      int indexId, @Nullable IndexEngineReference expectedReference,
      final Object rangeFrom, final boolean fromInclusive,
      final Object rangeTo, final boolean toInclusive, final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer, @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);
    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();
        final var engine = engineForDereference(internalIndexId, expectedReference);
        return engine.iterateEntriesBetween(
            rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer,
            atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error, false);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable, false);
    }
  }

  public Stream<RawPair<Object, RID>> iterateIndexEntriesBetween(
      int indexId,
      final Object rangeFrom,
      final boolean fromInclusive,
      final Object rangeTo,
      final boolean toInclusive,
      final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer, @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);
    try {
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        return doIterateIndexEntriesBetween(
            indexId, rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer,
            atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private Stream<RawPair<Object, RID>> doIterateIndexEntriesBetween(
      final int indexId,
      final Object rangeFrom,
      final boolean fromInclusive,
      final Object rangeTo,
      final boolean toInclusive,
      final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer, AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    checkIndexId(indexId);

    final var engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.iterateEntriesBetween(
        rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer, atomicOperation);
  }

  public Stream<RawPair<Object, RID>> iterateIndexEntriesMajor(
      int indexId, @Nullable IndexEngineReference expectedReference,
      final Object fromKey, final boolean isInclusive, final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer, @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);
    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();
        return engineForDereference(internalIndexId, expectedReference)
            .iterateEntriesMajor(
                fromKey, isInclusive, ascSortOrder, transformer, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error, false);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable, false);
    }
  }

  public Stream<RawPair<Object, RID>> iterateIndexEntriesMajor(
      int indexId,
      final Object fromKey,
      final boolean isInclusive,
      final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer, @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        return doIterateIndexEntriesMajor(indexId, fromKey, isInclusive, ascSortOrder,
            transformer, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private Stream<RawPair<Object, RID>> doIterateIndexEntriesMajor(
      final int indexId,
      final Object fromKey,
      final boolean isInclusive,
      final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer, AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    checkIndexId(indexId);

    final var engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder, transformer,
        atomicOperation);
  }

  public Stream<RawPair<Object, RID>> iterateIndexEntriesMinor(
      int indexId, @Nullable IndexEngineReference expectedReference,
      final Object toKey, final boolean isInclusive, final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer, AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);
    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();
        return engineForDereference(internalIndexId, expectedReference)
            .iterateEntriesMinor(
                toKey, isInclusive, ascSortOrder, transformer, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error, false);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable, false);
    }
  }

  public Stream<RawPair<Object, RID>> iterateIndexEntriesMinor(
      int indexId,
      final Object toKey,
      final boolean isInclusive,
      final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer, AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();

        return doIterateIndexEntriesMinor(indexId, toKey, isInclusive, ascSortOrder, transformer,
            atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private Stream<RawPair<Object, RID>> doIterateIndexEntriesMinor(
      final int indexId,
      final Object toKey,
      final boolean isInclusive,
      final boolean ascSortOrder,
      final IndexEngineValuesTransformer transformer, AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    checkIndexId(indexId);

    final var engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.iterateEntriesMinor(toKey, isInclusive, ascSortOrder, transformer,
        atomicOperation);
  }

  public Stream<RawPair<Object, RID>> getIndexStream(
      int indexId, @Nullable IndexEngineReference expectedReference,
      final IndexEngineValuesTransformer valuesTransformer,
      @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);
    try {
      stateLock.readLock().lock();
      try {
        return engineForDereference(internalIndexId, expectedReference)
            .stream(valuesTransformer, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error, false);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable, false);
    }
  }

  public Stream<RawPair<Object, RID>> getIndexStream(
      int indexId, final IndexEngineValuesTransformer valuesTransformer,
      @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      stateLock.readLock().lock();
      try {
        return doGetIndexStream(indexId, valuesTransformer, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private Stream<RawPair<Object, RID>> doGetIndexStream(
      final int indexId, final IndexEngineValuesTransformer valuesTransformer,
      AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    checkIndexId(indexId);

    final var engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.stream(valuesTransformer, atomicOperation);
  }

  public Stream<RawPair<Object, RID>> getIndexDescStream(
      int indexId, @Nullable IndexEngineReference expectedReference,
      final IndexEngineValuesTransformer valuesTransformer,
      @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);
    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();
        return engineForDereference(internalIndexId, expectedReference)
            .descStream(valuesTransformer, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error, false);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable, false);
    }
  }

  public Stream<RawPair<Object, RID>> getIndexDescStream(
      int indexId, final IndexEngineValuesTransformer valuesTransformer,
      @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();

        return doGetIndexDescStream(indexId, valuesTransformer, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private Stream<RawPair<Object, RID>> doGetIndexDescStream(
      final int indexId, final IndexEngineValuesTransformer valuesTransformer,
      AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    checkIndexId(indexId);

    final var engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.descStream(valuesTransformer, atomicOperation);
  }

  public Stream<Object> getIndexKeyStream(
      int indexId, @Nullable IndexEngineReference expectedReference,
      @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);
    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();
        return engineForDereference(internalIndexId, expectedReference).keyStream(atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error, false);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable, false);
    }
  }

  public Stream<Object> getIndexKeyStream(int indexId, @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();

        return doGetIndexKeyStream(indexId, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private Stream<Object> doGetIndexKeyStream(final int indexId, AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    checkIndexId(indexId);

    final var engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.keyStream(atomicOperation);
  }

  public long getIndexSize(
      int indexId, @Nullable IndexEngineReference expectedReference,
      final IndexEngineValuesTransformer transformer, @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    final var internalIndexId = extractInternalId(indexId);
    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();
        return engineForDereference(internalIndexId, expectedReference)
            .size(this, transformer, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final RuntimeException exception) {
      throw logAndPrepareForRethrow(exception);
    } catch (final Error error) {
      throw logAndPrepareForRethrow(error, false);
    } catch (final Throwable throwable) {
      throw logAndPrepareForRethrow(throwable, false);
    }
  }

  public long getIndexSize(int indexId, final IndexEngineValuesTransformer transformer,
      @Nonnull AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    indexId = extractInternalId(indexId);

    try {
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();

        return doGetIndexSize(indexId, transformer, atomicOperation);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final InvalidIndexEngineIdException ie) {
      throw logAndPrepareForRethrow(ie);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  private long doGetIndexSize(final int indexId, final IndexEngineValuesTransformer transformer,
      AtomicOperation atomicOperation)
      throws InvalidIndexEngineIdException {
    checkIndexId(indexId);

    final var engine = indexEngines.get(indexId);
    assert indexId == engine.getId();

    return engine.size(this, transformer, atomicOperation);
  }

  private void rollback(final Throwable error, @Nonnull AtomicOperation atomicOperation)
      throws IOException {
    atomicOperationsManager.endAtomicOperation(atomicOperation, error);
  }

  public void moveToErrorStateIfNeeded(final Throwable error) {
    if (error != null
        && !((error instanceof HighLevelException)
            || (error instanceof NeedRetryException)
            || (error instanceof InternalErrorException)
            || (error instanceof StorageStateContextException))) {
      setInError(error);
    }
  }

  @Override
  public final void synch() {
    try {
      stateLock.readLock().lock();
      try {
        // Flush dirty histogram stats before freezing — see
        // flushDirtyHistograms() Javadoc for deadlock details.
        if (!isInError()) {
          flushDirtyHistograms(false);
        }

        doSynch(false);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  /**
   * Core sync logic: freeze operations, flush engines and WAL, unfreeze.
   * Extracted so that {@link #freeze} can call it after flushing histograms
   * itself — calling {@link #synch()} from {@code freeze()} would deadlock
   * because the frozen {@code OperationsFreezer} would block the histogram
   * flush's {@code executeInsideAtomicOperation()} call.
   */
  private void doSynch() {
    doSynch(false);
  }

  /**
   * Runs the core synchronization logic, and optionally reports every flush failure.
   *
   * @param failOnFlushFailure true when a flush failure must reach the caller, which the
   *                           durability barrier of storage birth requires
   */
  private void doSynch(final boolean failOnFlushFailure) {
    final var synchStartedAt = System.nanoTime();
    // A TRANSIENT self-quiesce: bounded by the flush body, so schema commits may park behind it
    // exactly like data commits. Note the legal nesting: freeze() (an OPERATOR freeze) calls this
    // method inside its own frozen window, taking the freeze-request count 1->2->1 while the
    // operator-kind count stays 1 — the kind counters tolerate it by construction.
    final var lockId =
        atomicOperationsManager.freezeWriteOperations(FreezeKind.TRANSIENT_QUIESCE, null);
    try {
      checkOpennessAndMigration();

      if (!isInError()) {
        for (final var indexEngine : indexEngines) {
          try {
            if (indexEngine != null) {
              indexEngine.flush();
            }
          } catch (final Throwable t) {
            if (failOnFlushFailure) {
              throw BaseException.wrapException(
                  new StorageException(
                      name,
                      "The durability barrier of database '"
                          + name
                          + "' failed, because one index engine of class "
                          + indexEngine.getClass().getSimpleName()
                          + " could not flush its data"),
                  t,
                  name);
            }
            LogManager.instance()
                .error(
                    this,
                    "Error while flushing index via index engine of class %s.",
                    t,
                    indexEngine.getClass().getSimpleName());
          }
        }

        flushAllData();

      } else {
        if (failOnFlushFailure) {
          requireNoErrorStateForBarrier();
        }
        LogManager.instance()
            .error(
                this,
                "Sync can not be performed because of internal error in storage %s",
                null,
                this.name);
      }

    } finally {
      atomicOperationsManager.unfreezeWriteOperations(lockId);
      synchDuration.setNanos(System.nanoTime() - synchStartedAt);
    }
  }

  @Nullable @Override
  public final String getPhysicalCollectionNameById(final int iCollectionId) {
    try {
      // The commit window (a schema-carrying commit holding stateLock.writeLock())
      // reaches this method through the security check in session.executeReadRecord
      // while serializing/promoting the schema. Re-acquiring the read lock there would
      // deadlock the non-reentrant ScalableRWLock, so skip it: the held write lock
      // already excludes registrars and supplies the visibility edge.
      final boolean lockFree = isCommitWindowActive();
      if (!lockFree) {
        stateLock.readLock().lock();
      }
      try {
        checkOpennessAndMigration();

        return doGetPhysicalCollectionNameById(iCollectionId);
      } finally {
        if (!lockFree) {
          stateLock.readLock().unlock();
        }
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  /**
   * Lock-free body of {@link #getPhysicalCollectionNameById(int)}. Reads the plain
   * {@code collections} list directly without taking {@code stateLock}. The public
   * wrapper holds {@code stateLock.readLock()} for normal callers; the commit window
   * holds {@code stateLock.writeLock()} and skips the read lock. Either held lock is
   * the precondition: off-lock use is a data race on a plain {@code ArrayList}.
   */
  private String doGetPhysicalCollectionNameById(final int iCollectionId) {
    if (iCollectionId < 0 || iCollectionId >= collections.size()) {
      return null;
    }

    return collections.get(iCollectionId) != null ? collections.get(iCollectionId).getName()
        : null;
  }

  @Override
  public String getCollectionName(DatabaseSessionEmbedded database, int collectionId) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      if (collectionId == RID.COLLECTION_ID_INVALID) {
        throw new StorageException(name, "Invalid collection id was provided: " + collectionId);
      }

      return doGetAndCheckCollection(collectionId).getName();

    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  public final int getCollections() {
    try {
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        return collectionMap.size();
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public final Set<StorageCollection> getCollectionInstances() {
    try {
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        final Set<StorageCollection> result = new HashSet<>(1024);

        // ADD ALL THE COLLECTIONS
        for (final var c : collections) {
          if (c != null) {
            result.add(c);
          }
        }

        return result;

      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public final void freeze(DatabaseSessionEmbedded db, final boolean throwException) {
    try {
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        // Flush dirty histogram stats before freezing — see
        // flushDirtyHistograms() Javadoc for deadlock details.
        if (!isInError()) {
          flushDirtyHistograms();
        }

        // Both arms are OPERATOR freezes (the admin-initiated filesystem-snapshot freeze, of
        // unbounded duration until release()): the kind arms the schema-commit gate, so a
        // schema-carrying commit aborts loudly instead of parking inside its four-lock window
        // for the freeze's whole duration.
        //
        // LOAD-BEARING PREMISE: this is the ONLY production site that registers an operator
        // freeze, and it does so while holding stateLock.readLock (taken at the top of this
        // method). That is the belt two other things rely on: (a) the in-freezer loop-top and
        // park-decision gates and the operator-arm wake are exercised in production only through
        // this readLock-held path; and (b) the failure-path undo helpers
        // (revertCreatedCollectionStructure, revertCreatedIndexEngineStructure,
        // restoreReconciledDroppedIndexEngines) enter the freezer UNARMED while holding
        // stateLock.writeLock, and are safe precisely because no operator freeze can arm while
        // they hold the write lock — registering one here would first need this readLock, which
        // the writeLock excludes. A future operator-freeze registration added on a path that does
        // NOT hold stateLock.readLock would break both belts (see those helpers' notes).
        final long freezeId;
        if (throwException) {
          freezeId = atomicOperationsManager.freezeWriteOperations(
              FreezeKind.OPERATOR,
              () -> new ModificationOperationProhibitedException(name,
                  "Modification requests are prohibited"));
        } else {
          freezeId = atomicOperationsManager.freezeWriteOperations(FreezeKind.OPERATOR, null);
        }
        // Retain the real id immediately after registration (before anything below can throw),
        // so the paired release() releases by id and the freezer's id->kind record is removed
        // instead of leaking one entry per freeze/release cycle.
        operatorFreezeIds.push(freezeId);

        final List<FreezableStorageComponent> frozenIndexes = new ArrayList<>(indexEngines.size());
        try {
          for (final var indexEngine : indexEngines) {
            if (indexEngine instanceof FreezableStorageComponent freezable) {
              freezable.freeze(db, false);
              frozenIndexes.add(freezable);
            }
          }
        } catch (final Exception e) {
          // RELEASE ALL THE FROZEN INDEXES
          for (final var indexEngine : frozenIndexes) {
            indexEngine.release(db);
          }

          throw BaseException.wrapException(
              new StorageException(name, "Error on freeze of storage '" + name + "'"), e, name);
        }

        // Use doSynch() instead of synch() — histograms were already
        // flushed above and the operations are already frozen by us.
        doSynch();
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final void release(DatabaseSessionEmbedded db) {
    try {
      for (final var indexEngine : indexEngines) {
        if (indexEngine instanceof FreezableStorageComponent freezable) {
          freezable.release(db);
        }
      }

      // Release by the real id the paired freeze() retained, so the freezer removes the
      // freeze's id->kind record (see the operatorFreezeIds field javadoc for why LIFO pairing
      // across concurrent cycles is sound). The -1 sentinel is only the guarded fallback for a
      // release() without a matching freeze() (a double release): it still maps explicitly to
      // the OPERATOR decrement, whose CAS-floor guard then logs the underflow.
      final var freezeId = operatorFreezeIds.poll();
      atomicOperationsManager.unfreezeWriteOperations(freezeId != null ? freezeId : -1);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final boolean isRemote() {
    return false;
  }

  public boolean wereDataRestoredAfterOpen() {
    return wereDataRestoredAfterOpen;
  }

  /**
   * Test-observability only: the number of times {@code truncateOrphansAfterRecovery} has
   * been dispatched on this storage instance. Production code never reads it; a regression
   * test (YTDB-1039) reads it to prove the open-time pass is skipped on a clean reopen and
   * runs on a crash (WAL-replay) reopen.
   */
  int orphanTruncationDispatchCountForTests() {
    return orphanTruncationDispatchCountForTests.get();
  }

  public boolean wereNonTxOperationsPerformedInPreviousOpen() {
    return wereNonTxOperationsPerformedInPreviousOpen;
  }

  @Override
  public final void reload(DatabaseSessionEmbedded database) {
    try {
      close(database);
      open(new ContextConfiguration());
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  /** {@inheritDoc} */
  @Override
  public final void pageIsBroken(final String fileName, final long pageIndex) {
    LogManager.instance()
        .error(
            this,
            "In storage %s file with name '%s' has broken page under the index %d",
            null,
            name,
            fileName,
            pageIndex);

    if (status == STATUS.CLOSED) {
      return;
    }

    setInError(new StorageException(name, "Page " + pageIndex + " is broken in file " + fileName));

    try {
      makeStorageDirty();
    } catch (final IOException e) {
      // ignore
    }
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public final void requestCheckpoint() {
    try {
      if (!walVacuumInProgress.get() && walVacuumInProgress.compareAndSet(false, true)) {
        YouTrackDBEnginesManager.instance().getFuzzyCheckpointExecutor()
            .submit(new WALVacuum(this));
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  @Override
  public final PhysicalPosition[] higherPhysicalPositions(
      DatabaseSessionEmbedded session, final int currentCollectionId,
      final PhysicalPosition physicalPosition, int limit) {
    try {
      if (currentCollectionId == -1) {
        return new PhysicalPosition[0];
      }

      var transaction = session.getActiveTransaction();
      var atomicOperation = transaction.getAtomicOperation();

      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();

        final var collection = doGetAndCheckCollection(currentCollectionId);
        return collection.higherPositions(physicalPosition, limit, atomicOperation);
      } catch (final IOException ioe) {
        throw BaseException.wrapException(
            new StorageException(name,
                "Collection Id " + currentCollectionId + " is invalid in storage '" + name + '\''),
            ioe, name);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public final PhysicalPosition[] ceilingPhysicalPositions(
      DatabaseSessionEmbedded session, final int collectionId,
      final PhysicalPosition physicalPosition, int limit) {
    try {
      if (collectionId == -1) {
        return new PhysicalPosition[0];
      }

      var transaction = session.getActiveTransaction();
      var atomicOperation = transaction.getAtomicOperation();
      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();

        final var collection = doGetAndCheckCollection(collectionId);
        return collection.ceilingPositions(physicalPosition, limit, atomicOperation);
      } catch (final IOException ioe) {
        throw BaseException.wrapException(
            new StorageException(name,
                "Collection Id " + collectionId + " is invalid in storage '" + name + '\''),
            ioe, name);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public final PhysicalPosition[] lowerPhysicalPositions(
      DatabaseSessionEmbedded session, final int currentCollectionId,
      final PhysicalPosition physicalPosition, int limit) {
    try {
      if (currentCollectionId == -1) {
        return new PhysicalPosition[0];
      }

      var transaction = session.getActiveTransaction();
      var atomicOperation = transaction.getAtomicOperation();
      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();

        final var collection = doGetAndCheckCollection(currentCollectionId);

        return collection.lowerPositions(physicalPosition, limit, atomicOperation);
      } catch (final IOException ioe) {
        throw BaseException.wrapException(
            new StorageException(name,
                "Collection Id " + currentCollectionId + " is invalid in storage '" + name + '\''),
            ioe, name);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public final PhysicalPosition[] floorPhysicalPositions(
      DatabaseSessionEmbedded session, final int collectionId,
      final PhysicalPosition physicalPosition, int limit) {
    try {
      if (collectionId == -1) {
        return new PhysicalPosition[0];
      }

      var transaction = session.getActiveTransaction();
      var atomicOperation = transaction.getAtomicOperation();

      stateLock.readLock().lock();
      try {
        checkOpennessAndMigration();
        final var collection = doGetAndCheckCollection(collectionId);

        return collection.floorPositions(physicalPosition, limit, atomicOperation);
      } catch (final IOException ioe) {
        throw BaseException.wrapException(
            new StorageException(name,
                "Collection Id " + collectionId + " is invalid in storage '" + name + '\''),
            ioe, name);
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public final RecordConflictStrategy getRecordConflictStrategy() {
    return recordConflictStrategy;
  }

  @Override
  public final void setConflictStrategy(final RecordConflictStrategy conflictResolver) {
    Objects.requireNonNull(conflictResolver);
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> doSetConflictStrategy(conflictResolver, atomicOperation));
    } catch (final Exception e) {
      throw BaseException.wrapException(
          new StorageException(name,
              "Exception during setting of conflict strategy "
                  + conflictResolver.getName()
                  + " for storage "
                  + name),
          e, name);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  private void doSetConflictStrategy(
      RecordConflictStrategy conflictResolver, AtomicOperation atomicOperation) {

    if (recordConflictStrategy == null
        || !recordConflictStrategy.getName().equals(conflictResolver.getName())) {

      this.recordConflictStrategy = conflictResolver;
      ((CollectionBasedStorageConfiguration) configuration)
          .setConflictStrategy(atomicOperation, conflictResolver.getName());
    }
  }

  @SuppressWarnings("unused")
  protected abstract LogSequenceNumber copyWALToBackup(
      ZipOutputStream zipOutputStream, long startSegment) throws IOException;

  @Nullable @SuppressWarnings("unused")
  public StorageRecoverListener getRecoverListener() {
    return recoverListener;
  }

  @SuppressWarnings("unused")
  public void unregisterRecoverListener(final StorageRecoverListener recoverListener) {
    if (this.recoverListener == recoverListener) {
      this.recoverListener = null;
    }
  }

  @SuppressWarnings("unused")
  protected abstract File createWalTempDirectory();

  @SuppressWarnings("unused")
  protected abstract WriteAheadLog createWalFromIBUFiles(
      File directory,
      final ContextConfiguration contextConfiguration,
      final Locale locale,
      byte[] iv)
      throws IOException;

  /**
   * Checks if the storage is open. If it's closed an exception is raised.
   */
  protected final void checkOpennessAndMigration() {
    final var status = this.status;

    if (status == STATUS.MIGRATION) {
      throw new StorageException(name,
          "Storage data are under migration procedure, please wait till data will be migrated.");
    }

    if (status != STATUS.OPEN) {
      throw new StorageException(name, "Storage " + name + " is not opened.");
    }
  }

  protected boolean isInError() {
    return this.error.get() != null;
  }

  public void checkErrorState() {
    if (this.error.get() != null) {
      throw BaseException.wrapException(
          new StorageException(name,
              "Internal error happened in storage "
                  + name
                  + " please restart the server or re-open the storage to undergo the restore"
                  + " process and fix the error."),
          this.error.get(), name);
    }
  }

  public final void makeFuzzyCheckpoint() {
    // check every 1 ms.
    while (true) {
      try {
        if (stateLock.readLock().tryLock(1, TimeUnit.MILLISECONDS)) {
          break;
        }
      } catch (java.lang.InterruptedException e) {
        throw BaseException.wrapException(
            new ThreadInterruptedException("Fuzzy check point was interrupted"), e, name);
      }

      // Bail out if the storage is no longer in an operational state.
      // The original condition used || which is always true (tautology) —
      // it should be && to express "status is neither OPEN nor MIGRATION".
      if (status != STATUS.OPEN && status != STATUS.MIGRATION) {
        return;
      }
    }

    try {

      if (status != STATUS.OPEN && status != STATUS.MIGRATION) {
        return;
      }

      // Flush dirty histogram statistics to .ixs pages (Section 3.2).
      // Runs before the WAL checkpoint so that the flushed pages are
      // included in writeCache.syncDataFiles().
      flushDirtyHistograms();

      var beginLSN = writeAheadLog.begin();
      var endLSN = writeAheadLog.end();

      final var minLSNSegment = writeCache.getMinimalNotFlushedSegment();

      long fuzzySegment;

      if (minLSNSegment != null) {
        fuzzySegment = minLSNSegment;
      } else {
        if (endLSN == null) {
          return;
        }

        fuzzySegment = endLSN.getSegment();
      }

      atomicOperationsTable.compactTable();
      final var minAtomicOperationSegment =
          atomicOperationsTable.getSegmentEarliestNotPersistedOperation();
      if (minAtomicOperationSegment >= 0 && fuzzySegment > minAtomicOperationSegment) {
        fuzzySegment = minAtomicOperationSegment;
      }

      LogManager.instance()
          .debug(
              this,
              "Before fuzzy checkpoint: min LSN segment is %s, "
                  + "WAL begin is %s, WAL end is %s fuzzy segment is %d",
              logger, minLSNSegment,
              beginLSN,
              endLSN,
              fuzzySegment);

      if (fuzzySegment > beginLSN.getSegment() && beginLSN.getSegment() < endLSN.getSegment()) {
        LogManager.instance().debug(this, "Making fuzzy checkpoint", logger);
        writeCache.syncDataFiles(fuzzySegment);

        beginLSN = writeAheadLog.begin();
        endLSN = writeAheadLog.end();

        LogManager.instance()
            .debug(this, "After fuzzy checkpoint: WAL begin is %s WAL end is %s", logger, beginLSN,
                endLSN);
      } else {
        LogManager.instance().debug(this, "No reason to make fuzzy checkpoint", logger);
      }
    } catch (final IOException ioe) {
      throw BaseException.wrapException(new YTIOException("Error during fuzzy checkpoint"), ioe,
          name);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  public void deleteTreeLinkBag(final BTreeBasedLinkBag ridBag,
      @Nonnull AtomicOperation atomicOperation) {
    try {
      checkOpennessAndMigration();

      doDeleteTreeLinkBag(ridBag, atomicOperation);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    }
  }

  private void doDeleteTreeLinkBag(BTreeBasedLinkBag ridBag, AtomicOperation atomicOperation) {
    final var collectionPointer = ridBag.getCollectionPointer();
    checkOpennessAndMigration();

    try {
      makeStorageDirty();
      linkCollectionsBTreeManager.delete(atomicOperation, collectionPointer, name);
    } catch (final Exception e) {
      LogManager.instance().error(this, "Error during deletion of rid bag", e);
      throw BaseException.wrapException(
          new StorageException(name, "Error during deletion of ridbag"),
          e, name);
    }

    ridBag.confirmDelete();
  }

  /**
   * Iterates all B-tree index engines and flushes dirty histogram state
   * to their .ixs pages. Called during fuzzy checkpoint, synch, close,
   * and recovery.
   *
   * <p><b>Important:</b> this method must NOT be called inside a frozen
   * {@link com.jetbrains.youtrackdb.internal.core.storage.impl.local
   * .paginated.atomicoperations.operationsfreezer.OperationsFreezer}
   * scope. Each engine's flush creates its own AtomicOperation via
   * {@code executeInsideAtomicOperation()}, which would deadlock if the
   * freezer is already frozen.
   *
   * <p>Failures are logged but never propagated — histogram persistence is
   * best-effort and must not block checkpoint or shutdown.
   */
  private void flushDirtyHistograms() {
    flushDirtyHistograms(false);
  }

  /**
   * Flushes dirty index histogram data, and optionally reports every flush failure.
   *
   * <p>An index histogram is index statistics data that the index engine keeps in a separate file.
   * The write-ahead log never carries that data, so a swallowed failure loses that data forever.
   * The durability barrier of storage birth therefore reports every such failure to the caller.
   *
   * <p>The strict path calls the strict flush variant of the index histogram manager. The
   * best-effort variant logs an input and output failure and returns, so the strict path would
   * otherwise never observe such a failure.
   *
   * @param failOnFlushFailure true when a flush failure must reach the caller
   */
  private void flushDirtyHistograms(final boolean failOnFlushFailure) {
    for (var engine : indexEngines) {
      if (engine instanceof BTreeIndexEngine btreeEngine) {
        var mgr = btreeEngine.getHistogramManager();
        if (mgr != null) {
          try {
            if (failOnFlushFailure) {
              mgr.flushIfDirtyOrFail();
            } else {
              mgr.flushIfDirty();
            }
          } catch (Exception e) {
            if (failOnFlushFailure) {
              throw BaseException.wrapException(
                  new StorageException(
                      name,
                      "The durability barrier of database '"
                          + name
                          + "' failed, because the index statistics of index "
                          + mgr.getName()
                          + " could not reach durable media"),
                  e,
                  name);
            }
            LogManager.instance().error(this,
                "Failed to flush histogram stats for engine %d (%s)"
                    + " — histogram data may be stale after restart",
                e, btreeEngine.getId(), mgr.getName());
          }
        }
      }
    }
  }

  /**
   * Waits for any in-progress background histogram rebalances to finish
   * and permanently blocks future ones. Must be called before clearing
   * index engines and deleting/closing the cache to prevent background
   * rebalance threads from holding page references on deleted pages.
   *
   * <p>Uses {@link IndexHistogramManager#closeStatsFile()} which
   * internally calls {@code waitForAndBlockRebalance()}, flushes dirty
   * data, removes the cache entry, and resets the fileId.
   */
  private void cancelHistogramRebalances() {
    for (var engine : indexEngines) {
      if (engine instanceof BTreeIndexEngine btreeEngine) {
        var mgr = btreeEngine.getHistogramManager();
        if (mgr != null) {
          try {
            mgr.closeStatsFile();
          } catch (Exception e) {
            LogManager.instance().error(this,
                "Failed to close histogram stats for engine %d (%s)",
                e, btreeEngine.getId(), mgr.getName());
          }
        }
      }
    }
  }

  /**
   * Propagates a general-purpose executor to all histogram managers for
   * background rebalance work. Called after the database is fully open.
   * <p>
   * <b>Important:</b> The executor must NOT be the {@code ioExecutor} used
   * by {@code AsynchronousFileChannel} for I/O completions. Running blocking
   * page reads on the ioExecutor deadlocks because the completion callbacks
   * need the same thread pool.
   * <p>
   * Also triggers a proactive rebalance check on each manager — if mutations
   * accumulated before a crash exceeded the threshold, a background rebalance
   * is scheduled immediately.
   *
   * @param executor the general-purpose executor (must NOT be the ioExecutor
   *                 used by AsynchronousFileChannel — see class Javadoc)
   */
  public void setHistogramExecutor(ExecutorService executor) {
    this.histogramExecutor = executor;
    stateLock.readLock().lock();
    try {
      for (var engine : indexEngines) {
        if (engine instanceof BTreeIndexEngine btreeEngine) {
          var mgr = btreeEngine.getHistogramManager();
          if (mgr != null) {
            mgr.setBackgroundExecutor(executor);
          }
        }
      }
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Flushes WAL, write cache, and cuts the log. Does <b>not</b> flush
   * histogram data — callers that need histogram persistence must call
   * {@link #flushDirtyHistograms()} separately <b>before</b> this method,
   * and outside any frozen {@code OperationsFreezer} scope (see
   * {@link #flushDirtyHistograms()} Javadoc for details).
   */
  protected void flushAllData() {
    try {
      writeAheadLog.flush();

      // so we will be able to cut almost all the log
      writeAheadLog.appendNewSegment();

      final var lastLSN = writeAheadLog.log(new EmptyWALRecord());
      writeCache.flush();

      atomicOperationsTable.compactTable();
      final var operationSegment = atomicOperationsTable.getSegmentEarliestOperationInProgress();
      if (operationSegment >= 0) {
        throw new IllegalStateException(
            "Can not perform full checkpoint if some of atomic operations in progress");
      }

      writeAheadLog.flush();

      writeAheadLog.cutTill(lastLSN);

      clearStorageDirty();

    } catch (final IOException ioe) {
      throw BaseException.wrapException(
          new StorageException(name, "Error during checkpoint creation for storage " + name), ioe,
          name);
    }
  }

  protected StartupMetadata checkIfStorageDirty() throws IOException {
    return new StartupMetadata(-1);
  }

  protected void initConfiguration(
      final ContextConfiguration contextConfiguration,
      AtomicOperation atomicOperation) {
  }

  @SuppressWarnings({"EmptyMethod"})
  protected final void postCreateSteps() {
  }

  protected void preCreateSteps() throws IOException {
  }

  /**
   * Reports whether this image already holds content that a creation must not overwrite.
   *
   * <p>The default answer is the plain existence of this storage. A storage type that can hold
   * one residue file without any content overrides this method.
   */
  protected boolean existsBeforeCreation() {
    return exists();
  }

  protected abstract void initWalAndDiskCache(ContextConfiguration contextConfiguration)
      throws IOException, java.lang.InterruptedException;

  protected abstract void postCloseSteps(
      @SuppressWarnings("unused") boolean onDelete, boolean internalError, long lastTxId)
      throws IOException;

  @SuppressWarnings({"EmptyMethod"})
  protected Map<String, Object> preCloseSteps() {
    return new HashMap<>(2);
  }

  protected void postDeleteSteps() {
  }

  protected void makeStorageDirty() throws IOException {
  }

  protected void clearStorageDirty() throws IOException {
  }

  protected boolean isDirty() {
    return false;
  }

  @Nullable protected String getOpenedAtVersion() {
    return null;
  }

  @Nonnull
  private StorageReadResult readRecordInternal(@Nonnull final RecordIdInternal rid,
      @Nonnull AtomicOperation atomicOperation) {
    if (!rid.isPersistent()) {
      throw new RecordNotFoundException(name,
          rid, "Cannot read record "
              + rid
              + " since the position is invalid in database '"
              + name
              + '\'');
    }
    // The commit window (a schema-carrying commit holding stateLock.writeLock())
    // reaches this method through session.executeReadRecord on a genuine record-cache
    // miss while serializing/promoting the schema. Re-acquiring the read lock there
    // would deadlock the non-reentrant ScalableRWLock, so skip it: the held write lock
    // already excludes registrars and supplies the visibility edge.
    final boolean lockFree = isCommitWindowActive();
    if (!lockFree) {
      stateLock.readLock().lock();
    }
    try {
      checkOpennessAndMigration();
      final StorageCollection collection;
      try {
        collection = doGetAndCheckCollection(rid.getCollectionId());
      } catch (IllegalArgumentException e) {
        throw BaseException.wrapException(new RecordNotFoundException(name, rid), e, name);
      }
      return doReadRecord(collection, rid, atomicOperation);
    } finally {
      if (!lockFree) {
        stateLock.readLock().unlock();
      }
    }
  }

  @Override
  public boolean recordExists(DatabaseSessionEmbedded session, RID rid,
      @Nonnull AtomicOperation atomicOperation) {
    if (!rid.isPersistent()) {
      throw new RecordNotFoundException(name,
          rid, "Cannot read record "
              + rid
              + " since the position is invalid in database '"
              + name
              + '\'');
    }
    stateLock.readLock().lock();
    try {
      checkOpennessAndMigration();
      final StorageCollection collection;
      try {
        collection = doGetAndCheckCollection(rid.getCollectionId());
      } catch (IllegalArgumentException e) {
        return false;
      }
      return doRecordExists(name, collection, rid, atomicOperation);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  private void endTxCommit(AtomicOperation atomicOperation) throws IOException {
    final var failureHook = endTxCommitFailureTestHook;
    if (failureHook != null) {
      try {
        failureHook.run();
      } catch (final RuntimeException injected) {
        // Mirror the production endTxCommit failure shape (a lifecycle persist-hook failure
        // inside endAtomicOperation): end the operation with the injected error so it is rolled
        // back (rollbackInProgress set, nothing durable) and the freezer/lock teardown runs, then
        // rethrow. The commit finally's endTxCommit catch then routes the failure through the
        // registry undo/restore arms exactly as it would for the real failure.
        atomicOperationsManager.endAtomicOperation(atomicOperation, injected);
        throw injected;
      }
    }
    atomicOperationsManager.endAtomicOperation(atomicOperation, null);
    final var postDurabilityHook = endTxCommitPostDurabilityFailureTestHook;
    if (postDurabilityHook != null) {
      // A throwing hook simulates the commitChanges-throw failure shape: the operation is fully
      // ended (freezer/lock teardown done) with rollbackInProgress FALSE, exactly what the real
      // shape leaves behind — except that here durability is certain (endAtomicOperation
      // succeeded) where the real shape leaves it in-doubt. The commit finally's endTxCommit
      // catch must then leave the registry publication standing and move the storage to error
      // state instead of undoing.
      postDurabilityHook.run();
    }
  }

  public AtomicOperation startStorageTx() {
    checkOpennessAndMigration();

    var atomicOperation = atomicOperationsManager.startAtomicOperation();

    var holder = tsMinThreadLocal.get();
    assert holder.activeTxCount >= 0 : "activeTxCount is negative: " + holder.activeTxCount;

    // Capture diagnostic metadata before the volatile tsMin write, so the monitor never
    // sees an active tsMin without valid diagnostic fields. Symmetric with resetTsMin(),
    // which clears diagnostic fields before the opaque tsMin reset.
    if (holder.activeTxCount == 0) {
      boolean captureStackTrace = configuration != null
          && configuration.getContextConfiguration().getValueAsBoolean(
              GlobalConfiguration.STORAGE_TX_CAPTURE_STACK_TRACE);
      holder.captureDiagnostics(
          YouTrackDBEnginesManager.instance().getTicker().approximateNanoTime(),
          Thread.currentThread(),
          captureStackTrace);
    }

    long snapshotMin = atomicOperation.getAtomicOperationsSnapshot().minActiveOperationTs();
    // Use min to preserve the oldest snapshot when multiple txs overlap on this thread.
    // GC may retain entries slightly longer than necessary, but correctness is preserved.
    holder.tsMin = Math.min(holder.tsMin, snapshotMin);
    holder.activeTxCount++;
    if (!holder.registeredInTsMins) {
      tsMins.add(holder);
      holder.registeredInTsMins = true;
    }

    return atomicOperation;
  }

  /**
   * Decrements the current thread's active transaction count and resets {@code tsMin} to
   * {@code Long.MAX_VALUE} when no transactions remain active. Called at transaction end
   * (commit or rollback). Multiple sessions on the same thread may have overlapping
   * transactions, so {@code tsMin} is only cleared when the last one ends.
   *
   * <p>The reset uses an opaque write ({@link TsMinHolder#setTsMinOpaque}) instead of a
   * volatile write: no {@code StoreLoad} barrier is needed because the owning thread has no
   * subsequent loads that depend on the reset being globally visible immediately.
   *
   * <p>When the last transaction on the thread closes ({@code activeTxCount} reaches 0),
   * this method also triggers snapshot index cleanup. This ensures stale snapshot entries
   * are evicted after both read and write transactions, not just write commits. The cleanup
   * is best-effort: it uses {@code tryLock()} internally, so it never blocks, and any
   * failure is logged but does not propagate — it must not mask transaction close errors.
   */
  public void resetTsMin() {
    var holder = tsMinThreadLocal.get();
    assert holder.activeTxCount > 0 : "activeTxCount underflow: " + holder.activeTxCount;
    if (holder.activeTxCount <= 0) {
      throw new IllegalStateException(
          "resetTsMin called with no active transactions (activeTxCount="
              + holder.activeTxCount + ")");
    }
    holder.activeTxCount--;
    if (holder.activeTxCount == 0) {
      // Clear diagnostic fields before resetting tsMin. The monitor reads tsMin first:
      // if it sees tsMin != MAX_VALUE, it expects txStartTimeNanos to be non-zero.
      // Clearing diagnostic fields first ensures that if the monitor sees an active tsMin,
      // the diagnostic fields are still valid (not yet cleared). The opaque write of tsMin
      // below does not provide a StoreStore barrier, so without this ordering the monitor
      // could see stale tsMin but cleared txStartTimeNanos.
      holder.clearDiagnostics();
      holder.setTsMinOpaque(Long.MAX_VALUE);
      try {
        cleanupSnapshotIndex();
      } catch (final RuntimeException e) {
        // Cleanup is best-effort — its failure must never mask a successful transaction
        // close. Stale snapshot entries simply accumulate until the next successful
        // cleanup pass.
        LogManager.instance()
            .warn(this, "Snapshot index cleanup failed during resetTsMin", e);
      }
    }
  }

  /**
   * Starts the commit's apply window, threading the schema-arm signal into the freezer: a
   * schema-carrying commit's entrant carries the shared gate-exception factory so the freezer's
   * loop-top and park-decision checkpoints abort it loudly under an operator freeze instead of
   * parking it with all four locks held. Data commits enter unarmed — byte-for-byte the
   * historical park semantics.
   */
  private void startTxCommit(AtomicOperation atomicOperation, final boolean schemaArmed) {
    atomicOperationsManager.startToApplyOperations(atomicOperation, schemaArmed,
        schemaArmed ? this::operatorFreezeGateException : null);
  }

  /**
   * The poll bound of the checkpoint-(2) write-lock acquisition: the phase-1 per-attempt park
   * bound of {@code exclusiveLockWithAbort} and thus the coarsest operator-freeze abort latency
   * while queued; the phase-2 drain polls the predicate per iteration regardless. 1ms keeps the
   * abort latency negligible against an operator freeze's (human-scale) duration while adding no
   * measurable cost to the rare schema-commit path.
   */
  private static final long OPERATOR_FREEZE_ABORT_POLL_NANOS =
      TimeUnit.MILLISECONDS.toNanos(1);

  /**
   * The single kind probe of the schema-commit freezer gate (one shared helper): the entry
   * probe, the write-lock abort predicate, and — through the threaded supplier — the two
   * in-freezer checkpoints all read this one counter probe, so the four checkpoints cannot
   * drift.
   */
  private boolean isOperatorFreezeActive() {
    return atomicOperationsManager.isOperatorFreezeActive();
  }

  /**
   * The single gate-exception factory of the schema-commit freezer gate: the stable,
   * tested message names the storage and distinguishes a gate/probe abort from the legacy
   * throw-mode freeze supplier's wording. Type {@link ModificationOperationProhibitedException}
   * (a {@code HighLevelException}), so the abort is loud, retryable, and never moves the storage
   * to error state.
   */
  private ModificationOperationProhibitedException operatorFreezeGateException() {
    return new ModificationOperationProhibitedException(name,
        "Schema commit aborted: operator freeze in progress on storage '" + name
            + "' - modifications are prohibited until release; retry after the storage is"
            + " released");
  }

  private void recoverIfNeeded() throws Exception {
    if (isDirty()) {
      LogManager.instance()
          .warn(
              this,
              "Storage '"
                  + name
                  + "' was not closed properly. Will try to recover from write ahead log");
      try {
        final var openedAtVersion = getOpenedAtVersion();

        if (openedAtVersion != null && !openedAtVersion.equals(
            YouTrackDBConstants.getRawVersion())) {
          throw new StorageException(name,
              "Database has been opened at version "
                  + openedAtVersion
                  + " but is attempted to be restored at version "
                  + YouTrackDBConstants.getRawVersion()
                  + ". Please use correct version to restore database.");
        }

        wereDataRestoredAfterOpen = true;

        // Delete non-durable files before WAL replay — their data is unrecoverable
        // (no WAL records exist). The returned set is used by restoreAtomicUnit() to
        // skip WAL records referencing these files.
        deletedNonDurableFileIds = writeCache.deleteNonDurableFilesOnRecovery(readCache);

        try {
          restoreFromWAL();
        } finally {
          // Clear after replay — no longer needed, and prevents stale references
          deletedNonDurableFileIds = new IntOpenHashSet();
        }

        if (recoverListener != null) {
          recoverListener.onStorageRecover();
        }

        flushDirtyHistograms();
        flushAllData();
      } catch (final Exception e) {
        LogManager.instance().error(this, "Exception during storage data restore", e);
        throw e;
      }

      LogManager.instance().info(this, "Storage data recover was completed");
    }
  }

  /**
   * Creates a record inside a caller-owned atomic operation. One operation may use these record
   * primitives for one collection only. Future multi-collection support must lock collections in
   * ascending identifier order before mutation.
   *
   * @return the initial durable record version
   */
  public long createRecordInsideAtomicOperation(
      final AtomicOperation atomicOperation,
      final RecordIdInternal rid,
      @Nonnull final byte[] content,
      final byte recordType) throws IOException {
    checkCallerOwnsStateLock();
    return createRecordInsideCommitAtomicOperation(atomicOperation, rid, content, recordType);
  }

  /** Creates a record while the commit caller holds the storage state lock. */
  long createRecordInsideCommitAtomicOperation(
      final AtomicOperation atomicOperation,
      final RecordIdInternal rid,
      @Nonnull final byte[] content,
      final byte recordType) throws IOException {
    checkCallerOwnsStateLock();
    checkOpennessAndMigration();
    if (!rid.isNew() || !(rid instanceof ChangeableRecordId)) {
      throw new IllegalArgumentException(
          "Record creation requires a new changeable record identifier: " + rid);
    }

    atomicOperation.validateRecordCollectionLockOrder(rid.getCollectionId());
    makeStorageDirty();
    final var collection = doGetAndCheckCollection(rid.getCollectionId());
    collection.acquireAtomicExclusiveLock(atomicOperation);
    final var position =
        doCreateRecord(
            atomicOperation,
            rid,
            content,
            atomicOperation.getCommitTs(),
            recordType,
            collection,
            null);
    return position.recordVersion;
  }

  /** Deletes a record inside a caller-owned atomic operation. */
  public void deleteRecordInsideAtomicOperation(
      final AtomicOperation atomicOperation,
      final RecordIdInternal rid,
      final long expectedVersion) throws IOException {
    checkCallerOwnsStateLock();
    checkOpennessAndMigration();
    if (!rid.isPersistent()) {
      throw new IllegalArgumentException(
          "Record deletion requires a persistent identifier: " + rid);
    }

    atomicOperation.validateRecordCollectionLockOrder(rid.getCollectionId());
    makeStorageDirty();
    final var collection = doGetAndCheckCollection(rid.getCollectionId());
    collection.acquireAtomicExclusiveLock(atomicOperation);
    doDeleteRecord(atomicOperation, rid, expectedVersion, collection);
  }

  /**
   * Updates a record inside a caller-owned atomic operation and checks its durable version.
   *
   * @return the new durable record version
   */
  public long updateRecordInsideAtomicOperation(
      final AtomicOperation atomicOperation,
      final RecordIdInternal rid,
      @Nonnull final byte[] content,
      final long expectedVersion,
      final byte recordType) throws IOException {
    checkCallerOwnsStateLock();
    checkOpennessAndMigration();
    if (!rid.isPersistent()) {
      throw new IllegalArgumentException("Record update requires a persistent identifier: " + rid);
    }

    atomicOperation.validateRecordCollectionLockOrder(rid.getCollectionId());
    makeStorageDirty();
    final var collection = doGetAndCheckCollection(rid.getCollectionId());
    collection.acquireAtomicExclusiveLock(atomicOperation);
    return doUpdateRecord(
        atomicOperation, rid, true, content, expectedVersion, recordType, collection);
  }

  private void checkCallerOwnsStateLock() {
    if (!callerOwnsStateProtection()) {
      throw new StorageStateContextException(
          "Caller-owned record writes require the storage state lock or an active commit window");
    }
  }

  private PhysicalPosition doCreateRecord(
      final AtomicOperation atomicOperation,
      final RecordIdInternal rid,
      @Nonnull final byte[] content,
      long recordVersion,
      final byte recordType,
      final StorageCollection collection,
      final PhysicalPosition allocated) {
    //noinspection ConstantValue
    if (content == null) {
      throw new IllegalArgumentException("Record is null");
    }

    collection.meters().create().record();
    PhysicalPosition ppos;
    try {
      ppos = collection.createRecord(content, recordType, allocated, atomicOperation);
      if (rid instanceof ChangeableRecordId changeableRecordId) {
        changeableRecordId.setCollectionPosition(ppos.collectionPosition);
      } else {
        throw new DatabaseException(name,
            "Provided record is not new and its position can not be changed");
      }
    } catch (final Exception e) {
      LogManager.instance().error(this, "Error on creating record in collection: " + collection, e);
      throw DatabaseException.wrapException(
          new StorageException(name, "Error during creation of record"), e, name);
    }

    if (logger.isDebugEnabled()) {
      LogManager.instance()
          .debug(this, "Created record %s v.%s size=%d bytes", logger, rid, recordVersion,
              content.length);
    }

    return ppos;
  }

  private long doUpdateRecord(
      final AtomicOperation atomicOperation,
      final RecordIdInternal rid,
      final boolean updateContent,
      byte[] content,
      final long version,
      final byte recordType,
      final StorageCollection collection) {

    collection.meters().update().record();
    try {
      final var ppos =
          collection.getPhysicalPosition(new PhysicalPosition(rid.getCollectionPosition()),
              atomicOperation);

      if (ppos == null || ppos.recordVersion != version) {
        final var dbVersion = ppos == null ? -1 : ppos.recordVersion;
        throw new ConcurrentModificationException(
            name, rid, dbVersion, version, RecordOperation.UPDATED);
      }

      final var newRecordVersion = atomicOperation.getCommitTs();
      ppos.recordVersion = newRecordVersion;
      if (updateContent) {
        collection.updateRecord(
            rid.getCollectionPosition(), content, recordType, atomicOperation);
      } else {
        collection.updateRecordVersion(
            rid.getCollectionPosition(), atomicOperation);
      }

      if (logger.isDebugEnabled()) {
        LogManager.instance()
            .debug(this, "Updated record %s v.%s size=%d", logger, rid, newRecordVersion,
                content.length);
      }

      return newRecordVersion;
    } catch (final ConcurrentModificationException e) {
      collection.meters().conflict().record();
      throw e;
    } catch (final IOException ioe) {
      throw BaseException.wrapException(
          new StorageException(name,
              "Error on updating record " + rid + " (collection: " + collection.getName() + ")"),
          ioe, name);
    }
  }

  private void doDeleteRecord(
      final AtomicOperation atomicOperation,
      final RecordIdInternal rid,
      final long version,
      final StorageCollection collection) {
    collection.meters().delete().record();
    try {
      final var ppos =
          collection.getPhysicalPosition(new PhysicalPosition(rid.getCollectionPosition()),
              atomicOperation);

      if (ppos == null) {
        // ALREADY DELETED
        return;
      }

      // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
      if (version > -1 && ppos.recordVersion != version) {
        collection.meters().conflict().record();
        throw new ConcurrentModificationException(name, rid, ppos.recordVersion, version,
            RecordOperation.DELETED);
      }

      collection.deleteRecord(atomicOperation, ppos.collectionPosition);

      if (logger.isDebugEnabled()) {
        LogManager.instance().debug(this, "Deleted record %s v.%s", logger, rid, version);
      }

    } catch (final IOException ioe) {
      throw BaseException.wrapException(
          new StorageException(name,
              "Error on deleting record " + rid + "( collection: " + collection.getName() + ")"),
          ioe, name);
    }
  }

  @Nonnull
  private StorageReadResult doReadRecord(final StorageCollection collection,
      final RecordIdInternal rid, AtomicOperation atomicOperation) {
    collection.meters().read().record();
    try {
      final var result = collection.readRecord(rid.getCollectionPosition(),
          atomicOperation);
      if (logger.isDebugEnabled()) {
        LogManager.instance()
            .debug(
                this,
                "Read record %s v.%s size=%s bytes",
                logger, rid,
                result.recordVersion(),
                result instanceof RawBuffer rb
                    ? (rb.buffer() != null ? rb.buffer().length : 0)
                    : result instanceof RawPageBuffer rpb ? rpb.contentLength()
                        : 0);
      }

      return result;
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new StorageException(name, "Error during read of record with rid = " + rid), e, name);
    }
  }

  private static boolean doRecordExists(String dbName, final StorageCollection collectionSegment,
      final RID rid, AtomicOperation atomicOperation) {
    try {
      return collectionSegment.exists(rid.getCollectionPosition(), atomicOperation);
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new StorageException(dbName, "Error during read of record with rid = " + rid), e, dbName);
    }
  }

  private int createCollectionFromConfig(final StorageCollectionConfiguration config,
      AtomicOperation atomicOperation)
      throws IOException {
    var collection = collectionMap.get(config.name().toLowerCase(Locale.ROOT));

    if (collection != null) {
      collection.configure(this, config);
      return -1;
    }

    collection =
        StorageCollectionFactory.createCollection(
            config.name(), configuration.getVersion(atomicOperation), config.getBinaryVersion(),
            this);

    collection.configure(this, config);

    return registerCollection(collection);
  }

  private void setCollection(final int id, final StorageCollection collection) {
    if (collections.size() <= id) {
      while (collections.size() < id) {
        collections.add(null);
      }

      collections.add(collection);
    } else {
      collections.set(id, collection);
    }
  }

  /**
   * Register the collection internally.
   *
   * @param collection SQLCollection implementation
   * @return The id (physical position into the array) of the new collection just created. First is
   * 0.
   */
  private int registerCollection(final StorageCollection collection) {
    final int id;

    if (collection != null) {
      // CHECK FOR DUPLICATION OF NAMES
      if (collectionMap.containsKey(collection.getName().toLowerCase(Locale.ROOT))) {
        throw new ConfigurationException(name,
            "Cannot add collection '"
                + collection.getName()
                + "' because it is already registered in database '"
                + name
                + "'");
      }
      // CREATE AND ADD THE NEW REF SEGMENT
      collectionMap.put(collection.getName().toLowerCase(Locale.ROOT), collection);
      id = collection.getId();
    } else {
      id = collections.size();
    }

    setCollection(id, collection);

    return id;
  }

  private int doAddCollection(final AtomicOperation atomicOperation, final String collectionName)
      throws IOException {
    // FIND THE FIRST AVAILABLE COLLECTION ID
    var collectionPos = collections.size();
    for (var i = 0; i < collections.size(); ++i) {
      if (collections.get(i) == null) {
        collectionPos = i;
        break;
      }
    }

    return doAddCollection(atomicOperation, collectionName, collectionPos);
  }

  private int doAddCollection(
      final AtomicOperation atomicOperation, String collectionName, final int collectionPos)
      throws IOException {
    final var collection = doCreateCollection(atomicOperation, collectionName, collectionPos);

    if (collection == null) {
      return -1;
    }

    return registerCollection(collection);
  }

  /**
   * Creates the collection's file, its storage-configuration entry, and its
   * link-collections B-tree component inside the given atomic operation at the
   * caller-allocated {@code collectionPos}, without publishing the collection into the
   * in-memory collections list and the name-keyed collection map. Everything this
   * method touches is buffered as WAL-reverted intent in {@code atomicOperation}, so a
   * rolled-back or crashed-before-commit operation leaves no collection file behind.
   * Splitting creation from registry publication ({@link
   * #registerCollection(StorageCollection)}) lets a caller defer the in-memory publish past
   * commit so a failed commit leaves no phantom registration; the public {@link
   * #doAddCollection(AtomicOperation, String)} wrapper still publishes inside the same
   * atomic operation (its existing crash-safe behavior is unchanged), and the commit window
   * that defers the publish is wired separately. This is the collection-side analogue of
   * {@link #doAddIndexEngine(AtomicOperation, IndexEngineData)}.
   *
   * <p>The caller owns id allocation: the public {@link #doAddCollection(AtomicOperation,
   * String)} scans for the first null slot; the commit window uses a commit-local
   * allocator seeded inside the write lock. Returns {@code null} for a null
   * {@code collectionName} (the no-op case the legacy path returned id {@code -1} for).
   *
   * @param atomicOperation the in-flight atomic operation that buffers the file creates.
   * @param collectionName  the collection name, lower-cased here; {@code null} is a no-op.
   * @param collectionPos   the caller-allocated collection id.
   * @return the created collection, not yet published into the in-memory registries, or
   * {@code null} when {@code collectionName} is null.
   */
  private StorageCollection doCreateCollection(
      final AtomicOperation atomicOperation, String collectionName, final int collectionPos)
      throws IOException {
    if (collectionName == null) {
      return null;
    }

    collectionName = collectionName.toLowerCase(Locale.ROOT);

    final var collection =
        StorageCollectionFactory.createCollection(
            collectionName,
            configuration.getVersion(atomicOperation),
            configuration
                .getContextConfiguration()
                .getValueAsInteger(GlobalConfiguration.STORAGE_COLLECTION_VERSION),
            this);
    collection.configure(collectionPos, collectionName);

    collection.create(atomicOperation);

    ((CollectionBasedStorageConfiguration) configuration)
        .updateCollection(atomicOperation, collection.generateCollectionConfig());

    linkCollectionsBTreeManager.createComponent(atomicOperation, collectionPos);

    return collection;
  }

  @Override
  public void setCollectionAttribute(final int id, final ATTRIBUTES attribute,
      final Object value) {
    stateLock.writeLock().lock();
    try {
      checkOpennessAndMigration();
      if (id >= collections.size()) {
        return;
      }

      final var collection = collections.get(id);

      if (collection == null) {
        return;
      }

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> doSetCollectionAttributed(atomicOperation, attribute, value,
              collection));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  private void doSetCollectionAttributed(
      final AtomicOperation atomicOperation,
      final ATTRIBUTES attribute,
      final Object value,
      final StorageCollection collection) {
    final var stringValue = Optional.ofNullable(value).map(Object::toString).orElse(null);
    switch (attribute) {
      case NAME -> {
        Objects.requireNonNull(stringValue);

        final var oldName = collection.getName();
        collection.setCollectionName(stringValue);
        collectionMap.remove(oldName.toLowerCase(Locale.ROOT));
        collectionMap.put(stringValue.toLowerCase(Locale.ROOT), collection);
      }
      case CONFLICTSTRATEGY -> collection.setRecordConflictStrategy(stringValue);
      default -> throw new IllegalArgumentException(
          "Runtime change of attribute '" + attribute + "' is not supported");
    }

    ((CollectionBasedStorageConfiguration) configuration)
        .updateCollection(atomicOperation,
            ((PaginatedCollection) collection).generateCollectionConfig());
  }

  private boolean dropCollectionInternal(final AtomicOperation atomicOperation,
      final int collectionId)
      throws IOException {
    final var collection = collections.get(collectionId);

    if (collection == null) {
      return true;
    }

    collection.delete(atomicOperation);

    collectionMap.remove(collection.getName().toLowerCase(Locale.ROOT));
    collections.set(collectionId, null);

    return false;
  }

  protected void doShutdown() throws IOException {
    shutdownDuration.timed(() -> {
      if (status == STATUS.CLOSED) {
        return;
      }

      stopStaleTransactionMonitor();

      if (status != STATUS.OPEN && !isInError()) {
        throw BaseException.wrapException(
            new StorageException(name, "Storage " + name + " was not opened, so can not be closed"),
            this.error.get(), name);
      }

      status = STATUS.CLOSING;

      if (!isInError()) {
        // Cancel in-progress histogram rebalances, flush dirty data, and
        // block future rebalances — must happen before flushAllData so
        // that no background thread holds page references.
        cancelHistogramRebalances();
        flushAllData();
      }

      preCloseSteps();

      if (!isInError()) {
        atomicOperationsManager.executeInsideAtomicOperation(
            atomicOperation -> {
              // we close all files inside cache system so we only clear index metadata and close
              // non core indexes
              for (final var engine : indexEngines) {
                if (engine != null
                    && !(engine instanceof BTreeSingleValueIndexEngine
                        || engine instanceof BTreeMultiValueIndexEngine)) {
                  engine.close();
                }
              }
              ((CollectionBasedStorageConfiguration) configuration).close(atomicOperation);
            });
      } else {
        LogManager.instance()
            .error(
                this,
                "Because of JVM error happened inside of storage it can not be properly closed",
                null);
      }

      linkCollectionsBTreeManager.close();

      // we close all files inside cache system so we only clear collection metadata
      collections.clear();
      collectionMap.clear();
      indexEngines.clear();
      indexEngineNameMap.clear();
      sharedSnapshotIndex.clear();
      visibilityIndex.clear();
      snapshotIndexSize.set(0);
      sharedEdgeSnapshotIndex.clear();
      edgeVisibilityIndex.clear();
      edgeSnapshotIndexSize.set(0);
      sharedIndexesSnapshot.clear();
      indexesSnapshotVisibilityIndex.clear();
      sharedNullIndexesSnapshot.clear();
      nullIndexSnapshotVisibilityIndex.clear();
      indexesSnapshotEntriesCount.set(0);

      if (writeCache != null) {
        writeCache.removeBackgroundExceptionListener(this);
        writeCache.removePageIsBrokenListener(this);
      }

      writeAheadLog.removeCheckpointListener(this);

      try {
        if (readCache != null) {
          readCache.closeStorage(writeCache);
        }
      } catch (Exception e) {
        LogManager.instance().error(this, "Error during closing of disk cache", e);
      }

      try {
        writeAheadLog.close();
      } catch (Exception e) {
        LogManager.instance().error(this, "Error during closing of write ahead log", e);
      }

      postCloseSteps(false, isInError(), idGen.getLastId());

      migration = new CountDownLatch(1);
      status = STATUS.CLOSED;
    });
  }

  private void doShutdownOnDelete() {
    if (status == STATUS.CLOSED) {
      return;
    }

    stopStaleTransactionMonitor();

    if (status != STATUS.OPEN && !isInError()) {
      throw BaseException.wrapException(
          new StorageException(name, "Storage " + name + " was not opened, so can not be closed"),
          this.error.get(), name);
    }

    status = STATUS.CLOSING;
    try {
      if (!isInError()) {
        preCloseSteps();

        // Cancel any in-progress histogram rebalances and block future
        // ones before clearing engines and deleting the cache. A running
        // rebalance holds page read locks; deleting the cache while
        // those locks are held causes "page is used" errors and JVM
        // crashes.
        cancelHistogramRebalances();

        for (final var engine : indexEngines) {
          if (engine != null
              && !(engine instanceof BTreeSingleValueIndexEngine
                  || engine instanceof BTreeMultiValueIndexEngine)) {
            // delete method is implemented only in non native indexes, so they do not use ODB
            // atomic operation
            engine.delete(null);
          }
        }
      } else {
        LogManager.instance()
            .error(
                this,
                "Because of JVM error happened inside of storage it can not be properly closed",
                null);
      }

      // Resource releases below run in both branches, matching the
      // doShutdown() (close) path at lines 5107-5145 in this file. The WAL
      // holds two direct-memory write buffers allocated in
      // CASDiskWriteAheadLog.<init>; skipping writeAheadLog.delete() (which
      // routes through close(false) and deallocates both buffers in its
      // finally block) leaks the pointers and trips the
      // DirectMemoryAllocator.checkMemoryLeaks() assertion at JVM shutdown,
      // surfacing as "There was an error in the forked process" on
      // surefire/failsafe. Pure in-memory teardown (map clears,
      // linkCollectionsBTreeManager.close) and resource handoff (listener
      // removal, readCache.deleteStorage, writeAheadLog.delete) have no
      // data-correctness dependency on the storage state.
      linkCollectionsBTreeManager.close();
      collections.clear();
      collectionMap.clear();
      indexEngines.clear();
      indexEngineNameMap.clear();
      sharedSnapshotIndex.clear();
      visibilityIndex.clear();
      snapshotIndexSize.set(0);
      sharedEdgeSnapshotIndex.clear();
      edgeVisibilityIndex.clear();
      edgeSnapshotIndexSize.set(0);
      sharedIndexesSnapshot.clear();
      indexesSnapshotVisibilityIndex.clear();
      sharedNullIndexesSnapshot.clear();
      nullIndexSnapshotVisibilityIndex.clear();
      indexesSnapshotEntriesCount.set(0);

      // writeCache and readCache are guaranteed non-null past this
      // point. The entry guard at the top of doShutdownOnDelete returns
      // early on STATUS.CLOSED and throws unless status == STATUS.OPEN
      // or isInError(). writeCache is assigned during
      // initWalAndDiskCache (DiskStorage.java:790 for the disk engine,
      // DirectMemoryStorage.java:78 for the in-memory engine), and
      // readCache is assigned in the DiskStorage constructor
      // (DiskStorage.java:261) or during the in-memory engine's init
      // (DirectMemoryStorage.java:74). status = STATUS.OPEN is reached
      // only after that wiring completes; every open() failure path
      // that flips isInError() leaves status at STATUS.CLOSED, which
      // the early-return catches. The asserts document the invariant
      // and runtime-check it under -ea (the test JVM default;
      // production runs -ea off, so this is free).
      //
      // Listener teardown and read-cache deletion run inside a
      // defensive try/catch(Throwable) so any unexpected runtime
      // failure here (a future invariant violation under -ea off, an
      // implementation that throws from a listener removal, etc.)
      // cannot bypass writeAheadLog.delete() and re-leak the WAL's two
      // direct-memory write buffers (the exact failure mode this
      // commit fixed).
      assert writeCache != null;
      assert readCache != null;
      try {
        writeCache.removeBackgroundExceptionListener(this);
        writeCache.removePageIsBrokenListener(this);
        writeAheadLog.removeCheckpointListener(this);
        readCache.deleteStorage(writeCache);
      } catch (final Throwable t) {
        LogManager.instance()
            .error(this, "Error during listener teardown or read cache deletion", t);
      }

      try {
        writeAheadLog.delete();
      } catch (final Exception e) {
        LogManager.instance().error(this, "Error during deletion of write ahead log", e);
      }

      postCloseSteps(true, isInError(), idGen.getLastId());
      migration = new CountDownLatch(1);
      status = STATUS.CLOSED;
    } catch (final IOException e) {
      final var message = "Error on closing of storage '" + name;
      LogManager.instance().error(this, message, e);

      throw BaseException.wrapException(new StorageException(name, message), e, name);
    }
  }

  @SuppressWarnings("unused")
  protected void closeCollections() throws IOException {
    for (final var collection : collections) {
      if (collection != null) {
        collection.close(true);
      }
    }
    collections.clear();
    collectionMap.clear();
  }

  @SuppressWarnings("unused")
  protected void closeIndexes(final AtomicOperation atomicOperation) {
    for (final var engine : indexEngines) {
      if (engine != null) {
        engine.close();
      }
    }

    indexEngines.clear();
    indexEngineNameMap.clear();
  }

  private void commitEntry(
      FrontendTransactionImpl frontendTransaction,
      final AtomicOperation atomicOperation,
      final RecordOperation txEntry,
      final PhysicalPosition allocated,
      final RecordSerializer serializer) {
    final var rec = txEntry.record;
    if (txEntry.type != RecordOperation.DELETED && !rec.isDirty())
    // NO OPERATION
    {
      return;
    }
    final var rid = rec.getIdentity();

    if (txEntry.type == RecordOperation.UPDATED && rid.isNew())
    // OVERWRITE OPERATION AS CREATE
    {
      txEntry.type = RecordOperation.CREATED;
    }

    final var collection = doGetAndCheckCollection(rid.getCollectionId());

    var db = frontendTransaction.getDatabaseSession();

    switch (txEntry.type) {
      case RecordOperation.CREATED -> {
        final byte[] stream;
        try {
          stream = serializer.toStream(frontendTransaction.getDatabaseSession(), rec);
          if (stream == null) {
            throw new IllegalArgumentException("Record content is null");
          }
        } catch (RuntimeException e) {
          throw BaseException.wrapException(
              new CommitSerializationException(db.getDatabaseName(),
                  "Error During Record Serialization"),
              e, name);
        }
        if (allocated != null) {
          final PhysicalPosition ppos;
          final var recordType = rec.getRecordType();
          final var recordVersion = rec.getVersion() > -1 ? atomicOperation.getCommitTs() : 0;
          ppos =
              doCreateRecord(
                  atomicOperation,
                  rid,
                  stream,
                  recordVersion,
                  recordType,
                  collection, allocated);
          rec.setVersion(ppos.recordVersion);
        } else {
          final var updatedVersion =
              doUpdateRecord(
                  atomicOperation,
                  rid,
                  rec.isContentChanged(),
                  stream,
                  -2,
                  rec.getRecordType(),
                  collection);
          rec.setVersion(updatedVersion);
        }
      }
      case RecordOperation.UPDATED -> {
        final byte[] stream;
        try {
          stream = serializer.toStream(frontendTransaction.getDatabaseSession(), rec);
        } catch (RuntimeException e) {
          throw BaseException.wrapException(
              new CommitSerializationException(db.getDatabaseName(),
                  "Error During Record Serialization"),
              e, name);
        }

        final var version =
            doUpdateRecord(
                atomicOperation,
                rid,
                rec.isContentChanged(),
                stream,
                rec.getVersion(),
                rec.getRecordType(),
                collection);
        rec.setVersion(version);
      }
      case RecordOperation.DELETED -> {
        if (rec instanceof EntityImpl entity) {
          LinkBagDeleter.deleteAllRidBags(entity, frontendTransaction);
        }
        doDeleteRecord(atomicOperation, rid, rec.getVersionNoLoad(), collection);
      }
      default -> throw new StorageException(name, "Unknown record operation " + txEntry.type);
    }

    // RESET TRACKING
    if (rec instanceof EntityImpl entity) {
      entity.clearTrackData();
      entity.clearTransactionTrackData();
    }

    rec.unsetDirty();
  }

  private void checkCollectionSegmentIndexRange(final int iCollectionId) {
    if (iCollectionId < 0 || iCollectionId > collections.size() - 1) {
      throw new IllegalArgumentException(
          "Collection segment #" + iCollectionId + " does not exist in database '" + name + "'");
    }
  }

  private void restoreFromWAL() throws IOException {
    final var begin = writeAheadLog.begin();
    if (begin == null) {
      LogManager.instance()
          .error(this, "Restore is not possible because write ahead log is empty.", null);
      return;
    }

    LogManager.instance().info(this, "Looking for last checkpoint...");

    writeAheadLog.addCutTillLimit(begin);
    try {
      restoreFromBeginning();
    } finally {
      writeAheadLog.removeCutTillLimit(begin);
    }
  }

  public abstract String fullBackup(final Path backupDirectory);

  public abstract String fullBackup(Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, OutputStream> ibuOutputStreamSupplier,
      Consumer<String> ibuFileRemover);

  public abstract String backup(final Path backupDirectory);

  public abstract String backup(final Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, InputStream> ibuInputStreamSupplier,
      Function<String, OutputStream> ibuOutputStreamSupplier,
      final Consumer<String> ibuFileRemover);

  public abstract void restoreFromBackup(final Path backupDirectory, String expectedUUID);

  public abstract void restoreFromBackup(final Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, InputStream> ibuInputStreamSupplier, @Nullable String expectedUUID);

  /**
   * Replays one prepared and validated backup chain into this storage.
   *
   * <p>The caller owns the prepared chain. The caller prepared and validated every copy of that
   * chain before any change of this target. This method therefore selects no source file, copies
   * no source file, and removes no copy.
   *
   * @param chain the prepared copies of one restore request
   */
  public abstract void restoreFromPreparedBackupChain(PreparedBackupChain chain);

  private void restoreFromBeginning() throws IOException {
    LogManager.instance().info(this, "Data restore procedure is started.");

    final var lsn = writeAheadLog.begin();

    writeCache.restoreModeOn();
    try {
      restoreFrom(writeAheadLog, lsn);
    } finally {
      writeCache.restoreModeOff();
    }
  }

  @SuppressWarnings("UnusedReturnValue")
  protected LogSequenceNumber restoreFrom(WriteAheadLog writeAheadLog, LogSequenceNumber lsn)
      throws IOException {
    final var atLeastOnePageUpdate = new ModifiableBoolean();

    long recordsProcessed = 0;
    long maxOperationUnitId = -1;

    final var reportBatchSize =
        GlobalConfiguration.WAL_REPORT_AFTER_OPERATIONS_DURING_RESTORE.getValueAsInteger();
    final var operationUnits =
        new Long2ObjectOpenHashMap<List<WALRecord>>(1024);

    long lastReportTime = 0;
    LogSequenceNumber lastUpdatedLSN = null;

    try {
      var records = writeAheadLog.read(lsn, 1_000);

      while (!records.isEmpty()) {
        for (final var walRecord : records) {
          switch (walRecord) {
            case AtomicUnitEndRecord atomicUnitEndRecord -> {
              final var opId = atomicUnitEndRecord.getOperationUnitId();
              if (opId > maxOperationUnitId) {
                maxOperationUnitId = opId;
              }
              final var atomicUnit = operationUnits.remove(opId);

              // in case of data restore from fuzzy checkpoint part of operations may be already
              // flushed to the disk
              if (atomicUnit != null) {
                atomicUnit.add(walRecord);
                restoreAtomicUnit(atomicUnit, atLeastOnePageUpdate);
                lastUpdatedLSN = walRecord.getLsn();
              }
            }
            case AtomicUnitStartRecord oAtomicUnitStartRecord -> {
              final List<WALRecord> operationList = new ArrayList<>(1024);

              assert !operationUnits.containsKey(oAtomicUnitStartRecord.getOperationUnitId());

              operationUnits.put(oAtomicUnitStartRecord.getOperationUnitId(), operationList);
              operationList.add(walRecord);
            }
            case OperationUnitRecord operationUnitRecord -> {
              var operationList =
                  operationUnits.computeIfAbsent(
                      operationUnitRecord.getOperationUnitId(), k -> new ArrayList<>(1024));
              operationList.add(operationUnitRecord);
            }
            case NonTxOperationPerformedWALRecord ignored -> {
              if (!wereNonTxOperationsPerformedInPreviousOpen) {
                LogManager.instance()
                    .warn(
                        this,
                        "Non tx operation was used during data modification we will need index"
                            + " rebuild.");
                wereNonTxOperationsPerformedInPreviousOpen = true;
              }
            }
            case null, default -> LogManager.instance()
                .debug(this, "Record %s will be skipped during data restore",
                    logger, walRecord);
          }

          recordsProcessed++;

          final var currentTime = System.currentTimeMillis();
          if ((reportBatchSize > 0 && recordsProcessed % reportBatchSize == 0)
              || currentTime - lastReportTime > WAL_RESTORE_REPORT_INTERVAL) {
            final var additionalArgs =
                new Object[] {recordsProcessed, walRecord.getLsn(), writeAheadLog.end()};
            LogManager.instance()
                .info(
                    this,
                    "%d operations were processed, current LSN is %s last LSN is %s",
                    additionalArgs);
            lastReportTime = currentTime;
          }
        }

        records = writeAheadLog.next(records.getLast().getLsn(), 1_000);
      }
    } catch (final WALPageBrokenException e) {
      LogManager.instance()
          .error(
              this,
              "Data restore was paused because broken WAL page was found. The rest of changes will"
                  + " be rolled back.",
              e);
    } catch (final RuntimeException e) {
      LogManager.instance()
          .error(
              this,
              "Data restore was paused because of exception. The rest of changes will be rolled"
                  + " back.",
              e);
    }

    // After WAL replay, synchronize idGen with the highest operationUnitId seen.
    // This is critical after backup/restore where the idGen counter may be stale.
    if (maxOperationUnitId >= 0 && maxOperationUnitId >= idGen.getLastId()) {
      idGen.setStartId(maxOperationUnitId + 1);
    }

    return lastUpdatedLSN;
  }

  protected final void restoreAtomicUnit(
      final List<WALRecord> atomicUnit, final ModifiableBoolean atLeastOnePageUpdate)
      throws IOException {
    assert atomicUnit.getLast() instanceof AtomicUnitEndRecord;
    for (final var walRecord : atomicUnit) {
      switch (walRecord) {
        case FileDeletedWALRecord fileDeletedWALRecord -> {
          // Skip WAL records for files deleted during crash recovery (non-durable files)
          if (deletedNonDurableFileIds.contains(
              writeCache.internalFileId(fileDeletedWALRecord.getFileId()))) {
            continue;
          }
          if (writeCache.exists(fileDeletedWALRecord.getFileId())) {
            readCache.deleteFile(fileDeletedWALRecord.getFileId(), writeCache);
          }
        }
        case FileCreatedWALRecord fileCreatedCreatedWALRecord -> {
          // Skip re-creating non-durable files that were deleted during crash recovery
          if (deletedNonDurableFileIds.contains(
              writeCache.internalFileId(fileCreatedCreatedWALRecord.getFileId()))) {
            continue;
          }
          if (!writeCache.exists(fileCreatedCreatedWALRecord.getFileName())) {
            readCache.addFile(
                fileCreatedCreatedWALRecord.getFileName(),
                fileCreatedCreatedWALRecord.getFileId(),
                writeCache);
          }
        }
        case UpdatePageRecord updatePageRecord -> {
          var fileId = updatePageRecord.getFileId();

          // Skip page updates for non-durable files deleted during crash recovery
          if (deletedNonDurableFileIds.contains(writeCache.internalFileId(fileId))) {
            continue;
          }

          ensureFileForReplay(atomicUnit, fileId);

          final var pageIndex = updatePageRecord.getPageIndex();
          fileId = writeCache.externalFileId(writeCache.internalFileId(fileId));

          // loadOrAddForWrite is total on disk (delegates to WriteCache.loadOrAdd which
          // gap-fills any intermediate pages between currentSize and recordedPageIdx); WAL
          // replay never reaches the in-memory engine (MemoryWriteAheadLog is a no-op), so
          // the disk-engine totality is sufficient here.
          final var cacheEntry =
              readCache.loadOrAddForWrite(fileId, pageIndex, writeCache, true, null);
          // Asymmetric assert vs throw: see AtomicOperationBinaryTracking.commitChanges
          // for the rationale. This WAL-replay site is disk-only because
          // MemoryWriteAheadLog is a no-op, so -ea is sufficient; the in-memory-reachable
          // commitChanges site throws unconditionally.
          assert cacheEntry != null
              : "readCache.loadOrAddForWrite returned null during WAL replay"
                  + " UpdatePageRecord branch for fileId=" + fileId
                  + " pageIndex=" + pageIndex
                  + "; WriteCache.loadOrAdd totality contract violated";

          try {
            final var durablePage = new DurablePage(cacheEntry);
            var pageLsn = durablePage.getLsn();
            if (durablePage.getLsn().compareTo(walRecord.getLsn()) < 0) {
              if (!pageLsn.equals(updatePageRecord.getInitialLsn())) {
                LogManager.instance()
                    .error(
                        this,
                        "Page with index "
                            + pageIndex
                            + " and file "
                            + writeCache.fileNameById(fileId)
                            + " was changed before page restore was started. Page will be restored"
                            + " from WAL, but it may contain changes that were not present before"
                            + " storage crash and data may be lost. Initial LSN is "
                            + updatePageRecord.getInitialLsn()
                            + ", but page contains changes with LSN "
                            + pageLsn,
                        null);
              }
              durablePage.restoreChanges(updatePageRecord.getChanges());
              durablePage.setLsn(updatePageRecord.getLsn());
            }
          } finally {
            readCache.releaseFromWrite(cacheEntry, writeCache, true);
          }

          atLeastOnePageUpdate.setValue(true);
        }
        case PageOperation pageOp -> {
          var fileId = pageOp.getFileId();

          // Skip page updates for non-durable files deleted during crash recovery
          if (deletedNonDurableFileIds.contains(writeCache.internalFileId(fileId))) {
            continue;
          }

          ensureFileForReplay(atomicUnit, fileId);

          final var pageIndex = pageOp.getPageIndex();
          fileId = writeCache.externalFileId(writeCache.internalFileId(fileId));

          // loadOrAddForWrite is total on disk (delegates to WriteCache.loadOrAdd which
          // gap-fills any intermediate pages between currentSize and recordedPageIdx); WAL
          // replay never reaches the in-memory engine (MemoryWriteAheadLog is a no-op), so
          // the disk-engine totality is sufficient here.
          final var cacheEntry =
              readCache.loadOrAddForWrite(fileId, pageIndex, writeCache, true, null);
          // -ea assert is sufficient on this disk-only WAL-replay site
          // (MemoryWriteAheadLog is a no-op, so PageOperation never reaches the
          // in-memory engine); the throw-vs-assert rationale is documented in
          // AtomicOperationBinaryTracking.commitChanges, which throws because it is
          // the only site reachable from the in-memory engine.
          assert cacheEntry != null
              : "readCache.loadOrAddForWrite returned null during WAL replay"
                  + " PageOperation branch for fileId=" + fileId
                  + " pageIndex=" + pageIndex
                  + "; WriteCache.loadOrAdd totality contract violated";

          try {
            final var durablePage = new DurablePage(cacheEntry);
            var pageLsn = durablePage.getLsn();

            if (pageLsn.compareTo(pageOp.getLsn()) < 0) {
              // For multi-operation pages (common during B-tree splits), a given
              // operation's initialLsn typically does not match the current
              // pageLsn — prior operations in the same atomic unit have already
              // advanced it via redo. That mismatch is not a corruption signal
              // and must not be logged per-operation: on large restores it
              // produces millions of SEVERE entries whose stack-trace capture
              // (SLF4J fillCallerData) turns restore into a CPU bottleneck and
              // exceeds the CI watchdog. The sibling UpdatePageRecord branch
              // above retains an analogous log; it has the same latent issue
              // but is not the hot path after YTDB-626 and is left as
              // follow-up cleanup.
              pageOp.redo(durablePage);
              durablePage.setLsn(pageOp.getLsn());
            }
          } finally {
            readCache.releaseFromWrite(cacheEntry, writeCache, true);
          }

          atLeastOnePageUpdate.setValue(true);
        }
        //noinspection unused
        case AtomicUnitStartRecord atomicUnitStartRecord -> {
          //noinspection UnnecessaryContinue
          continue;
        }
        //noinspection unused
        case AtomicUnitEndRecord atomicUnitEndRecord -> {
          //noinspection UnnecessaryContinue
          continue;

        }
        //noinspection unused
        case HighLevelTransactionChangeRecord highLevelTransactionChangeRecord -> {
          //noinspection UnnecessaryContinue
          continue;
        }
        case null, default -> {
          assert walRecord != null;
          LogManager.instance()
              .error(
                  this,
                  "Invalid WAL record type was passed %s. Given record will be skipped.",
                  null,
                  walRecord.getClass());

          assert false : "Invalid WAL record type was passed " + walRecord.getClass().getName();
        }
      }
    }
  }

  /**
   * Materializes a file a page-redo record references but the cache has not seen yet, so
   * WAL replay can continue across atomic units. The open-time caller has already applied the
   * {@code deletedNonDurableFileIds} skip, so this is only reached for a genuinely missing
   * file on that path. The incremental-backup caller
   * ({@code DiskStorage.restoreFromIncrementalBackup}, the other of the two {@code restoreFrom}
   * callers) reaches this helper with {@code deletedNonDurableFileIds} as the empty no-op set
   * left behind by open-time recovery — incremental-backup restore has no non-durable-file
   * concept, so the skip is simply inert there rather than a universal precondition. The branch
   * order is the lazy-consult sequence (see issue YTDB-1099):
   *
   * <ol>
   *   <li><b>Pending-create consult.</b> A committed file-creating unit whose physical
   *       {@code addFile} was lost (the crash landed between its durable end record and
   *       the completion of its apply phase) leaves a later committed unit's page redo
   *       pointing at a file the cache never created. Scan this unit forward for the
   *       {@link FileCreatedWALRecord} that creates the file, matched on
   *       {@code internalFileId} because backup/restore rewrites the external high bits,
   *       and materialize the empty file through the same
   *       {@code readCache.addFile(name, id, writeCache)} path the
   *       {@code FileCreatedWALRecord} branch uses. The later {@code FileCreatedWALRecord}
   *       then replays as an idempotent no-op. Without this, the redo would throw and
   *       {@code restoreFrom}'s {@code catch (RuntimeException)} would discard every later
   *       unit.</li>
   *   <li><b>{@code restoreFileById} fallback.</b> A file deleted by a later, already-applied
   *       unit is resurrected from its persisted negative name-id entry. Load-bearing -- kept
   *       unchanged.</li>
   *   <li><b>Throw.</b> Neither path recovered the file, so the unit is genuinely incomplete
   *       (its create record was never made durable) and the rest of the restore is
   *       abandoned, exactly as before the fix.</li>
   * </ol>
   */
  private void ensureFileForReplay(final List<WALRecord> atomicUnit, final long fileId)
      throws IOException {
    if (writeCache.exists(fileId)) {
      return;
    }

    final var internalId = writeCache.internalFileId(fileId);
    for (final var record : atomicUnit) {
      // The !exists(name) guard before addFile is the same idiom the FileCreatedWALRecord replay
      // branch uses: it assumes the name's nameIdMap entry and its physical file are consistent.
      // exists(name) is stronger than "no positive nameIdMap entry" -- it also requires a non-null
      // files entry and a present on-disk file -- so an inconsistent cache state (positive nameIdMap
      // entry but absent file) would slip past it and make addFile throw. That window is not
      // reachable here: in the targeted scenario the physical addFile was lost, and nameIdMap.put
      // lives inside the same addFile write section, so a lost addFile leaves no nameIdMap entry and
      // addFile re-runs cleanly. The guard is not full collision protection; it relies on that
      // consistency, the same assumption the sibling branch makes.
      //
      // The downstream WOWCache.addFile shrink(0) truncation is a no-op precisely because of the
      // !writeCache.exists(fileId) precondition checked at the top of this method: at consult time
      // either the FileClassic is unregistered (addFile creates a fresh file, no truncation) or the
      // physical file is absent (shrink(0) is a no-op), so no committed file is ever truncated.
      //
      // The file-recycle (same-unit delete-then-recreate) shape emits no FileCreatedWALRecord,
      // so this forward scan finds nothing for it and correctly falls through to restoreFileById.
      if (record instanceof FileCreatedWALRecord fileCreated
          && writeCache.internalFileId(fileCreated.getFileId()) == internalId
          && !writeCache.exists(fileCreated.getFileName())) {
        readCache.addFile(fileCreated.getFileName(), fileCreated.getFileId(), writeCache);
        return;
      }
    }

    final var fileName = writeCache.restoreFileById(fileId);
    if (fileName != null) {
      LogManager.instance()
          .warn(
              this,
              "Previously deleted file with name "
                  + fileName
                  + " was deleted but new empty file was added to continue restore process");
      return;
    }

    throw new StorageException(name,
        "File with id "
            + fileId
            + " was deleted from storage, the rest of operations can not be restored");
  }

  @SuppressWarnings("unused")
  public void setStorageConfigurationUpdateListener(
      final StorageConfigurationUpdateListener storageConfigurationUpdateListener) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      ((CollectionBasedStorageConfiguration) configuration)
          .setConfigurationUpdateListener(storageConfigurationUpdateListener);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  public void pauseConfigurationUpdateNotifications() {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      ((CollectionBasedStorageConfiguration) configuration).pauseUpdateNotifications();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  public void fireConfigurationUpdateNotifications() {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();
      ((CollectionBasedStorageConfiguration) configuration).fireUpdateNotifications();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @SuppressWarnings("unused")
  protected static Int2ObjectMap<List<RecordIdInternal>> getRidsGroupedByCollection(
      final Collection<RecordIdInternal> rids) {
    final var ridsPerCollection = new Int2ObjectOpenHashMap<List<RecordIdInternal>>(8);
    for (final var rid : rids) {
      final var group =
          ridsPerCollection.computeIfAbsent(rid.getCollectionId(),
              k -> new ArrayList<>(rids.size()));
      group.add(rid);
    }
    return ridsPerCollection;
  }

  private static void lockIndexes(
      final SortedMap<String, FrontendTransactionIndexChanges> indexes,
      AtomicOperation atomicOperation) {
    for (final var changes : indexes.values()) {
      changes.getIndex().acquireAtomicExclusiveLock(atomicOperation);
    }
  }

  private static void lockCollections(final TreeMap<Integer, StorageCollection> collectionsToLock,
      AtomicOperation atomicOperation) {
    for (final var collection : collectionsToLock.values()) {
      collection.acquireAtomicExclusiveLock(atomicOperation);
    }
  }

  private void lockLinkBags(
      final TreeMap<Integer, StorageCollection> collections,
      @Nonnull AtomicOperation atomicOperation) {
    for (final var collectionId : collections.keySet()) {
      var bTree = linkCollectionsBTreeManager.getComponentByCollectionId(
          collectionId, atomicOperation);
      // The B-tree should always exist: it is created with the collection
      // (doAddCollection) and repaired during storage open
      // (checkRidBagsPresence). lockCollections() already serializes
      // concurrent commits to the same collection, so a null here would
      // not cause a race — but it would indicate a bug elsewhere.
      assert bTree != null
          : "Link bag B-tree missing for collection " + collectionId;
      if (bTree != null) {
        atomicOperationsManager.acquireExclusiveLockTillOperationComplete(
            atomicOperation, bTree);
      }
    }
  }

  protected RuntimeException logAndPrepareForRethrow(final RuntimeException runtimeException) {
    if (!(runtimeException instanceof HighLevelException
        || runtimeException instanceof NeedRetryException
        || runtimeException instanceof InternalErrorException
        || runtimeException instanceof IllegalArgumentException)) {
      final var iAdditionalArgs =
          new Object[] {
              System.identityHashCode(runtimeException), getURL(), YouTrackDBConstants.getVersion()
          };
      LogManager.instance()
          .error(this, "Exception `%08X` in storage `%s`: %s", runtimeException, iAdditionalArgs);
    }

    if (runtimeException instanceof BaseException baseException) {
      baseException.setDbName(name);
    }

    return runtimeException;
  }

  protected final Error logAndPrepareForRethrow(final Error error) {
    return logAndPrepareForRethrow(error, true);
  }

  protected Error logAndPrepareForRethrow(final Error error, final boolean putInReadOnlyMode) {
    if (!(error instanceof HighLevelException)) {
      if (putInReadOnlyMode) {
        setInError(error);
      }

      final var iAdditionalArgs =
          new Object[] {System.identityHashCode(error), getURL(), YouTrackDBConstants.getVersion()};
      LogManager.instance()
          .error(this, "Exception `%08X` in storage `%s`: %s", error, iAdditionalArgs);
    }

    return error;
  }

  protected final RuntimeException logAndPrepareForRethrow(final Throwable throwable) {
    return logAndPrepareForRethrow(throwable, true);
  }

  protected RuntimeException logAndPrepareForRethrow(
      final Throwable throwable, final boolean putInReadOnlyMode) {
    if (!(throwable instanceof HighLevelException
        || throwable instanceof NeedRetryException
        || throwable instanceof InternalErrorException)) {
      if (putInReadOnlyMode) {
        setInError(throwable);
      }
      final var iAdditionalArgs =
          new Object[] {System.identityHashCode(throwable), getURL(),
              YouTrackDBConstants.getVersion()};
      LogManager.instance()
          .error(this, "Exception `%08X` in storage `%s`: %s", throwable, iAdditionalArgs);
    }
    if (throwable instanceof BaseException baseException) {
      baseException.setDbName(name);
    }
    return new RuntimeException(throwable);
  }

  private InvalidIndexEngineIdException logAndPrepareForRethrow(
      final InvalidIndexEngineIdException exception) {
    final var iAdditionalArgs =
        new Object[] {System.identityHashCode(exception), getURL(),
            YouTrackDBConstants.getVersion()};
    if (exception instanceof IndexEngineReplacedException) {
      LogManager.instance()
          .debug(this, "Index engine replacement `%08X` in storage `%s`: %s", logger, exception,
              iAdditionalArgs);
    } else {
      LogManager.instance()
          .error(this, "Exception `%08X` in storage `%s` : %s", exception, iAdditionalArgs);
    }
    return exception;
  }

  @Override
  public final void setSchemaRecordId(final String schemaRecordId) {
    stateLock.readLock().lock();
    try {
      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> storageConfiguration.setSchemaRecordId(atomicOperation,
              schemaRecordId));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void setDateFormat(final String dateFormat) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> storageConfiguration.setDateFormat(atomicOperation, dateFormat));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void setTimeZone(final TimeZone timeZoneValue) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> storageConfiguration.setTimeZone(atomicOperation, timeZoneValue));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void setLocaleLanguage(final String locale) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> storageConfiguration.setLocaleLanguage(atomicOperation, locale));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void setCharset(final String charset) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> storageConfiguration.setCharset(atomicOperation, charset));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void setIndexMgrRecordId(final String indexMgrRecordId) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> storageConfiguration.setIndexMgrRecordId(atomicOperation,
              indexMgrRecordId));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void setDateTimeFormat(final String dateTimeFormat) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> storageConfiguration.setDateTimeFormat(atomicOperation,
              dateTimeFormat));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void setLocaleCountry(final String localeCountry) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> storageConfiguration.setLocaleCountry(atomicOperation, localeCountry));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void setValidation(final boolean validation) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> storageConfiguration.setValidation(atomicOperation, validation));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void removeProperty(final String property) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> storageConfiguration.removeProperty(atomicOperation, property));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void setProperty(final String property, final String value) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> storageConfiguration.setProperty(atomicOperation, property, value));
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Nullable @Override
  public final String getProperty(final String property) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      return configuration.getProperty(property);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void setRecordSerializer(final String recordSerializer, final int version) {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          atomicOperation -> {
            storageConfiguration.setRecordSerializer(atomicOperation, recordSerializer);
            storageConfiguration.setRecordSerializerVersion(atomicOperation, version);
          });
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public final void clearProperties() {
    stateLock.readLock().lock();
    try {

      checkOpennessAndMigration();

      final var storageConfiguration =
          (CollectionBasedStorageConfiguration) configuration;

      makeStorageDirty();

      atomicOperationsManager.executeInsideAtomicOperation(
          storageConfiguration::clearProperties);
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  void runWALVacuum() {
    stateLock.readLock().lock();
    try {

      if (status == STATUS.CLOSED) {
        return;
      }

      final var nonActiveSegments = writeAheadLog.nonActiveSegments();
      if (nonActiveSegments.length == 0) {
        return;
      }

      long flushTillSegmentId;
      if (nonActiveSegments.length == 1) {
        flushTillSegmentId = writeAheadLog.activeSegment();
      } else {
        flushTillSegmentId =
            (nonActiveSegments[0] + nonActiveSegments[nonActiveSegments.length - 1]) / 2;
      }

      long minDirtySegment;
      do {
        writeCache.flushTillSegment(flushTillSegmentId);

        // we should take active segment BEFORE min write cache LSN call
        // to avoid case when new data are changed before call
        final var activeSegment = writeAheadLog.activeSegment();
        final var minLSNSegment = writeCache.getMinimalNotFlushedSegment();

        minDirtySegment = Objects.requireNonNullElse(minLSNSegment, activeSegment);
      } while (minDirtySegment < flushTillSegmentId);

      atomicOperationsTable.compactTable();
      final var operationSegment = atomicOperationsTable.getSegmentEarliestNotPersistedOperation();
      if (operationSegment >= 0 && minDirtySegment > operationSegment) {
        minDirtySegment = operationSegment;
      }

      if (minDirtySegment <= nonActiveSegments[0]) {
        return;
      }

      writeCache.syncDataFiles(minDirtySegment);
    } catch (final Exception e) {
      LogManager.instance()
          .error(
              this, "Error during flushing of data for fuzzy checkpoint, in storage %s", e, name);
    } finally {
      stateLock.readLock().unlock();
      walVacuumInProgress.set(false);
    }
  }

  @Override
  public int[] getCollectionsIds(Set<String> filterCollections) {
    try {

      stateLock.readLock().lock();
      try {

        checkOpennessAndMigration();
        var result = new int[filterCollections.size()];
        var i = 0;
        for (var collectionName : filterCollections) {
          if (collectionName == null) {
            throw new IllegalArgumentException("Collection name is null");
          }

          if (collectionName.isEmpty()) {
            throw new IllegalArgumentException("Collection name is empty");
          }

          // SEARCH IT BETWEEN PHYSICAL COLLECTIONS
          final var segment = collectionMap.get(collectionName.toLowerCase(Locale.ROOT));
          if (segment != null) {
            result[i] = segment.getId();
          } else {
            result[i] = -1;
          }
          i++;
        }
        return result;
      } finally {
        stateLock.readLock().unlock();
      }
    } catch (final RuntimeException ee) {
      throw logAndPrepareForRethrow(ee);
    } catch (final Error ee) {
      throw logAndPrepareForRethrow(ee, false);
    } catch (final Throwable t) {
      throw logAndPrepareForRethrow(t, false);
    }
  }

  @Override
  public YouTrackDBInternalEmbedded getContext() {
    return this.context;
  }

  /**
   * Attempts to evict stale snapshot index entries if the index exceeds the configured threshold.
   * Called from {@link #resetTsMin()} at the end of every storage transaction (both read and
   * write), ensuring prompt cleanup even in read-heavy workloads where write commits are
   * infrequent.
   *
   * <p>Uses {@code tryLock} so only one thread cleans at a time — other threads skip cleanup
   * if the lock is already held (no point blocking; the cleaning thread will handle it). The
   * threshold is re-checked under the lock (double-checked pattern) to avoid redundant work
   * if another thread already cleaned enough entries.
   *
   * <p>Uses {@link #snapshotIndexSize} (an {@code AtomicLong} counter) instead of
   * {@code ConcurrentSkipListMap.size()} for the threshold check. The latter is O(n) — it
   * traverses the entire map — and calling it on every transaction close causes resource
   * exhaustion under sustained heavy concurrent load (e.g., 30-minute soak tests with 10+
   * threads). The counter is incremented during {@code flushSnapshotBuffers()} and decremented
   * during {@code evictStaleSnapshotEntries()}, providing an O(1) approximate size check.
   */
  private void cleanupSnapshotIndex() {
    int threshold = configuration.getContextConfiguration()
        .getValueAsInteger(GlobalConfiguration.STORAGE_SNAPSHOT_INDEX_CLEANUP_THRESHOLD);
    long combinedSize =
        snapshotIndexSize.get() + edgeSnapshotIndexSize.get() + indexesSnapshotEntriesCount.get();
    if (combinedSize <= threshold) {
      return;
    }
    if (!snapshotCleanupLock.tryLock()) {
      return;
    }
    try {
      combinedSize =
          snapshotIndexSize.get() + edgeSnapshotIndexSize.get() + indexesSnapshotEntriesCount.get();
      if (combinedSize <= threshold) {
        return;
      }
      long lwm = computeGlobalLowWaterMark();
      evictStaleSnapshotEntries(
          lwm, sharedSnapshotIndex, visibilityIndex,
          snapshotIndexSize, collections);
      evictStaleEdgeSnapshotEntries(
          lwm, sharedEdgeSnapshotIndex, edgeVisibilityIndex,
          edgeSnapshotIndexSize);
      evictStaleIndexesSnapshotEntries(lwm, sharedIndexesSnapshot,
          indexesSnapshotVisibilityIndex, indexesSnapshotEntriesCount);
      evictStaleIndexesSnapshotEntries(lwm, sharedNullIndexesSnapshot,
          nullIndexSnapshotVisibilityIndex, indexesSnapshotEntriesCount);
    } finally {
      snapshotCleanupLock.unlock();
    }
  }

  /**
   * Periodic records GC task entry point. Performs two duties:
   * <ol>
   *   <li>Opportunistically cleans the snapshot/visibility indexes (same work as
   *       {@link #cleanupSnapshotIndex()}, using {@code tryLock()} — if another thread is
   *       already cleaning, this step is skipped).</li>
   *   <li>Iterates over all collections in the storage and reclaims dead records from those
   *       that exceed the GC trigger threshold.</li>
   * </ol>
   *
   * <p>Called by the periodic scheduled task ({@code PeriodicRecordsGc}) on the
   * {@code fuzzyCheckpointExecutor}. The method is safe to call concurrently — snapshot
   * cleanup uses {@code tryLock()}, and the per-collection GC is serialized by the
   * collection's component lock inside {@code collectDeadRecords()}.
   *
   * <p><b>Deadlock prevention:</b> This method acquires {@code stateLock.readLock()} for
   * its entire duration. Without it, the delete path ({@code doShutdownOnDelete}) can
   * acquire {@code stateLock.writeLock()} and proceed to
   * {@code WOWCache.delete()} → {@code filesLock.writeLock()} while this GC task is
   * still running. The GC writes pages through the read cache, which acquires a
   * {@code PageFrame} exclusive lock and then calls {@code WOWCache.store()} →
   * {@code filesLock.readLock()}. This creates a 3-way deadlock:
   * <ol>
   *   <li>main holds {@code filesLock} (write) → waits for DeleteFileTask on flush
   *       executor</li>
   *   <li>GC thread holds {@code PageFrame} lock → needs {@code filesLock} (read)</li>
   *   <li>flush executor needs the same {@code PageFrame} lock</li>
   * </ol>
   * Holding {@code stateLock.readLock()} forces the delete path to wait until this
   * method completes, preventing the deadlock.
   */
  public void periodicRecordsGc() {
    if (status != STATUS.OPEN) {
      return;
    }

    // Acquire the state read lock to prevent concurrent storage deletion.
    // If the write lock is already held (storage is being deleted/closed),
    // bail out immediately — there is no point in GC'ing a dying storage.
    if (!stateLock.readLock().tryLock()) {
      return;
    }
    try {
      if (status != STATUS.OPEN) {
        return;
      }

      // Step 1: Opportunistically clean snapshot/visibility indexes.
      try {
        cleanupSnapshotIndex();
      } catch (Exception e) {
        LogManager.instance().error(this, "Error during snapshot index cleanup"
            + " in periodic records GC for storage '%s'", e, name);
      }

      // Step 2: Reclaim dead records from collections that exceed the threshold.
      var contextConfig = configuration.getContextConfiguration();
      int minThreshold = contextConfig
          .getValueAsInteger(GlobalConfiguration.STORAGE_COLLECTION_GC_MIN_THRESHOLD);
      float scaleFactor = contextConfig
          .getValueAsFloat(GlobalConfiguration.STORAGE_COLLECTION_GC_SCALE_FACTOR);

      for (var collection : collections) {
        if (status != STATUS.OPEN) {
          return;
        }
        if (collection instanceof PaginatedCollectionV2 pc
            && pc.isGcTriggered(minThreshold, scaleFactor)) {
          try {
            pc.collectDeadRecords(sharedSnapshotIndex);
          } catch (Exception e) {
            LogManager.instance().error(this, "Error during records GC"
                + " for collection '%s' in storage '%s'", e, pc.getName(), name);
          }
        }
      }
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Core eviction logic: removes all visibility/snapshot entries with {@code recordTs} strictly
   * below the given low-water-mark. Extracted as a static method for direct unit testing (same
   * pattern as {@link #computeGlobalLowWaterMark(Set, long)}).
   *
   * <p>The {@code lwm} parameter is always a concrete upper bound — either an active
   * transaction's snapshot timestamp or {@code idGen.getLastId()} when no transactions are
   * active. It is never {@code Long.MAX_VALUE} in normal operation because
   * {@link #computeGlobalLowWaterMark()} falls back to {@code idGen.getLastId()} when all
   * threads are idle.
   *
   * <p>Delegates to
   * {@link #evictStaleSnapshotEntries(long, ConcurrentSkipListMap, ConcurrentSkipListMap,
   * AtomicLong, List)} with a {@code null} collections list (no dead record counting).
   *
   * @param lwm the global low-water-mark; entries with {@code recordTs < lwm} are evicted
   * @param snapshotIndex the shared snapshot index to remove stale entries from
   * @param visibilityIdx the visibility index to scan and remove stale entries from
   * @param sizeCounter   approximate size counter to decrement for each evicted snapshot entry
   */
  static void evictStaleSnapshotEntries(
      long lwm,
      ConcurrentSkipListMap<SnapshotKey, PositionEntry> snapshotIndex,
      ConcurrentSkipListMap<VisibilityKey, SnapshotKey> visibilityIdx,
      @Nonnull AtomicLong sizeCounter) {
    evictStaleSnapshotEntries(lwm, snapshotIndex, visibilityIdx, sizeCounter, null);
  }

  /**
   * Core eviction logic: removes all visibility/snapshot entries with {@code recordTs} strictly
   * below the given low-water-mark, and optionally increments per-collection dead record
   * counters.
   *
   * <p>When {@code collections} is non-null, each evicted snapshot entry increments the
   * {@link PaginatedCollectionV2#deadRecordCount} of the corresponding collection (looked up
   * by {@link SnapshotKey#componentId()}). This feeds the records GC trigger condition.
   *
   * @param lwm the global low-water-mark; entries with {@code recordTs < lwm} are evicted
   * @param snapshotIndex the shared snapshot index to remove stale entries from
   * @param visibilityIdx the visibility index to scan and remove stale entries from
   * @param sizeCounter   approximate size counter to decrement for each evicted snapshot entry
   * @param collections   storage collections indexed by collection id; nullable for unit tests
   *                      or callers that do not need dead record counting
   */
  static void evictStaleSnapshotEntries(
      long lwm,
      ConcurrentSkipListMap<SnapshotKey, PositionEntry> snapshotIndex,
      ConcurrentSkipListMap<VisibilityKey, SnapshotKey> visibilityIdx,
      @Nonnull AtomicLong sizeCounter,
      @Nullable List<StorageCollection> collections) {
    // Sentinel key: (lwm, MIN, MIN) is the smallest possible key with recordTs==lwm,
    // so headMap(exclusive) captures everything with recordTs < lwm.
    var staleEntries = visibilityIdx.headMap(
        new VisibilityKey(lwm, Integer.MIN_VALUE, Long.MIN_VALUE), false);
    var iterator = staleEntries.entrySet().iterator();
    while (iterator.hasNext()) {
      var entry = iterator.next();
      var snapshotKey = entry.getValue();
      if (snapshotIndex.remove(snapshotKey) != null) {
        sizeCounter.decrementAndGet();
        // Increment the per-collection dead record counter so the records GC
        // trigger condition can detect when enough dead records have accumulated.
        if (collections != null) {
          int id = snapshotKey.componentId();
          if (id >= 0 && id < collections.size()) {
            var coll = collections.get(id);
            if (coll instanceof PaginatedCollectionV2 pc) {
              pc.incrementDeadRecordCount();
            }
          }
        }
      }
      iterator.remove();
    }
  }

  /**
   * Core eviction logic for edge snapshot entries: removes all edge visibility/snapshot entries
   * with {@code recordTs} strictly below the given low-water-mark. Follows the same pattern as
   * {@link #evictStaleSnapshotEntries} but for edge-specific key types and without dead record
   * counting (edge snapshots do not participate in records GC).
   *
   * @param lwm the global low-water-mark; entries with {@code recordTs < lwm} are evicted
   * @param edgeSnapshotIndex the shared edge snapshot index to remove stale entries from
   * @param edgeVisibilityIdx the edge visibility index to scan and remove stale entries from
   * @param sizeCounter approximate size counter to decrement for each evicted snapshot entry
   */
  static void evictStaleEdgeSnapshotEntries(
      long lwm,
      ConcurrentSkipListMap<EdgeSnapshotKey, LinkBagValue> edgeSnapshotIndex,
      ConcurrentSkipListMap<EdgeVisibilityKey, EdgeSnapshotKey> edgeVisibilityIdx,
      @Nonnull AtomicLong sizeCounter) {
    // Sentinel key: (lwm, MIN, MIN, MIN, MIN) is the smallest possible key with recordTs==lwm,
    // so headMap(exclusive) captures everything with recordTs < lwm.
    var staleEntries = edgeVisibilityIdx.headMap(
        new EdgeVisibilityKey(lwm, Integer.MIN_VALUE, Long.MIN_VALUE,
            Integer.MIN_VALUE, Long.MIN_VALUE),
        false);
    var iterator = staleEntries.entrySet().iterator();
    while (iterator.hasNext()) {
      var entry = iterator.next();
      var edgeSnapshotKey = entry.getValue();
      if (edgeSnapshotIndex.remove(edgeSnapshotKey) != null) {
        sizeCounter.decrementAndGet();
      }
      iterator.remove();
    }
  }

  /**
   * Core eviction logic for index snapshot entries: removes all index snapshot/version entries
   * with version strictly below the given low-water-mark. Follows the same pattern as
   * {@link #evictStaleSnapshotEntries} and {@link #evictStaleEdgeSnapshotEntries} but for
   * index-specific key types. Operates on the raw maps owned by AbstractStorage.
   *
   * @param lwm the global low-water-mark; entries with version < lwm are evicted
   * @param snapshotData the index snapshot data map to remove stale entries from
   * @param versionIdx the version index to scan and remove stale entries from
   * @param sizeCounter approximate entry count to decrement (2 per evicted pair)
   */
  static void evictStaleIndexesSnapshotEntries(
      long lwm,
      ConcurrentSkipListMap<CompositeKey, RID> snapshotData,
      ConcurrentSkipListMap<CompositeKey, CompositeKey> versionIdx,
      AtomicLong sizeCounter) {
    if (lwm == Long.MAX_VALUE) {
      return;
    }
    // Sentinel: a CompositeKey whose last element is LWM, with minimal preceding keys
    // so that headMap captures everything with version < LWM
    var sentinel = new CompositeKey(Long.MIN_VALUE, lwm);
    var stale = versionIdx.headMap(sentinel);
    long evicted = 0;
    // Use iterator.remove() instead of stale.clear() to avoid a race with
    // concurrent addSnapshotPair(): clear() removes ALL entries currently in the
    // live headMap view, including entries added after the for-loop iterator
    // passed them. That would orphan the corresponding snapshotData entries
    // (never cleaned up → slow memory leak). With iterator.remove(), only
    // entries whose snapshotData was already cleaned are removed from versionIdx.
    for (var it = stale.entrySet().iterator(); it.hasNext();) {
      var entry = it.next();
      snapshotData.remove(entry.getKey());
      snapshotData.remove(entry.getValue());
      it.remove();
      evicted += 2;
    }
    if (evicted > 0) {
      // Clamp to zero: concurrent clear() and eviction may both decrement
      // for the same entries (ConcurrentSkipListMap.remove is idempotent
      // but both callers counted entries during their own iteration pass).
      final long delta = evicted;
      sizeCounter.updateAndGet(current -> Math.max(0, current - delta));
    }
  }

  /**
   * Starts the stale transaction monitor if enabled in the configuration. Called once when
   * the storage transitions to OPEN status.
   */
  private void startStaleTransactionMonitor() {
    if (configuration == null) {
      return;
    }
    var ctx = configuration.getContextConfiguration();
    if (!ctx.getValueAsBoolean(GlobalConfiguration.STORAGE_TX_MONITOR_ENABLED)) {
      return;
    }
    var monitor = new StaleTransactionMonitor(
        name, tsMins, snapshotIndexSize, idGen,
        YouTrackDBEnginesManager.instance().getTicker(),
        ctx,
        YouTrackDBEnginesManager.instance().getMetricsRegistry());
    this.staleTransactionMonitor = monitor;
    monitor.start(YouTrackDBEnginesManager.instance().getScheduledPool());
  }

  /**
   * Stops the stale transaction monitor if running. Called during shutdown.
   */
  private void stopStaleTransactionMonitor() {
    var monitor = staleTransactionMonitor;
    if (monitor != null) {
      monitor.stop();
      staleTransactionMonitor = null;
    }
  }

  public IndexesSnapshot subIndexSnapshot(long indexId) {
    return new IndexesSnapshot(
        sharedIndexesSnapshot, indexesSnapshotVisibilityIndex, indexesSnapshotEntriesCount,
        indexId);
  }

  public IndexesSnapshot subNullIndexSnapshot(long indexId) {
    return new IndexesSnapshot(
        sharedNullIndexesSnapshot, nullIndexSnapshotVisibilityIndex, indexesSnapshotEntriesCount,
        indexId);
  }

  /**
   * Resolves the sub-{@link IndexesSnapshot} for an index engine by its name.
   * Used by test infrastructure for snapshot cleanup and assertions.
   *
   * <p><b>Warning:</b> This method acquires {@code stateLock.readLock()}. It must
   * not be called while holding a BTree component lock, as this would create a
   * lock ordering inversion. Use {@link #hasActiveIndexSnapshotEntriesById} from
   * within BTree code paths instead.
   *
   * @return the scoped snapshot, or {@code null} if no engine with this name exists
   */
  @Nullable public IndexesSnapshot getIndexSnapshotByEngineName(String engineName) {
    stateLock.readLock().lock();
    try {
      var engine = indexEngineNameMap.get(engineName);
      if (engine == null) {
        return null;
      }
      return subIndexSnapshot(engine.getId());
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Resolves the null-key sub-{@link IndexesSnapshot} for an index engine by its name.
   * Currently used only by test infrastructure for snapshot cleanup; reserved for
   * future null-key tree GC support.
   *
   * <p><b>Warning:</b> This method acquires {@code stateLock.readLock()}. It must
   * not be called while holding a BTree component lock, as this would create a
   * lock ordering inversion.
   *
   * @return the scoped null snapshot, or {@code null} if no engine with this name exists
   */
  @Nullable public IndexesSnapshot getNullIndexSnapshotByEngineName(String engineName) {
    stateLock.readLock().lock();
    try {
      var engine = indexEngineNameMap.get(engineName);
      if (engine == null) {
        return null;
      }
      return subNullIndexSnapshot(engine.getId());
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Checks whether any snapshot entries exist for the given user-key prefix with
   * {@code version >= lwm} in the index identified by {@code engineName}.
   * For null-key trees (name ending with {@code "$null"}), the null snapshot is used.
   *
   * <p>{@code engineName} is the LOGICAL index-engine name (the {@code indexEngineNameMap}
   * key, optionally suffixed with {@code "$null"}) — never an {@code ie_<fileBaseId>} file
   * stem: storage-component/file names and logical engine names are distinct domains.
   *
   * <p>Queries the shared {@code ConcurrentSkipListMap} directly without creating
   * an intermediate {@link IndexesSnapshot} instance.
   *
   * <p><b>Note:</b> This method acquires {@code stateLock.readLock()} to resolve
   * the engine name to an ID. If the caller already knows the engine ID (e.g.,
   * from a cached field), prefer {@link #hasActiveIndexSnapshotEntriesById} to
   * avoid the lock acquisition.
   *
   * @param engineName the index engine name (may end with "$null" for null-key trees)
   * @param userKeyPrefix the key prefix (all CompositeKey elements except the version)
   * @param lwm the global low-water mark
   * @return {@code true} if at least one snapshot entry exists with version >= lwm,
   *     or {@code false} if no engine or no matching entries exist
   */
  // Visible for testing — production code uses hasActiveIndexSnapshotEntriesById.
  public boolean hasActiveIndexSnapshotEntries(
      String engineName, CompositeKey userKeyPrefix, long lwm) {
    final String resolvedName;
    final NavigableMap<CompositeKey, RID> snapshotMap;
    if (engineName.endsWith(NULL_TREE_SUFFIX)) {
      resolvedName = engineName.substring(
          0, engineName.length() - NULL_TREE_SUFFIX.length());
      snapshotMap = sharedNullIndexesSnapshot;
    } else {
      resolvedName = engineName;
      snapshotMap = sharedIndexesSnapshot;
    }

    // indexEngineNameMap is a plain HashMap — guard with stateLock.readLock()
    // to avoid racing with concurrent index creation/deletion.
    final long indexId;
    stateLock.readLock().lock();
    try {
      var engine = indexEngineNameMap.get(resolvedName);
      if (engine == null) {
        return false;
      }
      indexId = engine.getId();
    } finally {
      stateLock.readLock().unlock();
    }

    return hasActiveSnapshotEntriesInMap(snapshotMap, indexId, userKeyPrefix, lwm);
  }

  /**
   * Lock-free variant of {@link #hasActiveIndexSnapshotEntries} that accepts a
   * pre-resolved engine ID and a flag to select the snapshot map. This avoids
   * acquiring {@code stateLock.readLock()} for the {@code indexEngineNameMap}
   * lookup, eliminating the lock-ordering inversion when called while holding
   * a BTree component lock.
   *
   * @param indexId the pre-resolved index engine ID
   * @param useNullSnapshot {@code true} to query the null-key snapshot map
   * @param userKeyPrefix the key prefix (all CompositeKey elements except the version)
   * @param lwm the global low-water mark
   * @return {@code true} if at least one snapshot entry exists with version >= lwm
   */
  public boolean hasActiveIndexSnapshotEntriesById(
      long indexId, boolean useNullSnapshot, CompositeKey userKeyPrefix, long lwm) {
    assert indexId >= 0 : "indexId must be non-negative, got " + indexId;
    final NavigableMap<CompositeKey, RID> snapshotMap =
        useNullSnapshot ? sharedNullIndexesSnapshot : sharedIndexesSnapshot;
    return hasActiveSnapshotEntriesInMap(snapshotMap, indexId, userKeyPrefix, lwm);
  }

  private static final Long LONG_MAX_VALUE = Long.MAX_VALUE;

  private static boolean hasActiveSnapshotEntriesInMap(
      NavigableMap<CompositeKey, RID> snapshotMap,
      long indexId,
      CompositeKey userKeyPrefix,
      long lwm) {
    assert lwm >= 0 : "LWM must be non-negative, got " + lwm;
    var prefixKeys = userKeyPrefix.getKeys();
    var lower = buildSnapshotBoundKey(indexId, prefixKeys, lwm);
    var upper = buildSnapshotBoundKey(indexId, prefixKeys, LONG_MAX_VALUE);
    return !snapshotMap.subMap(lower, true, upper, true).isEmpty();
  }

  private static CompositeKey buildSnapshotBoundKey(
      long indexId, List<Object> prefixKeys, long version) {
    var key = new CompositeKey(prefixKeys.size() + 2);
    key.addKey(indexId);
    for (var pk : prefixKeys) {
      key.addKey(pk);
    }
    key.addKey(version);
    return key;
  }

  /**
   * Computes the global low-water-mark. Reads {@code idGen.getLastId()} <b>before</b> scanning
   * {@code tsMins} so that any transaction starting after the read has {@code tsMin >= fallback},
   * eliminating the race where a new transaction's {@code tsMin} is lower than the fallback.
   *
   * @return the minimum active {@code tsMin}, or {@code idGen.getLastId()} when no transactions
   *     are active. Never returns {@code Long.MAX_VALUE}.
   */
  public long computeGlobalLowWaterMark() {
    // Read the fallback BEFORE scanning tsMins. Because idGen is monotonic
    // and every new transaction sets tsMin >= idGen.getLastId() at its start
    // time, any transaction that begins after this read will have
    // tsMin >= fallbackLwm.
    long fallbackLwm = idGen.getLastId();
    return computeGlobalLowWaterMark(tsMins, fallbackLwm);
  }

  public ConcurrentSkipListMap<SnapshotKey, PositionEntry> getSharedSnapshotIndex() {
    return sharedSnapshotIndex;
  }

  public ConcurrentSkipListMap<VisibilityKey, SnapshotKey> getVisibilityIndex() {
    return visibilityIndex;
  }

  public AtomicLong getSnapshotIndexSize() {
    return snapshotIndexSize;
  }

  public ConcurrentSkipListMap<EdgeSnapshotKey, LinkBagValue> getSharedEdgeSnapshotIndex() {
    return sharedEdgeSnapshotIndex;
  }

  public ConcurrentSkipListMap<EdgeVisibilityKey, EdgeSnapshotKey> getEdgeVisibilityIndex() {
    return edgeVisibilityIndex;
  }

  public AtomicLong getEdgeSnapshotIndexSize() {
    return edgeSnapshotIndexSize;
  }

  public AtomicLong getIndexesSnapshotEntriesCount() {
    return indexesSnapshotEntriesCount;
  }

  /**
   * Computes the global low-water-mark by iterating all registered {@link TsMinHolder}s and
   * returning the minimum {@link TsMinHolder#tsMin} value. Entries with {@code Long.MAX_VALUE}
   * represent idle threads (no active transaction) and are effectively ignored since any real
   * timestamp will be smaller.
   *
   * <p>When no holders are registered or all are idle (minimum is {@code Long.MAX_VALUE}), falls
   * back to {@code currentId}. This ensures that when no transactions are active, the LWM equals
   * the latest generated operation id — all committed record versions have
   * {@code version <= currentId}, and any transaction starting after the caller read
   * {@code currentId} has {@code tsMin >= currentId}, so stale versions with
   * {@code V_new <= currentId} are unreachable.
   *
   * @param tsMins the set of per-thread {@link TsMinHolder} instances
   * @param currentId fallback value when all holders are idle; typically
   *     {@code idGen.getLastId()} read <b>before</b> scanning {@code tsMins}
   * @return the minimum active timestamp, or {@code currentId} when no transactions are active
   */
  static long computeGlobalLowWaterMark(Set<TsMinHolder> tsMins, long currentId) {
    long min = Long.MAX_VALUE;
    // Weakly-consistent iteration over the Guava concurrent weak-key set.
    // No explicit synchronization needed — stale or missing entries are
    // acceptable (same TOCTOU tolerance as the previous synchronized version).
    for (TsMinHolder holder : tsMins) {
      long ts = holder.tsMin;
      if (ts < min) {
        min = ts;
      }
    }
    if (min == Long.MAX_VALUE) {
      min = currentId;
    }
    return min;
  }
}
