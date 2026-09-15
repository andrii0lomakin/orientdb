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

import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException;
import com.jetbrains.youtrackdb.api.exception.HighLevelException;
import com.jetbrains.youtrackdb.api.exception.RecordNotFoundException;
import com.jetbrains.youtrackdb.internal.common.concur.NeedRetryException;
import com.jetbrains.youtrackdb.internal.common.io.IOUtils;
import com.jetbrains.youtrackdb.internal.common.listener.ListenerManger;
import com.jetbrains.youtrackdb.internal.common.log.LogManager;
import com.jetbrains.youtrackdb.internal.common.profiler.metrics.CoreMetrics;
import com.jetbrains.youtrackdb.internal.common.profiler.metrics.Stopwatch;
import com.jetbrains.youtrackdb.internal.common.profiler.metrics.TimeRate;
import com.jetbrains.youtrackdb.internal.common.profiler.monitoring.QueryMonitoringMode;
import com.jetbrains.youtrackdb.internal.common.profiler.monitoring.TransactionMetricsListener;
import com.jetbrains.youtrackdb.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrackdb.internal.core.cache.LocalRecordCache;
import com.jetbrains.youtrackdb.internal.core.cache.WeakValueHashMap;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.config.ContextConfiguration;
import com.jetbrains.youtrackdb.internal.core.config.YouTrackDBConfig;
import com.jetbrains.youtrackdb.internal.core.conflict.RecordConflictStrategy;
import com.jetbrains.youtrackdb.internal.core.db.record.CurrentStorageComponentsFactory;
import com.jetbrains.youtrackdb.internal.core.db.record.EntityEmbeddedListImpl;
import com.jetbrains.youtrackdb.internal.core.db.record.EntityEmbeddedMapImpl;
import com.jetbrains.youtrackdb.internal.core.db.record.EntityEmbeddedSetImpl;
import com.jetbrains.youtrackdb.internal.core.db.record.EntityLinkListImpl;
import com.jetbrains.youtrackdb.internal.core.db.record.EntityLinkMapIml;
import com.jetbrains.youtrackdb.internal.core.db.record.EntityLinkSetImpl;
import com.jetbrains.youtrackdb.internal.core.db.record.RecordElement;
import com.jetbrains.youtrackdb.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Blob;
import com.jetbrains.youtrackdb.internal.core.db.record.record.DBRecord;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Direction;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Edge;
import com.jetbrains.youtrackdb.internal.core.db.record.record.EmbeddedEntity;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Entity;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Identifiable;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RecordHook;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RecordHook.TYPE;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Vertex;
import com.jetbrains.youtrackdb.internal.core.db.record.ridbag.LinkBag;
import com.jetbrains.youtrackdb.internal.core.exception.BaseException;
import com.jetbrains.youtrackdb.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrackdb.internal.core.exception.CommandSQLParsingException;
import com.jetbrains.youtrackdb.internal.core.exception.CommandScriptException;
import com.jetbrains.youtrackdb.internal.core.exception.DatabaseException;
import com.jetbrains.youtrackdb.internal.core.exception.LinksConsistencyException;
import com.jetbrains.youtrackdb.internal.core.exception.SchemaException;
import com.jetbrains.youtrackdb.internal.core.exception.SecurityAccessException;
import com.jetbrains.youtrackdb.internal.core.exception.SecurityException;
import com.jetbrains.youtrackdb.internal.core.exception.SessionNotActivatedException;
import com.jetbrains.youtrackdb.internal.core.exception.TransactionBlockedException;
import com.jetbrains.youtrackdb.internal.core.exception.TransactionException;
import com.jetbrains.youtrackdb.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrackdb.internal.core.id.RecordIdInternal;
import com.jetbrains.youtrackdb.internal.core.index.Index;
import com.jetbrains.youtrackdb.internal.core.index.IndexManagerEmbedded;
import com.jetbrains.youtrackdb.internal.core.iterator.RecordIteratorClass;
import com.jetbrains.youtrackdb.internal.core.iterator.RecordIteratorCollection;
import com.jetbrains.youtrackdb.internal.core.metadata.Metadata;
import com.jetbrains.youtrackdb.internal.core.metadata.MetadataDefault;
import com.jetbrains.youtrackdb.internal.core.metadata.function.FunctionLibraryImpl;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.PropertyTypeInternal;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.SchemaShared;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.TxSchemaState;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.PropertyType;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.Schema;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.SchemaClass;
import com.jetbrains.youtrackdb.internal.core.metadata.security.ImmutableUser;
import com.jetbrains.youtrackdb.internal.core.metadata.security.PropertyEncryptionNone;
import com.jetbrains.youtrackdb.internal.core.metadata.security.Role;
import com.jetbrains.youtrackdb.internal.core.metadata.security.Rule;
import com.jetbrains.youtrackdb.internal.core.metadata.security.Rule.ResourceGeneric;
import com.jetbrains.youtrackdb.internal.core.metadata.security.SecurityUserImpl;
import com.jetbrains.youtrackdb.internal.core.metadata.security.auth.AuthenticationInfo;
import com.jetbrains.youtrackdb.internal.core.metadata.sequence.SequenceLibraryImpl;
import com.jetbrains.youtrackdb.internal.core.metadata.sequence.SequenceLibraryProxy;
import com.jetbrains.youtrackdb.internal.core.query.ExecutionPlan;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.query.ResultSet;
import com.jetbrains.youtrackdb.internal.core.query.collection.embedded.EmbeddedList;
import com.jetbrains.youtrackdb.internal.core.query.collection.embedded.EmbeddedMap;
import com.jetbrains.youtrackdb.internal.core.query.collection.embedded.EmbeddedSet;
import com.jetbrains.youtrackdb.internal.core.query.collection.links.LinkList;
import com.jetbrains.youtrackdb.internal.core.query.collection.links.LinkMap;
import com.jetbrains.youtrackdb.internal.core.query.collection.links.LinkSet;
import com.jetbrains.youtrackdb.internal.core.record.RecordAbstract;
import com.jetbrains.youtrackdb.internal.core.record.impl.EdgeEntityImpl;
import com.jetbrains.youtrackdb.internal.core.record.impl.EdgeInternal;
import com.jetbrains.youtrackdb.internal.core.record.impl.EmbeddedEntityImpl;
import com.jetbrains.youtrackdb.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrackdb.internal.core.record.impl.RecordBytes;
import com.jetbrains.youtrackdb.internal.core.record.impl.VertexEntityImpl;
import com.jetbrains.youtrackdb.internal.core.schedule.ScheduledEvent;
import com.jetbrains.youtrackdb.internal.core.schedule.SchedulerImpl;
import com.jetbrains.youtrackdb.internal.core.security.SecurityUser;
import com.jetbrains.youtrackdb.internal.core.serialization.serializer.binary.BinarySerializerFactory;
import com.jetbrains.youtrackdb.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrackdb.internal.core.serialization.serializer.record.binary.RecordSerializerBinary;
import com.jetbrains.youtrackdb.internal.core.serialization.serializer.record.string.JSONSerializerJackson;
import com.jetbrains.youtrackdb.internal.core.sql.PlanNullPlacements;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.SQLEngine;
import com.jetbrains.youtrackdb.internal.core.sql.executor.AbstractExecutionStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.AggregateProjectionCalculationStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.DistinctExecutionStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.ExecutionStepInternal;
import com.jetbrains.youtrackdb.internal.core.sql.executor.InternalExecutionPlan;
import com.jetbrains.youtrackdb.internal.core.sql.executor.InternalResultSet;
import com.jetbrains.youtrackdb.internal.core.sql.executor.ProjectionCalculationStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrackdb.internal.core.sql.executor.SelectExecutionPlan;
import com.jetbrains.youtrackdb.internal.core.sql.executor.cache.AggregateCacheTapStep;
import com.jetbrains.youtrackdb.internal.core.sql.executor.cache.AggregateState;
import com.jetbrains.youtrackdb.internal.core.sql.executor.cache.CacheKey;
import com.jetbrains.youtrackdb.internal.core.sql.executor.cache.CacheableShape;
import com.jetbrains.youtrackdb.internal.core.sql.executor.cache.CachedEntry;
import com.jetbrains.youtrackdb.internal.core.sql.executor.cache.CachedResultSetView;
import com.jetbrains.youtrackdb.internal.core.sql.executor.cache.DeltaBuilder;
import com.jetbrains.youtrackdb.internal.core.sql.executor.cache.NonDeterministicQueryDetector;
import com.jetbrains.youtrackdb.internal.core.sql.executor.cache.QueryResultCache;
import com.jetbrains.youtrackdb.internal.core.sql.executor.cache.ShapeClassifier;
import com.jetbrains.youtrackdb.internal.core.sql.executor.cache.TxDeltaCursor;
import com.jetbrains.youtrackdb.internal.core.sql.executor.resultset.IdempotentExecutionStream;
import com.jetbrains.youtrackdb.internal.core.sql.executor.resultset.ResultMapper;
import com.jetbrains.youtrackdb.internal.core.sql.parser.LocalResultSet;
import com.jetbrains.youtrackdb.internal.core.sql.parser.LocalResultSetLifecycleDecorator;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLAlterClassStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLAlterPropertyStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLCreateClassStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLCreateIndexStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLCreatePropertyStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLDropClassStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLDropIndexStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLDropPropertyStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchFilter;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMatchStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMethodCall;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLNestedProjection;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLProjection;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLProjectionItem;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLTruncateClassStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLWhereClause;
import com.jetbrains.youtrackdb.internal.core.storage.RawBuffer;
import com.jetbrains.youtrackdb.internal.core.storage.RawPageBuffer;
import com.jetbrains.youtrackdb.internal.core.storage.StorageReadResult;
import com.jetbrains.youtrackdb.internal.core.storage.cache.OptimisticReadFailedException;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.AbstractStorage;
import com.jetbrains.youtrackdb.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrackdb.internal.core.storage.ridbag.LinkCollectionsBTreeManager;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransaction;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransaction.TXSTATUS;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransactionImpl;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransactionNoTx;
import com.jetbrains.youtrackdb.internal.core.tx.RollbackException;
import com.jetbrains.youtrackdb.internal.core.tx.Transaction;
import com.jetbrains.youtrackdb.internal.core.tx.TxBiConsumer;
import com.jetbrains.youtrackdb.internal.core.tx.TxBiFunction;
import com.jetbrains.youtrackdb.internal.core.tx.TxConsumer;
import com.jetbrains.youtrackdb.internal.core.tx.TxFunction;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseSessionEmbedded extends ListenerManger<SessionListener>
    implements AutoCloseable, QueryLifecycleListener {

  @SuppressWarnings("unused")
  public enum STATUS {
    OPEN, CLOSED, IMPORTING
  }

  public enum ATTRIBUTES {
    DATEFORMAT, DATE_TIME_FORMAT, TIMEZONE, LOCALE_COUNTRY, LOCALE_LANGUAGE, CHARSET,
  }

  public enum ATTRIBUTES_INTERNAL {
    VALIDATION
  }

  public record TransactionMeters(
      TimeRate totalTransactions,
      TimeRate writeTransactions,
      TimeRate writeRollbackTransactions) {

    @SuppressWarnings("unused")
    public static TransactionMeters NOOP = new TransactionMeters(
        TimeRate.NOOP,
        TimeRate.NOOP,
        TimeRate.NOOP);
  }

  private static final Logger logger = LoggerFactory.getLogger(DatabaseSessionEmbedded.class);

  private static final byte recordType = EntityImpl.RECORD_TYPE;
  private final boolean serverMode;

  private YouTrackDBConfigImpl config;
  private final AbstractStorage storage;

  private final Stopwatch freezeDurationMetric;
  private final Stopwatch releaseDurationMetric;

  private final TransactionMeters transactionMeters;

  private boolean ensureLinkConsistency = true;

  private final HashMap<String, Object> properties = new HashMap<>();
  private final HashSet<Identifiable> inHook = new HashSet<>();

  private RecordSerializer serializer = RecordSerializerBinary.INSTANCE;
  private String url;

  private STATUS status;
  private MetadataDefault metadata;
  private ImmutableUser user;

  private final ArrayList<RecordHook> hooks = new ArrayList<>();
  private boolean retainRecords = true;
  private final LocalRecordCache localCache = new LocalRecordCache();
  private final CurrentStorageComponentsFactory componentsFactory;
  private boolean initialized = false;
  // Volatile for the pool-teardown skip detection: hasInFlightForeignCommit() reads the
  // CURRENT transaction object from a foreign thread before consulting its volatile
  // status/storageTxThreadId fields. With a plain field the foreign read could return a stale
  // pre-begin placeholder (or a prior borrow's transaction object) with no happens-before edge to
  // the owner's begin(), false-no-skipping a genuinely in-flight commit and tearing its live
  // transaction object down — the exact hazard the skip protocol exists to prevent. The volatile
  // read pairs with the owner's volatile write here at every tx-boundary assignment, and the
  // subsequent volatile status read then observes that same transaction's current state. Owner-
  // thread reads are unaffected (program order); the write is tx-boundary-rare, so the cost is nil.
  private volatile FrontendTransaction currentTx;

  private SharedContext sharedContext;

  /**
   * The acquire ordinal of this session's currently engaged metadata-write-mutex permit, 0 when
   * none is engaged. It is a session field rather than transaction custom data on purpose: the
   * custom-data map is wiped by the transaction's {@code clear()} before the outermost teardown
   * runs the release, so a record living there would be gone by release time; this session-side
   * record survives that wipe. It is an {@link AtomicLong} because the release gate is an atomic
   * claim: every releaser — the owner's tx-close finally, a foreign teardown's release pass (pool
   * shutdown), and the engage-path Dekker self-release — funnels through one
   * {@code getAndSet(0)} in {@link #releaseMetadataWriteMutexForTx()}, so exactly one of any
   * number of racing teardowns harvests the ordinal and presents it to the mutex; the mutex's
   * {@code (session, ordinal)}-keyed CAS is the independent second belt.
   */
  private final AtomicLong engagedMutexOrdinal = new AtomicLong();

  /**
   * The mutex instance the current (or last) engage acquired, captured at engage time so the
   * release funnel never needs {@link #sharedContext} — which {@code internalClose} nulls before
   * its outer finally runs the widened release pass. Never cleared: a session binds to one storage
   * for its lifetime, so the reference stays valid and a stale read is impossible. Volatile so a
   * foreign teardown that harvests the ordinal (whose {@code AtomicLong} read gives the
   * happens-before edge from the engage) also reads the mutex reference the engage published.
   */
  @Nullable private volatile MetadataWriteMutex engagedMutex;

  /**
   * The Dekker teardown-intent mark: volatile, set by every teardown that actually tears (after
   * {@code internalClose}'s one-shot guard passes — a guard-returning no-op close must not plant a
   * stale mark) and by the pool-teardown entry ({@code realClose}) before it re-validates the
   * pool-teardown skip condition. The engage path stores its acquire ordinal FIRST and then re-reads this
   * mark (ordinal-store-before-mark-read): with the teardown writing the mark before its release
   * pass, at least one side always sees the other — a teardown that missed a mid-flight engage is
   * seen by the engage's re-check (which self-releases and throws), and an engage the teardown
   * did see is harvested by the teardown's release pass. Cleared on every pooled {@code reuse()}
   * (the second belt against a stale mark surviving into a fresh borrow).
   */
  private volatile boolean teardownIntent;

  /**
   * True while an {@code internalClose} body is running on this session. Read by the
   * owner-as-completer seam ({@link #completeDeferredTeardownAfterTxClose()}) to distinguish a
   * tx-close reached from a user commit/rollback (where a marked session means the pool skipped
   * its teardown and the owner must complete it) from a tx-close reached from inside
   * {@code internalClose}'s own rollback (where re-entering {@code internalClose} would recurse).
   * Deliberately a plain field: the guard only needs same-stack reentrance detection (program
   * order); a cross-thread overlap between an owner completer and a pool teardown is contained by
   * the atomic {@link #teardownClaim} (exactly one full teardown proceeds) and the atomic release
   * claim (exactly one permit release).
   */
  private boolean internalCloseInProgress;

  /**
   * The atomic one-shot teardown claim: exactly one {@code internalClose} teardown may proceed
   * per open cycle of this session. The plain {@code status != OPEN} guard alone is a
   * check-then-act — two racing teardowns (the pool close's fall-through and the owner's deferred
   * completer, or a pool close racing the borrower's own close) could BOTH pass it while the
   * status is still OPEN and both run the full body, double-firing the close listeners and
   * double-decrementing the storage session count (risking a premature storage auto-close under a
   * live session, or a permanently skewed count). The CAS makes the overlap race-free: the loser
   * no-ops. A teardown that THROWS before reaching {@code status = CLOSED} releases the claim on
   * unwind, preserving the pre-existing retry semantics for a broken session's cleanup close; a
   * completed teardown keeps it (the status guard blocks re-entry anyway). Reset on pooled
   * {@code reuse()} alongside the status flip back to OPEN.
   */
  private final AtomicBoolean teardownClaim = new AtomicBoolean();

  /**
   * True only while {@code ensureTxSchemaState} is building the tx-local schema copy (the
   * {@link SchemaShared#copyForTx} re-parse). The re-parse rebuilds the committed inheritance tree,
   * which ripples a subclass's collection into a superclass index and routes back into the
   * index-manager's tx-local seam. That seam consults this flag and treats any membership ripple
   * raised during seeding as a no-op: the copy's polymorphic ids are being reconstructed from the
   * already-committed shared index, so re-recording them into the tx-local changed-class set would
   * both pollute the set with unchanged committed classes and re-enter {@code ensureTxSchemaState}
   * before its marker is written -- re-engaging the single-permit mutex on the same thread and
   * self-deadlocking (or, with assertions on, tripping the engage-order check because the seed holds
   * the committed schema write lock). Thread-confined to the seeding thread.
   */
  private boolean seedingTxSchemaState = false;

  /**
   * True only while {@link SchemaShared#reload} is re-parsing the committed schema on this session
   * (the {@code runReloadingCommittedSchema} scope). The index-manager tx-local seam reads this to
   * treat the membership ripples the reload's {@code fromStream} inheritance rebuild raises as
   * handled no-ops, exactly as it does for the {@code copyForTx} seed via
   * {@link #isSeedingTxSchemaState()}: a reload reconstructs already-committed state, so the
   * ripples are not schema writes and must not seed a tx-local schema state (whose mutex engage
   * would run under the schema write lock the reload holds, tripping the engage-order guard).
   * Thread-confined to the reloading thread.
   */
  private boolean reloadingSchema = false;

  /**
   * Non-null only while a {@link #computeWithFreshCommittedReads} scope is running on this
   * session's thread. While set, {@link #executeReadRecord} bypasses the session's local record
   * cache and reads through this dedicated read-only atomic operation instead of the active
   * transaction's, so the reads observe the latest committed state rather than the transaction's
   * begin-time snapshot. Session state is thread-confined, so the field needs no volatile.
   */
  private AtomicOperation freshCommittedReadOperation;

  private boolean prefetchRecords;

  private final Map<String, ResultSet> activeQueries;
  private final int resultSetReportThreshold;

  /**
   * The null placement resolution shared by one plan build of this session. A plan build opens a
   * scope on it, every placement read of that build goes through it, and the plan cache stores what
   * the build read next to the plan it publishes. Thread-confined like the rest of session state.
   */
  private final PlanNullPlacements planNullPlacements =
      new PlanNullPlacements(this::getConfiguration);

  // database stats!
  private long loadedRecordsCount;
  private long totalRecordLoadMs;
  private long minRecordLoadMs;
  private long ridbagPrefetchCount;
  private long totalRidbagPrefetchMs;
  private long minRidbagPrefetchMs;
  private long maxRidbagPrefetchMs;

  private final ThreadLocal<Boolean> activeSession = new ThreadLocal<>();

  public DatabaseSessionEmbedded(final AbstractStorage storage, boolean serverMode) {
    super(false);
    this.serverMode = serverMode;
    // in server mode we don't enable result set auto-closing
    this.activeQueries = serverMode ? new HashMap<>() : new WeakValueHashMap<>();

    activateOnCurrentThread();

    try {
      status = STATUS.CLOSED;
      // OVERWRITE THE URL
      url = storage.getURL();
      this.storage = storage;
      this.componentsFactory = storage.getComponentsFactory();

      init();

      final var metrics = YouTrackDBEnginesManager.instance().getMetricsRegistry();
      freezeDurationMetric =
          metrics.databaseMetric(CoreMetrics.DATABASE_FREEZE_DURATION, getDatabaseName());
      releaseDurationMetric =
          metrics.databaseMetric(CoreMetrics.DATABASE_RELEASE_DURATION, getDatabaseName());

      this.transactionMeters = new TransactionMeters(
          metrics.databaseMetric(CoreMetrics.TRANSACTION_RATE, getDatabaseName()),
          metrics.databaseMetric(CoreMetrics.TRANSACTION_WRITE_RATE, getDatabaseName()),
          metrics.databaseMetric(CoreMetrics.TRANSACTION_WRITE_ROLLBACK_RATE, getDatabaseName()));

      this.resultSetReportThreshold =
          getConfiguration().getValueAsInteger(
              GlobalConfiguration.QUERY_RESULT_SET_OPEN_WARNING_THRESHOLD);

    } catch (Exception t) {
      activeSession.remove();
      throw BaseException.wrapException(
          new DatabaseException(
              getDatabaseName(), "Error on opening database "),
          t, getDatabaseName());
    }
  }

  public void init(YouTrackDBConfigImpl config, SharedContext sharedContext) {
    this.sharedContext = sharedContext;
    activateOnCurrentThread();
    this.config = config;
    applyAttributes(config);
    applyListeners(config);
    try {

      status = STATUS.OPEN;
      if (initialized) {
        return;
      }

      var serializeName = storage.getRecordSerializer();
      if (serializeName == null) {
        throw new DatabaseException(getDatabaseName(),
            "Impossible to open database from version before 2.x use export import instead");
      }

      if (storage.getRecordSerializerVersion()
          > serializer.getMinSupportedVersion()) {
        throw new DatabaseException(getDatabaseName(),
            "Persistent record serializer version is not support by the current implementation");
      }

      localCache.startup(this);

      loadMetadata();

      installHooksEmbedded();

      user = null;

      initialized = true;
    } catch (BaseException e) {
      activeSession.remove();
      throw e;
    } catch (Exception e) {
      activeSession.remove();
      throw BaseException.wrapException(
          new DatabaseException(getDatabaseName(), "Cannot open database url=" + getURL()), e,
          getDatabaseName());
    }
  }

  public void internalOpen(final AuthenticationInfo authenticationInfo) {
    try {
      var security = sharedContext.getSecurity();

      if (user == null || user.getVersion() != security.getVersion(this)) {
        final SecurityUser usr;

        usr = security.securityAuthenticate(this, authenticationInfo);
        if (usr != null) {
          user = new ImmutableUser(this, security.getVersion(this), usr);
        } else {
          user = null;
        }

        checkSecurity(ResourceGeneric.DATABASE, Role.PERMISSION_READ);
      }

    } catch (BaseException e) {
      activeSession.remove();
      throw e;
    } catch (Exception e) {
      activeSession.remove();
      throw BaseException.wrapException(
          new DatabaseException(getDatabaseName(), "Cannot open database url=" + getURL()), e,
          getDatabaseName());
    }
  }

  public void internalOpen(final String iUserName, final String iUserPassword) {
    internalOpen(iUserName, iUserPassword, true);
  }

  public void internalOpen(
      final String iUserName, final String iUserPassword, boolean checkPassword) {
    executeInTx(
        transaction -> {
          try {
            var security = sharedContext.getSecurity();

            if (user == null
                || user.getVersion() != security.getVersion(this)
                || !user.getName(this).equalsIgnoreCase(iUserName)) {
              final SecurityUser usr;

              if (checkPassword) {
                usr = security.securityAuthenticate(this, iUserName, iUserPassword);
              } else {
                usr = security.getUser(this, iUserName);
              }
              if (usr != null) {
                user = new ImmutableUser(this, security.getVersion(this), usr);
              } else {
                user = null;
              }

              checkSecurity(ResourceGeneric.DATABASE, Role.PERMISSION_READ);
            }
          } catch (BaseException e) {
            activeSession.remove();
            throw e;
          } catch (Exception e) {
            activeSession.remove();
            throw BaseException.wrapException(
                new DatabaseException(getDatabaseName(), "Cannot open database url=" + getURL()), e,
                getDatabaseName());
          }
        });
  }

  private void applyListeners(YouTrackDBConfigImpl config) {
    if (config != null) {
      for (var listener : config.getListeners()) {
        registerListener(listener);
      }
    }
  }

  public void internalCreate(YouTrackDBConfigImpl config, SharedContext ctx) {
    storage.setRecordSerializer(serializer.toString(), serializer.getCurrentVersion());
    storage.setProperty(SQLStatement.CUSTOM_STRICT_SQL, "true");

    this.setSerializer(serializer);

    this.sharedContext = ctx;
    this.status = STATUS.OPEN;
    // THIS IF SHOULDN'T BE NEEDED, CREATE HAPPEN ONLY IN EMBEDDED
    applyAttributes(config);
    applyListeners(config);
    metadata = new MetadataDefault(this);
    installHooksEmbedded();
    createMetadata(ctx);
  }

  public void callOnCreateListeners() {
    // WAKE UP DB LIFECYCLE LISTENER
    assert assertIfNotActive();
    for (var it = YouTrackDBEnginesManager.instance()
        .getDbLifecycleListeners();
        it.hasNext();) {
      it.next().onCreate(this);
    }
  }

  private void createMetadata(SharedContext shared) {
    assert assertIfNotActive();
    metadata.init(shared);

    shared.create(this);
  }

  private void loadMetadata() {
    assert assertIfNotActive();
    executeInTx(
        transaction -> {
          metadata = new MetadataDefault(this);
          metadata.init(sharedContext);
          sharedContext.load(this);
        });
  }

  private void applyAttributes(YouTrackDBConfigImpl config) {
    if (config != null) {
      for (var attrs : config.getAttributes().entrySet()) {
        this.set(attrs.getKey(), attrs.getValue());
      }
    }
  }

  public void set(final ATTRIBUTES iAttribute, final Object iValue) {
    assert assertIfNotActive();

    checkOpenness();

    if (iAttribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    final var stringValue = IOUtils.getStringContent(iValue != null ? iValue.toString() : null);
    final var storage = this.storage;
    switch (iAttribute) {
      case DATEFORMAT -> {
        if (stringValue == null) {
          throw new IllegalArgumentException("date format is null");
        }

        // Validate format by formatting the current instant
        new SimpleDateFormat(stringValue).format(Date.from(Instant.now()));

        storage.setDateFormat(stringValue);
      }

      case DATE_TIME_FORMAT -> {
        if (stringValue == null) {
          throw new IllegalArgumentException("date format is null");
        }

        // Validate format by formatting the current instant
        new SimpleDateFormat(stringValue).format(Date.from(Instant.now()));

        storage.setDateTimeFormat(stringValue);
      }

      case TIMEZONE -> {
        if (stringValue == null) {
          throw new IllegalArgumentException("Timezone can't be null");
        }

        // for backward compatibility, until 2.1.13 YouTrackDB accepted timezones in lowercase as well
        var timeZoneValue = TimeZone.getTimeZone(stringValue.toUpperCase(Locale.ENGLISH));
        if (timeZoneValue.equals(TimeZone.getTimeZone("GMT"))) {
          timeZoneValue = TimeZone.getTimeZone(stringValue);
        }

        storage.setTimeZone(timeZoneValue);
      }

      case LOCALE_COUNTRY -> storage.setLocaleCountry(stringValue);

      case LOCALE_LANGUAGE -> storage.setLocaleLanguage(stringValue);

      case CHARSET -> storage.setCharset(stringValue);

      default -> throw new IllegalArgumentException(
          "Option '" + iAttribute + "' not supported on alter database");
    }
  }

  private void clearCustomInternal() {
    storage.clearProperties();
  }

  private void removeCustomInternal(final String iName) {
    setCustomInternal(iName, null);
  }

  private void setCustomInternal(final String iName, final String iValue) {
    final var storage = this.storage;
    if (iValue == null || "null".equalsIgnoreCase(iValue))
    // REMOVE
    {
      storage.removeProperty(iName);
    } else
    // SET
    {
      storage.setProperty(iName, iValue);
    }
  }

  @SuppressWarnings("unused")
  public DatabaseSessionEmbedded setCustom(final String name, final Object iValue) {
    assert assertIfNotActive();

    checkOpenness();

    if ("clear".equalsIgnoreCase(name) && iValue == null) {
      clearCustomInternal();
    } else {
      var customValue = iValue == null ? null : "" + iValue;
      if (name == null || customValue.isEmpty()) {
        removeCustomInternal(name);
      } else {
        setCustomInternal(name, customValue);
      }
    }

    return this;
  }

  /**
   * Returns a copy of current database if it's open. The returned instance can be used by another
   * thread without affecting current instance. The database copy is not set in thread local.
   */
  public DatabaseSessionEmbedded copy() {
    assertIfNotActive();

    checkOpenness();

    var storage = getSharedContext().getStorage();
    storage.open(this, null, null, getConfiguration());
    String user;
    if (getCurrentUser() != null) {
      user = getCurrentUser().getName(this);
    } else {
      user = null;
    }

    var database = new DatabaseSessionEmbedded(storage, this.serverMode);
    database.init(config, this.sharedContext);
    database.internalOpen(user, null, false);
    database.callOnOpenListeners();

    database.activateOnCurrentThread();
    return database;
  }

  public boolean isClosed() {
    return status == STATUS.CLOSED || storage.isClosed(this);
  }

  public void rebuildIndexes() {
    assert assertIfNotActive();

    checkOpenness();

    var indexManager = sharedContext.getIndexManager();
    if (indexManager.autoRecreateIndexesAfterCrash(this)) {
      indexManager.recreateIndexes(this);
    }
  }

  private void installHooksEmbedded() {
    assert assertIfNotActive();
    hooks.clear();
  }

  public AbstractStorage getStorage() {
    return storage;
  }

  public ResultSet query(String query, Object... args) {
    checkOpenness();

    assert assertIfNotActive();
    if (currentTx.isCallBackProcessingInProgress()) {
      throw new CommandExecutionException(getDatabaseName(),
          "Cannot execute query while transaction processing callbacks. If you called this method in beforeCallbackXXX method "
              +
              "please move it to the afterCallbackXXX method");
    }
    try {
      beginReadOnly();
      currentTx.preProcessRecordsAndExecuteCallCallbacks();
      getSharedContext().getYouTrackDB().startCommand(null);
      try {
        var statement = SQLEngine.parse(query, this);
        if (!statement.isIdempotent()) {
          throw new CommandExecutionException(getDatabaseName(),
              "Cannot execute query on non idempotent statement: " + query);
        }
        var served = serveThroughCache(statement, args);
        return queryStartedLifecycle(served);
      } finally {
        getSharedContext().getYouTrackDB().endCommand();
      }
    } catch (Exception e) {
      rollback();
      throw e;
    }
  }

  public ResultSet query(String query, @SuppressWarnings("rawtypes") Map args) {
    return query(query, true, args);
  }

  public ResultSet query(String query, boolean syncTx, @SuppressWarnings("rawtypes") Map args)
      throws CommandSQLParsingException, CommandExecutionException {
    assert assertIfNotActive();

    checkOpenness();

    if (currentTx.isCallBackProcessingInProgress()) {
      throw new CommandExecutionException(getDatabaseName(),
          "Cannot execute query while transaction processing callbacks. If you called this method in beforeCallbackXXX method "
              +
              "please move it to the afterCallbackXXX method");
    }

    beginReadOnly();
    try {
      if (syncTx) {
        currentTx.preProcessRecordsAndExecuteCallCallbacks();
      }
      getSharedContext().getYouTrackDB().startCommand(null);
      try {
        var statement = SQLEngine.parse(query, this);
        if (!statement.isIdempotent()) {
          throw new CommandExecutionException(getDatabaseName(),
              "Cannot execute query on non idempotent statement: " + query);
        }
        @SuppressWarnings("unchecked")
        Map<Object, Object> typedArgs = args;
        var served = serveThroughCache(statement, typedArgs);
        return queryStartedLifecycle(served);
      } finally {
        getSharedContext().getYouTrackDB().endCommand();
      }
    } catch (Exception e) {
      rollback();
      throw e;
    }
  }

  /**
   * Shared cache entry point for both {@code query()} overloads. Decides whether the parsed,
   * already-confirmed-idempotent {@code statement} is eligible for the tx-result cache and, when it
   * is, serves the result from the cache (hit) or populates a fresh entry (miss); otherwise it runs
   * the ordinary uncached execution and returns its result unchanged.
   *
   * <p>The gate bypasses to uncached execution when any of the following holds: the feature flag is
   * off (the transaction's cache is {@code null}); a cache code path is already on the stack ({@code
   * cacheCodeDepth > 0}, the re-entrancy guard, so a {@code query()} issued from a UDF in a WHERE
   * clause does not recurse into the cache); the statement is not a SELECT or MATCH; the statement
   * references a non-deterministic function or per-row context variable; or the classified shape has no
   * wired path. {@code RECORD}, {@code K0_NONE}, the {@code AGGREGATE_*} family, {@code
   * DISTINCT_VALUES}, and {@code MATCH_TUPLE_MULTI} all route through the cache. The {@code AGGREGATE_*} miss takes its own
   * eager-drive populate path with a side-tap splice and falls back uncached on an untappable plan
   * shape. A {@code MATCH_TUPLE_MULTI} (multi-alias MATCH) entry replays verbatim under a class-scoped
   * version gate: a hit whose pattern read-classes saw a post-populate mutation is invalidated and
   * re-executed, and a pattern whose read-class closure is empty is not cached.
   *
   * @param statement the parsed, idempotent query statement
   * @param args      the positional {@code Object[]} or named {@code Map<Object,Object>} parameters
   *                  (or {@code null}); used to build the cache key and the view's command context
   */
  private ResultSet serveThroughCache(@Nonnull SQLStatement statement, @Nullable Object args) {
    // The cache lives only on a real transaction; a no-tx command path (FrontendTransactionNoTx) has
    // no cache, so route it straight to uncached execution. The query() overloads call
    // beginReadOnly() first, so this is normally a FrontendTransactionImpl, but the guard keeps the
    // path safe if a query ever runs outside an active transaction.
    if (!(currentTx instanceof FrontendTransactionImpl tx)) {
      return executeUncached(statement, args);
    }
    var cache = tx.getQueryResultCache();
    // Feature off (null cache) or re-entrant call: run the plain uncached path. The tx-level
    // re-entrancy depth brackets the whole lookup-and-view scope below (and, via the view, the lazy
    // stream pull during iteration), so any query() launched while we are inside it sees depth > 0
    // here and never touches the cache. The non-determinism and shape walks deliberately do NOT run
    // here: they descend the full AST, so running them ahead of the lookup would re-pay the
    // classification cost on every cache HIT — the exact work the cache exists to save. They run
    // only on the miss/populate branch below, where the result is needed to build the entry; a hit
    // trusts the stored entry's shape because the statement was already proven deterministic and
    // assigned a cacheable shape when that entry was populated.
    if (cache == null
        || tx.getCacheCodeDepth() > 0
        || !(statement instanceof SQLSelectStatement || statement instanceof SQLMatchStatement)) {
      return executeUncached(statement, args);
    }

    // When the cache holds no entries and no non-cacheable keys, the isNonCacheable() and lookup()
    // gates below can only report "nothing": a guaranteed miss, never a bypass. Skip them — and the
    // key build they need — and go straight to the populate decision. Capture the predicate once: the
    // cache is not mutated between here and the populate branch (enterCacheCode only bumps a depth
    // counter), so every "is the cache empty" read below stays consistent.
    var cacheEmpty = cache.isEmpty();

    // Memoised cache key, built at most once on the first get(). It is forced eagerly just below only
    // when the cache is non-empty (the isNonCacheable/lookup gates need it); on the empty-cache
    // short-circuit it stays unbuilt until a populate path reaches its put (or the aggregate
    // overflow-guard install), so a query that clears the cache gates but stores nothing never
    // allocates a CacheKey.
    var key = lazyCacheKey(statement, args);

    // A key already routed out of the cache (entry overflowed its size cap, or a K0_NONE entry
    // exceeded its re-population strike limit) stays uncached for the rest of the transaction; do
    // not build a doomed entry. Only reachable on a non-empty cache — an empty cache has no
    // non-cacheable keys — so this check and its key build are skipped on the short-circuit.
    if (!cacheEmpty && cache.isNonCacheable(key.get())) {
      return executeUncached(statement, args);
    }

    // Re-entrancy bracket: bump the tx-level cache-code depth around the synchronous lookup-and-build
    // scope so a nested query() issued while we are inside it — a UDF in a WHERE evaluated during the
    // delta build, or during the aggregate eager-drive below — bypasses the cache. The depth check at the
    // top of this method sits ahead of this enter, so a re-entrant query() rejects before reaching
    // lookup() and lookup never runs twice on the same thread. The guard is released unconditionally
    // in the finally:
    // the lazy stream pull that happens later, during view iteration, is bracketed separately by the
    // CachedResultSetView around each row it produces (see CachedResultSetView.hasNext), so the depth
    // is held exactly over the windows a re-entrant query could corrupt and not for the view's whole
    // idle lifetime. A query() issued by user code between two next() calls therefore still uses the
    // cache, and an abandoned view never silently disables it for the rest of the transaction.
    tx.enterCacheCode();
    // One placement resolution serves this whole query: the lookup gate below, the plan build a
    // populate drives, and the placement a fresh entry freezes. Sharing it is what keeps an entry's
    // recorded placement equal to the placement its own plan baked in.
    planNullPlacements.open();
    try {
      if (!cacheEmpty) {
        // Non-empty cache: consult it. On the empty-cache short-circuit this whole block — lookup,
        // the hit handling, and the post-lookup strike re-check — is skipped, and the key is not built
        // here, because lookup() could only miss and isNonCacheable() could only return false.
        var hit =
            cache.lookup(key.get(), tx.getMutationVersion(), planNullPlacements::resolve);
        if (hit != null) {
          if (hit.getShape() == CacheableShape.MATCH_TUPLE_MULTI
              && DeltaBuilder.matchMultiStale(hit, tx)) {
            // A multi-alias MATCH hit passes a class-scoped version gate. A post-populate mutation
            // touched one of the pattern's read classes, so the frozen tuple set is stale and cannot be
            // reconciled per-tuple (a projected RETURN row carries no bound records to rebuild tuples
            // from); drop the entry and re-populate on the miss path below.
            cache.invalidateMatchMulti(key.get());
          } else {
            // Hit: the stored entry was proven deterministic and a wired shape at populate, so neither
            // the non-determinism walk nor the shape classifier runs again; the entry carries its shape.
            // A multi-alias MATCH reaching here had no pattern-class mutation since populate. lookup()
            // defers its hit count to here so a stale MATCH hit is not double-counted as hit+invalidation.
            if (hit.getShape() == CacheableShape.MATCH_TUPLE_MULTI) {
              cache.recordHit();
            }
            return buildView(hit, tx, args);
          }
        }
        // A stale-entry invalidation that just ran can cross the repopulate-strike threshold and route
        // this key into the non-cacheable set on this very call: a K0_NONE version-gate strike inside
        // lookup() (the line above), or the multi-alias MATCH invalidateMatchMulti() on the stale branch.
        // Both share the strike counter, and either can be the strike that tips the key over. When it
        // does, populating now is unsafe: cache.put() refuses to store a non-cacheable key and closes the
        // fresh entry's stream, so buildView() would replay an empty result instead of the rows a fresh
        // run returns. Re-check and route to uncached execution before paying for populate. The pre-lookup
        // isNonCacheable check at the top of this method cannot catch this, because the key became
        // non-cacheable only just now, after that check ran.
        if (cache.isNonCacheable(key.get())) {
          return executeUncached(statement, args);
        }
      } else {
        // Empty-cache short-circuit: lookup() did not run, but it would have found the key absent and
        // counted a miss. Count it here so the hit/miss metric is identical whether or not the
        // short-circuit fired. A later query in the same transaction sees a non-empty cache and goes
        // through lookup(), which counts its own hits and misses.
        cache.recordMiss();
      }
      // Miss (or empty-cache short-circuit): now (and only now) pay for the AST analysis needed to
      // decide whether to populate an entry. A non-deterministic statement or an unwired shape
      // bypasses the cache uncached.
      if (NonDeterministicQueryDetector.containsNonDeterministicReference(statement)) {
        return executeUncached(statement, args);
      }
      var shape = ShapeClassifier.classify(statement);
      if (shape.isAggregate() && statement instanceof SQLSelectStatement select) {
        // AGGREGATE_* shapes take a separate populate path: the side-tap must observe every
        // contributing record, so the miss builds a per-execution plan copy, splices the tap upstream
        // of the aggregation step, and eager-drives it (RECORD/K0_NONE lazy-pull cannot seed an
        // aggregate). On any unexpected plan shape (e.g. a hardwired CountFromClassStep) it falls back
        // to uncached execution and counts a splice failure. The eager-drive runs inside this guarded
        // scope, so a UDF in the aggregate's WHERE that issues a nested query() is bypassed.
        return populateAndBuildAggregateView(select, args, key, shape, tx, cache);
      }
      if (shape == CacheableShape.DISTINCT_VALUES
          && statement instanceof SQLSelectStatement distinctSelect) {
        // SELECT distinct(prop) / SELECT DISTINCT prop: the distinct value set, reconciled through the
        // same per-value buckets as an aggregate but emitted as one row per distinct value. Its tap
        // splices above the pre-distinct projection; unexpected plan shapes fall back uncached.
        return populateAndBuildDistinctValuesView(distinctSelect, args, key, tx, cache);
      }
      if (shape == CacheableShape.MATCH_TUPLE_MULTI) {
        // Multi-alias MATCH caches via a class-scoped version gate: replay verbatim while no pattern-
        // class mutation has happened, re-execute once one has. The gate can fire only if the entry
        // knows its read-class closure; an empty closure (no resolvable pattern class) would replay a
        // stale tuple set forever after any mutation, so refuse to cache it and run uncached. Compute
        // the closure once here and thread it into the populate so it is not re-walked when the entry
        // is built.
        var matchClasses = effectiveFromClasses(statement);
        if (matchClasses.isEmpty()) {
          return executeUncached(statement, args);
        }
        return populateAndBuildView(statement, args, key, shape, tx, cache, matchClasses);
      }
      if (shape != CacheableShape.RECORD && shape != CacheableShape.K0_NONE) {
        // Any other not-yet-wired shape bypasses the cache uncached.
        return executeUncached(statement, args);
      }
      return populateAndBuildView(statement, args, key, shape, tx, cache, null);
    } finally {
      // Always release the synchronous-scope guard. A returned view holds no guard of its own; it
      // re-enters the bracket per row during iteration (CachedResultSetView.hasNext), so it cannot
      // leak the depth and an abandoned view no longer disables the cache for the rest of the tx.
      planNullPlacements.close();
      tx.exitCacheCode();
    }
  }

  /** Runs the ordinary uncached execution path, dispatching on the parameter shape. */
  private ResultSet executeUncached(@Nonnull SQLStatement statement, @Nullable Object args) {
    if (args instanceof Map) {
      return statement.execute(this, asParamMap(args), true);
    }
    return statement.execute(this, (Object[]) args, true);
  }

  @SuppressWarnings("unchecked")
  private static Map<Object, Object> asParamMap(@Nonnull Object args) {
    return (Map<Object, Object>) args;
  }

  /**
   * A memoised supplier of the cache key for {@code statement} and its {@code args}. The key is built
   * on the first {@link Supplier#get()} and the same instance is returned on every later call.
   * {@link #serveThroughCache} forces it eagerly only when the cache is non-empty, for the {@code
   * isNonCacheable}/{@code lookup} gates; on the empty-cache short-circuit it is left unbuilt and a
   * populate path forces it at its {@code cache.put} (or, on the aggregate/distinct path, at the
   * overflow-guard install that precedes the put), so a query that clears the cache gates but never
   * stores an entry never allocates a {@link CacheKey}. Not synchronised: the cache and this supplier
   * are touched only on the transaction's owning thread.
   */
  private static Supplier<CacheKey> lazyCacheKey(
      @Nonnull SQLStatement statement, @Nullable Object args) {
    return new Supplier<>() {
      private CacheKey built;

      @Override
      public CacheKey get() {
        if (built == null) {
          built = (args instanceof Map)
              ? CacheKey.forParams(statement, asParamMap(args))
              : CacheKey.forArgs(statement, (Object[]) args);
        }
        return built;
      }
    };
  }

  /**
   * Populates a fresh cache entry for a miss: stamps the populate mutation version before driving the
   * executor (so the delta builder later admits only post-populate mutations), runs the real
   * execution, lifts the live stream and plan off the resulting {@link LocalResultSet}, wraps the
   * stream in an {@link IdempotentExecutionStream}, stores the entry, and returns a view over it. The
   * entry is the sole owner of the paused stream and its plan, closing both on evict or tx-end; the
   * view never closes the stream. The wrapper is also threaded back into the {@code LocalResultSet}'s
   * stream slot, but that {@code LocalResultSet} is orphaned here (never returned, never registered in
   * the active-query set), so its close never fires — the wrapper's idempotency is a defensive guard
   * against a future second owner, not a live double-close path (see {@link IdempotentExecutionStream}
   * and {@link CachedEntry#close()}).
   */
  private ResultSet populateAndBuildView(
      @Nonnull SQLStatement statement,
      @Nullable Object args,
      @Nonnull Supplier<CacheKey> key,
      @Nonnull CacheableShape shape,
      @Nonnull FrontendTransactionImpl tx,
      @Nonnull QueryResultCache cache,
      @Nullable Set<String> precomputedEffectiveFromClasses) {
    // Stamp before execution: the LocalResultSet constructor calls plan.start(), so the populate
    // version must be captured first so the delta builder later filters in only post-populate ops.
    var populateMutationVersion = tx.getMutationVersion();

    // Resolve the placement before the execution below builds its plan. The resolver memoizes for
    // this query, so the plan build reads the very pair the entry freezes and the two cannot differ.
    var placements = populatePlacements(statement);

    var original = executeUncached(statement, args);
    if (!(original instanceof LocalResultSet localResult)) {
      // Invariant breach, not a routine fallback. The serveThroughCache gate routes only
      // RECORD / K0_NONE / MATCH_TUPLE_MULTI SELECT/MATCH here, and these shapes classify from
      // execution that yields a
      // LocalResultSet, so reaching this branch means a query we classified cacheable produced an
      // unexpected result type. The fallback stays correct — `original` carries the right rows, and the
      // cache is left untouched (its only mutation, the cache.put below, runs after this gate, so there
      // is no half-built entry and no dangling liveViewCount) — so production rides the WARN and returns
      // the unwrapped result rather than throwing. The paired assert makes it fail fast under -ea (the
      // test suite), where this should-never-happen branch must surface loudly rather than silently.
      //
      // REVISIT when a cacheable shape that legitimately returns a non-LocalResultSet is wired into the
      // serveThroughCache gate (the anticipated aggregate-splice / future-shape work): this branch would
      // then become a normal path and the WARN a noise trap. Gate such a shape out before here, or
      // narrow the check to the shapes that must return a LocalResultSet.
      LogManager.instance().warn(this,
          "tx-result cache: cacheable %s (shape=%s) returned a result of type %s"
              + ", not a LocalResultSet; the cache cannot lift its stream, running uncached. This"
              + " should not happen today (every cacheable shape returns a LocalResultSet); if a shape"
              + " that legitimately returns another type was just wired into the cache gate, revisit"
              + " the gate rather than treating this WARN as an anomaly. Statement: %s",
          statement.getClass().getSimpleName(),
          shape,
          original.getClass().getSimpleName(),
          statement);

      assert original instanceof LocalResultSet
          : "cacheable " + shape + " statement returned " + original.getClass().getName()
              + " instead of a LocalResultSet: " + statement;
      return original;
    }

    var plan = localResult.getInternalExecutionPlan();
    var ctx = plan.getContext();

    // A single-alias MATCH folds onto the RECORD path: the executor stream yields projected
    // RETURN tuples, but the cache must store raw, RID-identifiable records so the RECORD skip-set /
    // sorted-merge stay RID-addressable. Map the projected stream back to raw single-record rows and
    // carry the RETURN projector on the entry; the view re-derives the tuple at the emit boundary.
    //
    // Scope this fold strictly to the RECORD shape. A single-alias MATCH can still classify K0_NONE
    // (statement SKIP/LIMIT, while:/maxDepth:/optional:), and the K0_NONE replay path
    // (CachedResultSetView.computeNextK0None) never applies a returnProjector — it emits stored rows
    // verbatim. Installing the raw-record mapper + projector on a K0_NONE entry would store raw
    // entities and then emit them unprojected, so the view would return raw records instead of the
    // RETURN tuple a fresh execution yields. Gating on shape==RECORD keeps a K0_NONE single-alias MATCH
    // on the normal replay path, where it stores and emits the executor's real RETURN tuples.
    var matchOrigin =
        (shape == CacheableShape.RECORD && statement instanceof SQLMatchStatement match)
            ? singleAliasOrigin(match) : null;
    if (matchOrigin != null) {
      var alias = matchOrigin.getAlias();
      // A single-alias MATCH always names its bound alias; without one the projector/mapper cannot key
      // the record, so this would be a classify/populate divergence rather than a recoverable shape.
      assert alias != null : "single-alias MATCH origin has no alias";
      // Rewrite the projected RETURN stream into raw RID-identifiable record rows before the entry is
      // built. LocalResultSet owns its stream slot; the cache asks it to map rather than reaching in.
      localResult.mapStream(rawAliasRecordMapper(alias));
    }
    // Wrap the backing stream idempotent (LocalResultSet performs the wrap-and-store) and lift the
    // wrapper for the cached entry. That LocalResultSet is orphaned — not returned, not registered in
    // activeQueries — so its own close never fires; the entry is the sole closer today. The wrapper's
    // idempotency is the defensive guard for a future second owner, not a live double-close path.
    var wrapped = localResult.makeStreamIdempotent();

    // Reuse the closure the caller already computed (the multi-alias MATCH miss path), else compute it.
    var entryEffectiveFromClasses = precomputedEffectiveFromClasses != null
        ? precomputedEffectiveFromClasses
        : effectiveFromClasses(statement);
    var entry = new CachedEntry(
        shape,
        entryEffectiveFromClasses,
        whereClauseOf(statement),
        orderByOf(statement),
        wrapped,
        plan,
        ctx,
        populateMutationVersion);
    seedNullPlacement(entry, placements);
    if (matchOrigin != null && statement instanceof SQLMatchStatement match) {
      assert shape == CacheableShape.RECORD : "Pojector installed on a non-RECORD entry";
      entry.setReturnProjector(buildMatchReturnProjector(match, matchOrigin, args));
    }
    // Force the lazy key now (the populate has committed to storing an entry); on the empty-cache
    // short-circuit this is the first and only build of the key.
    cache.put(key.get(), entry);

    return buildView(entry, tx, args);
  }

  /**
   * Populates a fresh cache entry for an aggregate-shape miss and returns a view over it, or falls back
   * to a plain uncached execution (counting a splice failure) on any unexpected plan shape.
   *
   * <p>Unlike the RECORD/K0_NONE populate, the aggregate path cannot reuse {@link
   * #populateAndBuildView}: a collapsed aggregate result carries only the scalar, so the per-RID
   * material a delta replay needs must be seeded by a side-tap that observes every contributing record
   * before the aggregation collapses them. The path therefore builds its own per-execution plan copy
   * ({@code select.createExecutionPlan} returns a private {@code copy(ctx)} or a fresh plan, never the
   * shared cached instance, so rewiring its {@code prev} pointers cannot corrupt other callers), finds
   * the aggregation step, splices an {@link AggregateCacheTapStep} upstream of it, and eager-drives the
   * plan to full drain so the tap observes every contributor. The single resulting scalar is fully
   * computed at populate, so the entry holds no live stream (its stream/plan/ctx are {@code null} on the
   * {@link CachedEntry}).
   *
   * <p>It independently enforces the same three populate contracts as the record-shaped path: stamp the
   * populate mutation version BEFORE driving so the delta builder admits only post-populate ops; release
   * the cache-code guard on every exit (the caller's {@code serveThroughCache} finally always exits the
   * guard, whether this method returns a cache view or an uncached fallback; the returned view re-enters
   * the guard per row during iteration); and close the plan idempotently (the outer finally closes it,
   * the entry holds no stream to close).
   *
   * @param select the parsed single-aggregate SELECT (already shape-gated by the caller)
   */
  private ResultSet populateAndBuildAggregateView(
      @Nonnull SQLSelectStatement select,
      @Nullable Object args,
      @Nonnull Supplier<CacheKey> key,
      @Nonnull CacheableShape shape,
      @Nonnull FrontendTransactionImpl tx,
      @Nonnull QueryResultCache cache) {
    var metadata = ShapeClassifier.aggregateMetadata(select, shape);
    if (metadata == null || metadata.kind() != shape) {
      // The classifier accepted the shape but the metadata derivation found no clean single-aggregate
      // (e.g. a computed-expression argument that classify routed elsewhere): there is nothing to seed,
      // so run uncached and count a splice failure.
      LogManager.instance().warn(this,
          "tx-result cache: AGGREGATE-classified statement yielded no single-aggregate metadata"
              + " (shape=%s); running uncached. Statement: %s",
          shape, select);
      cache.incrementSpliceFailures();
      return executeUncached(select, args);
    }

    // Build a command context mirroring SQLSelectStatement.execute's setup so :param bindings resolve
    // identically when the side-tap drives the plan.
    var ctx = freshContext(args);

    // Stamp BEFORE building/driving the plan so the delta builder later admits only post-populate ops.
    var populateMutationVersion = tx.getMutationVersion();

    // Resolve the placement before the plan build below reads it, for the reason given in
    // populateAndBuildView: one resolution keeps the entry and its plan on one placement.
    var placements = populatePlacements(select);

    var plan = select.createExecutionPlan(ctx, false);
    if (!(plan instanceof SelectExecutionPlan selectPlan)) {
      LogManager.instance().warn(this,
          "tx-result cache: aggregate plan is a %s"
              + ", not a SelectExecutionPlan; cannot splice the cache tap, running uncached."
              + " Statement: %s",
          plan.getClass().getSimpleName(),
          select);
      plan.close();
      cache.incrementSpliceFailures();
      return executeUncached(select, args);
    }

    var aggregateStep = findAggregateStep(selectPlan);
    if (aggregateStep == null) {
      // No AggregateProjectionCalculationStep to splice above — e.g. a bare or single-field-indexed
      // COUNT(*) the planner hardwired to a CountFromClassStep. Fall back uncached and count the splice
      // failure (the global splice-failure rate picks it up through the metric bridge). Log the plan's
      // step types so an unexpected planner shape (a planner-versioning drift) is diagnosable, not just
      // counted.
      var stepTypes = selectPlan.getSteps().stream()
          .map(s -> s.getClass().getSimpleName()).toList();
      LogManager.instance().warn(this,
          "tx-result cache: no AggregateProjectionCalculationStep to splice the cache tap above;"
              + " running uncached. Plan steps: %s. Statement: %s",
          stepTypes,
          select);
      selectPlan.close();
      cache.incrementSpliceFailures();
      return executeUncached(select, args);
    }

    var state = new AggregateState(metadata.kind(), metadata.propertyName(), metadata.alias());
    // The populate has committed to caching (plan shape validated above), so force the lazy key now
    // and reuse the one instance for the overflow guard and the put. On the empty-cache short-circuit
    // this is the first build of the key; earlier splice-failure fallbacks returned before reaching it.
    var resolvedKey = key.get();
    // Install the contributor cap BEFORE driving so an over-cap populate routes the key non-cacheable
    // mid-drive; the put below is then a no-op (the cache's nonCacheableKeys guard closes the entry).
    cache.installAggregateOverflowGuard(resolvedKey, state);
    spliceTap(aggregateStep, new AggregateCacheTapStep(state, ctx));

    try {
      // Eager-drive: pull the plan's single-row stream to exhaustion. The aggregation is blocking, so
      // draining it forces every upstream (tapped) record to be observed before the scalar is emitted.
      var stream = plan.start();
      try {
        while (stream.hasNext(ctx)) {
          stream.next(ctx);
        }
      } finally {
        stream.close(ctx);
      }
    } finally {
      // The entry holds no live stream (the scalar is fully computed), so the plan is closed here and
      // the entry is built with null stream/plan/ctx. Idempotent close mirrors the uncached path.
      plan.close();
    }

    // The scalar is fully in the seeded state; the entry carries no stream/plan/ctx to lazy-pull.
    var entry = new CachedEntry(
        shape,
        effectiveFromClasses(select),
        select.getWhereClause(),
        select.getOrderBy(),
        null,
        null,
        null,
        populateMutationVersion);
    seedNullPlacement(entry, placements);
    entry.setAggregateState(state);
    // put is a no-op if the cap already routed the key non-cacheable during the drive; otherwise it
    // stores the entry. Either way the view below is built directly over this entry's seeded state.
    cache.put(resolvedKey, entry);

    return buildView(entry, tx, args);
  }

  /**
   * Populate path for {@link CacheableShape#DISTINCT_VALUES} (SELECT distinct(prop) / SELECT DISTINCT
   * prop). Reuses the aggregate side-tap + bucket machinery: the plan is
   * {@code Fetch -> [Filter] -> ProjectionCalculationStep -> DistinctExecutionStep}, so the tap splices
   * ABOVE the pre-distinct projection to observe each raw, RID-bearing filtered record before dedup and
   * bucket it by {@code (value -> RID)}. The plan is eager-driven to full population (the deduped output
   * rows are discarded; the view emits the bucket keys). An unexpected plan shape falls back uncached and
   * counts a splice failure, so a dedup-free RECORD-style result is never cached.
   */
  private ResultSet populateAndBuildDistinctValuesView(
      @Nonnull SQLSelectStatement select,
      @Nullable Object args,
      @Nonnull Supplier<CacheKey> key,
      @Nonnull FrontendTransactionImpl tx,
      @Nonnull QueryResultCache cache) {
    var metadata = ShapeClassifier.distinctValueMetadata(select, CacheableShape.DISTINCT_VALUES);
    if (metadata == null || metadata.propertyName() == null) {
      LogManager.instance().warn(this,
          "tx-result cache: DISTINCT_VALUES select yielded no bare-property metadata; running"
              + " uncached. Statement: %s",
          select);
      cache.incrementSpliceFailures();
      return executeUncached(select, args);
    }

    var ctx = freshContext(args);
    // Stamp BEFORE building/driving the plan so the delta builder later admits only post-populate ops.
    var populateMutationVersion = tx.getMutationVersion();

    // Resolve the placement before the plan build below reads it, for the reason given in
    // populateAndBuildView: one resolution keeps the entry and its plan on one placement.
    var placements = populatePlacements(select);

    var plan = select.createExecutionPlan(ctx, false);
    if (!(plan instanceof SelectExecutionPlan selectPlan)) {
      LogManager.instance().warn(this,
          "tx-result cache: distinct plan is a %s, not a SelectExecutionPlan; running uncached. Statement: %s ",
          plan.getClass().getSimpleName(), select);
      plan.close();
      cache.incrementSpliceFailures();
      return executeUncached(select, args);
    }

    var projection = findDistinctSourceProjection(selectPlan);
    if (projection == null) {
      // No DistinctExecutionStep with a pre-distinct ProjectionCalculationStep to tap above (observing
      // raw, RID-bearing records). Fall back uncached and log the plan steps for planner-versioning
      // diagnosis.
      var stepTypes = selectPlan.getSteps().stream()
          .map(s -> s.getClass().getSimpleName()).toList();
      LogManager.instance().warn(this,
          "tx-result cache: no Distinct+Projection step pair to splice the distinct tap above;"
              + " running uncached. Plan steps: %s. Statement: %s",
          stepTypes, select);
      selectPlan.close();
      cache.incrementSpliceFailures();
      return executeUncached(select, args);
    }

    // The bucket machinery is kind AGGREGATE_COUNT_DISTINCT; the entry shape (DISTINCT_VALUES) is what
    // makes the view emit the keys as rows rather than the scalar count.
    var state = new AggregateState(
        CacheableShape.AGGREGATE_COUNT_DISTINCT, metadata.propertyName(), metadata.alias());
    // Committed to caching (plan shape validated above): force the lazy key once and reuse it for the
    // overflow guard and the put. First build of the key on the empty-cache short-circuit.
    var resolvedKey = key.get();
    cache.installAggregateOverflowGuard(resolvedKey, state);
    spliceTapAbove(projection, new AggregateCacheTapStep(state, ctx));

    try {
      var stream = plan.start();
      try {
        while (stream.hasNext(ctx)) {
          stream.next(ctx);
        }
      } finally {
        stream.close(ctx);
      }
    } finally {
      plan.close();
    }

    var entry = new CachedEntry(
        CacheableShape.DISTINCT_VALUES,
        effectiveFromClasses(select),
        select.getWhereClause(),
        select.getOrderBy(),
        null,
        null,
        null,
        populateMutationVersion);
    seedNullPlacement(entry, placements);
    entry.setAggregateState(state);
    cache.put(resolvedKey, entry);

    return buildView(entry, tx, args);
  }

  /**
   * Finds the pre-distinct {@link ProjectionCalculationStep} the distinct tap splices above. The tap
   * observes the raw, RID-bearing filtered records that enter the projection (the projection computes
   * the distinct value and strips identity; {@link AggregateState#observe} requires a non-null RID).
   * Returns the (first plain) projection only when the plan also carries a {@link DistinctExecutionStep},
   * which confirms the distinct shape; a {@code null} return (no DistinctExecutionStep, or no plain
   * projection) drives the splice-failure fallback. The projection need not be adjacent to the
   * DistinctExecutionStep — an ORDER BY inserts an {@code OrderByStep} between them — so the scan finds
   * each independently. {@link AggregateProjectionCalculationStep} is excluded so the tap stays on raw
   * records.
   */
  @Nullable private static ProjectionCalculationStep findDistinctSourceProjection(
      @Nonnull SelectExecutionPlan plan) {
    var hasDistinct = false;
    ProjectionCalculationStep projection = null;
    for (var step : plan.getSteps()) {
      if (step instanceof DistinctExecutionStep) {
        hasDistinct = true;
      } else if (projection == null
          && step instanceof ProjectionCalculationStep candidate
          && !(step instanceof AggregateProjectionCalculationStep)) {
        projection = candidate;
      }
    }
    return hasDistinct ? projection : null;
  }

  /**
   * Splices {@code tap} immediately upstream of {@code below}, so the tap observes the raw records that
   * enter {@code below}. The shared splice primitive behind {@link #spliceTap}'s splice-above-projection
   * arm and the distinct tap.
   */
  private static void spliceTapAbove(
      @Nonnull AbstractExecutionStep below, @Nonnull AggregateCacheTapStep tap) {
    var upstream = below.prev;
    tap.setPrevious(upstream);
    if (upstream != null) {
      upstream.setNext(tap);
    }
    below.setPrevious(tap);
    tap.setNext(below);
  }

  /**
   * Finds the aggregation step the side-tap splices above. Returns the first {@link
   * AggregateProjectionCalculationStep} in the plan's step chain, or {@code null} when the plan has none
   * (the hardwired-COUNT fallback case). The plan is a SELECT plan, so a present aggregation step is the
   * single one the planner emits for a single-aggregate query.
   */
  @Nullable private static AggregateProjectionCalculationStep findAggregateStep(
      @Nonnull SelectExecutionPlan plan) {
    for (var step : plan.getSteps()) {
      if (step instanceof AggregateProjectionCalculationStep aggregateStep) {
        return aggregateStep;
      }
    }
    return null;
  }

  /**
   * Splices {@code tap} into the plan chain immediately upstream of the aggregation step. When a
   * pre-aggregate {@link ProjectionCalculationStep} sits directly above the aggregation (every value
   * aggregate carries one, projecting the per-row field), the tap is spliced ABOVE that projection: the
   * projection strips record identity, and {@link AggregateState#observe} requires a non-null RID, so the
   * tap must see the raw filtered records. The projection still sits between the tap and the aggregation,
   * leaving the collapsed scalar unchanged. Otherwise (e.g. COUNT(*) over a WHERE, no pre-aggregate
   * projection) the tap is spliced directly upstream of the aggregation.
   */
  private static void spliceTap(
      @Nonnull AggregateProjectionCalculationStep aggregateStep,
      @Nonnull AggregateCacheTapStep tap) {
    // AggregateProjectionCalculationStep extends ProjectionCalculationStep, so the instanceof must
    // exclude the aggregation step itself; only a *plain* pre-aggregate projection is the splice-above
    // case. Both step types expose prev through AbstractExecutionStep.
    ExecutionStepInternal spliceBelow = aggregateStep;
    var prev = aggregateStep.prev;
    if (prev instanceof ProjectionCalculationStep
        && !(prev instanceof AggregateProjectionCalculationStep)) {
      spliceBelow = (AbstractExecutionStep) prev;
    }
    var below = (AbstractExecutionStep) spliceBelow;
    var upstream = below.prev;
    tap.setPrevious(upstream);
    if (upstream != null) {
      upstream.setNext(tap);
    }
    below.setPrevious(tap);
    tap.setNext(below);
  }

  /**
   * Builds the consumer-facing view over a cached entry. RECORD entries carry a {@link TxDeltaCursor}
   * reconciling post-populate mutations; K0_NONE entries replay directly (the lookup-time version
   * gate already proved no mutation happened since populate, so there is nothing to merge). The view's
   * command context for the merge is the entry's populate-time context when still live, else a fresh
   * context with the same parameter bindings. Rebuilding from params alone is sound for every shape
   * that reaches the delta path: every shape that builds a delta here (RECORD plain SELECT /
   * single-alias MATCH, the {@code AGGREGATE_*} family, and {@code DISTINCT_VALUES}) re-evaluates WHERE
   * and compares ORDER BY against the rebuilt context, and a statement
   * referencing any per-row context variable ({@code $current}, {@code $parent}, a {@code LET}
   * binding, ...) is excluded upstream — the non-determinism detector bypasses {@code $}-prefixed
   * identifiers, and the shape classifier routes {@code LET} to K0_NONE (which builds no delta). So
   * the WHERE re-eval and ORDER BY {@code compare} that run against the rebuilt context never read any
   * context state beyond the {@code :param} bindings, which the rebuild reproduces exactly. A future
   * shape broadening that lets a context-variable query reach the delta path must revisit this (the
   * populate context's variables would then need to be carried, not just its params).
   */
  private ResultSet buildView(
      @Nonnull CachedEntry entry, @Nonnull FrontendTransactionImpl tx, @Nullable Object args) {
    var ctx = entry.getCtx();
    if (ctx == null) {
      // The entry's stream has been exhausted and its context released, or this is a streamless
      // aggregate / distinct entry that never carried one; rebuild an equivalent context so ORDER BY
      // comparison and WHERE re-evaluation still resolve the same :param bindings. Sound because the
      // wired delta-path shapes carry no non-param context state (see method Javadoc). Memoized on the
      // entry so repeated replays (a hot aggregate re-queried thousands of times) reuse one context
      // instead of allocating a fresh one plus a param-map copy on every replay.
      ctx = entry.getOrCreateReplayCtx(() -> freshContext(args));
    }

    if (entry.getShape() == CacheableShape.DISTINCT_VALUES || entry.getShape().isAggregate()) {
      var delta = DeltaBuilder.buildForAggregate(entry, tx, ctx);
      return CachedResultSetView.forAggregateState(entry, delta, this, tx, entry.getPlan(), ctx);
    }

    TxDeltaCursor delta = entry.getShape() == CacheableShape.RECORD
        ? DeltaBuilder.buildForRecord(entry, tx, ctx)
        : null;
    return new CachedResultSetView(entry, delta, this, tx, entry.getPlan(), ctx);
  }

  /**
   * The null placement a fresh cache entry for {@code statement} must freeze, or {@code null} when
   * the statement ranks nothing and so cannot go stale on a placement change.
   *
   * <p>Call it before the populating execution builds its plan. The session resolver memoizes the
   * pair for the whole query, so the plan build reads the same pair and the entry can never claim a
   * placement its own rows were not ordered by.
   */
  @Nullable private ResolvedOrderByNullsPlacement populatePlacements(
      @Nonnull SQLStatement statement) {
    return rankingOrderBy(statement) == null ? null : planNullPlacements.resolve();
  }

  /**
   * Freezes the null placement of a freshly populated cache entry, before any row of it is compared
   * or served.
   *
   * <p>Every later comparison on one served entry then ranks rows the way its frozen rows are
   * already ranked, and the serve path rejects the entry after a placement change. A statement that
   * ranks nothing passes {@code null} here and the entry never reads a placement at all.
   */
  private static void seedNullPlacement(
      @Nonnull CachedEntry entry, @Nullable ResolvedOrderByNullsPlacement placements) {
    if (placements != null) {
      entry.seedNullsDefault(placements);
    }
  }

  /**
   * The ORDER BY that ranks the rows of a cacheable statement, for every cacheable shape.
   *
   * <p>Distinct from {@link #orderByOf}, which reports only the ORDER BY the entry itself replays
   * through its merge. A multi-alias MATCH ranks its projected tuples but replays them verbatim, so
   * its entry carries no merge ORDER BY while its rows still depend on the placement. Reading the
   * statement clause makes the placement gate cover that shape too.
   */
  @Nullable private static SQLOrderBy rankingOrderBy(@Nonnull SQLStatement statement) {
    if (statement instanceof SQLSelectStatement select) {
      return select.getOrderBy();
    }
    if (statement instanceof SQLMatchStatement match) {
      return match.getOrderBy();
    }
    return null;
  }

  /** A command context carrying the query's parameter bindings, mirroring the executor's setup. */
  private CommandContext freshContext(@Nullable Object args) {
    var ctx = new BasicCommandContext();
    ctx.setDatabaseSession(this);
    Map<Object, Object> params = new HashMap<>();
    if (args instanceof Map) {
      params.putAll(asParamMap(args));
    } else if (args instanceof Object[] positional) {
      for (var i = 0; i < positional.length; i++) {
        params.put(i, positional[i]);
      }
    }
    ctx.setInputParameters(params);
    return ctx;
  }

  /**
   * The subclass closure of the statement's read classes for the entry's delta / version-gate class
   * filter. A plain SELECT resolves its FROM target class; a single-alias MATCH resolves its one
   * bound alias's class so the RECORD delta filter is non-empty and reconciles in-tx mutations; a
   * multi-alias MATCH ({@code MATCH_TUPLE_MULTI}) returns the union of every alias-node and
   * traversal-edge class (each with its subclass closure) for the class-scoped version gate; anything
   * without a resolvable class target (a subquery or RID target) yields the empty set, so the filter
   * matches nothing and the entry behaves as a pure replay.
   */
  private Set<String> effectiveFromClasses(@Nonnull SQLStatement statement) {
    if (statement instanceof SQLSelectStatement select && select.getTarget() != null) {
      var item = select.getTarget().getItem();
      if (item != null) {
        SchemaClass fromClass = item.getSchemaClass(this);
        return CachedEntry.computeEffectiveFromClasses(fromClass);
      }
    }
    if (statement instanceof SQLMatchStatement match) {
      var origin = singleAliasOrigin(match);
      if (origin != null) {
        // Single-alias MATCH (RECORD): the one bound alias's class closure.
        var className = origin.getClassName(null);
        if (className != null) {
          SchemaClass fromClass = getMetadata().getImmutableSchemaSnapshot().getClass(className);
          return CachedEntry.computeEffectiveFromClasses(fromClass);
        }
        return Set.of();
      }
      // Multi-alias MATCH (MATCH_TUPLE_MULTI): the union of every alias node class and every traversal-
      // edge class, each with its subclass closure. The class-scoped version gate invalidates the entry
      // when a post-populate mutation touches any class in this set.
      return matchMultiEffectiveFromClasses(match);
    }
    return Set.of();
  }

  /**
   * The multi-alias MATCH read-class closure for the class-scoped version gate: every alias node class
   * (origin and each traversal target) plus every traversal-edge class named by a {@code .out/.in/.both}
   * step, each expanded to its subclass closure. The shape classifier has already routed any pattern
   * with a non-statically-resolvable class or edge label to {@code K0_NONE}, so every label reaching
   * here is a literal the schema snapshot can resolve; an unresolvable name still resolves to {@code
   * null} and is dropped (it names no live records). Edge classes fold in so an edge create or delete
   * trips the gate rather than slipping past the alias-class filter.
   */
  private Set<String> matchMultiEffectiveFromClasses(@Nonnull SQLMatchStatement match) {
    var aliasClasses = new ArrayList<SchemaClass>();
    var edgeClasses = new ArrayList<SchemaClass>();
    for (var expr : match.getMatchExpressions()) {
      addMatchNodeClass(expr.getOrigin(), aliasClasses);
      for (var item : expr.getItems()) {
        addMatchNodeClass(item.getFilter(), aliasClasses);
        addMatchEdgeClasses(item.getMethod(), edgeClasses);
      }
    }
    return CachedEntry.computeMatchEffectiveFromClasses(aliasClasses, edgeClasses);
  }

  /** Resolves a pattern node's declared {@code class:} (context-free) and adds it to {@code out}. */
  private void addMatchNodeClass(@Nullable SQLMatchFilter node, @Nonnull List<SchemaClass> out) {
    if (node == null) {
      return;
    }
    var className = node.getClassName(null);
    if (className != null) {
      var resolved = getMetadata().getImmutableSchemaSnapshot().getClass(className);
      if (resolved != null) {
        out.add(resolved);
      }
    }
  }

  /**
   * Resolves the edge class(es) a traversal step names and adds them to {@code out}. The edge labels are
   * the step method's parameters (the parser folds a bare {@code out()} to the literal {@code "E"}, so a
   * traversal step always carries at least one label parameter); a multi-label step contributes each.
   * A string-literal label is rendered with surrounding quotes, so they are stripped before resolution.
   *
   * <p>This reads the label from {@code param.toString()}, the same rendered form {@link
   * ShapeClassifier} tests with its {@code STATIC_LABEL} pattern. The two are coupled by that rendering:
   * the classifier runs first and routes any non-statically-resolvable label (parameterized, computed)
   * to {@code K0_NONE}, so every label reaching this extraction is a bare identifier or a single
   * string literal. A non-resolvable name still resolves to {@code null} here and is dropped.
   */
  private void addMatchEdgeClasses(@Nullable SQLMethodCall method, @Nonnull List<SchemaClass> out) {
    if (method == null) {
      return;
    }
    var params = method.getParams();
    if (params == null) {
      return;
    }
    for (var param : params) {
      var label = stripLabelQuotes(param.toString());
      var resolved = getMetadata().getImmutableSchemaSnapshot().getClass(label);
      if (resolved != null) {
        out.add(resolved);
      }
    }
  }

  /**
   * Strips a single pair of surrounding single or double quotes from a rendered edge label. Edge and
   * class names are bare identifiers, so the stripped content needs no further unescaping; a label that
   * would require it is not a resolvable class name and resolves to {@code null} at the call site.
   */
  private static String stripLabelQuotes(@Nonnull String rendered) {
    if (rendered.length() >= 2) {
      var first = rendered.charAt(0);
      if ((first == '\'' || first == '"') && rendered.charAt(rendered.length() - 1) == first) {
        return rendered.substring(1, rendered.length() - 1);
      }
    }
    return rendered;
  }

  @Nullable private SQLWhereClause whereClauseOf(@Nonnull SQLStatement statement) {
    if (statement instanceof SQLSelectStatement select) {
      return select.getWhereClause();
    }
    if (statement instanceof SQLMatchStatement match) {
      var origin = singleAliasOrigin(match);
      // The single bound alias's pattern WHERE re-evaluates against a mutated record at delta-build,
      // exactly like a plain SELECT's WHERE; its properties are unqualified relative to the alias, so
      // it reads the raw record directly. A null (no where:) matches every record.
      return origin == null ? null : origin.getFilter();
    }
    return null;
  }

  @Nullable private SQLOrderBy orderByOf(@Nonnull SQLStatement statement) {
    if (statement instanceof SQLSelectStatement select) {
      return select.getOrderBy();
    }
    if (statement instanceof SQLMatchStatement match) {
      // The statement ORDER BY ranks the projected RETURN tuples (e.g. ORDER BY u.name). The view and
      // delta builder compare projected merge heads, so the entry carries it verbatim; the classifier
      // already proved every ORDER BY item is record-local for the single-alias fold.
      return singleAliasOrigin(match) == null ? null : match.getOrderBy();
    }
    return null;
  }

  /**
   * The origin vertex node of a single-alias MATCH (one match expression, no traversal edge), or
   * {@code null} when the statement is multi-alias or not a single-binding shape. Mirrors the
   * single-alias test the shape classifier uses, evaluated here with the session available so the
   * three populate helpers above resolve the alias's class / WHERE / ORDER BY consistently.
   */
  @Nullable private static SQLMatchFilter singleAliasOrigin(@Nonnull SQLMatchStatement match) {
    var expressions = match.getMatchExpressions();
    if (expressions.size() != 1) {
      return null;
    }
    SQLMatchExpression expr = expressions.getFirst();
    var origin = expr.getOrigin();
    if (origin == null || !expr.getItems().isEmpty()) {
      return null;
    }
    return origin;
  }

  /**
   * Builds a RETURN projector for a single-alias MATCH: a transform that takes a raw cached
   * or inject record row (an identifiable {@link Result} wrapping the single bound record), wraps it
   * back into the {@code {alias -> record}} shape the MATCH executor's first step produces, and runs
   * the RETURN projection over it so the emitted row is the RETURN tuple a fresh MATCH would yield
   * (e.g. {@code RETURN u, u.name}). The projection is rebuilt from the statement's RETURN items the
   * same way the MATCH planner builds it, then applied per row. The command context carries the
   * query's parameter bindings so a parameterized RETURN/projection resolves identically.
   */
  private Function<Result, Result> buildMatchReturnProjector(
      @Nonnull SQLMatchStatement match, @Nonnull SQLMatchFilter origin, @Nullable Object args) {
    var alias = origin.getAlias();
    var returnItems = match.getReturnItems();
    var returnAliases = match.getReturnAliases();
    var returnNested = match.getReturnNestedProjections();
    var projectionItems = new ArrayList<SQLProjectionItem>(returnItems.size());
    for (var i = 0; i < returnItems.size(); i++) {
      SQLIdentifier itemAlias = i < returnAliases.size() ? returnAliases.get(i) : null;
      SQLNestedProjection nested = i < returnNested.size() ? returnNested.get(i) : null;
      projectionItems.add(new SQLProjectionItem(returnItems.get(i), itemAlias, nested));
    }
    var projection = new SQLProjection(projectionItems, false);
    var projectorCtx = freshContext(args);
    return rawRow -> {
      // The raw row's own record is the single bound record (the cache stores raw ResultInternal rows
      // wrapping it, and the delta builder injects new ResultInternal(session, record) rows the same
      // way). Re-wrap it into the alias-keyed { alias -> record } row the MATCH executor's first step
      // emits, then run the RETURN projection over it.
      var entity = rawRow.asEntityOrNull();
      // A well-formed single-alias MATCH binds exactly one class-constrained record per row, so the raw
      // cached / injected row always carries that record. A null here means the binding vanished
      // between stream production and projection (a deleted/unreadable record the RID skip-set should
      // have suppressed) — an edge the design treats as not-reached. Fail loudly rather than emit a
      // non-identifiable empty tuple (new ResultInternal(session, null) leaves identifiable null), which
      // would be a silent wrong row; a future shape-broadening that lets a null binding through must
      // surface here rather than corrupt the result.
      if (entity == null) {
        throw new IllegalStateException(
            "single-alias MATCH RETURN projector found no bound record for alias " + alias);
      }
      var aliasRow = new ResultInternal(this);
      aliasRow.setProperty(alias, entity);
      return projection.calculateSingle(projectorCtx, aliasRow, false);
    };
  }

  /**
   * Maps a populated single-alias MATCH stream row (the executor's projected RETURN tuple, which
   * still carries the bound record under the alias key) back to a raw, RID-identifiable single-record
   * {@link Result}. Storing the raw record — not the projected tuple — keeps the RECORD skip-set /
   * sorted-merge RID-addressable; the {@code returnProjector} re-derives the tuple at the view emit
   * boundary. The stream is already ordered by the statement ORDER BY (the executor sorted the
   * projected tuples), so the raw-record cache stream stays in the same projected order.
   */
  @Nonnull
  private ResultMapper rawAliasRecordMapper(@Nonnull String alias) {
    return (projectedRow, ctx) -> {
      var entity = projectedRow.getEntity(alias);
      // A single-alias MATCH binds exactly one record per row; the alias key always resolves to it. A
      // null binding would make new ResultInternal(session, null) a non-identifiable empty row, which
      // the RID skip-set cannot address and which emits as a silent wrong tuple. Fail loudly instead so
      // a future shape-broadening that lets a null binding through surfaces here rather than corrupting
      // the cached result.
      if (entity == null) {
        throw new IllegalStateException(
            "single-alias MATCH row carries no record under alias " + alias);
      }
      return new ResultInternal(this, entity);
    };
  }

  /**
   * Bulk-DML cache invalidation hook. A {@code TRUNCATE CLASS} run mid-transaction removes stored
   * records without flowing through {@code addRecordOperation}, so the cache cannot see the change via
   * the delta build and must drop every entry. Called per statement from both the single-statement
   * command path ({@code executeInternal}) and the script path ({@code SqlScriptExecutor}), so a
   * TRUNCATE embedded in a script invalidates a sibling {@code query()}'s cached entry the same way the
   * direct command does. Regular INSERT/UPDATE/DELETE need no hook here: they land in {@code
   * recordOperations} and the next query's delta build picks them up.
   * Schema DDL (CREATE/DROP/ALTER CLASS|PROPERTY|INDEX) is unreachable mid-transaction (the schema is
   * immutable for the life of a transaction), guarded by a {@code Java assert} canary so a future
   * relaxation that lets schema DDL run mid-tx surfaces loudly in test builds rather than silently
   * serving a stale cache.
   */
  public void invalidateCacheForBulkDml(@Nonnull SQLStatement statement) {
    // executeInternal does not begin a transaction, so currentTx may be a no-tx placeholder
    // (FrontendTransactionNoTx) with no cache to invalidate; only a real transaction carries one.
    if (!(currentTx instanceof FrontendTransactionImpl tx)) {
      return;
    }
    var cache = tx.getQueryResultCache();
    if (cache == null) {
      return;
    }
    if (statement instanceof SQLTruncateClassStatement) {
      cache.invalidateAll();
      return;
    }
    // Schema-immutability canary: schema DDL must not reach the cache hook while a transaction is
    // active. TRUNCATE CLASS (handled above) is the only legitimately mid-tx-runnable DDL; every
    // other schema-DDL statement throws before any cache effect would matter, so reaching here is a
    // contract breach.
    assert !isSchemaDdl(statement)
        : "Schema DDL reached the tx-result cache hook while a transaction was active: "
            + statement.getClass().getSimpleName();
  }

  /** Whether the statement is a schema-DDL statement (CREATE/DROP/ALTER CLASS|PROPERTY|INDEX). */
  private static boolean isSchemaDdl(@Nonnull SQLStatement statement) {
    return statement instanceof SQLCreateClassStatement
        || statement instanceof SQLDropClassStatement
        || statement instanceof SQLAlterClassStatement
        || statement instanceof SQLCreatePropertyStatement
        || statement instanceof SQLDropPropertyStatement
        || statement instanceof SQLAlterPropertyStatement
        || statement instanceof SQLCreateIndexStatement
        || statement instanceof SQLDropIndexStatement;
  }

  public ResultSet execute(String query, Object... args) {
    return executeInternal(query, null, args);
  }

  public ResultSet execute(String query, @SuppressWarnings("rawtypes") Map args) {
    return executeInternal(query, null, args);
  }

  public ResultSet execute(SQLStatement statement, Map<?, ?> args) {
    return executeInternal(null, statement, args);
  }

  @SuppressWarnings("unchecked")
  private ResultSet executeInternal(
      String stringStatement,
      SQLStatement parsedStatement,
      Object args) {

    assert assertIfNotActive();

    checkOpenness();

    if (currentTx.isCallBackProcessingInProgress()) {
      throw new CommandExecutionException(getDatabaseName(),
          "Cannot execute SQL command while transaction processing callbacks. If you called this method in beforeCallbackXXX method "
              +
              "please move it to the afterCallbackXXX method");
    }
    try {
      currentTx.preProcessRecordsAndExecuteCallCallbacks();
      getSharedContext().getYouTrackDB().startCommand(null);
      try {
        var statement =
            (parsedStatement != null) ? parsedStatement : SQLEngine.parse(stringStatement, this);
        invalidateCacheForBulkDml(statement);
        ResultSet original;
        switch (args) {
          case Map<?, ?> map -> {
            original = statement.execute(this, (Map<Object, Object>) map, true);
          }
          case Object[] objects -> original = statement.execute(this, objects, true);
          case null -> original = statement.execute(this, (Object[]) null, true);
          default -> original = statement.execute(this, new Object[] {args}, true);
        }

        if (!statement.isIdempotent()) {
          // fetch all, close and detach
          var prefetched = new InternalResultSet(this);
          original.forEachRemaining(prefetched::add);
          original.close();
          var result = new LocalResultSetLifecycleDecorator(prefetched);
          return result;
        } else {
          // stream, keep open and attach to the current DB
          return queryStartedLifecycle(original);
        }
      } finally {
        getSharedContext().getYouTrackDB().endCommand();
      }
    } catch (Exception e) {
      rollback();
      throw e;
    }
  }

  public ResultSet computeScript(String language, String script, Object... args) {
    assert assertIfNotActive();

    checkOpenness();

    if (currentTx.isCallBackProcessingInProgress()) {
      throw new CommandExecutionException(getDatabaseName(),
          "Cannot execute SQL script while transaction processing callbacks. If you called this method in beforeCallbackXXX method "
              +
              "please move it to the afterCallbackXXX method");
    }
    try {
      if (!"sql".equalsIgnoreCase(language)) {
        checkSecurity(ResourceGeneric.COMMAND, Role.PERMISSION_EXECUTE, language);
      }
      currentTx.preProcessRecordsAndExecuteCallCallbacks();
      getSharedContext().getYouTrackDB().startCommand(null);
      try {
        var executor =
            getSharedContext()
                .getYouTrackDB()
                .getScriptManager()
                .getCommandManager()
                .getScriptExecutor(language);

        this.storage.pauseConfigurationUpdateNotifications();
        ResultSet original;
        try {
          original = executor.execute(this, script, args);
        } finally {
          this.storage.fireConfigurationUpdateNotifications();
        }
        return queryStartedLifecycle(original);
      } finally {
        getSharedContext().getYouTrackDB().endCommand();
      }
    } catch (Exception e) {
      rollback();
      throw e;
    }
  }

  /**
   * Registers the result set for lifecycle tracking. If the result set is already a
   * {@link LocalResultSet}, adds lifecycle tracking directly to avoid the decorator
   * overhead. Otherwise, wraps in a {@link LocalResultSetLifecycleDecorator}.
   */
  private ResultSet queryStartedLifecycle(ResultSet original) {
    if (original instanceof LocalResultSet localResult) {
      this.queryStarted(localResult.getQueryId(), localResult);
      localResult.addLifecycleListener(this);
      return localResult;
    } else {
      var result = new LocalResultSetLifecycleDecorator(original);
      this.queryStarted(result.getQueryId(), result);
      result.addLifecycleListener(this);
      return result;
    }
  }

  public ResultSet computeScript(String language, String script, Map<String, ?> args) {
    assert assertIfNotActive();

    checkOpenness();

    if (currentTx.isCallBackProcessingInProgress()) {
      throw new CommandExecutionException(getDatabaseName(),
          "Cannot execute SQL script while transaction processing callbacks. If you called this method in beforeCallbackXXX method "
              +
              "please move it to the afterCallbackXXX method");
    }
    if (!"sql".equalsIgnoreCase(language)) {
      checkSecurity(ResourceGeneric.COMMAND, Role.PERMISSION_EXECUTE, language);
    }

    try {
      currentTx.preProcessRecordsAndExecuteCallCallbacks();
      getSharedContext().getYouTrackDB().startCommand(null);
      try {
        var executor =
            sharedContext
                .getYouTrackDB()
                .getScriptManager()
                .getCommandManager()
                .getScriptExecutor(language);
        ResultSet original;

        this.storage.pauseConfigurationUpdateNotifications();
        try {
          original = executor.execute(this, script, args);
        } finally {
          this.storage.fireConfigurationUpdateNotifications();
        }

        return queryStartedLifecycle(original);
      } finally {
        getSharedContext().getYouTrackDB().endCommand();
      }
    } catch (Exception e) {
      rollback();
      throw e;
    }

  }

  public ResultSet query(ExecutionPlan plan, Map<Object, Object> params) {
    assert assertIfNotActive();

    checkOpenness();

    if (currentTx.isCallBackProcessingInProgress()) {
      throw new CommandExecutionException(getDatabaseName(),
          "Cannot execute query transaction processing callbacks. If you called this method in beforeCallbackXXX method "
              +
              "please move it to the afterCallbackXXX method");
    }

    try {
      currentTx.preProcessRecordsAndExecuteCallCallbacks();
      getSharedContext().getYouTrackDB().startCommand(null);
      try {
        var ctx = new BasicCommandContext();
        ctx.setDatabaseSession(this);
        ctx.setInputParameters(params);

        var result = new LocalResultSet(this, (InternalExecutionPlan) plan);
        this.queryStarted(result.getQueryId(), result);
        result.addLifecycleListener(this);
        return result;
      } finally {
        getSharedContext().getYouTrackDB().endCommand();
      }
    } catch (Exception e) {
      rollback();
      throw e;
    }
  }

  public YouTrackDBConfig getConfig() {
    assert assertIfNotActive();
    return config;
  }

  public void addBlobCollection(final String iCollectionName, final Object... iParameters) {
    assert assertIfNotActive();

    checkOpenness();

    int id;
    if (!existsCollection(iCollectionName)) {
      id = addCollection(iCollectionName, iParameters);
    } else {
      id = getCollectionIdByName(iCollectionName);
    }
    getMetadata().getSchema().addBlobCollection(id);
  }

  public Blob newBlob(byte[] bytes) {
    assert assertIfNotActive();

    checkOpenness();

    var blob = new RecordBytes(new ChangeableRecordId(), this, bytes);
    blob.setInternalStatus(RecordElement.STATUS.LOADED);

    var tx = (FrontendTransactionImpl) currentTx;
    tx.addRecordOperation(blob, RecordOperation.CREATED);

    return blob;
  }

  public Blob newBlob() {
    assert assertIfNotActive();

    checkOpenness();

    var blob = new RecordBytes(this, new ChangeableRecordId());
    blob.setInternalStatus(RecordElement.STATUS.LOADED);

    var tx = (FrontendTransactionImpl) currentTx;
    tx.addRecordOperation(blob, RecordOperation.CREATED);

    return blob;
  }

  /**
   * Creates a entity with specific class.
   *
   * @param className the name of class that should be used as a class of created entity.
   * @return new instance of entity.
   */
  public EntityImpl newInstance(final String className) {
    assert assertIfNotActive();

    checkOpenness();

    var cls = getMetadata().getImmutableSchemaSnapshot().getClass(className);
    if (cls == null) {
      throw new IllegalArgumentException("Class " + className + " not found");
    }

    if (cls.isVertexType()) {
      throw new IllegalArgumentException(
          "The class " + cls.getName()
              + " is a vertex type and cannot be used to create an entity, "
              + "please use newVertex() method");
    }
    if (cls.isEdgeType()) {
      throw new IllegalArgumentException(
          "The class " + cls.getName() + " is an edge type and cannot be used to create an entity, "
              + "please use newEdge() method");
    }
    var entity = new EntityImpl(new ChangeableRecordId(), this, className);
    entity.setInternalStatus(RecordElement.STATUS.LOADED);

    currentTx.addRecordOperation(entity, RecordOperation.CREATED);
    //init default property values, can not do that in constructor as it is not registerd in tx
    entity.convertPropertiesToClassAndInitDefaultValues(entity.getImmutableSchemaClass(this));
    return entity;
  }

  public EntityImpl newInternalInstance() {
    assert assertIfNotActive();

    checkOpenness();

    var collectionId = getCollectionIdByName(MetadataDefault.COLLECTION_INTERNAL_NAME);
    var rid = new ChangeableRecordId();

    rid.setCollectionId(collectionId);

    var entity = new EntityImpl(this, rid);
    entity.setInternalStatus(RecordElement.STATUS.LOADED);

    var tx = (FrontendTransactionImpl) currentTx;
    tx.addRecordOperation(entity, RecordOperation.CREATED);

    return entity;
  }

  public EdgeEntityImpl newEdgeInternal(final String className) {
    var cls = getMetadata().getImmutableSchemaSnapshot().getClass(className);
    if (cls == null) {
      throw new IllegalArgumentException("Class " + className + " not found");
    }
    if (!cls.isEdgeType()) {
      throw new IllegalArgumentException(
          "The class " + cls.getName()
              + " is not an edge type and cannot be used to create an edge, "
              + "please use newInstance() method");
    }

    checkSecurity(ResourceGeneric.CLASS, Role.PERMISSION_CREATE, className);
    var edge = new EdgeEntityImpl(new ChangeableRecordId(), this, className);
    currentTx.addRecordOperation(edge, RecordOperation.CREATED);

    edge.convertPropertiesToClassAndInitDefaultValues(edge.getImmutableSchemaClass(this));

    return edge;
  }

  private EdgeInternal addEdgeInternal(
      final Vertex toVertex,
      final Vertex inVertex,
      String className) {
    Objects.requireNonNull(toVertex, "From vertex is null");
    Objects.requireNonNull(inVertex, "To vertex is null");

    EdgeInternal edge;
    EntityImpl outEntity;
    EntityImpl inEntity;

    if (getTransactionInternal().isDeletedInTx(toVertex.getIdentity())) {
      throw new RecordNotFoundException(getDatabaseName(),
          toVertex.getIdentity(), "The vertex " + toVertex.getIdentity() + " has been deleted");
    }

    if (getTransactionInternal().isDeletedInTx(inVertex.getIdentity())) {
      throw new RecordNotFoundException(getDatabaseName(),
          inVertex.getIdentity(), "The vertex " + inVertex.getIdentity() + " has been deleted");
    }

    outEntity = (EntityImpl) toVertex;
    inEntity = (EntityImpl) inVertex;

    Schema schema = getMetadata().getImmutableSchemaSnapshot();
    final var edgeType = schema.getClass(className);

    if (edgeType == null) {
      throw new IllegalArgumentException("Class " + className + " does not exist");
    }

    className = edgeType.getName();

    final var outFieldName = Vertex.getEdgeLinkFieldName(Direction.OUT, className);
    final var inFieldName = Vertex.getEdgeLinkFieldName(Direction.IN, className);

    // All edges are record-based: primary RID is the edge record,
    // secondary RID is the opposite vertex
    var edgeEntity = newEdgeInternal(className);
    var tx = getActiveTransaction();
    edgeEntity.setPropertyInternal(EdgeInternal.DIRECTION_OUT, tx.load(toVertex));
    edgeEntity.setPropertyInternal(Edge.DIRECTION_IN, inEntity);

    // OUT-VERTEX ---> edge record as primary, IN-VERTEX as secondary
    VertexEntityImpl.createLink(this, outEntity, edgeEntity, inEntity, outFieldName);

    // IN-VERTEX ---> edge record as primary, OUT-VERTEX as secondary
    VertexEntityImpl.createLink(this, inEntity, edgeEntity, outEntity, inFieldName);
    edge = edgeEntity;
    // OK

    return edge;
  }

  public Edge newEdge(Vertex from, Vertex to, String type) {
    assert assertIfNotActive();

    checkOpenness();

    var cl = getMetadata().getImmutableSchemaSnapshot().getClass(type);
    if (cl == null || !cl.isEdgeType()) {
      throw new IllegalArgumentException(type + " is not an edge class");
    }
    if (cl.isAbstract()) {
      throw new IllegalArgumentException(
          type + " is an abstract class and cannot be used for edge creation");
    }

    return (Edge) addEdgeInternal(from, to, type);
  }

  @Nullable public RecordAbstract executeReadRecord(
      @Nonnull RecordIdInternal rid,
      @Nullable RawBuffer prefetchedBuffer,
      boolean throwExceptionIfRecordNotFound) {
    checkOpenness();

    getMetadata().makeThreadLocalSchemaSnapshot();
    try {
      checkSecurity(
          ResourceGeneric.COLLECTION,
          Role.PERMISSION_READ,
          getCollectionNameById(rid.getCollectionId()));
      // SEARCH IN LOCAL TX
      var txInternal = getTransactionInternal();
      if (txInternal.isDeletedInTx(rid)) {
        // DELETED IN TX
        return createRecordNotFoundResult(rid, throwExceptionIfRecordNotFound);
      }

      var record = getTransactionInternal().getRecord(rid);
      var localCacheHit = false;
      if (record == null) {
        record = localCache.findRecord(rid);
        localCacheHit = record != null;
      }
      if (record != null && record.isUnloaded()) {
        throw new IllegalStateException(
            "Unloaded record with rid " + rid + " was found in local cache");
      }
      if (localCacheHit && freshCommittedReadOperation != null) {
        // Inside a fresh-committed-read scope a cached instance may be stale: it was read at the
        // transaction's begin-time snapshot, which can predate the committed state the scope must
        // observe. The session's one-instance-per-rid invariant (assertIfAlreadyLoaded) forbids
        // materializing a second instance for the same rid, so the cached instance is refreshed in
        // place from a fresh committed read. A tx-enrolled record (the branch above) is the
        // transaction's own working copy and is never refreshed.
        if (!refreshRecordFromFreshCommittedRead(record)) {
          // The record no longer exists in the latest committed state: evict the stale instance
          // and report not-found, exactly as a fresh-scope storage-read miss below would. The
          // eviction removes the cache entry without unloading the instance, so a caller still
          // holding the stale reference keeps a usable (if outdated) record while a later load
          // materializes a fresh instance — a deliberate, bounded relaxation of the
          // one-instance-per-rid invariant for a record the committed state no longer contains.
          localCache.deleteRecord(rid);
          return createRecordNotFoundResult(rid, throwExceptionIfRecordNotFound);
        }
      }

      if (record != null) {
        if (beforeReadOperations(record)) {
          return createRecordNotFoundResult(rid, throwExceptionIfRecordNotFound);
        }

        afterReadOperations(record);
        if (record instanceof EntityImpl entity) {
          entity.checkClass(this);
        }

        localCache.updateRecord(record, this);

        assert !record.isUnloaded();
        assert record.getSession() == this;

        return record;
      }

      loadedRecordsCount++;

      if (!rid.isValidPosition()) {
        throw new DatabaseException(getDatabaseName(), "Invalid record id " + rid);
      }

      final StorageReadResult readResult;
      if (prefetchedBuffer != null) {
        readResult = prefetchedBuffer;
      } else {
        try {
          readResult = storage.readRecord(rid, getEffectiveReadAtomicOperation());
        } catch (RecordNotFoundException e) {
          if (throwExceptionIfRecordNotFound) {
            throw e;
          } else {
            return null;
          }
        }
      }

      if (readResult instanceof RawBuffer recordBuffer) {
        record =
            YouTrackDBEnginesManager.instance()
                .getRecordFactoryManager()
                .newInstance(recordBuffer.recordType(), rid, this);
        final var rec = record;
        rec.unsetDirty();

        if (record.getRecordType() != recordBuffer.recordType()) {
          throw new DatabaseException(getDatabaseName(),
              "Record type is different from the one in the database");
        }

        record.recordSerializer = serializer;
        record.fill(recordBuffer.version(), recordBuffer.buffer(), false);

        if (record instanceof EntityImpl entity) {
          entity.checkClass(this);
        }

        localCache.updateRecord(record, this);
        record.fromStream(recordBuffer.buffer());
      } else if (readResult instanceof RawPageBuffer pageBuffer) {
        // Zero-copy PageFrame path: store PageFrame reference for lazy
        // deserialization at property-access time.
        record =
            YouTrackDBEnginesManager.instance()
                .getRecordFactoryManager()
                .newInstance(pageBuffer.recordType(), rid, this);
        record.unsetDirty();

        if (record.getRecordType() != pageBuffer.recordType()) {
          throw new DatabaseException(getDatabaseName(),
              "Record type is different from the one in the database");
        }

        record.recordSerializer = serializer;

        if (record instanceof EntityImpl entity) {
          entity.fillFromPage(
              pageBuffer.recordVersion(), pageBuffer.recordType(),
              pageBuffer.pageFrame(), pageBuffer.stamp(),
              pageBuffer.contentOffset(), pageBuffer.contentLength());
          entity.checkClass(this);
        } else {
          // Non-EntityImpl (e.g., Blob): extract bytes from PageFrame.
          // toRawBuffer() validates the stamp after byte extraction and throws
          // OptimisticReadFailedException if the page was modified. Retry until
          // we get a consistent byte[] copy.
          var currentResult = (StorageReadResult) pageBuffer;
          while (true) {
            try {
              var rawBuffer = currentResult.toRawBuffer();
              record.fill(rawBuffer.version(), rawBuffer.buffer(), false);
              record.fromStream(rawBuffer.buffer());
              break;
            } catch (OptimisticReadFailedException e) {
              currentResult = storage.readRecord(rid, getEffectiveReadAtomicOperation());
            }
          }
        }
        localCache.updateRecord(record, this);
      } else {
        throw new IllegalStateException(
            "Unknown StorageReadResult type: " + readResult.getClass());
      }

      if (beforeReadOperations(record)) {
        return createRecordNotFoundResult(rid, throwExceptionIfRecordNotFound);
      }

      afterReadOperations(record);

      assert !record.isUnloaded();
      assert record.getSession() == this;

      return record;
    } catch (RecordNotFoundException t) {
      throw t;
    } catch (Exception t) {
      if (rid.isTemporary()) {
        throw BaseException.wrapException(
            new DatabaseException(getDatabaseName(),
                "Error on retrieving record using temporary RID: " + rid),
            t, getDatabaseName());
      } else {
        throw BaseException.wrapException(
            new DatabaseException(getDatabaseName(),
                "Error on retrieving record "
                    + rid
                    + " (collection: "
                    + storage.getPhysicalCollectionNameById(rid.getCollectionId())
                    + ")"),
            t, getDatabaseName());
      }
    } finally {
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  @Nullable private RecordAbstract createRecordNotFoundResult(RecordIdInternal rid,
      boolean throwExceptionIfRecordNotFound) {
    if (throwExceptionIfRecordNotFound) {
      throw new RecordNotFoundException(getDatabaseName(), rid);
    } else {
      return null;
    }
  }

  @SuppressWarnings("unused")
  public Edge newRegularEdge(String iClassName, Vertex from, Vertex to) {
    assert assertIfNotActive();

    checkOpenness();

    var cl = getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);

    if (cl == null || !cl.isEdgeType()) {
      throw new IllegalArgumentException(iClassName + " is not an edge class");
    }

    return addEdgeInternal(from, to, iClassName);
  }

  public EmbeddedEntity newEmbeddedEntity(SchemaClass schemaClass) {
    assert assertIfNotActive();

    checkOpenness();

    return new EmbeddedEntityImpl(schemaClass != null ? schemaClass.getName() : null, this);
  }

  public Vertex newVertex(final String className) {
    assert assertIfNotActive();

    checkOpenness();

    var cls = getMetadata().getImmutableSchemaSnapshot().getClass(className);
    if (cls == null) {
      throw new IllegalArgumentException("Class " + className + " not found");
    }
    if (!cls.isVertexType()) {
      throw new IllegalArgumentException(
          "The class " + cls.getName()
              + " is not a vertex type and cannot be used to create a vertex, "
              + "please use newInstance() method");
    }

    checkSecurity(ResourceGeneric.CLASS, Role.PERMISSION_CREATE, className);
    var vertex = new VertexEntityImpl(new ChangeableRecordId(), this, className);
    currentTx.addRecordOperation(vertex, RecordOperation.CREATED);
    vertex.convertPropertiesToClassAndInitDefaultValues(vertex.getImmutableSchemaClass(this));

    return vertex;
  }

  public EmbeddedEntity newEmbeddedEntity(String schemaClass) {
    assert assertIfNotActive();

    checkOpenness();

    return new EmbeddedEntityImpl(schemaClass, this);
  }

  public EmbeddedEntity newEmbeddedEntity() {
    assert assertIfNotActive();

    checkOpenness();

    return new EmbeddedEntityImpl(this);
  }

  private FrontendTransactionImpl newTxInstance(long txId) {
    assert assertIfNotActive();

    return new FrontendTransactionImpl(this, txId, false);
  }

  private FrontendTransactionImpl newReadOnlyTxInstance(long txId) {
    assert assertIfNotActive();

    return new FrontendTransactionImpl(this, txId, true);
  }

  public void setNoTxMode() {
    if (!(currentTx instanceof FrontendTransactionNoTx)) {
      currentTx = new FrontendTransactionNoTx(this);
    }
  }

  public FrontendTransactionImpl begin() {
    assert assertIfNotActive();

    checkOpenness();

    if (currentTx.isActive()) {
      currentTx.beginInternal();
      return (FrontendTransactionImpl) currentTx;
    }

    begin(newTxInstance(FrontendTransactionImpl.generateTxId()));
    return (FrontendTransactionImpl) currentTx;
  }

  public void beginReadOnly() {
    assert assertIfNotActive();

    checkOpenness();

    if (currentTx.isActive()) {
      return;
    }

    begin(newReadOnlyTxInstance(FrontendTransactionImpl.generateTxId()));
  }

  private void init() {
    assert assertIfNotActive();
    currentTx = new FrontendTransactionNoTx(this);
  }

  /**
   * Deletes a entity. Behavior depends by the current running transaction if any. If no transaction
   * is running then the record is deleted immediately. If an Optimistic transaction is running then
   * the record will be deleted at commit time. The current transaction will continue to see the
   * record as deleted, while others not. If a Pessimistic transaction is running, then an exclusive
   * lock is acquired against the record. Current transaction will continue to see the record as
   * deleted, while others cannot access to it since it's locked.
   *
   * <p>If MVCC is enabled and the version of the entity is different by the version stored in
   * the database, then a {@link ConcurrentModificationException} exception is thrown.
   *
   * @param record record to delete
   */
  public void delete(@Nonnull DBRecord record) {
    assert assertIfNotActive();

    checkOpenness();

    record.delete();
  }

  public void beforeCreateOperations(final RecordAbstract recordAbstract, String collectionName) {
    assert assertIfNotActive();

    checkSecurity(Role.PERMISSION_CREATE, recordAbstract, collectionName);

    if (recordAbstract instanceof EntityImpl entity) {
      if (!getSharedContext().getSecurity().canCreate(this, entity)) {
        throw new SecurityException(getDatabaseName(),
            "Cannot update record "
                + entity
                + ": the resource has restricted access due to security policies");
      }

      var clazz = entity.getImmutableSchemaClass(this);
      ensureLinksConsistencyBeforeModification(entity);

      if (clazz != null) {
        checkSecurity(ResourceGeneric.CLASS, Role.PERMISSION_CREATE, clazz.getName());
        if (clazz.isUser()) {
          entity.validate();
        } else if (clazz.isFunction()) {
          FunctionLibraryImpl.validateFunctionRecord(entity);
        }

        entity.propertyEncryption = PropertyEncryptionNone.instance();
      }
    }

    callbackHooks(TYPE.BEFORE_CREATE, recordAbstract);
  }

  public void afterCreateOperations(RecordAbstract recordAbstract) {
    assert assertIfNotActive();

    if (recordAbstract instanceof EntityImpl entity) {
      var clazz = entity.getImmutableSchemaClass(this);
      if (clazz != null) {
        if (clazz.isUser()) {
          SecurityUserImpl.encodePassword(this, entity);
          sharedContext.getSecurity().incrementVersion(this);
        }
        if (clazz.isScheduler()) {
          getSharedContext().getScheduler().initScheduleRecord(entity);
        }
        if (clazz.isRole() || clazz.isSecurityPolicy()) {
          sharedContext.getSecurity().incrementVersion(this);
        }
      }
    }

    callbackHooks(TYPE.AFTER_CREATE, recordAbstract);
  }

  public void beforeUpdateOperations(final RecordAbstract recordAbstract, String collectionName) {
    assert assertIfNotActive();

    checkSecurity(Role.PERMISSION_UPDATE, recordAbstract, collectionName);

    if (recordAbstract instanceof EntityImpl entity) {

      var clazz = entity.getImmutableSchemaClass(this);
      ensureLinksConsistencyBeforeModification(entity);

      if (clazz != null) {
        if (clazz.isScheduler()) {
          getSharedContext().getScheduler().preHandleUpdateScheduleInTx(this, entity);
        }
        if (clazz.isFunction()) {
          FunctionLibraryImpl.validateFunctionRecord(entity);
        }
        if (!getSharedContext().getSecurity().canUpdate(this, entity)) {
          throw new SecurityException(getDatabaseName(),
              "Cannot update record "
                  + entity.getIdentity()
                  + ": the resource has restricted access due to security policies");
        }
        entity.propertyEncryption = PropertyEncryptionNone.instance();
      }
    }

    callbackHooks(TYPE.BEFORE_UPDATE, recordAbstract);
  }

  public void afterUpdateOperations(RecordAbstract recordAbstract) {
    assert assertIfNotActive();

    if (recordAbstract instanceof EntityImpl entity) {
      var clazz = entity.getImmutableSchemaClass(this);
      if (clazz != null) {

        if (clazz.isUser()) {
          SecurityUserImpl.encodePassword(this, entity);
          sharedContext.getSecurity().incrementVersion(this);
        } else if (clazz.isRole() || clazz.isSecurityPolicy()) {
          sharedContext.getSecurity().incrementVersion(this);
        }
      }
    }

    callbackHooks(TYPE.AFTER_UPDATE, recordAbstract);
  }

  public void beforeDeleteOperations(final RecordAbstract recordAbstract,
      String collectionName) {
    assert assertIfNotActive();

    checkSecurity(Role.PERMISSION_DELETE, recordAbstract, collectionName);

    if (recordAbstract instanceof EntityImpl entity) {
      ensureLinksConsistencyBeforeDeletion(entity);

      var clazz = entity.getImmutableSchemaClass(this);
      if (clazz != null) {
        if (!getSharedContext().getSecurity().canDelete(this, entity)) {
          throw new SecurityException(getDatabaseName(),
              "Cannot delete record "
                  + entity.getIdentity()
                  + ": the resource has restricted access due to security policies");
        }

      }
    }

    callbackHooks(TYPE.BEFORE_DELETE, recordAbstract);
  }

  public void afterDeleteOperations(RecordAbstract recordAbstract) {
    assert assertIfNotActive();

    if (recordAbstract instanceof EntityImpl entity) {
      var clazz = entity.getImmutableSchemaClass(this);
      if (clazz != null) {
        if (clazz.isSequence()) {
          SequenceLibraryImpl.onAfterSequenceDropped((FrontendTransactionImpl) this.currentTx,
              entity);
        } else if (clazz.isFunction()) {
          FunctionLibraryImpl.onAfterFunctionDropped((FrontendTransactionImpl) this.currentTx,
              entity);
        } else if (clazz.isScheduler()) {
          SchedulerImpl.onAfterEventDropped((FrontendTransactionImpl) this.currentTx, entity);
        }
      }
    }

    callbackHooks(TYPE.AFTER_DELETE, recordAbstract);
  }

  public void afterReadOperations(RecordAbstract identifiable) {
    assert assertIfNotActive();

    callbackHooks(TYPE.READ, identifiable);
  }

  public UUID uuid() {
    assert assertIfNotActive();
    checkOpenness();

    return storage.getUuid();
  }

  public boolean beforeReadOperations(RecordAbstract identifiable) {
    assert assertIfNotActive();

    if (identifiable instanceof EntityImpl entity) {
      var clazz = entity.getImmutableSchemaClass(this);
      if (clazz != null) {
        try {
          checkSecurity(ResourceGeneric.CLASS, Role.PERMISSION_READ, clazz.getName());
        } catch (SecurityException e) {
          return true;
        }

        if (!getSharedContext().getSecurity().canRead(this, entity)) {
          return true;
        }

        entity.initPropertyAccess();
      }
    }

    return false;
  }

  public void afterCommitOperations(Map<RID, RID> updatedRids) {
    assert assertIfNotActive();

    for (var operation : currentTx.getRecordOperationsInternal()) {
      var record = operation.record;

      if (record instanceof EntityImpl entity) {
        var clazz = entity.getImmutableSchemaClass(this);
        if (clazz != null) {
          if (operation.type == RecordOperation.CREATED) {
            if (clazz.isSequence()) {
              ((SequenceLibraryProxy) getMetadata().getSequenceLibrary())
                  .getDelegate()
                  .onSequenceCreated(this, entity);
            }
            if (clazz.isScheduler()) {
              getMetadata().getScheduler().scheduleEvent(this, new ScheduledEvent(entity, this));
            }
            if (clazz.isFunction()) {
              this.getSharedContext().getFunctionLibrary().createdFunction(this, entity);
            }
          } else if (operation.type == RecordOperation.UPDATED) {
            if (clazz.isFunction()) {
              this.getSharedContext().getFunctionLibrary().updatedFunction(this, entity);
            }
            if (clazz.isScheduler()) {
              getSharedContext().getScheduler().postHandleUpdateScheduleAfterTxCommit(this, entity);
            }
          } else if (operation.type == RecordOperation.DELETED) {
            if (clazz.isFunction()) {
              this.getSharedContext().getFunctionLibrary()
                  .onFunctionDropped(this, entity.getIdentity());
            } else if (clazz.isSequence()) {
              ((SequenceLibraryProxy) getMetadata().getSequenceLibrary())
                  .getDelegate()
                  .onSequenceDropped(this, entity.getIdentity());
            } else if (clazz.isScheduler()) {
              getSharedContext().getScheduler().onEventDropped(this, entity.getIdentity());
            }
          }
        } else {
          var schemaId = metadata.getSchemaInternal().getIdentity();

          if (record.getIdentity().equals(schemaId)) {
            var schema = sharedContext.getSchema();
            for (var listener : sharedContext.browseListeners()) {
              listener.onSchemaUpdate(this, getDatabaseName(), schema);
            }
          }
        }

      }
    }

    for (var listener : browseListeners()) {
      try {
        listener.onAfterTxCommit(currentTx, updatedRids);
      } catch (Exception e) {
        final var message =
            "Error after the transaction has been committed. The transaction remains valid. The"
                + " exception caught was on execution of "
                + listener.getClass()
                + ".onAfterTxCommit() `%08X`";

        LogManager.instance().error(this, message, e, System.identityHashCode(e));

        throw BaseException.wrapException(
            new TransactionBlockedException(getDatabaseName(), message), e,
            getDatabaseName());
      }
    }

  }

  public void set(ATTRIBUTES_INTERNAL attribute, Object value) {
    assert assertIfNotActive();

    checkOpenness();

    if (attribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    final var stringValue = IOUtils.getStringContent(value != null ? value.toString() : null);
    final var storage = this.storage;

    if (attribute == ATTRIBUTES_INTERNAL.VALIDATION) {
      var validation = Boolean.parseBoolean(stringValue);
      storage.setValidation(validation);
    } else {
      throw new IllegalArgumentException(
          "Option '" + attribute + "' not supported on alter database");
    }

  }

  public void afterRollbackOperations() {
    assert assertIfNotActive();

    for (var listener : browseListeners()) {
      try {
        listener.onAfterTxRollback(currentTx);
      } catch (Exception t) {
        LogManager.instance()
            .error(this, "Error after transaction rollback `%08X`", t, System.identityHashCode(t));
      }
    }

  }

  public String getCollectionName(final @Nonnull DBRecord record) {
    assert assertIfNotActive();

    checkOpenness();

    var collectionId = record.getIdentity().getCollectionId();
    if (collectionId == RID.COLLECTION_ID_INVALID) {
      // COMPUTE THE COLLECTION ID
      SchemaClassInternal schemaClass = null;
      if (record instanceof EntityImpl entity) {
        schemaClass = entity.getImmutableSchemaClass(this);
      }
      if (schemaClass != null) {
        // FIND THE RIGHT COLLECTION AS CONFIGURED IN CLASS
        if (schemaClass.isAbstract()) {
          throw new SchemaException(getDatabaseName(),
              "Entity belongs to abstract class '"
                  + schemaClass.getName()
                  + "' and cannot be saved");
        }
        collectionId = schemaClass.getCollectionForNewInstance((EntityImpl) record);
        return getCollectionNameById(collectionId);
      } else {
        throw new SchemaException(getDatabaseName(),
            "Cannot find the collection id for record "
                + record
                + " because the schema class is not defined");
      }

    } else {
      return getCollectionNameById(collectionId);
    }
  }

  public boolean executeExists(@Nonnull RID rid) {
    assert assertIfNotActive();

    checkOpenness();

    try {
      checkSecurity(
          ResourceGeneric.COLLECTION,
          Role.PERMISSION_READ,
          getCollectionNameById(rid.getCollectionId()));

      var tx = getActiveTransaction();
      if (tx.isDeletedInTx(rid)) {
        // DELETED IN TX
        return false;
      }
      DBRecord record = getTransactionInternal().getRecord(rid);
      if (record != null) {
        return true;
      }

      if (!rid.isPersistent()) {
        return false;
      }

      if (localCache.findRecord(rid) != null) {
        return true;
      }

      return storage.recordExists(this, rid, tx.getAtomicOperation());
    } catch (Exception t) {
      throw BaseException.wrapException(
          new DatabaseException(getDatabaseName(),
              "Error on retrieving record "
                  + rid
                  + " (collection: "
                  + storage.getPhysicalCollectionNameById(rid.getCollectionId())
                  + ")"),
          t, getDatabaseName());
    }
  }

  public void checkSecurity(
      final ResourceGeneric resourceGeneric,
      final String resourceSpecific,
      final int iOperation) {
    assert assertIfNotActive();

    checkOpenness();

    if (user != null) {
      try {
        user.allow(this, resourceGeneric, resourceSpecific, iOperation);
      } catch (SecurityAccessException e) {

        if (logger.isDebugEnabled()) {
          LogManager.instance()
              .debug(
                  this,
                  "User '%s' tried to access the reserved resource '%s.%s', operation '%s'",
                  logger, getCurrentUser(),
                  resourceGeneric,
                  resourceSpecific,
                  iOperation);
        }

        throw e;
      }
    }
  }

  public void checkSecurity(
      final ResourceGeneric iResourceGeneric,
      final int iOperation,
      final Object... iResourcesSpecific) {
    assert assertIfNotActive();

    checkOpenness();

    if (iResourcesSpecific == null || iResourcesSpecific.length == 0) {
      checkSecurity(iResourceGeneric, null, iOperation);
    } else {
      for (var target : iResourcesSpecific) {
        checkSecurity(iResourceGeneric, target == null ? null : target.toString(), iOperation);
      }
    }
  }

  public void checkSecurity(
      final ResourceGeneric iResourceGeneric,
      final int iOperation,
      final Object iResourceSpecific) {
    assert assertIfNotActive();

    checkOpenness();

    checkSecurity(
        iResourceGeneric,
        iResourceSpecific == null ? null : iResourceSpecific.toString(),
        iOperation);
  }

  @Deprecated
  public void checkSecurity(final String iResource, final int iOperation) {
    assert assertIfNotActive();

    checkOpenness();

    final var resourceSpecific = Rule.mapLegacyResourceToSpecificResource(iResource);
    final var resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*")) {
      checkSecurity(resourceGeneric, null, iOperation);
    }

    checkSecurity(resourceGeneric, resourceSpecific, iOperation);
  }

  @Deprecated
  public void checkSecurity(
      final String iResourceGeneric, final int iOperation, final Object iResourceSpecific) {
    assert assertIfNotActive();

    checkOpenness();

    final var resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResourceGeneric);
    if (iResourceSpecific == null || iResourceSpecific.equals("*")) {
      checkSecurity(resourceGeneric, iOperation, (Object) null);
    }

    checkSecurity(resourceGeneric, iOperation, iResourceSpecific);
  }

  @Deprecated
  public void checkSecurity(
      final String iResourceGeneric, final int iOperation, final Object... iResourcesSpecific) {
    assert assertIfNotActive();

    checkOpenness();

    final var resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResourceGeneric);
    checkSecurity(resourceGeneric, iOperation, iResourcesSpecific);
  }

  public int addCollection(final String iCollectionName, final Object... iParameters) {
    assert assertIfNotActive();

    checkOpenness();

    return storage.addCollection(this, iCollectionName, iParameters);
  }

  @SuppressWarnings("unused")
  public int addCollection(final String iCollectionName, final int iRequestedId) {
    assert assertIfNotActive();

    checkOpenness();

    return storage.addCollection(this, iCollectionName, iRequestedId);
  }

  @SuppressWarnings("unused")
  public RecordConflictStrategy getConflictStrategy() {
    assert assertIfNotActive();

    return storage.getRecordConflictStrategy();
  }

  @SuppressWarnings("unused")
  public DatabaseSessionEmbedded setConflictStrategy(final String iStrategyName) {
    assert assertIfNotActive();

    checkOpenness();

    storage.setConflictStrategy(
        YouTrackDBEnginesManager.instance().getRecordConflictStrategy().getStrategy(iStrategyName));
    return this;
  }

  @SuppressWarnings("unused")
  public DatabaseSessionEmbedded setConflictStrategy(final RecordConflictStrategy iResolver) {
    assert assertIfNotActive();

    checkOpenness();

    storage.setConflictStrategy(iResolver);
    return this;
  }

  public long countCollectionElements(int iCollectionId, boolean countTombstones) {
    assert assertIfNotActive();

    checkOpenness();

    final var name = getCollectionNameById(iCollectionId);
    if (name == null) {
      return 0;
    }
    checkSecurity(ResourceGeneric.COLLECTION, Role.PERMISSION_READ, name);
    assert assertIfNotActive();
    return storage.count(this, iCollectionId, countTombstones);
  }

  public long countCollectionElements(int[] iCollectionIds, boolean countTombstones) {
    assert assertIfNotActive();

    checkOpenness();

    String name;
    for (var iCollectionId : iCollectionIds) {
      name = getCollectionNameById(iCollectionId);
      checkSecurity(ResourceGeneric.COLLECTION, Role.PERMISSION_READ, name);
    }
    return storage.count(this, iCollectionIds, countTombstones);
  }

  public long countCollectionElements(final String iCollectionName) {
    assert assertIfNotActive();

    checkOpenness();

    checkSecurity(ResourceGeneric.COLLECTION, Role.PERMISSION_READ, iCollectionName);

    final var collectionId = getCollectionIdByName(iCollectionName);
    if (collectionId < 0) {
      throw new IllegalArgumentException("Collection '" + iCollectionName + "' was not found");
    }
    return storage.count(this, collectionId);
  }

  /**
   * Returns the approximate number of records in the collection identified by its numeric ID. The
   * count is maintained incrementally on each create/delete and is O(1) to read. Under snapshot
   * isolation the value reflects the latest committed state, so it may be slightly stale for
   * concurrent readers.
   *
   * @param collectionId the numeric identifier of the collection
   * @return the approximate record count
   * @throws IllegalArgumentException if the collection does not exist
   */
  public long getApproximateCollectionCount(final int collectionId) {
    assert assertIfNotActive();

    checkOpenness();

    final var name = getCollectionNameById(collectionId);
    if (name == null) {
      throw new IllegalArgumentException(
          "Collection with id " + collectionId + " was not found");
    }
    checkSecurity(ResourceGeneric.COLLECTION, Role.PERMISSION_READ, name);

    return storage.getApproximateRecordsCount(collectionId);
  }

  /**
   * Returns the approximate number of records in the named collection. The count is maintained
   * incrementally on each create/delete and is O(1) to read. Under snapshot isolation the value
   * reflects the latest committed state, so it may be slightly stale for concurrent readers.
   *
   * @param collectionName the name of the collection
   * @return the approximate record count
   * @throws IllegalArgumentException if the collection does not exist
   */
  public long getApproximateCollectionCount(final String collectionName) {
    assert assertIfNotActive();

    checkOpenness();

    checkSecurity(ResourceGeneric.COLLECTION, Role.PERMISSION_READ, collectionName);

    final var collectionId = getCollectionIdByName(collectionName);
    if (collectionId < 0) {
      throw new IllegalArgumentException(
          "Collection '" + collectionName + "' was not found");
    }
    return storage.getApproximateRecordsCount(collectionId);
  }

  /**
   * Returns the sum of approximate record counts across the given collection IDs. Each individual
   * count is O(1) to read. Security checks are assumed to be done upstream (via {@code
   * SchemaClassImpl.readableCollections()}).
   *
   * @param collectionIds the numeric identifiers of the collections
   * @return the total approximate record count across all collections
   */
  public long getApproximateCollectionElementsCount(final int[] collectionIds) {
    assert assertIfNotActive();

    checkOpenness();

    long total = 0;
    for (var collectionId : collectionIds) {
      if (collectionId > -1) {
        total += storage.getApproximateRecordsCount(collectionId);
      }
    }
    return total;
  }

  public void dropCollection(final String iCollectionName) {
    assert assertIfNotActive();

    checkOpenness();

    final var collectionId = getCollectionIdByName(iCollectionName);
    var schema = metadata.getSchema();

    var clazz = schema.getClassByCollectionId(collectionId);
    if (clazz != null) {
      throw new DatabaseException(this,
          "Cannot drop collection '" + getCollectionNameById(collectionId)
              + "' because it is mapped to class '" + clazz.getName() + "'");
    }
    if (schema.getBlobCollections().contains(collectionId)) {
      schema.removeBlobCollection(iCollectionName);
    }

    localCache.freeCollection(collectionId);
    dropCollectionInternal(iCollectionName);
  }

  private void dropCollectionInternal(final String iCollectionName) {
    assert assertIfNotActive();
    storage.dropCollection(this, iCollectionName);
  }

  @SuppressWarnings("unused")
  public boolean dropCollection(final int collectionId) {
    assert assertIfNotActive();

    checkOpenness();

    checkSecurity(
        ResourceGeneric.COLLECTION, Role.PERMISSION_DELETE,
        getCollectionNameById(collectionId));

    var schema = metadata.getSchema();
    final var clazz = schema.getClassByCollectionId(collectionId);
    if (clazz != null) {
      throw new DatabaseException(this,
          "Cannot drop collection '" + getCollectionNameById(collectionId)
              + "' because it is mapped to class '" + clazz.getName() + "'");
    }

    localCache.freeCollection(collectionId);
    if (schema.getBlobCollections().contains(collectionId)) {
      schema.removeBlobCollection(getCollectionNameById(collectionId));
    }

    final var collectionName = getCollectionNameById(collectionId);
    if (collectionName == null) {
      return false;
    }

    final var iteratorCollection = browseCollection(collectionName);
    if (iteratorCollection == null) {
      return false;
    }

    executeInTxBatches(iteratorCollection, (session, record) -> delete(record));

    return dropCollectionInternal(collectionId);
  }

  public boolean dropCollectionInternal(int collectionId) {
    assert assertIfNotActive();

    checkOpenness();

    return storage.dropCollection(this, collectionId);
  }

  public DatabaseStats getStats() {
    assert assertIfNotActive();
    var stats = new DatabaseStats();
    stats.loadedRecords = loadedRecordsCount;
    stats.minLoadRecordTimeMs = minRecordLoadMs;
    stats.maxLoadRecordTimeMs = minRecordLoadMs;
    stats.averageLoadRecordTimeMs =
        loadedRecordsCount == 0 ? 0 : (this.totalRecordLoadMs / loadedRecordsCount);

    stats.prefetchedRidbagsCount = ridbagPrefetchCount;
    stats.minRidbagPrefetchTimeMs = minRidbagPrefetchMs;
    stats.maxRidbagPrefetchTimeMs = maxRidbagPrefetchMs;
    stats.ridbagPrefetchTimeMs = totalRidbagPrefetchMs;
    return stats;
  }

  public void resetRecordLoadStats() {
    assert assertIfNotActive();
    this.loadedRecordsCount = 0L;
    this.totalRecordLoadMs = 0L;
    this.minRecordLoadMs = 0L;
    this.ridbagPrefetchCount = 0L;
    this.totalRidbagPrefetchMs = 0L;
    this.minRidbagPrefetchMs = 0L;
    this.maxRidbagPrefetchMs = 0L;
  }

  public String backup(final Path path) {
    assert assertIfNotActive();

    checkOpenness();
    checkSecurity(ResourceGeneric.DATABASE, "backup", Role.PERMISSION_EXECUTE);

    return storage.backup(path);
  }

  public String fullBackup(final Path path) {
    assert assertIfNotActive();

    checkOpenness();
    checkSecurity(ResourceGeneric.DATABASE, "backup", Role.PERMISSION_EXECUTE);

    return storage.fullBackup(path);
  }

  public void backup(Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, InputStream> ibuInputStreamSupplier,
      Function<String, OutputStream> ibuOutputStreamSupplier,
      Consumer<String> ibuFileRemover) {
    assert assertIfNotActive();

    checkOpenness();

    checkSecurity(Rule.ResourceGeneric.DATABASE, "backup", Role.PERMISSION_EXECUTE);

    storage.backup(ibuFilesSupplier, ibuInputStreamSupplier, ibuOutputStreamSupplier,
        ibuFileRemover);
  }

  @Nullable public TimeZone getDatabaseTimeZone() {
    assert assertIfNotActive();

    checkOpenness();

    return storage.getTimeZone();
  }

  public void freeze(final boolean throwException) {
    assert assertIfNotActive();

    checkOpenness();

    freezeDurationMetric.timed(() -> storage.freeze(this, throwException));
  }

  public void freeze() {
    assert assertIfNotActive();
    freeze(false);
  }

  public void release() {
    assert assertIfNotActive();

    checkOpenness();

    releaseDurationMetric.timed(() -> storage.release(this));
  }

  public LinkCollectionsBTreeManager getBTreeCollectionManager() {
    assert assertIfNotActive();
    return storage.getLinkCollectionsBtreeCollectionManager();
  }

  public void reload() {
    assert assertIfNotActive();

    checkOpenness();

    if (this.isClosed()) {
      throw new DatabaseException(getDatabaseName(), "Cannot reload a closed db");
    }
    metadata.reload();
    storage.reload(this);
  }

  public void internalCommit(@Nonnull FrontendTransactionImpl transaction) {
    assert assertIfNotActive();

    this.storage.commit(transaction);
  }

  public void internalClose(boolean recycle) {
    if (status != STATUS.OPEN) {
      // Fast-path guard. A guard-returning no-op close must NOT plant the teardown-intent mark: a
      // stale mark surviving into a later pooled borrow would make that borrower's first engage
      // self-abort ("session was closed while engaging") on a healthy session.
      return;
    }
    // The assert sits BEFORE the claim and the flag writes: a misuse detected under -ea must not
    // consume the teardown claim nor strand internalCloseInProgress/teardownIntent on a session
    // whose teardown never actually started.
    assert assertIfNotActive();
    if (!teardownClaim.compareAndSet(false, true)) {
      // Another teardown of this open cycle is running (or completed): the pool fall-through
      // racing the owner's completer, or a pool close racing the borrower's own close. Exactly
      // one full teardown may proceed — the loser no-ops here, leaving the winner's listener
      // firing and session-count decrement single.
      return;
    }
    // Every teardown that actually tears marks first (both the recycle and the full arm), set
    // strictly AFTER the guards passed. The mark is the teardown side of the Dekker pair with the
    // mutex engage: mark-write before the release pass in the outer finally below, so a
    // mid-flight engage this teardown's release pass cannot see will see the mark in its own
    // post-acquire re-check and self-release.
    teardownIntent = true;
    internalCloseInProgress = true;

    try {
      closeActiveQueries();
      localCache.shutdown();

      if (isClosed()) {
        status = STATUS.CLOSED;
        return;
      }

      try {
        rollback();
      } catch (Exception e) {
        LogManager.instance().error(this, "Exception during rollback of active transaction", e);
      }

      callOnCloseListeners();

      status = STATUS.CLOSED;
      if (!recycle) {
        sharedContext = null;
        storage.close(this);
      }

    } finally {
      if (status != STATUS.CLOSED) {
        // The teardown unwound before completing (a throw escaped the body — e.g. an Error from
        // a close listener — or a pre-rollback failure): release the teardown claim so a later
        // cleanup close can retry this broken session's teardown, preserving the pre-claim retry
        // semantics. A completed teardown keeps the claim; re-entry is then blocked by the
        // status guard anyway.
        teardownClaim.set(false);
      }
      // Widened release pass: the belt that keeps the mutex permit from stranding when this
      // teardown throws BEFORE the transaction's own close() finally could release it — including
      // the pre-rollback throw points (closeActiveQueries/localCache.shutdown), a rollback that
      // threw before reaching tx.close(), and the storage-already-closed early return above. A
      // no-op when nothing is engaged (the atomic claim returns 0) and idempotent against the
      // normal tx-close release (the claim makes exactly one releaser proceed). The pool-teardown
      // skip path returns from realClose() before ever entering this method, so this pass can
      // never release
      // a live foreign commit's permit. releaseMetadataWriteMutexForTx never throws by contract.
      releaseMetadataWriteMutexForTx();
      planNullPlacements.reset();
      internalCloseInProgress = false;
      // ALWAYS RESET TL
      activeSession.remove();
    }
  }

  @SuppressWarnings("unused")
  public String getCollectionRecordConflictStrategy(int collectionId) {
    assert assertIfNotActive();

    checkOpenness();

    return storage.getCollectionRecordConflictStrategy(collectionId);
  }

  public int[] getCollectionsIds(@Nonnull Set<String> filterCollections) {
    assert assertIfNotActive();

    checkOpenness();

    return storage.getCollectionsIds(filterCollections);
  }

  public long truncateClass(String name, boolean polimorfic) {
    assert assertIfNotActive();

    checkOpenness();

    this.checkSecurity(ResourceGeneric.CLASS, Role.PERMISSION_UPDATE);
    var clazz = getClass(name);
    int[] collectionIds;
    if (polimorfic) {
      collectionIds = clazz.getPolymorphicCollectionIds();
    } else {
      collectionIds = clazz.getCollectionIds();
    }
    long count = 0;
    for (var id : collectionIds) {
      if (id < 0) {
        continue;
      }
      final var collectionName = getCollectionNameById(id);
      if (collectionName == null) {
        continue;
      }
      count += truncateCollectionInternal(collectionName);
    }
    return count;
  }

  @SuppressWarnings("unused")
  public void truncateClass(String name) {
    assert assertIfNotActive();
    truncateClass(name, true);
  }

  public long truncateCollectionInternal(String collectionName) {
    assert assertIfNotActive();

    checkOpenness();

    checkSecurity(ResourceGeneric.COLLECTION, Role.PERMISSION_DELETE, collectionName);

    var id = getCollectionIdByName(collectionName);
    if (id == -1) {
      throw new DatabaseException(getDatabaseName(),
          "Collection with name " + collectionName + " does not exist");
    }
    final var clazz = getMetadata().getSchema().getClassByCollectionId(id);
    if (clazz != null) {
      checkSecurity(ResourceGeneric.CLASS, Role.PERMISSION_DELETE, clazz.getName());
    }

    try (var iteratorCollection = new RecordIteratorCollection<>(this, id, true)) {

      final var count = new long[] {0};
      executeInTxBatches(iteratorCollection, (session, record) -> {
        delete(record);
        count[0]++;
      });

      return count[0];
    }
  }

  public TransactionMeters transactionMeters() {
    return transactionMeters;
  }

  public void callOnOpenListeners() {
    assert assertIfNotActive();

    wakeupOnOpenDbLifecycleListeners();
  }

  public void callOnCloseListeners() {
    assert assertIfNotActive();

    wakeupOnCloseDbLifecycleListeners();
    wakeupOnCloseListeners();
  }

  private void wakeupOnOpenDbLifecycleListeners() {
    for (var it = YouTrackDBEnginesManager.instance()
        .getDbLifecycleListeners();
        it.hasNext();) {
      it.next().onOpen(this);
    }
  }

  private void wakeupOnCloseDbLifecycleListeners() {
    for (var it = YouTrackDBEnginesManager.instance()
        .getDbLifecycleListeners();
        it.hasNext();) {
      it.next().onClose(this);
    }
  }

  private void wakeupOnCloseListeners() {
    for (var listener : getListenersCopy()) {
      try {
        listener.onClose(this);
      } catch (Exception e) {
        LogManager.instance().error(this, "Error during call of database listener", e);
      }
    }
  }

  public static byte getRecordType() {
    return recordType;
  }

  public long countCollectionElements(final int[] iCollectionIds) {
    assert assertIfNotActive();
    return countCollectionElements(iCollectionIds, false);
  }

  public long countCollectionElements(final int iCollectionId) {
    assert assertIfNotActive();
    return countCollectionElements(iCollectionId, false);
  }

  public MetadataDefault getMetadata() {
    assert assertIfNotActive();
    checkOpenness();

    return metadata;
  }

  /**
   * The transaction-scoped key under which the tx-local schema state is stashed in the active
   * transaction's custom data. The state lives only as long as the transaction object, so it is
   * dropped automatically when the outermost transaction frame closes.
   *
   * <p>Public so {@link com.jetbrains.youtrackdb.internal.core.tx.FrontendTransactionImpl} can
   * treat the presence of a tx-local schema state as a write signal: a schema-only transaction (a
   * class create with no records) enrols no record or index operations, so without this signal the
   * commit would be skipped and the schema change silently dropped.
   */
  public static final String TX_SCHEMA_STATE_KEY = "txSchemaState";

  /**
   * Returns the tx-local schema write-view seeded for the current transaction, or {@code null}
   * when no schema write has been routed in this transaction yet (or no transaction is active). A
   * non-null result is the signal that the proxy routing seam must resolve schema reads and writes
   * against the tx-local copy (tier 3) rather than the committed shared instance (tier 2). This is
   * a read-only probe: it never seeds the state.
   */
  @Nullable public TxSchemaState getTxSchemaState() {
    assert assertIfNotActive();
    var tx = getTransactionInternal();
    if (!tx.isActive()) {
      return null;
    }
    return (TxSchemaState) tx.getCustomData(TX_SCHEMA_STATE_KEY);
  }

  /**
   * Returns the tx-local schema write-view for the current transaction, seeding it on the first
   * call. Seeding builds a private {@link SchemaShared} copy of the committed schema
   * ({@link SchemaShared#copyForTx}) and stashes it in the transaction's custom data, so the copy
   * is built at most once per transaction and discarded when the transaction object is. The caller
   * must have an open transaction; seeding loads records that ride the open user transaction.
   */
  public TxSchemaState ensureTxSchemaState() {
    assert assertIfNotActive();
    var tx = getTransactionInternal();
    assert tx.isActive()
        : "ensureTxSchemaState requires an open transaction to seed the tx-local schema copy";
    var existing = (TxSchemaState) tx.getCustomData(TX_SCHEMA_STATE_KEY);
    if (existing != null) {
      return existing;
    }

    // First schema/index write of this transaction. Engage the metadata-write mutex strictly above
    // any shared metadata lock and before the tx-local copy is seeded, so a second schema-changing
    // transaction blocks here rather than racing. This is the single seam every de-guarded write
    // path funnels through (the proxy resolveForWrite and the index-manager paths all reach seeding
    // here), so engaging here covers every write path with one placement.
    engageMetadataWriteMutex();
    try {
      SchemaShared committed = getSharedContext().getSchema();
      // Guard the copyForTx re-parse: its inheritance rebuild ripples committed index membership back
      // through the index-manager tx-local seam, which would otherwise re-enter this method before
      // the marker below is set (re-engaging the mutex and self-deadlocking) and pollute the
      // changed-class set with committed classes the seed merely reconstructs. The seam no-ops every
      // ripple raised while this flag is set; the finally clears it even when the re-parse throws.
      seedingTxSchemaState = true;
      final TxSchemaState state;
      try {
        state = new TxSchemaState(committed.copyForTx(this));
      } finally {
        seedingTxSchemaState = false;
      }
      tx.setCustomData(TX_SCHEMA_STATE_KEY, state);
      return state;
    } catch (RuntimeException | Error e) {
      // The seed loads and re-parses records and can throw (record-not-found, I/O, a malformed
      // per-class record, a non-persistent linked id). The custom-data marker that records "the seed
      // exists" has not been written yet, so undo the engage before rethrowing: otherwise the permit
      // would be stranded if the transaction is later abandoned, and a same-tx retry of the schema
      // write would re-enter here with no seed recorded and trip the engage's same-session
      // stranded-holder throw. releaseMetadataWriteMutexForTx clears
      // the marker and the permit and is idempotent, so the normal teardown release that still fires
      // on close() is a no-op.
      releaseMetadataWriteMutexForTx();
      throw e;
    }
  }

  /**
   * Whether this session is mid-seed of the tx-local schema copy (the {@link SchemaShared#copyForTx}
   * re-parse running inside {@code ensureTxSchemaState}). The index-manager tx-local seam reads this
   * to skip the membership-ripple recording the seed's inheritance rebuild raises against the
   * already-committed shared index, which would otherwise re-enter {@code ensureTxSchemaState} and
   * self-deadlock on the single-permit mutex.
   */
  public boolean isSeedingTxSchemaState() {
    return seedingTxSchemaState;
  }

  /**
   * Whether this session is mid-reload of the committed schema (the {@link SchemaShared#reload}
   * re-parse running inside {@link #runReloadingCommittedSchema}). The index-manager tx-local seam
   * reads this to skip the membership-ripple recording the reload's inheritance rebuild raises
   * against the already-committed shared index — the reload holds the schema write lock, so seeding
   * the tx-local state from that ripple would engage the metadata-write mutex under
   * {@code SchemaShared.lock} and trip the engage-order guard.
   */
  public boolean isReloadingSchema() {
    return reloadingSchema;
  }

  /**
   * Runs {@code reloadBody} (the {@link SchemaShared#reload} re-parse) with the schema-reload guard
   * set, so the index-manager membership seam treats the inheritance-rebuild ripples raised inside
   * as reconstructions of committed state rather than schema writes (see {@link
   * #isReloadingSchema()}). Exception-safe: the guard clears even when the reload body throws. Not
   * reentrant — a reload never runs inside another reload, and the assert pins that so a future
   * nesting cannot silently clear the guard early.
   */
  public void runReloadingCommittedSchema(Runnable reloadBody) {
    assert !reloadingSchema : "schema-reload guard scopes must not nest";
    reloadingSchema = true;
    try {
      reloadBody.run();
    } finally {
      reloadingSchema = false;
    }
  }

  /**
   * Runs {@code reads} in a fresh-committed-read scope: every record load inside the scope reads
   * the latest committed state through a dedicated read-only atomic operation (whose snapshot is
   * taken now) instead of the active transaction's begin-time snapshot, and bypasses this session's
   * local record cache (whose entries were read at that begin-time snapshot and may be stale).
   * Loaded records still bind to this session and refresh the local cache, so later reads of the
   * same records — including the commit-time schema serialization — serve the fresh state.
   *
   * <p>The consumers are the tx-local schema seed ({@link SchemaShared#copyForTx}) and the
   * schema-carry commit's serialization, enrollment, and promotion loads: the metadata-write mutex
   * serializes schema-changing transactions, so a transaction that parked on the mutex behind a
   * concurrent schema commit resumes with a begin-time snapshot that predates that commit. Reading
   * through the transaction's snapshot would re-parse or rewrite the pre-commit schema state (for
   * example a phantom collection drop in the commit-time set-diff, or a root write at a stale
   * record version). The scope is not reentrant and throws on nesting — always-on rather than an
   * assert, because a silently nested scope would strand the outer scope on the stale snapshot in
   * production where asserts are disabled.
   *
   * <p>Vacuum-safety premise: the dedicated read-only operation registers no {@code tsMin} of its
   * own. Its snapshot is taken while the calling thread's transaction is active, so it is strictly
   * newer than the transaction's begin-time snapshot, whose {@code tsMin} pin (held from
   * {@code begin()} to the transaction's close) is what keeps every snapshot-index entry the fresh
   * snapshot could need from being vacuumed. The scope must therefore only ever run inside an
   * active transaction on this session — which its consumers guarantee.
   */
  public <T> T computeWithFreshCommittedReads(Supplier<T> reads) {
    if (freshCommittedReadOperation != null) {
      // Always-on (not an assert): a nested scope's finally would clear the field and deactivate
      // its operation mid-outer-scope, silently reverting the outer scope's remaining reads to the
      // transaction's stale begin-time snapshot — the exact corruption the scope exists to prevent.
      throw new IllegalStateException("fresh-committed-read scopes must not nest");
    }
    // A lightweight read-only atomic operation: startAtomicOperation only captures a snapshot of
    // the operations table (nothing registers thread- or table-side until an apply starts, which
    // never happens here), so deactivating it after the reads is the complete teardown. Mirrors
    // the read-only-operation pattern IndexHistogramManager uses for its rebalance scan.
    final var freshReadOperation = storage.getAtomicOperationsManager().startAtomicOperation();
    freshCommittedReadOperation = freshReadOperation;
    try {
      return reads.get();
    } finally {
      freshCommittedReadOperation = null;
      freshReadOperation.deactivate();
    }
  }

  /**
   * The atomic operation record reads ride: the fresh-committed-read operation while a
   * {@link #computeWithFreshCommittedReads} scope is active on this session, otherwise the active
   * transaction's operation. Public because the {@code EntityImpl} page-frame fallback re-read
   * resolves its read operation through this seam too — a stamp-invalidated re-read inside the
   * scope must not silently fall back to the transaction's stale begin-time snapshot.
   */
  public AtomicOperation getEffectiveReadAtomicOperation() {
    if (freshCommittedReadOperation != null) {
      return freshCommittedReadOperation;
    }
    return getActiveTransaction().getAtomicOperation();
  }

  /**
   * Re-fills a locally cached record instance in place from a fresh committed read, so a
   * fresh-committed-read scope observes the latest committed record state through the SAME instance
   * (the session's one-instance-per-rid invariant forbids materializing a second instance for a
   * cached rid). No-op when the fresh read carries the version the instance already has, and for a
   * dirty instance (a dirty record is this transaction's own working copy; {@code fill()} rejects
   * dirty records). Returns {@code false} when the record no longer exists in the latest committed
   * state, so the caller can evict the stale instance and report not-found.
   *
   * <p>The retry loop below deliberately does not repeat the initial read's
   * {@link RecordNotFoundException} handling: a retry re-read happens only after the initial read
   * already answered (the record existed in the fresh snapshot), and the snapshot is immutable, so
   * a not-found on the re-read of the SAME snapshot is impossible by construction — were it ever
   * thrown, it would signal a snapshot-stability bug and should escape loudly rather than be
   * silently converted to an eviction.
   */
  private boolean refreshRecordFromFreshCommittedRead(RecordAbstract record) {
    assert freshCommittedReadOperation != null
        : "refreshRecordFromFreshCommittedRead requires an active fresh-committed-read scope";
    if (record.isDirty()) {
      return true;
    }
    var rid = record.getIdentity();
    StorageReadResult readResult;
    try {
      readResult = storage.readRecord(rid, freshCommittedReadOperation);
    } catch (RecordNotFoundException e) {
      return false;
    }
    // toRawBuffer() validates the optimistic-read stamp after byte extraction; retry on
    // invalidation until a consistent copy is read, mirroring the blob re-read loop in
    // executeReadRecord.
    while (true) {
      try {
        var rawBuffer = readResult.toRawBuffer();
        if (rawBuffer.version() != record.getVersion()) {
          record.fill(rawBuffer.version(), rawBuffer.buffer(), false);
          record.fromStream(rawBuffer.buffer());
        }
        return true;
      } catch (OptimisticReadFailedException e) {
        readResult = storage.readRecord(rid, freshCommittedReadOperation);
      }
    }
  }

  /**
   * Force-rebuilds this session's schema snapshot after a mid-transaction schema or index change so
   * the next read re-materializes the classes, property constraint rules, and per-class index lists
   * against the tx-local schema state. The immutable snapshot materializes once and is then reused;
   * without this rebuild a validation, serialization, insert, or query later in the same
   * transaction reads the stale snapshot and silently misses a class, rule, or index the
   * transaction changed. Invalidation is lazy: it discards the cached snapshot so the next snapshot
   * read rebuilds on demand, at O(1) cost here.
   *
   * <p>It clears only this session's thread-local pinned snapshot, not the process-shared
   * {@code SchemaShared} snapshot. The tx-local state is session-scoped, so a tx-dependent view must
   * not be cached process-wide where a concurrent session would read it; {@link SchemaProxy#makeSnapshot()}
   * builds a session-private uncached snapshot while a schema/index transaction is active, so
   * clearing the shared cache is both unnecessary and incorrect here (it would let this session's
   * tx-local view leak into the shared snapshot a concurrent reader picks up). The thread-local
   * clear is guarded on a zero pin count. On the supported paths the count is expected to be zero
   * because a schema or index DDL change is not issued from inside a pinned read-record operation;
   * the force-clear itself throws when the count is non-zero, surfacing a misplaced call (a DDL
   * issued while a read pin is held) rather than treating such a call as impossible.
   *
   * <p>It also invalidates the session-private snapshot memoized on {@code TxSchemaState}
   * (see {@link TxSchemaState#invalidateOverlaySnapshot()}), so the next snapshot read on the
   * tx-aware branch of {@link SchemaProxy#makeSnapshot()} rebuilds once against the changed
   * tx-local state while intervening unpinned reads between changes reuse the built snapshot.
   */
  public void forceRebuildTxSchemaSnapshot() {
    getMetadata().forceClearThreadLocalSchemaSnapshot();
    var txState = getTxSchemaState();
    if (txState != null) {
      txState.invalidateOverlaySnapshot();
    }
  }

  /**
   * Engages the storage's metadata-write mutex for the current transaction, throwing first when a
   * shared metadata lock is already held by this thread. That runtime check proves the engage sits
   * strictly above {@link SchemaShared#isWriteLockHeldByCurrentThread() the schema write lock} and
   * the {@link IndexManagerEmbedded#isWriteLockHeldByCurrentThread() index-manager write lock}:
   * engaging from inside a shared-lock acquisition would let a second transaction park on the mutex
   * while holding a shared write lock, freezing lock-based reads and deadlocking against a
   * commit-side lock acquisition. The de-guarded index-manager membership and create/drop paths are
   * the dangerous placements because they themselves take those locks, so the check guards every
   * write path, not just the canonical proxy path. It is an always-on guard rather than an assert
   * because production JVMs run with assertions disabled, where a silent park is the worst outcome.
   * The engage itself throws loudly when the current thread already holds the permit through a
   * different session (a same-thread embedded session).
   */
  private void engageMetadataWriteMutex() {
    var sharedCtx = getSharedContext();
    // Enforce the engage-order invariant at runtime, not by assert alone: production JVMs run with
    // assertions disabled, and the one invariant whose violation produces a cross-session deadlock
    // (a second transaction parking on the permit while holding a shared write lock, freezing
    // lock-based reads and deadlocking against the commit-side lock acquisition) must not vanish
    // under -da. Throwing fails fast and turns a silent production hang into a diagnosable error.
    if (sharedCtx.getSchema().isWriteLockHeldByCurrentThread()) {
      throw new IllegalStateException(
          "the metadata-write mutex must engage above SchemaShared.lock, but the current thread"
              + " already holds the schema write lock");
    }
    if (sharedCtx.getIndexManager().isWriteLockHeldByCurrentThread()) {
      throw new IllegalStateException(
          "the metadata-write mutex must engage above the index-manager lock, but the current thread"
              + " already holds the index-manager write lock");
    }
    final var mutex = sharedCtx.getMetadataWriteMutex();
    final long ordinal = mutex.engage(this);
    engagedMutex = mutex;
    // Mandatory ordering (the Dekker pair's formal soundness depends on it): store the acquire
    // ordinal STRICTLY BEFORE reading the teardown-intent mark. The teardown side writes the mark
    // and then runs its release pass; with these orders, either the teardown's getAndSet harvests
    // the ordinal we just stored (the teardown releases), or its claim returned 0 — in which case
    // its mark-write precedes our ordinal-store in synchronization order, so the mark-read below
    // sees the mark and this engage self-releases. No interleaving leaves the permit without a
    // releaser.
    engagedMutexOrdinal.set(ordinal);
    if (teardownIntent) {
      // Dekker post-acquire re-check: the session was marked for teardown while this engage was
      // mid-flight (permit acquired, ordinal not yet visible to the teardown's release pass).
      // Self-release through the same atomic claim every releaser uses, then fail loudly — the
      // session is being torn down, so the transaction cannot proceed.
      releaseMetadataWriteMutexForTx();
      throw new DatabaseException(getDatabaseName(),
          "the session was closed while engaging the metadata-write mutex");
    }
  }

  /**
   * Releases the metadata-write mutex permit this session engaged for the current transaction, if
   * any — the SINGLE release funnel all three release sites go through: the owner's tx-close
   * finally ({@code FrontendTransactionImpl.close()}), the teardown release pass in
   * {@code internalClose}'s outer finally (which a pool shutdown can drive cross-thread), and the
   * engage-path Dekker self-release. The gate is an atomic claim: {@code getAndSet(0)} on the
   * session-side ordinal record makes exactly one of any number of racing releasers harvest the
   * ordinal; the harvester presents {@code (session, ordinal)} to the mutex, whose keyed CAS is
   * the independent second belt (it also warn-noops a stale ordinal from a recycled session's
   * earlier acquisition). Never throws: it runs inside teardown finallys where a throw would mask
   * the real exception, and {@code releaseFor} is warn-noop by contract. Uses the engage-captured
   * mutex reference rather than {@link #getSharedContext()}, because {@code internalClose} nulls
   * {@code sharedContext} before its outer finally runs this pass.
   */
  public void releaseMetadataWriteMutexForTx() {
    final long ordinal = engagedMutexOrdinal.getAndSet(0);
    if (ordinal == 0) {
      return;
    }
    final var mutex = engagedMutex;
    if (mutex == null) {
      // Unreachable by construction (the engage publishes the mutex reference before the ordinal);
      // log rather than throw — this runs in teardown finallys.
      LogManager.instance()
          .error(this,
              "metadata-write mutex release claimed ordinal %d but no mutex reference was"
                  + " recorded; the permit may be stranded",
              null, ordinal);
      return;
    }
    mutex.releaseFor(this, ordinal);
  }

  /**
   * Whether this session has been marked for teardown (the Dekker teardown-intent mark). Read by
   * the mutex engage path: the wait loop aborts a waiter whose session is being torn down, and the
   * post-acquire re-check self-releases an engage the teardown's release pass could not see.
   */
  public boolean isTeardownIntentMarked() {
    return teardownIntent;
  }

  /** Sets the Dekker teardown-intent mark; see {@link #isTeardownIntentMarked()}. */
  protected void markTeardownIntent() {
    teardownIntent = true;
  }

  /**
   * Clears the Dekker teardown-intent mark on a pooled session's re-open ({@code reuse()}). For a
   * recycled borrow this clear is the ONLY belt: the recycle teardown legitimately marks (it
   * tears), so every returned session arrives at {@code reuse()} marked, and without the clear the
   * borrower's first engage would self-abort forever.
   */
  protected void clearTeardownIntent() {
    teardownIntent = false;
  }

  /**
   * Resets the atomic one-shot teardown claim on a pooled session's re-open ({@code reuse()}): the
   * recycle teardown consumed it, and the next close of the fresh borrow must be able to claim its
   * own teardown.
   */
  protected void resetTeardownClaim() {
    teardownClaim.set(false);
  }

  /**
   * The current transaction's status for diagnostics, readable lock-free from any thread (both
   * {@code currentTx} and the transaction status are volatile). Never throws; answers
   * {@code "<none>"} when no transaction object exists.
   */
  protected String getTransactionStatusForDiagnostics() {
    final var tx = currentTx;
    return tx == null ? "<none>" : String.valueOf(tx.getStatus());
  }

  /**
   * Owner-as-completer for the pool-teardown skip: called at the transaction-close boundary
   * (the tail finally of {@code FrontendTransactionImpl.close()}, after the mutex release), on
   * both the commit and rollback outcomes. If a pool teardown skipped this session because our
   * commit was in flight (it set the teardown-intent mark and deferred), the owner — the sole
   * legitimate completer — now runs the full {@code internalClose} on the owning thread. The
   * Dekker completer handshake makes at least one side act: the pool writes the mark FIRST and
   * then re-validates the skip condition (falling through to a full teardown itself when the
   * commit already finished); the owner publishes its commit-completion state (the volatile tx
   * status write inside {@code close()}) FIRST and then reads the mark here. Throw-isolated: a
   * teardown failure is logged and never masks the commit outcome (a durable commit must never be
   * reported failed because a close listener threw — the client would retry a durably applied
   * commit). The {@code internalCloseInProgress} guard keeps the tx-close reached from inside
   * {@code internalClose}'s own rollback from re-entering {@code internalClose} recursively; a
   * cross-thread overlap with a pool teardown is contained by the atomic teardown claim inside
   * {@code internalClose} (exactly one full teardown proceeds).
   */
  public void completeDeferredTeardownAfterTxClose() {
    if (!teardownIntent || internalCloseInProgress || status != STATUS.OPEN) {
      return;
    }
    try {
      internalClose(false);
    } catch (final Throwable deferredTeardownFailure) {
      LogManager.instance()
          .warn(this,
              String.format(
                  "Deferred teardown of session %08X of database '%s' after a pool-close skip"
                      + " failed; the session may leak resources until the storage closes",
                  System.identityHashCode(this), getDatabaseName()),
              deferredTeardownFailure);
    }
  }

  /**
   * Whether this session's current transaction is COMMITTING on a thread other than the caller's
   * — the skip-protocol detection a pool teardown runs before tearing down a checked-out
   * session. Reads only the transaction's volatile {@code status}/{@code storageTxThreadId}, so it
   * is safe from a foreign thread; the residual TOCTOU is the accepted one (late-skip = the commit
   * already finished and the owner completes; late-rollback = today's behavior).
   */
  protected boolean hasInFlightForeignCommit() {
    // Happens-before chain: currentTx is volatile, so this foreign read observes the owner's
    // tx-boundary assignment (never a stale prior borrow's object), and the subsequent volatile
    // status/storageTxThreadId reads inside isCommittingOnForeignThread observe that same
    // transaction's current state. The residual is only the accepted TOCTOU: late-skip = the
    // commit already finished (owner completer / pool re-validation handles teardown);
    // late-rollback = today's behavior.
    final var tx = currentTx;
    return tx instanceof FrontendTransactionImpl impl && impl.isCommittingOnForeignThread();
  }

  public boolean isRetainRecords() {
    assert assertIfNotActive();
    return retainRecords;
  }

  public DatabaseSessionEmbedded setRetainRecords(boolean retainRecords) {
    assert assertIfNotActive();
    this.retainRecords = retainRecords;
    return this;
  }

  public void setStatus(final STATUS status) {
    assert assertIfNotActive();
    this.status = status;
  }

  public void setInternal(final ATTRIBUTES iAttribute, final Object iValue) {
    set(iAttribute, iValue);
  }

  public SecurityUser getCurrentUser() {
    assert assertIfNotActive();

    checkOpenness();
    return user;
  }

  @SuppressWarnings("unused")
  @Nullable public String getCurrentUserName() {
    var user = getCurrentUser();
    if (user == null) {
      return null;
    }

    return user.getName(this);
  }

  public void setUser(final SecurityUser user) {
    assert assertIfNotActive();
    if (user instanceof SecurityUserImpl) {
      final Metadata metadata = getMetadata();
      if (metadata != null) {
        final var security = sharedContext.getSecurity();
        this.user = new ImmutableUser(this, security.getVersion(this), user);
      } else {
        this.user = new ImmutableUser(this, -1, user);
      }
    } else {
      this.user = (ImmutableUser) user;
    }
  }

  public void reloadUser() {
    assert assertIfNotActive();

    if (user != null) {
      if (user.checkIfAllowed(this, ResourceGeneric.CLASS, SecurityUserImpl.CLASS_NAME,
          Role.PERMISSION_READ)
          != null) {

        Metadata metadata = getMetadata();
        if (metadata != null) {
          final var security = sharedContext.getSecurity();
          final var secGetUser = security.getUser(this, user.getName(this));
          if (secGetUser != null) {
            user = new ImmutableUser(this, security.getVersion(this), secGetUser);
          } else {
            throw new SecurityException(url, "User not found");
          }
        } else {
          throw new SecurityException(url, "Metadata not found");
        }
      }
    }
  }

  @SuppressWarnings({"unused", "SameReturnValue"})
  public boolean isMVCC() {
    assert assertIfNotActive();
    return true;
  }

  @SuppressWarnings({"unused", "MethodMayBeStatic"})
  public DatabaseSessionEmbedded setMVCC(boolean mvcc) {
    throw new UnsupportedOperationException();
  }

  public RecordHook registerHook(final @Nonnull RecordHook iHookImpl) {
    assert assertIfNotActive();

    checkOpenness();

    if (!hooks.contains(iHookImpl)) {
      hooks.add(iHookImpl);
    }

    return iHookImpl;
  }

  public void unregisterHook(final @Nonnull RecordHook iHookImpl) {
    assert assertIfNotActive();

    checkOpenness();

    if (hooks.remove(iHookImpl)) {
      iHookImpl.onUnregister();
    }
  }

  public LocalRecordCache getLocalCache() {
    return localCache;
  }

  @Nonnull
  public RID refreshRid(@Nonnull RID rid) {
    if (rid.isPersistent()) {
      return rid;
    }

    checkTxActive();
    var record = currentTx.getRecordEntry(rid);
    if (record == null) {
      throw new RecordNotFoundException(this, rid);
    }

    return record.record.getIdentity();
  }

  public List<RecordHook> getHooks() {
    assert assertIfNotActive();

    checkOpenness();

    return Collections.unmodifiableList(hooks);
  }

  public void deleteInternal(@Nonnull RecordAbstract record) {
    assert assertIfNotActive();

    checkOpenness();

    if (record instanceof EntityImpl entity) {
      ensureEdgeConsistencyOnDeletion(entity);
    }

    currentTx.preProcessRecordsAndExecuteCallCallbacks();
    record.dirty++;

    try {
      checkTxActive();
      currentTx.deleteRecord(record);
    } catch (BaseException e) {
      throw e;
    } catch (Exception e) {
      if (record instanceof EntityImpl entity) {
        throw BaseException.wrapException(
            new DatabaseException(getDatabaseName(),
                "Error on deleting record "
                    + record.getIdentity()
                    + " of class '"
                    + entity.getSchemaClassName()
                    + "'"),
            e, getDatabaseName());
      } else {
        throw BaseException.wrapException(
            new DatabaseException(getDatabaseName(),
                "Error on deleting record " + record.getIdentity()),
            e, getDatabaseName());
      }
    }
  }

  /**
   * Callback the registered hooks if any.
   *
   * @param type   Hook type. Define when hook is called.
   * @param record Record received in the callback
   */
  public void callbackHooks(final TYPE type, final RecordAbstract record) {
    assert assertIfNotActive();
    if (record == null || hooks.isEmpty() || record.getIdentity().getCollectionId() == 0) {
      return;
    }

    var identity = record.getIdentity().copy();
    if (!pushInHook(identity)) {
      return;
    }

    try {
      for (var hook : hooks) {
        hook.onTrigger(type, record);
      }
    } finally {
      popInHook(identity);
    }
  }

  public boolean isValidationEnabled() {
    assert assertIfNotActive();
    return (Boolean) get(ATTRIBUTES_INTERNAL.VALIDATION);
  }

  public void setValidationEnabled(final boolean iEnabled) {
    assert assertIfNotActive();
    set(ATTRIBUTES_INTERNAL.VALIDATION, iEnabled);
  }

  /**
   * The per-build null placement resolution of this session. A planner opens a scope around its
   * build so every placement read of that build returns one pair, and the plan cache compares the
   * recorded pair before it serves a stored plan again.
   */
  @Nonnull
  public PlanNullPlacements getPlanNullPlacements() {
    return planNullPlacements;
  }

  @Nullable public ContextConfiguration getConfiguration() {
    assert assertIfNotActive();

    return storage.getContextConfiguration();
  }

  @Override
  public void close() {
    internalClose(false);
  }

  public STATUS getStatus() {

    return status;
  }

  public String getDatabaseName() {
    return storage.getName();
  }

  public String getURL() {
    if (url == null) {
      url = storage.getURL();
    }

    return url;
  }

  public int getCollections() {
    assert assertIfNotActive();

    return storage.getCollections();
  }

  public boolean existsCollection(final String iCollectionName) {
    assert assertIfNotActive();
    checkOpenness();

    return storage.getCollectionNames()
        .contains(iCollectionName.toLowerCase(Locale.ENGLISH));
  }

  public Collection<String> getCollectionNames() {
    assert assertIfNotActive();

    checkOpenness();

    return storage.getCollectionNames();
  }

  public int getCollectionIdByName(final String iCollectionName) {
    if (iCollectionName == null) {
      return -1;
    }

    assert assertIfNotActive();

    checkOpenness();

    return storage.getCollectionIdByName(iCollectionName.toLowerCase(Locale.ENGLISH));
  }

  @Nullable public String getCollectionNameById(final int collectionId) {
    if (collectionId < 0) {
      return null;
    }

    assert assertIfNotActive();

    checkOpenness();

    return storage.getPhysicalCollectionNameById(collectionId);
  }

  public Object setProperty(final String iName, final Object iValue) {
    assert assertIfNotActive();

    checkOpenness();

    if (iValue == null) {
      return properties.remove(iName.toLowerCase(Locale.ENGLISH));
    } else {
      return properties.put(iName.toLowerCase(Locale.ENGLISH), iValue);
    }
  }

  public Object getProperty(final String iName) {
    assert assertIfNotActive();

    checkOpenness();

    return properties.get(iName.toLowerCase(Locale.ENGLISH));
  }

  public Iterator<Entry<String, Object>> getProperties() {
    assert assertIfNotActive();

    checkOpenness();

    return properties.entrySet().iterator();
  }

  public Object get(final ATTRIBUTES iAttribute) {
    assert assertIfNotActive();

    checkOpenness();

    if (iAttribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    return switch (iAttribute) {
      case DATEFORMAT -> storage.getDateFormat();
      case DATE_TIME_FORMAT -> storage.getDateTimeFormat();
      case TIMEZONE -> storage.getTimeZone().getID();
      case LOCALE_COUNTRY -> storage.getLocaleCountry();
      case LOCALE_LANGUAGE -> storage.getLocaleLanguage();
      case CHARSET -> storage.getCharset();
    };
  }

  public Object get(ATTRIBUTES_INTERNAL attribute) {
    assert assertIfNotActive();

    if (attribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    checkOpenness();

    if (attribute == ATTRIBUTES_INTERNAL.VALIDATION) {
      return storage.isValidationEnabled();
    }

    throw new IllegalArgumentException("attribute is not supported: " + attribute);
  }

  public FrontendTransaction getTransactionInternal() {
    assert assertIfNotActive();
    return currentTx;
  }

  /**
   * Returns the schema of the database.
   *
   * @return the schema of the database
   */
  public Schema getSchema() {
    assert assertIfNotActive();
    return getMetadata().getSchema();
  }

  @Nonnull
  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"}) // Intentional convenience cast API
  public <RET extends DBRecord> RET load(final RID recordId) {
    assert assertIfNotActive();

    checkOpenness();

    return (RET) currentTx.loadRecord(recordId);
  }

  public boolean exists(RID rid) {
    assert assertIfNotActive();

    checkOpenness();

    return currentTx.exists(rid);
  }

  public BinarySerializerFactory getSerializerFactory() {
    assert assertIfNotActive();
    return componentsFactory.binarySerializerFactory;
  }

  @SuppressWarnings("unused")
  public void setPrefetchRecords(boolean prefetchRecords) {
    assert assertIfNotActive();

    checkOpenness();

    this.prefetchRecords = prefetchRecords;
  }

  @SuppressWarnings("unused")
  public boolean isPrefetchRecords() {
    assert assertIfNotActive();

    checkOpenness();

    return prefetchRecords;
  }

  public int assignAndCheckCollection(DBRecord record) {
    assert assertIfNotActive();

    if (!storage.isAssigningCollectionIds()) {
      return RID.COLLECTION_ID_INVALID;
    }

    var rid = (RecordIdInternal) record.getIdentity();
    SchemaClassInternal schemaClass = null;
    // if collection id is not set yet try to find it out
    if (rid.getCollectionId() <= RID.COLLECTION_ID_INVALID) {
      if (record instanceof EntityImpl entity) {
        schemaClass = entity.getImmutableSchemaClass(this);
        if (schemaClass != null) {
          if (schemaClass.isAbstract()) {
            throw new SchemaException(getDatabaseName(),
                "Entity belongs to abstract class "
                    + schemaClass.getName()
                    + " and cannot be saved");
          }

          // For a class created inside this still-open transaction the id below is provisional
          // (<= -2): it backs no physical collection until commit. The record id carries the
          // provisional id anyway. The transaction keys its record operations under it, so a
          // same-transaction scan of the tx-created class serves the rows from the transaction
          // phase, and the commit rewrites every provisional record-collection id to the
          // reconciled real id before any record locks a collection, allocates a position, or
          // serializes.
          return schemaClass.getCollectionForNewInstance(entity);
        } else {
          throw new DatabaseException(getDatabaseName(),
              "Cannot save (1) entity " + record + ": no class or collection defined");
        }
      } else {
        if (record instanceof RecordBytes) {
          var blobs = getBlobCollectionIds();
          if (blobs.length == 0) {
            throw new DatabaseException(getDatabaseName(),
                "Cannot save blob (2) " + record + ": no collection defined");
          } else {
            return blobs[ThreadLocalRandom.current().nextInt(blobs.length)];
          }

        } else {
          throw new DatabaseException(getDatabaseName(),
              "Cannot save (3) entity " + record + ": no class or collection defined");
        }
      }
    } else {
      if (record instanceof EntityImpl entity) {
        schemaClass = entity.getImmutableSchemaClass(this);
      }
    }
    // If the collection id was set check is validity
    if (rid.getCollectionId() > RID.COLLECTION_ID_INVALID) {
      if (schemaClass != null) {
        var messageCollectionName = getCollectionNameById(rid.getCollectionId());
        checkRecordClass(schemaClass, messageCollectionName, rid);
        if (!schemaClass.hasCollectionId(rid.getCollectionId())) {
          throw new IllegalArgumentException(
              "Collection name '"
                  + messageCollectionName
                  + "' (id="
                  + rid.getCollectionId()
                  + ") is not configured to store the class '"
                  + schemaClass.getName()
                  + "', valid are "
                  + Arrays.toString(schemaClass.getCollectionIds()));
        }
      }
    }

    var collectionId = rid.getCollectionId();
    if (collectionId < 0) {
      throw new DatabaseException(getDatabaseName(),
          "Impossible to set collection for record " + record + " class : " + schemaClass);
    }

    return collectionId;
  }

  @SuppressWarnings("UnusedReturnValue")
  public int begin(FrontendTransactionImpl transaction) {
    assert assertIfNotActive();

    // CHECK IT'S NOT INSIDE A HOOK
    if (!inHook.isEmpty()) {
      throw new IllegalStateException("Cannot begin a transaction while a hook is executing");
    }

    if (currentTx.isActive()) {
      if (currentTx instanceof FrontendTransactionImpl) {
        return currentTx.beginInternal();
      }
    }

    // WAKE UP LISTENERS
    for (var listener : browseListeners()) {
      try {
        listener.onBeforeTxBegin(currentTx);
      } catch (Exception e) {
        LogManager.instance().error(this, "Error before tx begin", e);
      }
    }

    currentTx = transaction;
    return currentTx.beginInternal();
  }

  /**
   * Creates a new EntityImpl.
   */
  public EntityImpl newInstance() {
    assert assertIfNotActive();
    return newInstance(Entity.DEFAULT_CLASS_NAME);
  }

  public Entity newEntity() {
    assert assertIfNotActive();
    return newInstance(Entity.DEFAULT_CLASS_NAME);
  }

  @SuppressWarnings("TypeParameterUnusedInFormals") // Intentional convenience cast API
  public <T extends DBRecord> T createOrLoadRecordFromJson(String json) {
    assert assertIfNotActive();

    checkOpenness();

    //noinspection unchecked
    return (T) JSONSerializerJackson.INSTANCE.fromString(this, json);
  }

  public Entity createOrLoadEntityFromJson(String json) {
    assert assertIfNotActive();

    checkOpenness();

    var result = JSONSerializerJackson.INSTANCE.fromString(this, json);

    if (result instanceof Entity entity) {
      return entity;
    }

    throw new DatabaseException(getDatabaseName(), "The record is not an entity");
  }

  public Entity newEntity(String className) {
    assert assertIfNotActive();
    return newInstance(className);
  }

  public Entity newEntity(SchemaClass clazz) {
    assert assertIfNotActive();
    return newInstance(clazz.getName());
  }

  public Vertex newVertex(SchemaClass type) {
    assert assertIfNotActive();
    if (type == null) {
      return newVertex("V");
    }
    return newVertex(type.getName());
  }

  public Edge newEdge(Vertex from, Vertex to, SchemaClass type) {
    assert assertIfNotActive();
    if (type == null) {
      return newEdge(from, to, "E");
    }

    return newEdge(from, to, type.getName());
  }

  public RecordIteratorClass browseClass(final @Nonnull String className) {
    assert assertIfNotActive();
    return browseClass(className, true);
  }

  public RecordIteratorClass browseClass(
      final @Nonnull String className, final boolean iPolymorphic) {
    return browseClass(className, iPolymorphic, true);
  }

  public RecordIteratorClass browseClass(@Nonnull String className, boolean iPolymorphic,
      boolean forwardDirection) {
    assert assertIfNotActive();

    checkOpenness();

    if (getMetadata().getImmutableSchemaSnapshot().getClass(className) == null) {
      throw new IllegalArgumentException(
          "Class '" + className + "' not found in current database");
    }

    checkSecurity(ResourceGeneric.CLASS, Role.PERMISSION_READ, className);
    return new RecordIteratorClass(this, className, iPolymorphic, forwardDirection);
  }

  @SuppressWarnings("unused")
  public RecordIteratorClass browseClass(@Nonnull SchemaClass clz) {
    assert assertIfNotActive();

    checkOpenness();

    checkSecurity(ResourceGeneric.CLASS, Role.PERMISSION_READ, clz.getName());
    return new RecordIteratorClass(this, (SchemaClassInternal) clz,
        true, true);
  }

  public <REC extends RecordAbstract> RecordIteratorCollection<REC> browseCollection(
      final String iCollectionName) {
    assert assertIfNotActive();

    checkOpenness();

    checkSecurity(ResourceGeneric.COLLECTION, Role.PERMISSION_READ, iCollectionName);
    return new RecordIteratorCollection<>(this, getCollectionIdByName(iCollectionName), true);
  }

  public Iterable<SessionListener> getListeners() {
    assert assertIfNotActive();
    return getListenersCopy();
  }

  /**
   * Returns the number of the records of the class iClassName.
   */
  public long countClass(final String iClassName) {
    assert assertIfNotActive();
    return countClass(iClassName, true);
  }

  /**
   * Returns the number of the records of the class iClassName considering also sub classes if
   * polymorphic is true.
   */
  public long countClass(final String iClassName, final boolean iPolymorphic) {
    assert assertIfNotActive();

    final var cls =
        (SchemaImmutableClass) getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);
    if (cls == null) {
      throw new IllegalArgumentException("Class not found in database");
    }

    return countClass(cls, iPolymorphic);
  }

  private long countClass(final SchemaImmutableClass cls, final boolean iPolymorphic) {
    assert assertIfNotActive();

    checkOpenness();

    var totalOnDb = cls.countImpl(iPolymorphic, this);
    return adjustCountForTransaction(totalOnDb, cls.getName(), iPolymorphic);
  }

  /**
   * Returns the approximate number of records for the named class, including subclasses.
   */
  public long approximateCountClass(final String className) {
    assert assertIfNotActive();
    return approximateCountClass(className, true);
  }

  /**
   * Returns the approximate number of records for the named class, optionally including subclasses.
   * Uses the O(1) approximate record count from the storage layer, adjusted for any pending
   * transaction operations (creates/deletes).
   */
  public long approximateCountClass(final String className, final boolean isPolymorphic) {
    assert assertIfNotActive();

    final var cls =
        (SchemaImmutableClass) getMetadata().getImmutableSchemaSnapshot().getClass(className);
    if (cls == null) {
      throw new IllegalArgumentException("Class '" + className + "' not found in database");
    }

    return approximateCountClass(cls, isPolymorphic);
  }

  private long approximateCountClass(
      final SchemaImmutableClass cls, final boolean isPolymorphic) {
    assert assertIfNotActive();

    checkOpenness();

    var totalOnDb = cls.approximateCountImpl(isPolymorphic, this);
    return adjustCountForTransaction(totalOnDb, cls.getName(), isPolymorphic);
  }

  /**
   * Adjusts a base record count for uncommitted transaction operations. Scans the active
   * transaction's pending record operations and increments/decrements the count for any
   * creates/deletes that match the given class (polymorphically or exactly, as requested).
   *
   * @param baseCount    the committed record count (exact or approximate)
   * @param className    the class name to match
   * @param isPolymorphic whether to include subclass records in the adjustment
   * @return the adjusted count reflecting pending transaction state
   */
  private long adjustCountForTransaction(
      long baseCount, String className, boolean isPolymorphic) {
    long deletedInTx = 0;
    long addedInTx = 0;
    if (getTransactionInternal().isActive()) {
      for (var op : getTransactionInternal().getRecordOperationsInternal()) {
        if (op.type == RecordOperation.DELETED) {
          final DBRecord rec = op.record;
          if (rec instanceof EntityImpl entity) {
            var schemaClass = entity.getImmutableSchemaClass(this);
            if (schemaClass != null) {
              if (isPolymorphic) {
                if (schemaClass.isSubClassOf(className)) {
                  deletedInTx++;
                }
              } else {
                if (className.equals(schemaClass.getName())) {
                  deletedInTx++;
                }
              }
            }
          }
        }
        if (op.type == RecordOperation.CREATED) {
          final DBRecord rec = op.record;
          if (rec instanceof EntityImpl entity) {
            var schemaClass = entity.getImmutableSchemaClass(this);
            if (schemaClass != null) {
              if (isPolymorphic) {
                if (schemaClass.isSubClassOf(className)) {
                  addedInTx++;
                }
              } else {
                if (className.equals(schemaClass.getName())) {
                  addedInTx++;
                }
              }
            }
          }
        }
      }
    }

    return (baseCount + addedInTx) - deletedInTx;
  }

  public Map<RID, RID> commit() {
    return commitImpl(null, null, null);
  }

  /// Commit the current transaction with transaction metrics collection enabled.
  /// The listener, mode, and tracking ID are passed through to the frontend
  /// transaction so that timing data is captured around the storage commit.
  public void monitoredCommit(
      @Nonnull TransactionMetricsListener listener,
      @Nonnull QueryMonitoringMode mode,
      @Nonnull String trackingId) {
    commitImpl(listener, mode, trackingId);
  }

  private Map<RID, RID> commitImpl(
      @Nullable TransactionMetricsListener listener,
      @Nullable QueryMonitoringMode mode,
      @Nullable String trackingId) {
    assert assertIfNotActive();

    checkOpenness();

    if (currentTx.getStatus() == TXSTATUS.ROLLBACKING) {
      throw new RollbackException("Transaction is rolling back");
    }

    if (!currentTx.isActive()) {
      throw new TransactionException(getDatabaseName(),
          "No active transaction to commit. Call begin() first");
    }

    if (currentTx.amountOfNestedTxs() > 1) {
      // This just do count down no real commit here
      return currentTx.commitInternal();
    }

    // WAKE UP LISTENERS

    try {
      beforeCommitOperations();
    } catch (BaseException e) {
      try {
        rollback();
      } catch (Exception re) {
        LogManager.instance()
            .error(this, "Exception during rollback `%08X`", re, System.identityHashCode(re));
      }
      throw e;
    }
    try {
      if (listener != null) {
        assert mode != null && trackingId != null;
        return currentTx.monitoredCommitInternal(listener, mode, trackingId);
      } else {
        return currentTx.commitInternal();
      }
    } catch (RuntimeException e) {

      if ((e instanceof HighLevelException) || (e instanceof NeedRetryException)) {
        if (logger.isDebugEnabled()) {
          LogManager.instance()
              .debug(this, "Error on transaction commit `%08X`", logger, e,
                  System.identityHashCode(e));
        }
      } else {
        LogManager.instance()
            .error(this, "Error on transaction commit `%08X`", e, System.identityHashCode(e));
      }

      // WAKE UP ROLLBACK LISTENERS
      try {
        // ROLLBACK TX AT DB LEVEL
        if (currentTx.isActive()) {
          currentTx.rollbackInternal();
        }
      } catch (Exception re) {
        LogManager.instance()
            .error(
                this, "Error during transaction rollback `%08X`", re, System.identityHashCode(re));
      }

      // WAKE UP ROLLBACK LISTENERS
      throw e;
    }
  }

  private void beforeCommitOperations() {
    assert assertIfNotActive();
    for (var listener : browseListeners()) {
      try {
        listener.onBeforeTxCommit(currentTx);
      } catch (Exception e) {
        LogManager.instance()
            .error(
                this,
                "Cannot commit the transaction: caught exception on execution of"
                    + " %s.onBeforeTxCommit() `%08X`",
                e,
                listener.getClass().getName(),
                System.identityHashCode(e));
        throw BaseException.wrapException(
            new TransactionException(getDatabaseName(),
                "Cannot commit the transaction: caught exception on execution of "
                    + listener.getClass().getName()
                    + "#onBeforeTxCommit()"),
            e, getDatabaseName());
      }
    }
  }

  public final void beforeRollbackOperations() {
    assert assertIfNotActive();
    for (var listener : browseListeners()) {
      try {
        listener.onBeforeTxRollback(currentTx);
      } catch (Exception t) {
        LogManager.instance()
            .error(this, "Error before transaction rollback `%08X`", t, System.identityHashCode(t));
      }
    }
  }

  public void rollback() {
    assert assertIfNotActive();

    checkOpenness();

    if (currentTx.isActive()) {
      // WAKE UP LISTENERS
      currentTx.rollbackInternal();
      // WAKE UP LISTENERS
    }
  }

  @SuppressWarnings("unused")
  public CurrentStorageComponentsFactory getStorageVersions() {
    assert assertIfNotActive();
    return componentsFactory;
  }

  public RecordSerializer getSerializer() {
    assert assertIfNotActive();
    return serializer;
  }

  /**
   * Sets serializer for the database which will be used for entity serialization.
   *
   * @param serializer the serializer to set.
   */
  public void setSerializer(RecordSerializer serializer) {
    assert assertIfNotActive();
    this.serializer = serializer;
  }

  @SuppressWarnings("unused")
  public void resetInitialization() {
    assert assertIfNotActive();

    checkOpenness();

    for (var h : hooks) {
      h.onUnregister();
    }

    hooks.clear();
    close();

    initialized = false;
  }

  public void checkSecurity(final int operation, final Identifiable record, String collection) {
    assert assertIfNotActive();

    checkOpenness();

    if (collection == null) {
      collection = getCollectionNameById(record.getIdentity().getCollectionId());
    }
    checkSecurity(ResourceGeneric.COLLECTION, operation, collection);

    if (record instanceof EntityImpl entity) {
      var clazzName = entity.getSchemaClassName();
      if (clazzName != null) {
        checkSecurity(ResourceGeneric.CLASS, operation, clazzName);
      }
    }
  }

  /**
   * Use #activateOnCurrentThread instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public void setCurrentDatabaseInThreadLocal() {
    activateOnCurrentThread();
  }

  /**
   * Activates current database instance on current thread.
   */
  public void activateOnCurrentThread() {
    activeSession.set(true);
  }

  public boolean isActiveOnCurrentThread() {
    var isActive = activeSession.get();
    return isActive != null && isActive;
  }

  private void checkOpenness() {
    if (status == STATUS.CLOSED) {
      throw new DatabaseException(getDatabaseName(), "Database '" + getURL() + "' is closed");
    }
  }

  private void popInHook(Identifiable id) {
    inHook.remove(id);
  }

  private boolean pushInHook(Identifiable id) {
    return inHook.add(id);
  }

  private void checkRecordClass(
      final SchemaClass recordClass, final String iCollectionName, final RecordIdInternal rid) {
    assert assertIfNotActive();

    final var collectionIdClass =
        metadata.getImmutableSchemaSnapshot().getClassByCollectionId(rid.getCollectionId());
    if ((recordClass == null && collectionIdClass != null)
        || (collectionIdClass == null && recordClass != null)
        || (recordClass != null && !recordClass.equals(collectionIdClass))) {
      throw new IllegalArgumentException(
          "Record saved into collection '"
              + iCollectionName
              + "' should be saved with class '"
              + collectionIdClass
              + "' but has been created with class '"
              + recordClass
              + "'");
    }
  }

  // Called via assert, always returns true (throws otherwise)
  @SuppressWarnings("SameReturnValue")
  public boolean assertIfNotActive() {
    var currentDatabase = activeSession.get();

    if (currentDatabase == null || !currentDatabase) {
      throw new SessionNotActivatedException(getDatabaseName());
    }

    return true;
  }

  public int[] getBlobCollectionIds() {
    assert assertIfNotActive();
    return getMetadata().getSchema().getBlobCollections().toIntArray();
  }

  public SharedContext getSharedContext() {
    assert assertIfNotActive();
    return sharedContext;
  }

  public void queryStarted(String id, ResultSet resultSet) {
    assert assertIfNotActive();

    final var activeQueriesSize = activeQueries.size();

    if (this.resultSetReportThreshold > 0 &&
        activeQueriesSize > 1 &&
        activeQueriesSize % resultSetReportThreshold == 0) {
      var msg =
          "This database instance has "
              + activeQueriesSize
              + " open command/query result sets, please make sure you close them with"
              + " ResultSet.close()";
      LogManager.instance().warn(this, msg);
      if (logger.isDebugEnabled()) {
        activeQueries.values().stream()
            .map(ResultSet::getExecutionPlan)
            .forEach(plan -> LogManager.instance().debug(this, plan.toString(), logger));
      }
    }

    this.activeQueries.put(id, resultSet);
    getListeners().forEach((it) -> it.onCommandStart(this, resultSet));
  }

  @Override
  public void queryClosed(String id) {
    assert assertIfNotActive();

    @SuppressWarnings("resource")
    var removed = this.activeQueries.remove(id);
    getListeners().forEach((it) -> it.onCommandEnd(this, removed));
  }

  public void closeActiveQueries() {
    for (var rs : new ArrayList<>(activeQueries.values())) {
      rs.close();
    }
  }

  @SuppressWarnings("unused")
  public Map<String, ResultSet> getActiveQueries() {
    assert assertIfNotActive();

    return activeQueries;
  }

  @SuppressWarnings("unused")
  public ResultSet getActiveQuery(String id) {
    assert assertIfNotActive();

    return activeQueries.get(id);
  }

  @SuppressWarnings("unused")
  public boolean isCollectionEdge(int collection) {
    assert assertIfNotActive();

    var clazz = getMetadata().getImmutableSchemaSnapshot().getClassByCollectionId(collection);
    return clazz != null && clazz.isEdgeType();
  }

  @SuppressWarnings("unused")
  public boolean isCollectionVertex(int collection) {
    assert assertIfNotActive();

    var clazz = getMetadata().getImmutableSchemaSnapshot().getClassByCollectionId(collection);
    return clazz != null && clazz.isVertexType();
  }

  public <X extends Exception> void executeInTx(
      @Nonnull TxConsumer<Transaction, X> code) throws X {
    executeInTxInternal(code::accept);
  }

  @Nullable public <R, X extends Exception> R computeInTx(
      TxFunction<Transaction, R, X> supplier) throws X {
    return computeInTxInternal(supplier::apply);
  }

  public <T, X extends Exception> void executeInTxBatches(
      @Nonnull Iterator<T> iterator, int batchSize,
      TxBiConsumer<Transaction, T, X> consumer) throws X {
    executeInTxBatchesInternal(iterator, batchSize, consumer::accept);
  }

  public <T, X extends Exception> void executeInTxBatches(Iterator<T> iterator,
      TxBiConsumer<Transaction, T, X> consumer) throws X {
    executeInTxBatchesInternal(iterator, consumer::accept);
  }

  @SuppressWarnings("unused")
  public <T, X extends Exception> void executeInTxBatches(Stream<T> stream,
      TxBiConsumer<Transaction, T, X> consumer) throws X {
    executeInTxBatchesInternal(stream, consumer::accept);
  }

  public <X extends Exception> void executeInTxInternal(
      @Nonnull TxConsumer<FrontendTransactionImpl, X> code) throws X {
    if (currentTx.getStatus() == TXSTATUS.COMMITTING ||
        currentTx.getStatus() == TXSTATUS.ROLLBACKING) {
      throw new TransactionException(getDatabaseName(),
          "Cannot start a new transaction while a transaction is committing or rolling back");
    }
    var ok = false;
    assert assertIfNotActive();
    begin();
    try {
      code.accept((FrontendTransactionImpl) currentTx);
      ok = true;
    } finally {
      finishTx(ok);
    }
  }

  public <T, X extends Exception> void executeInTxBatches(
      Iterable<T> iterable, int batchSize, TxBiConsumer<Transaction, T, X> consumer) throws X {
    executeInTxBatches(iterable.iterator(), batchSize, consumer);
  }

  public <T, X extends Exception> void forEachInTx(Iterator<T> iterator,
      TxBiConsumer<Transaction, T, X> consumer) throws X {
    assert assertIfNotActive();
    forEachInTx(iterator, (db, t) -> {
      consumer.accept(db, t);
      return true;
    });
  }

  @SuppressWarnings("unused")
  public <T, X extends Exception> void forEachInTx(Iterable<T> iterable,
      TxBiConsumer<Transaction, T, X> consumer) throws X {
    assert assertIfNotActive();

    forEachInTx(iterable.iterator(), consumer);
  }

  @SuppressWarnings("unused")
  public <T, X extends Exception> void forEachInTx(Stream<T> stream,
      TxBiConsumer<Transaction, T, X> consumer) throws X {
    assert assertIfNotActive();

    try (var s = stream) {
      forEachInTx(s.iterator(), consumer);
    }
  }

  public <T, X extends Exception> void forEachInTx(Iterator<T> iterator,
      TxBiFunction<Transaction, T, Boolean, X> consumer) throws X {
    var ok = false;
    assert assertIfNotActive();

    var tx = begin();
    try {
      while (iterator.hasNext()) {
        var cont = consumer.apply(tx, iterator.next());
        commit();

        if (!cont) {
          break;
        }
        begin();
      }

      ok = true;
    } finally {
      finishTx(ok);
    }
  }

  @SuppressWarnings("unused")
  public <T, X extends Exception> void forEachInTx(Iterable<T> iterable,
      TxBiFunction<Transaction, T, Boolean, X> consumer) throws X {
    assert assertIfNotActive();

    forEachInTx(iterable.iterator(), consumer);
  }

  @SuppressWarnings("unused")
  public <T, X extends Exception> void forEachInTx(Stream<T> stream,
      TxBiFunction<Transaction, T, Boolean, X> consumer) throws X {
    assert assertIfNotActive();

    try (stream) {
      forEachInTx(stream.iterator(), consumer);
    }
  }

  private void finishTx(boolean ok) {
    if (currentTx.isActive()) {
      if (ok && currentTx.getStatus() != TXSTATUS.ROLLBACKING) {
        commit();
      } else {
        if (isActiveOnCurrentThread()) {
          rollback();
        } else {
          currentTx.rollbackInternal();
        }
      }
    }
  }

  public <T, X extends Exception> void executeInTxBatchesInternal(
      @Nonnull Iterator<T> iterator, int batchSize,
      TxBiConsumer<FrontendTransaction, T, X> consumer) throws X {
    var ok = false;
    assert assertIfNotActive();
    var counter = 0;

    begin();
    try {
      while (iterator.hasNext()) {
        consumer.accept(currentTx, iterator.next());
        counter++;

        if (counter % batchSize == 0) {
          commit();
          begin();
        }
      }

      ok = true;
    } finally {
      finishTx(ok);
    }
  }

  public <T, X extends Exception> void executeInTxBatchesInternal(
      Iterator<T> iterator, TxBiConsumer<FrontendTransaction, T, X> consumer) throws X {
    assert assertIfNotActive();

    executeInTxBatchesInternal(
        iterator,
        getConfiguration().getValueAsInteger(GlobalConfiguration.TX_BATCH_SIZE),
        consumer);
  }

  public <T, X extends Exception> void executeInTxBatches(
      Iterable<T> iterable, TxBiConsumer<Transaction, T, X> consumer) throws X {
    assert assertIfNotActive();
    executeInTxBatches(
        iterable,
        getConfiguration().getValueAsInteger(GlobalConfiguration.TX_BATCH_SIZE),
        consumer);
  }

  @SuppressWarnings("unused")
  public <T, X extends Exception> void executeInTxBatches(
      Stream<T> stream, int batchSize, TxBiConsumer<Transaction, T, X> consumer) throws X {
    assert assertIfNotActive();

    try (stream) {
      executeInTxBatches(stream.iterator(), batchSize, consumer);
    }
  }

  public <T, X extends Exception> void executeInTxBatchesInternal(Stream<T> stream,
      TxBiConsumer<FrontendTransaction, T, X> consumer) throws X {
    assert assertIfNotActive();

    try (stream) {
      executeInTxBatchesInternal(stream.iterator(), consumer);
    }
  }

  public <R, X extends Exception> R computeInTxInternal(TxFunction<FrontendTransaction, R, X> code)
      throws X {
    assert assertIfNotActive();
    var ok = false;
    begin();
    try {
      var result = code.apply(currentTx);
      ok = true;
      return result;
    } finally {
      finishTx(ok);
    }
  }

  public int activeTxCount() {
    assert assertIfNotActive();

    checkOpenness();

    var transaction = getTransactionInternal();
    return transaction.amountOfNestedTxs();
  }

  public <T> EmbeddedList<T> newEmbeddedList() {
    assert assertIfNotActive();

    checkOpenness();

    return new EntityEmbeddedListImpl<>();
  }

  public <T> EmbeddedList<T> newEmbeddedList(int size) {
    assert assertIfNotActive();

    checkOpenness();

    return new EntityEmbeddedListImpl<>(size);
  }

  public <T> EmbeddedList<T> newEmbeddedList(Collection<T> list) {
    assert assertIfNotActive();

    checkOpenness();

    //noinspection unchecked
    return (EmbeddedList<T>) PropertyTypeInternal.EMBEDDEDLIST.copy(list, this);
  }

  public EmbeddedList<String> newEmbeddedList(String[] source) {
    assert assertIfNotActive();

    checkOpenness();

    var trackedList = new EntityEmbeddedListImpl<String>(source.length);
    trackedList.addAll(Arrays.asList(source));
    return trackedList;
  }

  public EmbeddedList<Date> newEmbeddedList(Date[] source) {
    assert assertIfNotActive();

    checkOpenness();

    var trackedList = new EntityEmbeddedListImpl<Date>(source.length);
    trackedList.addAll(Arrays.asList(source));
    return trackedList;
  }

  public EmbeddedList<Byte> newEmbeddedList(byte[] source) {
    assert assertIfNotActive();

    checkOpenness();

    var trackedList = new EntityEmbeddedListImpl<Byte>(source.length);
    for (var b : source) {
      trackedList.add(b);
    }
    return trackedList;
  }

  public EmbeddedList<Short> newEmbeddedList(short[] source) {
    assert assertIfNotActive();

    checkOpenness();

    var trackedList = new EntityEmbeddedListImpl<Short>(source.length);
    for (var s : source) {
      trackedList.add(s);
    }
    return trackedList;
  }

  public EmbeddedList<Integer> newEmbeddedList(int[] source) {
    assert assertIfNotActive();

    checkOpenness();

    var trackedList = new EntityEmbeddedListImpl<Integer>(source.length);
    for (var i : source) {
      trackedList.add(i);
    }
    return trackedList;
  }

  public EmbeddedList<Long> newEmbeddedList(long[] source) {
    assert assertIfNotActive();

    checkOpenness();

    var trackedList = new EntityEmbeddedListImpl<Long>(source.length);
    for (var l : source) {
      trackedList.add(l);
    }
    return trackedList;
  }

  public EmbeddedList<Float> newEmbeddedList(float[] source) {
    assert assertIfNotActive();

    checkOpenness();

    var trackedList = new EntityEmbeddedListImpl<Float>(source.length);
    for (var f : source) {
      trackedList.add(f);
    }
    return trackedList;
  }

  public EmbeddedList<Double> newEmbeddedList(double[] source) {
    assert assertIfNotActive();

    checkOpenness();

    var trackedList = new EntityEmbeddedListImpl<Double>(source.length);
    for (var d : source) {
      trackedList.add(d);
    }
    return trackedList;
  }

  public EmbeddedList<Boolean> newEmbeddedList(boolean[] source) {
    assert assertIfNotActive();

    checkOpenness();

    var trackedList = new EntityEmbeddedListImpl<Boolean>(source.length);
    for (var b : source) {
      trackedList.add(b);
    }
    return trackedList;
  }

  public LinkList newLinkList() {
    assert assertIfNotActive();

    checkOpenness();

    return new EntityLinkListImpl(this);
  }

  public LinkList newLinkList(int size) {
    assert assertIfNotActive();

    checkOpenness();

    return new EntityLinkListImpl(this, size);
  }

  public LinkList newLinkList(Collection<? extends Identifiable> source) {
    assert assertIfNotActive();

    checkOpenness();

    var list = new EntityLinkListImpl(this, source.size());
    list.addAll(source);
    return list;
  }

  public <T> EmbeddedSet<T> newEmbeddedSet() {
    assert assertIfNotActive();

    checkOpenness();

    return new EntityEmbeddedSetImpl<>();
  }

  public <T> EmbeddedSet<T> newEmbeddedSet(int size) {
    assert assertIfNotActive();

    checkOpenness();

    return new EntityEmbeddedSetImpl<>(size);
  }

  public <T> EmbeddedSet<T> newEmbeddedSet(Collection<T> set) {
    assert assertIfNotActive();

    checkOpenness();

    //noinspection unchecked
    return (EmbeddedSet<T>) PropertyTypeInternal.EMBEDDEDSET.copy(set, this);
  }

  public LinkSet newLinkSet() {
    assert assertIfNotActive();

    checkOpenness();

    return new EntityLinkSetImpl(this);
  }

  public LinkSet newLinkSet(Collection<? extends Identifiable> source) {
    assert assertIfNotActive();

    checkOpenness();

    var linkSet = new EntityLinkSetImpl(this);
    linkSet.addAll(source);
    return linkSet;
  }

  public <V> EmbeddedMap<V> newEmbeddedMap() {
    assert assertIfNotActive();

    checkOpenness();

    return new EntityEmbeddedMapImpl<>();
  }

  public <V> EmbeddedMap<V> newEmbeddedMap(int size) {
    assert assertIfNotActive();

    checkOpenness();

    return new EntityEmbeddedMapImpl<>(size);
  }

  public <V> EmbeddedMap<V> newEmbeddedMap(Map<String, V> map) {
    assert assertIfNotActive();

    checkOpenness();

    //noinspection unchecked
    return (EmbeddedMap<V>) PropertyTypeInternal.EMBEDDEDMAP.copy(map, this);
  }

  public LinkMap newLinkMap() {
    assert assertIfNotActive();

    checkOpenness();

    return new EntityLinkMapIml(this);
  }

  public LinkMap newLinkMap(int size) {
    assert assertIfNotActive();

    checkOpenness();

    return new EntityLinkMapIml(size, this);
  }

  public LinkMap newLinkMap(Map<String, ? extends Identifiable> source) {
    assert assertIfNotActive();

    checkOpenness();

    var linkMap = new EntityLinkMapIml(source.size(), this);
    linkMap.putAll(source);
    return linkMap;
  }

  @Nonnull
  public FrontendTransactionImpl getActiveTransaction() {
    assert assertIfNotActive();

    checkOpenness();

    if (currentTx.isActive()) {
      return (FrontendTransactionImpl) currentTx;
    }

    throw new DatabaseException(this, "There is no active transaction in session");
  }

  @Nullable public FrontendTransactionImpl getActiveTransactionOrNull() {
    assert assertIfNotActive();

    checkOpenness();

    if (currentTx.isActive()) {
      return (FrontendTransactionImpl) currentTx;
    }

    return null;
  }

  private void checkTxActive() {
    if (currentTx == null || !currentTx.isActive()) {
      throw new TransactionException(getDatabaseName(), "There is no active transaction");
    }
  }

  private void ensureEdgeConsistencyOnDeletion(@Nonnull EntityImpl entity) {
    if (entity.isVertex()) {
      VertexEntityImpl.deleteLinks(entity.asVertex());
    } else if (entity.isEdge()) {
      EdgeEntityImpl.deleteLinks(this, entity.asEdge());
    }
  }

  private void ensureLinksConsistencyBeforeDeletion(@Nonnull EntityImpl entity) {
    if (!ensureLinkConsistency) {
      return;
    }

    var properties = entity.getPropertyNamesInternal(true, false);
    var linksToUpdateMap = new HashMap<RecordIdInternal, int[]>();
    for (var propertyName : properties) {
      linksToUpdateMap.clear();

      if (propertyName.charAt(0) == EntityImpl.OPPOSITE_LINK_CONTAINER_PREFIX) {
        var oppositeLinksContainer = (LinkBag) entity.getPropertyInternal(propertyName, false);

        for (var link : oppositeLinksContainer) {
          var transaction = getActiveTransaction();
          var oppositeEntity = (EntityImpl) transaction.loadEntityOrNull(link.primaryRid());
          //skip self-links and already deleted entities
          if (oppositeEntity == null || oppositeEntity.equals(entity)) {
            continue;
          }

          var linkName = propertyName.substring(1);
          var oppositeLinkProperty = oppositeEntity.getPropertyInternal(linkName, false);

          if (oppositeLinkProperty == null) {
            throw new LinksConsistencyException(this, "Cannot remove link " + entity.getIdentity()
                + " from opposite entity because system property "
                + linkName + " does not exist");
          }
          switch (oppositeLinkProperty) {
            case Identifiable identifiable -> {
              if (identifiable.getIdentity().equals(entity.getIdentity())) {
                oppositeEntity.setPropertyInternal(linkName, null);
              } else {
                throw new LinksConsistencyException(this,
                    "Cannot remove link" + linkName + ":" + entity.getIdentity()
                        + " from opposite entity because it does not exist");
              }
            }
            case EntityLinkListImpl linkList -> {
              var removed = linkList.remove(entity);
              if (!removed) {
                throw new LinksConsistencyException(this,
                    "Cannot remove link " + linkName + ":" + entity.getIdentity()
                        + " from opposite entity because it does not exist in the list");
              }
            }
            case EntityLinkSetImpl linkSet -> {
              var removed = linkSet.remove(entity);
              if (!removed) {
                throw new LinksConsistencyException(this,
                    "Cannot remove link " + linkName + ":" + entity.getIdentity()
                        + " from opposite entity because it does not exist in the set");
              }
            }
            case EntityLinkMapIml linkMap -> {
              var removed = false;
              var entrySetIterator = linkMap.entrySet().iterator();
              while (entrySetIterator.hasNext()) {
                var entry = entrySetIterator.next();

                if (entry.getValue().equals(entity)) {
                  entrySetIterator.remove();
                  removed = true;
                  break;
                }
              }

              if (!removed) {
                throw new LinksConsistencyException(this,
                    "Cannot remove link " + linkName + ":" + entity.getIdentity()
                        + " from opposite entity because it does not exist in the map");
              }
            }
            case LinkBag linkBag -> {
              var removed = linkBag.remove(entity.getIdentity());
              if (!removed) {
                throw new LinksConsistencyException(this,
                    "Cannot remove link " + linkName + ":" + entity.getIdentity()
                        + " from opposite entity because it does not exist in link bag");
              }
            }
            default -> {
              throw new IllegalStateException("Unexpected type of link property: "
                  + oppositeLinkProperty.getClass().getName());
            }
          }
        }
      } else {
        if (entity.isVertex()) {
          if (VertexEntityImpl.isEdgeProperty(propertyName)) {
            continue;
          }
        } else if (entity.isEdge()) {
          if (EdgeInternal.isEdgeConnectionProperty(propertyName)) {
            continue;
          }
        }

        var currentPropertyValue = entity.getPropertyInternal(propertyName);
        subtractFromLinksContainer(currentPropertyValue, linksToUpdateMap);
        updateOppositeLinks(entity, propertyName, linksToUpdateMap);
      }
    }
  }

  private void ensureLinksConsistencyBeforeModification(@Nonnull final EntityImpl entity) {
    if (!ensureLinkConsistency) {
      return;
    }

    var dirtyProperties = entity.getDirtyPropertiesBetweenCallbacksInternal(false,
        false);
    if (entity.isVertex()) {
      dirtyProperties = filterVertexProperties(dirtyProperties);
    } else if (entity.isEdge()) {
      dirtyProperties = filterEdgeProperties(dirtyProperties);
    }

    var linksToUpdateMap = new HashMap<RecordIdInternal, int[]>();
    for (var propertyName : dirtyProperties) {
      linksToUpdateMap.clear();

      var originalValue = entity.getOriginalValue(propertyName);
      var currentPropertyValue = entity.getPropertyInternal(propertyName, false);

      if (originalValue == null) {
        var timeLine = entity.getCollectionTimeLine(propertyName);
        if (timeLine != null) {
          if (currentPropertyValue instanceof EntityLinkListImpl
              || currentPropertyValue instanceof EntityLinkSetImpl ||
              currentPropertyValue instanceof LinkBag
              || currentPropertyValue instanceof EntityLinkMapIml) {
            var isLinkBag = currentPropertyValue instanceof LinkBag;
            for (var event : timeLine.getMultiValueChangeEvents()) {
              switch (event.getChangeType()) {
                case ADD -> {
                  var addedRid = isLinkBag ? event.getKey() : event.getValue();
                  assert addedRid != null;
                  incrementLinkCounter((RecordIdInternal) addedRid, linksToUpdateMap);
                }
                case REMOVE -> {
                  var removedRid = isLinkBag ? event.getKey() : event.getOldValue();
                  assert removedRid != null;
                  decrementLinkCounter((RecordIdInternal) removedRid, linksToUpdateMap);
                }
                case UPDATE -> {
                  assert event.getValue() != null;
                  assert event.getOldValue() != null;
                  incrementLinkCounter((RecordIdInternal) event.getValue(), linksToUpdateMap);
                  decrementLinkCounter((RecordIdInternal) event.getOldValue(), linksToUpdateMap);
                }
              }
            }
          }
        } else {
          addToLinksContainer(currentPropertyValue, linksToUpdateMap);
        }
      } else {
        subtractFromLinksContainer(originalValue, linksToUpdateMap);
        addToLinksContainer(currentPropertyValue, linksToUpdateMap);
      }

      updateOppositeLinks(entity, propertyName, linksToUpdateMap);
    }
  }

  private void updateOppositeLinks(@Nonnull EntityImpl entity, String propertyName,
      Map<RecordIdInternal, int[]> linksToUpdateMap) {
    var oppositeLinkBagPropertyName = EntityImpl.getOppositeLinkBagPropertyName(propertyName);
    for (var entitiesToUpdate : linksToUpdateMap.entrySet()) {
      var oppositeLink = entitiesToUpdate.getKey();
      var diff = entitiesToUpdate.getValue()[0];

      if (currentTx.isDeletedInTx(oppositeLink)) {
        if (diff > 0) {
          throw new LinksConsistencyException(this, "Cannot add link " + entity.getIdentity()
              + " to opposite entity because it was deleted in transaction");
        }
        continue;
      }

      var oppositeRecord = load(oppositeLink);
      if (!oppositeRecord.isEntity()) {
        continue;
      }

      var oppositeEntity = (EntityImpl) oppositeRecord;
      //skip self-links
      if (oppositeEntity.equals(entity)) {
        continue;
      }
      var linkBag = (LinkBag) oppositeEntity.getPropertyInternal(oppositeLinkBagPropertyName);

      if (linkBag == null) {
        if (diff < 0) {
          // Invariant: if we're removing a back-reference, the opposite entity
          // must have the link bag property. For new entities created and deleted
          // in the same TX, the CREATED callback runs first (adding back-refs),
          // so this path is not expected for new entities.
          assert !entity.getIdentity().isNew()
              : "New entity " + entity.getIdentity()
                  + " has missing opposite link bag " + oppositeLinkBagPropertyName
                  + " on " + oppositeEntity.getIdentity();
          throw new LinksConsistencyException(this,
              describeMissingBackReference(
                  "Cannot remove link " + propertyName + " for " + entity.getIdentity()
                      + " from opposite entity " + oppositeEntity.getIdentity()
                      + " because required opposite link bag property "
                      + oppositeLinkBagPropertyName + " does not exist",
                  entity, propertyName, oppositeEntity, oppositeLinkBagPropertyName,
                  null, diff));
        }
        linkBag = new LinkBag(this);
        oppositeEntity.setPropertyInternal(oppositeLinkBagPropertyName, linkBag);
      }

      var rid = entity.getIdentity();
      for (var i = 0; i < Math.abs(diff); i++) {
        if (diff > 0) {
          linkBag.add(rid);
          assert linkBag.contains(rid);
        } else {
          var removed = linkBag.remove(entity.getIdentity());
          if (!removed) {
            // Invariant: if the link bag exists but doesn't contain the entity,
            // the entity must have been added by a prior CREATED callback.
            // For new entities, the CREATED callback always runs before the
            // DELETED callback within preProcessRecordsAndExecuteCallCallbacks.
            assert !entity.getIdentity().isNew()
                : "New entity " + entity.getIdentity()
                    + " not found in opposite link bag " + oppositeLinkBagPropertyName
                    + " on " + oppositeEntity.getIdentity();
            // This violation is rare and so far reproduces only under heavy concurrency on
            // weak-memory-model (ARM) hardware (LocalPaginatedStorageRestoreFromWALIT). The
            // base phrase is preserved verbatim for log-grep continuity; the appended
            // diagnostics capture the exact state so the next occurrence can be root-caused
            // straight from CI logs without a reproduction.
            throw new LinksConsistencyException(this,
                describeMissingBackReference(
                    "Cannot remove link " + rid
                        + " from opposite entity because it does not exist in opposite link"
                        + " bag : " + oppositeLinkBagPropertyName,
                    entity, propertyName, oppositeEntity, oppositeLinkBagPropertyName,
                    linkBag, diff));
          }
        }
      }

      if (linkBag.isEmpty()) {
        oppositeEntity.removePropertyInternal(oppositeLinkBagPropertyName);
      }
    }
  }

  /**
   * Builds a diagnostic message for a back-reference consistency violation detected in
   * {@link #updateOppositeLinks}. The violation means the opposite entity is missing the
   * back-reference for the source link we are removing — either the back-reference bag
   * exists but does not hold that entry, or the bag property is absent entirely
   * ({@code oppositeLinkBag == null}). Either case should never happen, because each
   * back-reference bag has a single writer. It has so far reproduced only rarely and only
   * under heavy concurrency on weak-memory-model (ARM) hardware, so this method records the
   * exact state at the point of failure: the source and opposite record identities and
   * versions, the source's forward links, and the back-reference bag's type, size, pointer,
   * and contents. With that state in the CI log the loss can be classified (whole bag lost
   * vs. one entry lost vs. wrong bag type) without a local reproduction.
   *
   * <p>Every field is read inside {@link #appendDiagnosticField} so a failure while
   * gathering diagnostics can never throw and mask the original consistency error. Reading
   * {@code sourceForwardLinks} can force lazy deserialization of the source record, which is
   * benign and idempotent on this already-failing path.
   *
   * @param baseMessage the human-readable violation summary, kept verbatim at the start so
   *     existing log searches still match
   * @param oppositeLinkBag the opposite entity's back-reference bag, or {@code null} when
   *     the bag property is absent entirely
   */
  static String describeMissingBackReference(
      String baseMessage,
      EntityImpl sourceEntity,
      String sourcePropertyName,
      EntityImpl oppositeEntity,
      String oppositeLinkBagPropertyName,
      @Nullable LinkBag oppositeLinkBag,
      int diff) {
    var sb = new StringBuilder(256);
    sb.append(baseMessage).append(" [diag");
    appendDiagnosticField(sb, "sourceProperty", () -> sourcePropertyName);
    appendDiagnosticField(sb, "source", () -> describeEntityState(sourceEntity));
    appendDiagnosticField(
        sb, "sourceForwardLinks",
        // Objects.toString forces the Object rendering: getPropertyInternal's unbounded
        // generic return type <RET> would make an unqualified String.valueOf(...) call
        // resolve to the char[] overload (javac infers RET := char[] as the most specific
        // applicable type), inserting a checkcast that throws ClassCastException for every
        // non-char[] value and an NPE for null — destroying exactly the source-side
        // evidence this diagnostic exists to capture.
        () -> Objects.toString(sourceEntity.getPropertyInternal(sourcePropertyName, false)));
    appendDiagnosticField(sb, "oppositeBagProperty", () -> oppositeLinkBagPropertyName);
    appendDiagnosticField(sb, "opposite", () -> describeEntityState(oppositeEntity));
    if (oppositeLinkBag == null) {
      sb.append(" bag=absent");
    } else {
      appendDiagnosticField(sb, "bagType",
          () -> oppositeLinkBag.isEmbedded() ? "embedded" : "btree");
      appendDiagnosticField(sb, "bagSize", () -> Integer.toString(oppositeLinkBag.size()));
      appendDiagnosticField(sb, "bagPointer", () -> String.valueOf(oppositeLinkBag.getPointer()));
      appendDiagnosticField(sb, "bagContents", () -> dumpLinkBagContents(oppositeLinkBag));
    }
    appendDiagnosticField(sb, "diff", () -> Integer.toString(diff));
    return sb.append(']').toString();
  }

  /** Renders identity, version, new-ness, and dirty state of a record for diagnostics. */
  private static String describeEntityState(EntityImpl entity) {
    var identity = entity.getIdentity();
    return identity + " v" + entity.getVersion() + (identity.isNew() ? " NEW" : "")
        + " dirty=" + entity.isDirty();
  }

  /**
   * Lists the primary RIDs held by a back-reference bag, capped so a pathologically large
   * bag cannot produce an unbounded log line. The dropped count is reported when the cap is
   * hit so the reader still knows the true size.
   */
  private static String dumpLinkBagContents(LinkBag bag) {
    final var max = 64;
    var sb = new StringBuilder().append('[');
    var i = 0;
    for (var pair : bag) {
      if (i >= max) {
        sb.append(",...(").append(bag.size() - max).append(" more)");
        break;
      }
      if (i > 0) {
        sb.append(',');
      }
      sb.append(pair.primaryRid());
      i++;
    }
    return sb.append(']').toString();
  }

  /**
   * Appends {@code name=value} to the diagnostic buffer, substituting an error marker if
   * the value supplier throws. This keeps diagnostic gathering total: a corrupt bag or an
   * unloadable record degrades one field rather than masking the original error.
   */
  private static void appendDiagnosticField(
      StringBuilder sb, String name, Supplier<Object> valueSupplier) {
    sb.append(' ').append(name).append('=');
    try {
      sb.append(valueSupplier.get());
    } catch (Throwable t) {
      sb.append("<error:").append(t.getClass().getSimpleName()).append('>');
    }
  }

  private static void decrementLinkCounter(@Nonnull RecordIdInternal recordId,
      @Nonnull Map<RecordIdInternal, int[]> linksToUpdateMap) {
    linksToUpdateMap.compute(
        recordId,
        (key, value) -> {
          if (value == null) {
            return new int[] {-1};
          } else {
            value[0]--;
            if (value[0] == 0) {
              return null;
            }
            return value;
          }
        });
  }

  private static void incrementLinkCounter(@Nonnull RecordIdInternal recordId,
      @Nonnull Map<RecordIdInternal, int[]> linksToUpdateMap) {
    linksToUpdateMap.compute(
        recordId,
        (key, value) -> {
          if (value == null) {
            return new int[] {1};
          } else {
            value[0]++;
            return value;
          }
        });
  }

  private static void addToLinksContainer(Object value,
      Map<RecordIdInternal, int[]> links) {
    if (value == null) {
      return;
    }

    switch (value) {
      case Identifiable identifiable -> {
        if (identifiable instanceof Entity entity) {
          if (!entity.isEmbedded()) {
            incrementLinkCounter((RecordIdInternal) identifiable.getIdentity(), links);
          }
        } else {
          incrementLinkCounter((RecordIdInternal) identifiable.getIdentity(), links);
        }
      }
      case EntityLinkListImpl linkList -> {
        for (var link : linkList) {
          incrementLinkCounter((RecordIdInternal) link, links);
        }
      }
      case EntityLinkSetImpl linkSet -> {
        for (var link : linkSet) {
          incrementLinkCounter((RecordIdInternal) link, links);
        }
      }
      case EntityLinkMapIml linkMap -> {
        for (var link : linkMap.values()) {
          incrementLinkCounter((RecordIdInternal) link, links);
        }
      }
      case LinkBag linkBag -> {
        for (var link : linkBag) {
          incrementLinkCounter((RecordIdInternal) link.primaryRid(), links);
        }
      }
      default -> {
      }
    }
  }

  private static void subtractFromLinksContainer(Object value,
      Map<RecordIdInternal, int[]> links) {
    if (value == null) {
      return;
    }

    switch (value) {
      case Identifiable identifiable -> {
        if (identifiable instanceof Entity entity) {
          if (!entity.isEmbedded()) {
            decrementLinkCounter((RecordIdInternal) identifiable.getIdentity(), links);
          }
        } else {
          decrementLinkCounter((RecordIdInternal) identifiable.getIdentity(), links);
        }
      }
      case EntityLinkListImpl linkList -> {
        for (var link : linkList) {
          decrementLinkCounter((RecordIdInternal) link, links);
        }
      }
      case EntityLinkSetImpl linkSet -> {
        for (var link : linkSet) {
          decrementLinkCounter((RecordIdInternal) link, links);
        }
      }
      case EntityLinkMapIml linkMap -> {
        for (var link : linkMap.values()) {
          decrementLinkCounter((RecordIdInternal) link, links);
        }
      }
      case LinkBag linkBag -> {
        for (var link : linkBag) {
          decrementLinkCounter((RecordIdInternal) link.primaryRid(), links);
        }
      }
      default -> {
      }
    }

  }

  private static List<String> filterEdgeProperties(Collection<String> properties) {
    var result = new ArrayList<String>(properties.size());
    for (var property : properties) {
      if (!EdgeInternal.isEdgeConnectionProperty(property)) {
        result.add(property);
      }
    }

    return result;
  }

  private static List<String> filterVertexProperties(@Nonnull Collection<String> properties) {
    var result = new ArrayList<String>(properties.size());
    for (var property : properties) {
      if (!VertexEntityImpl.isConnectionToEdge(Direction.BOTH, property)) {
        result.add(property);
      }
    }

    return result;
  }

  public void disableLinkConsistencyCheck() {
    this.ensureLinkConsistency = false;
  }

  public void enableLinkConsistencyCheck() {
    this.ensureLinkConsistency = true;
  }

  /**
   * Whether the bidirectional-link-consistency check is currently enabled. A caller that disables
   * the check around a nested operation reads this first and restores the captured value afterward
   * rather than forcing the check back on, so a nested disable inside an outer disabled window is
   * preserved.
   */
  public boolean isLinkConsistencyEnabled() {
    return this.ensureLinkConsistency;
  }

  // --- Default methods migrated from DatabaseSessionEmbedded ---

  @SuppressWarnings("unused")
  public ResultSet computeSQLScript(String script, Object... args)
      throws CommandExecutionException, CommandScriptException {
    return computeScript("sql", script, args);
  }

  @SuppressWarnings("unused")
  public ResultSet computeGremlinScript(String script, Object... args)
      throws CommandExecutionException, CommandScriptException {
    return computeScript("gremlin", script, args);
  }

  public void executeScript(String language, String script, Object... args)
      throws CommandExecutionException, CommandScriptException {
    computeScript(language, script, args).close();
  }

  public void executeSQLScript(String script, Object... args)
      throws CommandExecutionException, CommandScriptException {
    executeScript("sql", script, args);
  }

  @SuppressWarnings("unused")
  public void executeGremlinScript(String script, Object... args)
      throws CommandExecutionException, CommandScriptException {
    executeScript("gremlin", script, args);
  }

  @SuppressWarnings("unused")
  public ResultSet computeSQLScript(String script, Map<String, ?> args)
      throws CommandExecutionException, CommandScriptException {
    return computeScript("sql", script, args);
  }

  @SuppressWarnings("unused")
  public ResultSet computeGremlinScript(String script, Map<String, ?> args) {
    return computeScript("gremlin", script, args);
  }

  public void executeScript(String language, String script, Map<String, ?> args)
      throws CommandExecutionException, CommandScriptException {
    computeScript(language, script, args).close();
  }

  @SuppressWarnings("unused")
  public void executeSQLScript(String script, Map<String, ?> args)
      throws CommandExecutionException, CommandScriptException {
    executeScript("sql", script, args);
  }

  @SuppressWarnings("unused")
  public void executeGremlinScript(String script, Map<String, ?> args)
      throws CommandExecutionException, CommandScriptException {
    executeScript("gremlin", script, args);
  }

  // --- Default methods migrated from DatabaseSessionEmbedded ---

  @SuppressWarnings("unused")
  @Nullable public <R> R transaction(Function<Transaction, R> action) {
    return computeInTx(action::apply);
  }

  public boolean isTxActive() {
    return activeTxCount() > 0;
  }

  // --- Default methods migrated from DatabaseSessionEmbedded ---

  @SuppressWarnings("unused")
  public void realClose() {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unused")
  public void reuse() {
    throw new UnsupportedOperationException();
  }

  @Nullable @SuppressWarnings("TypeParameterUnusedInFormals")
  public <RET extends DBRecord> RET loadOrNull(RID recordId) {
    try {
      return load(recordId);
    } catch (RecordNotFoundException e) {
      return null;
    }
  }

  @Nonnull
  public Entity loadEntity(RID id) throws DatabaseException, RecordNotFoundException {
    var record = load(id);
    if (record instanceof Entity element) {
      return element;
    }

    throw new DatabaseException(getDatabaseName(),
        "Record with id " + id + " is not an entity, but a "
            + record.getClass().getSimpleName());
  }

  @Nonnull
  public Vertex loadVertex(RID id) throws DatabaseException, RecordNotFoundException {
    var record = load(id);
    if (record instanceof Vertex vertex) {
      return vertex;
    }

    throw new DatabaseException(getDatabaseName(),
        "Record with id " + id + " is not a vertex, but a "
            + record.getClass().getSimpleName());
  }

  @Nonnull
  public Edge loadEdge(RID id) throws DatabaseException, RecordNotFoundException {
    var record = load(id);

    if (record instanceof Edge edge) {
      return edge;
    }

    throw new DatabaseException(getDatabaseName(),
        "Record with id " + id + " is not an edge, but a "
            + record.getClass().getSimpleName());
  }

  @Nonnull
  public Blob loadBlob(RID id) throws DatabaseException, RecordNotFoundException {
    var record = load(id);

    if (record instanceof Blob blob) {
      return blob;
    }

    throw new DatabaseException(getDatabaseName(),
        "Record with id " + id + " is not a blob, but a "
            + record.getClass().getSimpleName());
  }

  public Vertex newVertex() {
    return newVertex("V");
  }

  public Edge newEdge(Vertex from, Vertex to) {
    return newEdge(from, to, "E");
  }

  public void command(String query, Object... args)
      throws CommandSQLParsingException, CommandExecutionException {
    execute(query, args).close();
  }

  public void command(String query, @SuppressWarnings("rawtypes") Map args)
      throws CommandSQLParsingException, CommandExecutionException {
    execute(query, args).close();
  }

  public void command(
      com.jetbrains.youtrackdb.internal.core.sql.parser.SQLStatement statement,
      Map<?, ?> args) throws CommandSQLParsingException, CommandExecutionException {
    execute(statement, args).close();
  }

  public SchemaClassInternal getClassInternal(String className) {
    var schema = getMetadata().getSchemaInternal();
    return schema.getClassInternal(className);
  }

  public SchemaClass createVertexClass(String className) throws SchemaException {
    return createClass(className, SchemaClass.VERTEX_CLASS_NAME);
  }

  public SchemaClass createEdgeClass(String className) {
    var edgeClass = createClass(className, SchemaClass.EDGE_CLASS_NAME);

    edgeClass.createProperty(Edge.DIRECTION_IN, PropertyType.LINK);
    edgeClass.createProperty(Edge.DIRECTION_OUT, PropertyType.LINK);

    return edgeClass;
  }

  public SchemaClass getClass(String className) {
    var schema = getSchema();
    return schema.getClass(className);
  }

  public SchemaClass createClass(String className, String... superclasses)
      throws SchemaException {
    var schema = getSchema();
    SchemaClass[] superclassInstances = null;
    if (superclasses != null) {
      superclassInstances = new SchemaClass[superclasses.length];
      for (var i = 0; i < superclasses.length; i++) {
        var superclass = superclasses[i];
        var superClass = schema.getClass(superclass);
        if (superClass == null) {
          throw new SchemaException(
              getDatabaseName(), "Class " + superclass + " does not exist");
        }
        superclassInstances[i] = superClass;
      }
    }
    var result = schema.getClass(className);
    if (result != null) {
      throw new SchemaException(
          getDatabaseName(), "Class " + className + " already exists");
    }
    if (superclassInstances == null) {
      return schema.createClass(className);
    } else {
      return schema.createClass(className, superclassInstances);
    }
  }

  public SchemaClass createAbstractClass(String className, String... superclasses)
      throws SchemaException {
    var schema = getSchema();
    SchemaClass[] superclassInstances = null;
    if (superclasses != null) {
      superclassInstances = new SchemaClass[superclasses.length];
      for (var i = 0; i < superclasses.length; i++) {
        var superclass = superclasses[i];
        var superClass = schema.getClass(superclass);
        if (superClass == null) {
          throw new SchemaException(
              getDatabaseName(), "Class " + superclass + " does not exist");
        }
        superclassInstances[i] = superClass;
      }
    }
    var result = schema.getClass(className);
    if (result != null) {
      throw new SchemaException(
          getDatabaseName(), "Class " + className + " already exists");
    }
    if (superclassInstances == null) {
      return schema.createAbstractClass(className);
    } else {
      return schema.createAbstractClass(className, superclassInstances);
    }
  }

  public SchemaClass createClassIfNotExist(String className, String... superclasses)
      throws SchemaException {
    var schema = getSchema();

    var result = schema.getClass(className);
    if (result == null) {
      result = createClass(className, superclasses);
    }

    return result;
  }

  public Index getIndex(String indexName) {
    var indexManager = getSharedContext().getIndexManager();
    return indexManager.getIndex(indexName);
  }
}
