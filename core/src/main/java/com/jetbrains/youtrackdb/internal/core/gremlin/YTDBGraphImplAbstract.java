package com.jetbrains.youtrackdb.internal.core.gremlin;

import com.jetbrains.youtrackdb.api.exception.RecordNotFoundException;
import com.jetbrains.youtrackdb.api.gremlin.embedded.YTDBVertex;
import com.jetbrains.youtrackdb.internal.common.profiler.monitoring.YTDBQueryMetricsStrategy;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Entity;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Identifiable;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrackdb.internal.core.exception.CommandSQLParsingException;
import com.jetbrains.youtrackdb.internal.core.gremlin.io.YTDBIoRegistry;
import com.jetbrains.youtrackdb.internal.core.gremlin.sqlcommand.GremlinResultMapper;
import com.jetbrains.youtrackdb.internal.core.gremlin.sqlcommand.SqlCommandExecutionResult;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy.GremlinToMatchStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.translator.strategy.RepeatDeclineStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization.YTDBGraphCountStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization.YTDBGraphIoStepStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization.YTDBGraphMatchStepStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization.YTDBGraphStepStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization.YTDBOrderCollationStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization.YTDBOrderNullsStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization.YTDBOrderRidTieBreakStrategy;
import com.jetbrains.youtrackdb.internal.core.gremlin.traversal.strategy.optimization.YTDBStandardOrderSemanticsStrategy;
import com.jetbrains.youtrackdb.internal.core.id.RecordIdInternal;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.SchemaClass;
import com.jetbrains.youtrackdb.internal.core.sql.SQLEngine;
import com.jetbrains.youtrackdb.internal.core.sql.parser.DDLStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.ParseException;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLAlterSequenceStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBeginStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLCommitStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLDropSequenceStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLRollbackStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.TokenMgrError;
import com.jetbrains.youtrackdb.internal.core.sql.parser.YouTrackDBSql;
import com.jetbrains.youtrackdb.internal.core.util.CloseableIteratorWithCallback;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.function.FailableSupplier;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction.Status;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("resource")
public abstract class YTDBGraphImplAbstract implements YTDBGraphInternal, Consumer<Status> {

  public static void registerOptimizationStrategies(Class<? extends YTDBGraphImplAbstract> cls) {
    TraversalStrategies.GlobalCache.registerStrategies(
        cls,
        TraversalStrategies.GlobalCache.getStrategies(Graph.class)
            .clone()
            .addStrategies(
                // Position in this list is informational — the strategy resolver topologically
                // sorts by each strategy's applyPrior()/applyPost(). Tie-break names the translator
                // in applyPost(), so ORDER BY steps gain by(T.id) before translation. Strategies
                // below the translator name it in applyPrior() and become the decline fallback.
                // Collation applies through engine comparison after translation. OrderNulls waits
                // on the translator, record identifier tie-break, and standard-order strategy. Its
                // wrapping hits only native-decline order() steps after their final slots and
                // missing-key behavior are known. RepeatDeclineStrategy is the one
                // entry that is not a provider optimization. It is a decoration strategy, and
                // category ordering puts it before RepeatUnrollStrategy.
                RepeatDeclineStrategy.instance(),
                YTDBOrderRidTieBreakStrategy.instance(),
                GremlinToMatchStrategy.instance(),
                YTDBOrderCollationStrategy.instance(),
                YTDBGraphStepStrategy.instance(),
                YTDBGraphCountStrategy.instance(),
                YTDBGraphMatchStepStrategy.instance(),
                YTDBGraphIoStepStrategy.instance(),
                // Registered unconditionally: this list is built once per graph class for the
                // whole process, so a registration gated on configuration would freeze the
                // decision at first class load. The strategy reads the setting in its own apply().
                YTDBStandardOrderSemanticsStrategy.instance(),
                YTDBOrderNullsStrategy.instance(),
                YTDBQueryMetricsStrategy.instance()));
  }

  public static final Logger logger = LoggerFactory.getLogger(YTDBGraphImplAbstract.class);

  private final Features features;
  private final Configuration configuration;

  private final ThreadLocal<ThreadLocalState> threadLocalState = ThreadLocal.withInitial(
      () -> new ThreadLocalState(new YTDBTransaction(this)));

  public YTDBGraphImplAbstract(final Configuration configuration) {
    this.configuration = configuration;
    this.features = YouTrackDBFeatures.YTDBFeatures.INSTANCE;
  }

  @Override
  public Features features() {
    return features;
  }

  @Override
  public YTDBVertex addVertex(Object... keyValues) {
    var tx = tx();
    tx.readWrite();

    ElementHelper.legalPropertyKeyValueArray(keyValues);
    if (ElementHelper.getIdValue(keyValues).isPresent()) {
      throw Vertex.Exceptions.userSuppliedIdsNotSupported();
    }

    var label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
    var vertex = createVertexWithClass(tx.getDatabaseSession(), label);
    ElementHelper.attachProperties(vertex, keyValues);

    return vertex;
  }

  @Override
  public YTDBVertex addVertex(String label) {
    return this.addVertex(T.label, label);
  }

  private YTDBVertex createVertexWithClass(DatabaseSessionEmbedded sessionEmbedded, String label) {
    executeSchemaCode(session -> {
      // Read the class existence and vertex-type check off the lock-free immutable schema
      // snapshot rather than the lock-based shared schema, so this per-vertex-create hot path
      // never stalls behind a schema-carrying commit that holds the schema write lock for its
      // whole duration. The write path below is taken only when the snapshot reports the label
      // absent; getOrCreateClass re-checks under the write lock and is idempotent, so a snapshot
      // momentarily stale against a concurrent create still resolves correctly.
      // getImmutableSchemaSnapshot() is @Nullable: it returns null when the schema is not yet (or
      // no longer) materialized on the session. That does not happen on an open session in
      // production, but a null deref here would NPE rather than degrade gracefully, so a null
      // snapshot falls back to the lock-based shared schema for the existence and vertex-type
      // check. The two reads return different schema-class types (the snapshot's immutable view vs
      // the shared impl), so the existence and vertex-type outcomes are computed per branch rather
      // than bound to one cross-type variable.
      var snapshot = session.getMetadata().getImmutableSchemaSnapshot();
      final boolean exists;
      final boolean isVertexType;
      if (snapshot != null) {
        var vertexClass = snapshot.getClass(label);
        exists = vertexClass != null;
        isVertexType = exists && vertexClass.isVertexType();
      } else {
        var vertexClass = session.getSharedContext().getSchema().getClass(label);
        exists = vertexClass != null;
        isVertexType = exists && vertexClass.isVertexType();
      }

      if (!exists) {
        var schema = session.getSharedContext().getSchema();
        var vClass = schema.getClass(SchemaClass.VERTEX_CLASS_NAME);
        schema.getOrCreateClass(session, label, vClass);
      } else if (!isVertexType) {
        throw new IllegalArgumentException("Class " + label + " is not a vertex type");
      }
    });

    var transaction = sessionEmbedded.getActiveTransaction();
    var ytdbVertex = transaction.newVertex(label);

    return new YTDBVertexImpl(this, ytdbVertex);
  }

  @Override
  public <C extends GraphComputer> C compute(Class<C> graphComputerClass)
      throws IllegalArgumentException {
    throw new NotImplementedException();
  }

  @Override
  public GraphComputer compute() throws IllegalArgumentException {
    throw new NotImplementedException();
  }

  @Override
  public Iterator<Vertex> vertices(Object... vertexIds) {
    var tx = tx();
    tx.readWrite();
    return elements(
        tx.getDatabaseSession(),
        SchemaClass.VERTEX_CLASS_NAME,
        entity -> new YTDBVertexImpl(this,
            entity.asVertex()),
        vertexIds);
  }

  @Override
  public Iterator<Edge> edges(Object... edgeIds) {
    var tx = tx();
    tx.readWrite();

    return elements(
        tx.getDatabaseSession(),
        SchemaClass.EDGE_CLASS_NAME,
        entity -> new YTDBEdgeImpl(this, entity.asEdge()),
        edgeIds);
  }

  private static <A extends Element> Iterator<A> elements(
      DatabaseSessionEmbedded session, String elementClass, Function<Entity, A> toA,
      Object... elementIds) {
    var polymorphic = true;
    if (elementIds.length == 0) {
      // return all vertices as stream
      var itty = session.browseClass(elementClass, polymorphic);
      return IteratorUtils.map(itty, toA::apply);
    } else {
      var tx = session.getActiveTransaction();
      var ids = Stream.of(elementIds)
          .filter(Objects::nonNull) // looks like Gremlin allows nulls in here.
          .map(YTDBGraphImplAbstract::createRecordId);
      var entities =
          ids.filter(id -> ((RecordIdInternal) id).isValidPosition()).map(rid -> {
            try {
              return tx.loadEntity(rid);
            } catch (RecordNotFoundException e) {
              return null;
            }
          }).filter(Objects::nonNull);
      return entities.map(toA).iterator();
    }
  }

  private static RID createRecordId(Object id) {
    if (id instanceof RID rid) {
      return rid;
    }
    if (id instanceof String strRid) {
      return RecordIdInternal.fromString(strRid, false);
    }
    if (id instanceof Element gremlinElement) {
      return createRecordId(gremlinElement.id());
    }
    if (id instanceof Identifiable identifiable) {
      return identifiable.getIdentity();
    }

    throw new IllegalArgumentException(
        "YouTrackDB IDs have to be a String or RID - you provided a " + id.getClass());
  }

  @Override
  public YTDBTransaction tx() {
    return this.threadLocalState.get().transaction;
  }

  @Override
  public void executeSchemaCode(Consumer<DatabaseSessionEmbedded> code) {
    try (var session = acquireSession()) {
      code.accept(session);
    }
  }

  @Override
  public SqlCommandExecutionResult executeCommand(String sqlCommand, Map<?, ?> params) {
    if (sqlCommand == null || sqlCommand.isBlank()) {
      throw new IllegalArgumentException("Command cannot be null or empty");
    }

    var tx = tx();

    // When the transaction is open, use the cached parse path (statement cache +
    // originalStatement set for execution plan cache). When no transaction is active
    // (only possible for BEGIN), fall back to uncached parse.
    var statement = tx.isOpen()
        ? SQLEngine.parse(sqlCommand, tx.getDatabaseSession())
        : parseSqlUncached(sqlCommand);

    if (statement instanceof SQLBeginStatement) {
      if (!tx.isOpen()) {
        tx.readWrite();
      }
      return SqlCommandExecutionResult.unit();
    }

    if (statement instanceof SQLCommitStatement) {
      if (tx.isOpen()) {
        tx.commit();
      } else {
        throw new IllegalStateException("No active transaction to commit");
      }
      return SqlCommandExecutionResult.unit();
    }

    if (statement instanceof SQLRollbackStatement) {
      if (tx.isOpen()) {
        tx.rollback();
      } else {
        throw new IllegalStateException("No active transaction to rollback");
      }
      return SqlCommandExecutionResult.unit();
    }

    // Sequence DDL statements (ALTER SEQUENCE, DROP SEQUENCE) read sequence metadata internally,
    // which requires an active transaction. Route them through the regular tx-aware path (like
    // non-DDL statements) instead of the no-tx schema session used by other DDL.
    if (statement instanceof DDLStatement
        && !(statement instanceof SQLAlterSequenceStatement)
        && !(statement instanceof SQLDropSequenceStatement)) {
      // The traversal strategy evaluation (isPolymorphic) may have auto-opened
      // the Gremlin transaction via readWrite(), taking a snapshot BEFORE the
      // DDL runs. Commit it first so any pending work from prior g.command()
      // calls (e.g., CREATE SEQUENCE) is persisted. After the DDL runs on its
      // own schema session, the closed transaction forces the next operation to
      // start a fresh transaction with a snapshot that sees the DDL's changes.
      if (tx.isOpen()) {
        tx.commit();
      }
      try (var schemaSession = acquireSession()) {
        schemaSession.command(statement, params);
      }
      return SqlCommandExecutionResult.unit();
    }

    var session = tx.getDatabaseSession();
    var resultSet = session.execute(statement, params);
    var schema = session.getMetadata().getImmutableSchemaSnapshot();
    var mapped =
        IteratorUtils.map(resultSet, r -> GremlinResultMapper.toGremlinValue(this, schema, r));
    var closeable = new EagerCloseIterator<>(mapped, resultSet::close);
    return SqlCommandExecutionResult.results(closeable);
  }

  /// Iterator that closes the underlying resource eagerly on last element or on error,
  /// so the ResultSet doesn't survive past transaction teardown.
  private static final class EagerCloseIterator<T> extends CloseableIteratorWithCallback<T> {

    EagerCloseIterator(Iterator<T> underlying, Runnable onClose) {
      super(underlying, onClose);
    }

    @Override
    public T next() {
      try {
        var value = super.next();
        if (!hasNext()) {
          close();
        }
        return value;
      } catch (RuntimeException e) {
        close();
        throw e;
      }
    }
  }

  /// Uncached YQL parse — used only when no database session is available (e.g., BEGIN
  /// before a transaction is open). All other paths use [SQLEngine#parse] which goes
  /// through [YqlStatementCache] and sets [SQLStatement#originalStatement] for
  /// [YqlExecutionPlanCache].
  private static SQLStatement parseSqlUncached(String command) {
    SQLStatement statement;
    try {
      var is = new ByteArrayInputStream(command.getBytes(StandardCharsets.UTF_8));
      var parser = new YouTrackDBSql(is);
      statement = parser.parse();
    } catch (ParseException | TokenMgrError e) {
      throw new CommandSQLParsingException("Error on parsing command: " + command,
          String.valueOf(e));
    } catch (Exception e) {
      throw new CommandExecutionException("Error on executing command: " + command,
          String.valueOf(e));
    }
    return statement;
  }

  @Override
  public Variables variables() {
    throw new NotImplementedException();
  }

  @Override
  public Configuration configuration() {
    return configuration;
  }

  @Override
  public void close() {
    var threadLocalState = this.threadLocalState.get();

    var tx = threadLocalState.transaction;
    if (tx.isOpen()) {
      tx.close();
    }

    var session = threadLocalState.sessionEmbedded;
    if (session != null) {
      session.close();
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public <I extends Io> I io(Io.Builder<I> builder) {
    //noinspection unchecked
    return (I) YTDBGraphInternal.super.io(
        builder.onMapper(mb -> mb.addRegistry(YTDBIoRegistry.instance())));
  }

  @Override
  public String toString() {
    return YTDBGraph.class.getSimpleName() + "[" + configuration.getString(
        YTDBGraphFactory.CONFIG_DB_NAME) + "]";
  }

  @SuppressWarnings("resource")
  public DatabaseSessionEmbedded getUnderlyingDatabaseSession() {
    var threadLocalState = this.threadLocalState.get();
    var currentSession = threadLocalState.sessionEmbedded;

    if (currentSession != null) {
      if (!currentSession.isTxActive()) {
        tx().addTransactionListener(this);
      }

      return currentSession;
    }

    currentSession = acquireSession();
    tx().addTransactionListener(this);

    threadLocalState.sessionEmbedded = currentSession;
    return currentSession;
  }

  @Override
  public void accept(Status status) {
    var threadLocalState = this.threadLocalState.get();
    var currentSession = threadLocalState.sessionEmbedded;
    if (currentSession == null) {
      return;
    }

    if (currentSession.isTxActive()) {
      throw new IllegalStateException("Transaction is still active");
    }

    // Null the field before closing so that cleanup code (e.g., withSuspendedTransaction)
    // never attempts a second close on the same session if close() throws.
    threadLocalState.sessionEmbedded = null;
    currentSession.close();
  }

  @Override
  public void backup(Supplier<Iterator<String>> ibuFilesSupplier,
      Function<String, InputStream> ibuInputStreamSupplier,
      Function<String, OutputStream> ibuOutputStreamSupplier,
      Consumer<String> ibuFileRemover) {
    try (var session = acquireSession()) {
      session.backup(ibuFilesSupplier, ibuInputStreamSupplier, ibuOutputStreamSupplier,
          ibuFileRemover);
    }
  }

  @Override
  public String backup(Path path) {
    try (var session = acquireSession()) {
      return session.backup(path);
    }
  }

  @Override
  public String fullBackup(Path path) {
    try (var session = acquireSession()) {
      return session.fullBackup(path);
    }
  }

  @Override
  public UUID uuid() {
    try (var session = acquireSession()) {
      return session.uuid();
    }
  }

  @Override
  public <T, X extends Exception> T withSuspendedTransaction(
      @Nonnull FailableSupplier<T, X> block) throws X {
    // Save the entire thread-local state and replace it with a fresh one.
    // The outer state is fully isolated — the lambda gets its own transaction,
    // session, and listener set.
    var savedState = threadLocalState.get();
    var innerState = new ThreadLocalState(new YTDBTransaction(this));
    threadLocalState.set(innerState);
    try {
      return block.get();
    } finally {
      try {
        // Clean up any transaction/session left open by the lambda.
        // Using tx().rollback() goes through the normal TinkerPop path:
        // doRollback() nulls activeSession, fireOnRollback() calls accept(Status)
        // which closes sessionEmbedded. This handles both in a single call.
        // The isOpen() + rollback() is wrapped in a single try-catch because isOpen()
        // itself can throw if the lambda left the session in a bad state (e.g., manually
        // closed the session without going through tx().rollback()).
        try {
          if (innerState.transaction.isOpen()) {
            logger.warn(
                "withSuspendedTransaction: lambda left an open transaction; rolling back");
            innerState.transaction.rollback();
          }
        } catch (Exception e) {
          logger.warn("withSuspendedTransaction: error during leftover tx rollback", e);
        }

        // If the lambda acquired a session without opening a transaction (e.g., via
        // getUnderlyingDatabaseSession()), sessionEmbedded is non-null but the rollback
        // above didn't touch it. Close it directly.
        if (innerState.sessionEmbedded != null) {
          try {
            innerState.sessionEmbedded.close();
          } catch (Exception e) {
            logger.warn("withSuspendedTransaction: error closing leftover session", e);
          }
        }
      } finally {
        // Restore the suspended state regardless of cleanup outcome.
        threadLocalState.set(savedState);
      }
    }
  }

  @Override
  public abstract boolean isOpen();

  public abstract DatabaseSessionEmbedded acquireSession();

  private static final class ThreadLocalState {

    @Nullable private DatabaseSessionEmbedded sessionEmbedded;
    private final YTDBTransaction transaction;

    private ThreadLocalState(YTDBTransaction transaction) {
      this.transaction = transaction;
    }
  }
}
