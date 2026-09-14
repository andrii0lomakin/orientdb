package com.jetbrains.youtrackdb.internal.core.gremlin;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversal;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import com.jetbrains.youtrackdb.internal.common.profiler.monitoring.QueryMetricsListener;
import com.jetbrains.youtrackdb.internal.common.profiler.monitoring.QueryMonitoringMode;
import com.jetbrains.youtrackdb.internal.common.profiler.monitoring.TransactionMetricsListener;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.commons.lang3.function.FailableFunction;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class YTDBTransaction extends AbstractTransaction {

  private static final Logger logger = LoggerFactory.getLogger(YTDBTransaction.class);

  private Consumer<Transaction> readWriteConsumerInternal = READ_WRITE_BEHAVIOR.AUTO;
  private Consumer<Transaction> closeConsumerInternal = CLOSE_BEHAVIOR.ROLLBACK;

  private final CopyOnWriteArraySet<Consumer<Status>> transactionListeners =
      new CopyOnWriteArraySet<>();
  private final YTDBGraphImplAbstract graph;
  private DatabaseSessionEmbedded activeSession;

  // Query monitoring
  private QueryMonitoringMode queryMonitoringMode = QueryMonitoringMode.LIGHTWEIGHT;
  private String trackingId;
  private QueryMetricsListener queryMetricsListener = QueryMetricsListener.NO_OP;
  private TransactionMetricsListener transactionMetricsListener = TransactionMetricsListener.NO_OP;

  public YTDBTransaction(YTDBGraphImplAbstract graph) {
    super(graph);
    this.graph = graph;
  }

  public static <X extends Exception> void executeInTX(
      FailableConsumer<YTDBGraphTraversalSource, X> code, YTDBGraphTraversalSource g) throws X {
    var ok = false;
    var tx = g.tx();
    try {
      code.accept(tx.begin(YTDBGraphTraversalSource.class));
      ok = true;
    } finally {
      finishTx(ok, tx);
    }
  }

  public static <X extends Exception> void executeInTX(
      FailableFunction<YTDBGraphTraversalSource, YTDBGraphTraversal<?, ?>, X> code,
      YTDBGraphTraversalSource g) throws X {
    var ok = false;
    var tx = g.tx();
    try {
      var traversal = code.apply(tx.begin(YTDBGraphTraversalSource.class));
      traversal.iterate();
      ok = true;
    } finally {
      finishTx(ok, tx);
    }
  }

  public static <X extends Exception, R> R computeInTx(
      FailableFunction<YTDBGraphTraversalSource, R, X> code, YTDBGraphTraversalSource g) throws X {
    var ok = false;
    R result;
    var tx = g.tx();
    try {
      result = code.apply(tx.begin(YTDBGraphTraversalSource.class));
      ok = true;
    } finally {
      finishTx(ok, tx);
    }
    return result;
  }

  // Commit-time exceptions (e.g. ConcurrentModificationException from MVCC) are
  // rethrown so callers can react — retry, surface an error, etc. Rollback
  // failures are logged and swallowed: when the body throws, its exception is
  // what the caller needs to see, and rethrowing a secondary rollback failure
  // from the finally block would mask it.
  private static void finishTx(boolean ok, Transaction tx) {
    if (!tx.isOpen()) {
      return;
    }
    if (ok) {
      tx.commit();
    } else {
      try {
        tx.rollback();
      } catch (Exception e) {
        logger.error("Failed to rollback transaction", e);
      }
    }
  }

  @Override
  public boolean isOpen() {
    if (activeSession != null) {
      return activeSession.isTxActive();
    }

    return false;
  }

  @Override
  public Transaction onReadWrite(Consumer<Transaction> consumer) {
    this.readWriteConsumerInternal =
        Optional.ofNullable(consumer)
            .orElseThrow(Exceptions::onReadWriteBehaviorCannotBeNull);
    return this;
  }

  @Override
  public Transaction onClose(Consumer<Transaction> consumer) {
    this.closeConsumerInternal =
        Optional.ofNullable(consumer)
            .orElseThrow(Exceptions::onReadWriteBehaviorCannotBeNull);
    return this;
  }

  @Override
  public void addTransactionListener(Consumer<Status> listener) {
    transactionListeners.add(listener);
  }

  @Override
  public void removeTransactionListener(Consumer<Status> listener) {
    transactionListeners.remove(listener);
  }

  @Override
  public void clearTransactionListeners() {
    transactionListeners.clear();
  }

  @Override
  protected void doClose() {
    closeConsumerInternal.accept(this);
  }

  @Override
  protected void doReadWrite() {
    readWriteConsumerInternal.accept(this);
  }

  @Override
  protected void doOpen() {
    var ok = false;
    try {
      activeSession = graph.getUnderlyingDatabaseSession();
      activeSession.begin();
      ok = true;
    } finally {
      if (!ok) {
        activeSession = null;
      }
    }
  }

  @Override
  protected void doCommit() throws TransactionException {
    if (activeSession != null) {
      try {
        if (isTransactionMetricsEnabled()) {
          activeSession.monitoredCommit(
              transactionMetricsListener,
              queryMonitoringMode,
              getTrackingId());
        } else {
          activeSession.commit();
        }
      } finally {
        activeSession = null;
      }
    }
  }

  @Override
  protected void doRollback() throws TransactionException {
    if (activeSession != null) {
      try {
        activeSession.rollback();
      } finally {
        activeSession = null;
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public YTDBGraphTraversalSource begin() {
    return new YTDBGraphTraversalSource(graph);
  }

  @Override
  protected void fireOnCommit() {
    this.transactionListeners.forEach(c -> c.accept(Status.COMMIT));
    clearMonitoringState();
  }

  @Override
  protected void fireOnRollback() {
    this.transactionListeners.forEach(c -> c.accept(Status.ROLLBACK));
    clearMonitoringState();
  }

  private void clearMonitoringState() {
    this.trackingId = null;
    this.queryMonitoringMode = QueryMonitoringMode.LIGHTWEIGHT;
    this.queryMetricsListener = QueryMetricsListener.NO_OP;
    this.transactionMetricsListener = TransactionMetricsListener.NO_OP;
  }

  public DatabaseSessionEmbedded getDatabaseSession() {
    if (activeSession == null) {
      throw new IllegalStateException("Transaction is not active");
    }

    return activeSession;
  }

  /// Set the tracking ID for this transaction that can be obtained from the QueryDetails inside the
  /// listener. If not set, YTDB will generate its own tracking ID.
  public YTDBTransaction withTrackingId(@Nonnull String trackingId) {
    Objects.requireNonNull(trackingId);
    this.trackingId = trackingId;
    return this;
  }

  /// Set the mode under which the listener will operate. Lightweight mode uses approximate
  /// timestamps and lower precision of durations. Exact mode uses precise timestamp values but is
  /// heavier performance-wise.
  public YTDBTransaction withQueryMonitoringMode(@Nonnull QueryMonitoringMode mode) {
    Objects.requireNonNull(mode);
    this.queryMonitoringMode = mode;
    return this;
  }

  /// Register a query metrics listener for this transaction. Supported only when YouTrackDB is run
  /// in embedded mode.
  public YTDBTransaction withQueryListener(@Nonnull QueryMetricsListener listener) {
    Objects.requireNonNull(listener);
    this.queryMetricsListener = listener;
    return this;
  }

  public boolean isQueryMetricsEnabled() {
    return queryMetricsListener != null && queryMetricsListener != QueryMetricsListener.NO_OP;
  }

  public @Nonnull QueryMonitoringMode getQueryMonitoringMode() {
    return queryMonitoringMode;
  }

  public @Nonnull String getTrackingId() {
    return trackingId != null ? trackingId
        : String.valueOf(getDatabaseSession().getActiveTransaction().getId());
  }

  public QueryMetricsListener getQueryMetricsListener() {
    return queryMetricsListener;
  }

  /// Register a metrics listener for this transaction. Supported only when YouTrackDB is run in
  /// embedded mode.
  public YTDBTransaction withTransactionListener(@Nonnull TransactionMetricsListener listener) {
    Objects.requireNonNull(listener);
    this.transactionMetricsListener = listener;
    return this;
  }

  public boolean isTransactionMetricsEnabled() {
    return transactionMetricsListener != null
        && transactionMetricsListener != TransactionMetricsListener.NO_OP;
  }
}
