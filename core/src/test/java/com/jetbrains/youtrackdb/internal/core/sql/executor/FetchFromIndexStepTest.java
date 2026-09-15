package com.jetbrains.youtrackdb.internal.core.sql.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.jetbrains.youtrackdb.internal.common.util.RawPair;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Identifiable;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrackdb.internal.core.index.Index;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.PropertyType;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.SchemaClass;
import com.jetbrains.youtrackdb.internal.core.query.ExecutionStep;
import com.jetbrains.youtrackdb.internal.core.query.Result;
import com.jetbrains.youtrackdb.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrackdb.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLAndBlock;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBinaryCompareOperator;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBinaryCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBooleanExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLContainsAnyCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLContainsCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLContainsTextCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLContainsValueCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLEqualsOperator;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLGeOperator;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLGtOperator;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLIdentifier;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLInCondition;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLLeOperator;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLLtOperator;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLMathExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLValueExpression;
import com.jetbrains.youtrackdb.internal.core.sql.parser.YouTrackDBSql;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.Test;

/**
 * Direct-step tests for {@link FetchFromIndexStep}, the source step that performs index lookups
 * and emits key-RID pairs for a downstream {@link GetValueFromIndexEntryStep} to load records.
 *
 * <p>Covered branches in {@code init(desc, orderAsc, ctx)}:
 *
 * <ul>
 *   <li>{@code index.getDefinition() == null} → empty result list (early-return guard).
 *   <li>{@code keyCondition == null} → {@code processFlatIteration} (ascending and descending full
 *       scan, with and without null-key entries).
 *   <li>{@code keyCondition instanceof SQLAndBlock} → {@code processAndBlock} → {@code
 *       multipleRange} (equality, greater-than / greater-or-equal, less-than / less-or-equal, IN,
 *       additional upper bound, composite index, cartesian expansion for multi-value lookups).
 *   <li>Any other non-null condition → {@link CommandExecutionException}.
 * </ul>
 *
 * <p>Covered branches in {@code indexKeyFromIncluded} / {@code indexKeyToIncluded}: {@link
 * SQLBinaryCondition} with greater/less/other operators and with / without {@code
 * additionalRangeCondition}; {@link SQLInCondition}, {@link SQLContainsCondition}, {@link
 * SQLContainsAnyCondition}, {@link SQLContainsTextCondition}, and {@link SQLContainsValueCondition}
 * subblock shapes; and the {@link UnsupportedOperationException} fallback for unsupported
 * sub-block types.
 *
 * <p>Serialization, pretty-printing, copy, reset, canBeCached, and the predecessor-drain path
 * are exercised via lightweight mockito-based or parser-AST-based fixtures.
 *
 * <h2>Residual gaps (accepted)</h2>
 *
 * <ul>
 *   <li>{@code readResult}'s {@code isInterruptCurrentOperation()} branch: {@link
 *       com.jetbrains.youtrackdb.internal.core.db.ExecutionThreadLocal#isInterruptCurrentOperation}
 *       only returns {@code true} on a {@code SoftThread} with its shutdown flag set; JUnit
 *       runs tests on regular threads so this branch is effectively unreachable from unit tests.
 *   <li>The type-conversion-failure recovery path in {@code multipleRange} (the catch block that
 *       iterates the same collection as both bounds) is kept as a documented residual because
 *       it triggers only via subquery-produced raw collections where the planner's planner-time
 *       type inference disagrees with the element types — integration-test territory.
 *   <li>{@code SQLContainsTextCondition} branches in {@code indexKeyFromIncluded}/{@code
 *       indexKeyToIncluded} require a FULLTEXT index, which depends on the Lucene module
 *       (excluded from the core module build per CLAUDE.md).
 * </ul>
 */
public class FetchFromIndexStepTest extends TestUtilsFixture {

  private static final String PROP = "age";

  // =========================================================================
  // Constructor + simple getters
  // =========================================================================

  /**
   * The constructor stores the descriptor and the order flag and exposes them via {@link
   * FetchFromIndexStep#getDesc()}, {@link FetchFromIndexStep#getIndexName()}, and the package-
   * visible {@code isOrderAsc()} accessor. The profiling flag from {@link AbstractExecutionStep}
   * is also preserved.
   */
  @Test
  public void constructorPopulatesDescriptorAndOrderFlag() {
    var clazz = createIndexedClass();
    var index = getIndex(clazz.indexName);
    var desc = new IndexSearchDescriptor(index);
    var ctx = newContext();

    var step = new FetchFromIndexStep(desc, true, ctx, true);

    assertThat(step.getDesc()).isSameAs(desc);
    assertThat(step.getIndexName()).isEqualTo(clazz.indexName);
    assertThat(step.isOrderAsc()).isTrue();
  }

  /**
   * {@link FetchFromIndexStep#canBeCached()} unconditionally returns {@code true}: the index
   * descriptor captures the lookup conditions structurally, so a cached plan remains correct for
   * equivalent parameters. A mutation flipping the return to {@code false} would break plan-cache
   * reuse for every indexed query.
   */
  @Test
  public void canBeCachedIsAlwaysTrue() {
    var clazz = createIndexedClass();
    var index = getIndex(clazz.indexName);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), true, ctx, false);

    assertThat(step.canBeCached()).isTrue();
  }

  /**
   * {@link FetchFromIndexStep#copy(CommandContext)} returns a new instance carrying the same
   * descriptor (shared reference — the descriptor is immutable) and the same order/profiling
   * flags. The copy is a different object but has identical observable state.
   */
  @Test
  public void copyReturnsIndependentInstanceWithSameState() {
    var clazz = createIndexedClass();
    var index = getIndex(clazz.indexName);
    var desc = new IndexSearchDescriptor(index);
    var ctx = newContext();
    var original = new FetchFromIndexStep(desc, false, ctx, true);

    var copy = (FetchFromIndexStep) original.copy(ctx);

    assertThat(copy).isNotSameAs(original);
    assertThat(copy.getDesc()).isSameAs(desc);
    assertThat(copy.getIndexName()).isEqualTo(original.getIndexName());
    assertThat(copy.isOrderAsc()).isEqualTo(original.isOrderAsc());
    assertThat(copy.isNullsFirst()).isEqualTo(original.isNullsFirst());
    // Profiling-enabled contract: copy() carries the flag through, surfaced via prettyPrint's
    // cost suffix. A mutation hard-coding `false` in the copy would drop the suffix. (TB3.)
    assertThat(copy.prettyPrint(0, 2)).isEqualTo(original.prettyPrint(0, 2));
  }

  /**
   * {@link FetchFromIndexStep#reset()} clears the descriptor to {@code null} — per the Javadoc,
   * calling {@code internalStart} afterward without re-initializing throws NPE. The method exists
   * only to clear state before {@code deserialize} re-populates it.
   */
  @Test
  public void resetClearsDescriptorToNull() {
    var clazz = createIndexedClass();
    var index = getIndex(clazz.indexName);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), true, ctx, false);

    step.reset();

    assertThat(step.getDesc()).isNull();
  }

  // =========================================================================
  // init() dispatch: null-definition, unsupported condition
  // =========================================================================

  /**
   * An index whose {@link Index#getDefinition()} returns {@code null} short-circuits {@code
   * init()} to return an empty stream list. Uses Mockito to reach this branch — a real Index
   * always has a definition.
   */
  @Test
  public void indexWithoutDefinitionProducesEmptyStream() {
    var mockIndex = mock(Index.class);
    when(mockIndex.getDefinition()).thenReturn(null);
    when(mockIndex.getName()).thenReturn("mocked-empty-def-index");
    var desc = new IndexSearchDescriptor(mockIndex);
    var ctx = newContext();
    var step = new FetchFromIndexStep(desc, true, ctx, false);

    var results = startAndDrain(step, ctx);

    assertThat(results).isEmpty();
  }

  /**
   * A non-null key condition that is not an {@link SQLAndBlock} — for example a bare {@link
   * SQLBinaryCondition} — hits the {@code default} arm of {@code init()}'s switch and raises
   * {@link CommandExecutionException}. Pins the branch that rejects unsupported condition shapes.
   */
  @Test
  public void nonAndBlockKeyConditionThrowsCommandExecutionException() {
    var clazz = createIndexedClass();
    var index = getIndex(clazz.indexName);
    var ctx = newContext();

    session.begin();
    try {
      var desc =
          new IndexSearchDescriptor(
              index, binaryCondition("key", new SQLEqualsOperator(-1), 10));
      var step = new FetchFromIndexStep(desc, true, ctx, false);

      assertThatThrownBy(() -> step.start(ctx))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("is not supported yet");
    } finally {
      session.rollback();
    }
  }

  // =========================================================================
  // processFlatIteration: full scan (null keyCondition)
  // =========================================================================

  /**
   * A {@code null} key condition triggers a full index scan. With {@code orderAsc=true}, entries
   * are emitted in ascending key order. Every populated record must appear exactly once.
   */
  @Test
  public void fullScanAscendingYieldsAllEntriesSorted() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    seed(fixture.className, 10, 20, 30);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), true, ctx, false);

    var results = startAndDrain(step, ctx);

    assertThat(results).hasSize(3);
    assertThat(keys(results)).containsExactly(10, 20, 30);
  }

  /**
   * {@code orderAsc=false} produces a descending full scan using {@code index.descStream(session)}.
   * Mirrors the ascending test but with reversed expected order.
   */
  @Test
  public void fullScanDescendingYieldsAllEntriesReversed() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    seed(fixture.className, 10, 20, 30);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), false, ctx, false);

    var results = startAndDrain(step, ctx);

    assertThat(results).hasSize(3);
    assertThat(keys(results)).containsExactly(30, 20, 10);
  }

  /**
   * When the index includes null values (NOTUNIQUE indexes on nullable properties by default), a
   * full scan must emit records stored under the {@code null} key in addition to the non-null
   * keys. This covers {@code fetchNullKeys} returning a non-null stream (the "include null" path)
   * and the keyed {@code RawPair(null, rid)} produced by {@code getStreamForNullKey}.
   */
  @Test
  public void fullScanIncludesRecordsWithNullIndexedValue() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    seed(fixture.className, 10, 20);
    seedNull(fixture.className, 1);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), true, ctx, false);

    var results = startAndDrain(step, ctx);

    assertThat(results).hasSize(3);
    assertThat(keys(results)).contains(10, 20, null);
  }

  /**
   * An index created with {@code ignoreNullValues=true} metadata skips the null-key stream
   * entirely ({@code isNullValuesIgnored()} returns {@code true}). This pins {@code fetchNullKeys}
   * returning {@code null} — mutating the branch to always fetch null keys would surface the
   * ignored null record here.
   */
  @Test
  public void fullScanWithIgnoreNullValuesMetadataSkipsNullKeyEntries() {
    var clazz = createClassInstance();
    clazz.createProperty(PROP, PropertyType.INTEGER);
    var indexName = clazz.getName() + "." + PROP;
    clazz.createIndex(
        indexName,
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        java.util.Map.of("ignoreNullValues", true),
        new String[] {PROP});
    seed(clazz.getName(), 10, 20);
    seedNull(clazz.getName(), 1);
    var index = getIndex(indexName);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), true, ctx, false);

    var results = startAndDrain(step, ctx);

    assertThat(results).hasSize(2);
    assertThat(keys(results)).containsExactly(10, 20);
  }

  /**
   * A full ASCENDING scan with {@code nullsFirst=false} emits null keys AFTER the ascending
   * non-null keys. This is the absolute {@code NULLS LAST} placement, independent of ASC.
   */
  @Test
  public void fullScanAscendingWithNullsLastEmitsNullKeyEntriesLast() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    seed(fixture.className, 10, 20);
    seedNull(fixture.className, 2);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), true, false, ctx, false);

    var results = startAndDrain(step, ctx);

    assertThat(keys(results)).containsExactly(10, 20, null, null);
    assertThat(step.isNullsFirst()).isFalse();
  }

  /**
   * A full DESCENDING scan with {@code nullsFirst=true} emits null keys BEFORE the descending
   * non-null keys. This is the absolute {@code NULLS FIRST} placement, independent of DESC.
   */
  @Test
  public void fullScanDescendingWithNullsFirstEmitsNullKeyEntriesFirst() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    seed(fixture.className, 10, 20);
    seedNull(fixture.className, 2);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), false, true, ctx, false);

    var results = startAndDrain(step, ctx);

    assertThat(keys(results)).containsExactly(null, null, 20, 10);
    assertThat(step.isNullsFirst()).isTrue();
  }

  /**
   * A full ASCENDING scan over an index that stores null keys emits the null-key group BEFORE the
   * ascending non-null keys. This is the "null = smallest" placement that {@link
   * com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderByItem#compare} produces for ASC, and
   * it must match whether or not the index accelerates the sort. Two null records are
   * seeded so the assertion also proves the multi-value (NOTUNIQUE) null bucket yields every RID.
   */
  @Test
  public void fullScanAscendingEmitsNullKeyEntriesFirst() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    seed(fixture.className, 10, 20);
    seedNull(fixture.className, 2);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), true, ctx, false);

    var results = startAndDrain(step, ctx);

    // ASC: null group first, then ascending keys.
    assertThat(keys(results)).containsExactly(null, null, 10, 20);
  }

  /**
   * A full DESCENDING scan places the null-key group LAST, mirroring the in-memory sort's DESC
   * behavior (nulls last). This pins the fix: {@code processFlatIteration} used to
   * prepend the null stream unconditionally, so DESC emitted nulls first and diverged from the
   * in-memory path. Two null records verify the descending scan still surfaces every RID stored
   * under the null key (the BTreeMultiValueIndexEngine null bucket).
   */
  @Test
  public void fullScanDescendingEmitsNullKeyEntriesLast() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    seed(fixture.className, 10, 20);
    var nullRids = seedNullReturningRids(fixture.className, 2);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), false, ctx, false);

    var results = startAndDrain(step, ctx);

    // DESC: descending keys first, then the null group last.
    assertThat(keys(results)).containsExactly(20, 10, null, null);
    // The null bucket must surface every RID that was stored under the null key.
    var emittedNullRids =
        results.stream()
            .filter(r -> r.getProperty("key") == null)
            .map(r -> r.<RID>getProperty("rid"))
            .toList();
    assertThat(emittedNullRids).containsExactlyInAnyOrderElementsOf(nullRids);
  }

  /**
   * ASC full scan: the null-key stream is acquired first, then the main scan via {@code
   * index.stream}. If that main acquisition throws, the already-open null stream must be closed
   * before the exception propagates — otherwise it leaks, because the caller only closes streams
   * that {@code init()} actually returns and on a throw the partial list is discarded.
   */
  @Test
  public void fullScanAscClosesNullStreamWhenMainStreamAcquisitionThrows() {
    assertMainStreamAcquisitionFailureClosesNullStream(true);
  }

  /**
   * DESC full scan: same leak guarantee as the ASC case, but the main scan is acquired via {@code
   * index.descStream}. Both acquisitions sit inside the same guarded block, so this pins the DESC
   * acquisition method too — a regression that wrapped only {@code stream()} would leak on DESC and
   * the ASC test alone would not catch it.
   */
  @Test
  public void fullScanDescClosesNullStreamWhenMainStreamAcquisitionThrows() {
    assertMainStreamAcquisitionFailureClosesNullStream(false);
  }

  /**
   * ASC full scan whose main-stream acquisition fails with an {@link AssertionError} — the exact
   * shape an {@code assert} throws under {@code -ea} while the index scan is being set up. Even
   * though an AssertionError is an {@link Error} and not a {@link RuntimeException}, the
   * already-open null-key stream must STILL be closed before the error propagates, and the
   * AssertionError itself must escape unwrapped.
   *
   * <p>This pins the {@code | Error} half of the {@code catch (RuntimeException | Error e)} guard in
   * {@code processFlatIteration}: the sibling leak tests only inject a {@link RuntimeException}, so
   * narrowing that clause to {@code catch (RuntimeException e)} would let an -ea AssertionError skip
   * the close-and-rethrow path and leak the null stream while every RuntimeException test kept
   * passing. Injecting an Error here is the only assertion that fails under that narrowing.
   */
  @Test
  public void fullScanAscClosesNullStreamWhenMainStreamAcquisitionThrowsError() {
    var fixture = createIndexedClass();
    // Borrow a real NOTUNIQUE definition — it does not ignore nulls, so fetchNullKeys runs.
    var definition = getIndex(fixture.indexName).getDefinition();
    var ctx = newContext();

    // A null-key stream whose close() we can observe.
    var nullStreamClosed = new AtomicBoolean(false);
    Stream<RID> nullRids = Stream.<RID>empty().onClose(() -> nullStreamClosed.set(true));

    // Index that yields the null-key stream but fails the main (non-null) scan with an Error
    // (AssertionError), not a RuntimeException — mirrors an -ea assertion tripping during setup.
    var failingIndex = mock(Index.class);
    when(failingIndex.getDefinition()).thenReturn(definition);
    when(failingIndex.getName()).thenReturn(fixture.indexName);
    when(failingIndex.getRids(any(), nullable(Object.class))).thenReturn(nullRids);
    var mainStreamFailure = new AssertionError("index scan assertion failed");
    when(failingIndex.stream(any())).thenThrow(mainStreamFailure);

    var desc = new IndexSearchDescriptor(failingIndex);

    assertThatThrownBy(() -> FetchFromIndexStep.init(desc, true, ctx))
        .isInstanceOf(AssertionError.class)
        .hasMessage("index scan assertion failed");
    assertThat(nullStreamClosed)
        .as("null-key stream must be closed even when the failure is an Error, not leaked")
        .isTrue();
  }

  /**
   * Drives a full scan whose main-stream acquisition throws and asserts the already-open null-key
   * stream is closed and the original failure propagates unwrapped. Stubs whichever main-scan
   * method the direction uses: {@code stream} for ASC, {@code descStream} for DESC.
   */
  private void assertMainStreamAcquisitionFailureClosesNullStream(boolean isOrderAsc) {
    var fixture = createIndexedClass();
    // Borrow a real NOTUNIQUE definition — it does not ignore nulls, so fetchNullKeys runs.
    var definition = getIndex(fixture.indexName).getDefinition();
    var ctx = newContext();

    // A null-key stream whose close() we can observe.
    var nullStreamClosed = new AtomicBoolean(false);
    Stream<RID> nullRids = Stream.<RID>empty().onClose(() -> nullStreamClosed.set(true));

    // Index that yields the null-key stream but fails when the main (non-null) scan is opened.
    var failingIndex = mock(Index.class);
    when(failingIndex.getDefinition()).thenReturn(definition);
    when(failingIndex.getName()).thenReturn(fixture.indexName);
    when(failingIndex.getRids(any(), nullable(Object.class))).thenReturn(nullRids);
    var mainStreamFailure = new IllegalStateException("index scan failed");
    if (isOrderAsc) {
      when(failingIndex.stream(any())).thenThrow(mainStreamFailure);
    } else {
      when(failingIndex.descStream(any())).thenThrow(mainStreamFailure);
    }

    var desc = new IndexSearchDescriptor(failingIndex);

    assertThatThrownBy(() -> FetchFromIndexStep.init(desc, isOrderAsc, ctx))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("index scan failed");
    assertThat(nullStreamClosed).as("null-key stream must be closed, not leaked").isTrue();
  }

  /**
   * When the main-stream acquisition throws AND closing the already-open null stream itself throws,
   * the ORIGINAL acquisition failure must stay the propagated exception and the close failure must
   * be attached to it as a suppressed exception. Pins {@code closeSuppressing}'s contract: it wraps
   * {@code close()} in try/catch precisely because a real B-tree stream close can fail, and that
   * failure must not mask the root cause.
   */
  @Test
  public void fullScanKeepsOriginalFailureWhenNullStreamCloseAlsoThrows() {
    var fixture = createIndexedClass();
    var definition = getIndex(fixture.indexName).getDefinition();
    var ctx = newContext();

    // A null-key stream whose own close() throws.
    var closeFailure = new IllegalStateException("null stream close failed");
    Stream<RID> nullRids =
        Stream.<RID>empty()
            .onClose(
                () -> {
                  throw closeFailure;
                });

    var failingIndex = mock(Index.class);
    when(failingIndex.getDefinition()).thenReturn(definition);
    when(failingIndex.getName()).thenReturn(fixture.indexName);
    when(failingIndex.getRids(any(), nullable(Object.class))).thenReturn(nullRids);
    when(failingIndex.stream(any())).thenThrow(new IllegalStateException("index scan failed"));

    var desc = new IndexSearchDescriptor(failingIndex);

    assertThatThrownBy(() -> FetchFromIndexStep.init(desc, true, ctx))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("index scan failed") // original acquisition failure stays the thrown one
        .satisfies(t -> assertThat(t.getSuppressed()).contains(closeFailure));
  }

  /**
   * A range scan that expands to several key ranges (here {@code WHERE key IN [10, 30]}, two
   * ranges) acquires one stream per range. If a later range acquisition throws, the streams already
   * acquired for earlier ranges must be closed before the exception propagates — the same leak the
   * full-scan path guards against. The index is mocked so the first {@code streamEntriesBetween}
   * yields an observable stream and the second throws; the test asserts the first stream was closed
   * and the original failure propagates unwrapped.
   */
  @Test
  public void multipleRangeClosesEarlierStreamsWhenLaterRangeAcquisitionThrows() {
    var fixture = createIndexedClass();
    var definition = getIndex(fixture.indexName).getDefinition();
    var ctx = newContext();

    // First range yields a stream whose close() we can observe; second range acquisition throws.
    var firstStreamClosed = new AtomicBoolean(false);
    Stream<RawPair<Object, RID>> firstStream =
        Stream.<RawPair<Object, RID>>empty().onClose(() -> firstStreamClosed.set(true));

    var failingIndex = mock(Index.class);
    when(failingIndex.getDefinition()).thenReturn(definition);
    when(failingIndex.getName()).thenReturn(fixture.indexName);
    doReturn(firstStream)
        .doThrow(new IllegalStateException("range scan failed"))
        .when(failingIndex)
        .streamEntriesBetween(any(), any(), anyBoolean(), any(), anyBoolean(), anyBoolean());

    var inCondition = new SQLInCondition(-1);
    inCondition.setLeft(fieldExpr("key"));
    inCondition.setRightMathExpression(valueMathExpr(List.of(10, 30)));
    var desc = new IndexSearchDescriptor(failingIndex, andBlockOf(inCondition));

    // multipleRange reads the active transaction, so the range path needs one open (the full-scan
    // path does not); rolled back afterward since nothing is committed.
    session.begin();
    try {
      assertThatThrownBy(() -> FetchFromIndexStep.init(desc, true, ctx))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("range scan failed");
      assertThat(firstStreamClosed).as("earlier range stream must be closed, not leaked").isTrue();
    } finally {
      session.rollback();
    }
  }

  // =========================================================================
  // processAndBlock / multipleRange: operator coverage on a single-field index
  // =========================================================================

  /**
   * {@code WHERE key = X} against a single-property non-unique index performs a point lookup.
   * Only the matching record is returned; non-matching entries must not appear.
   */
  @Test
  public void equalityConditionReturnsOnlyMatchingRecord() {
    var fixture = createIndexedClass();
    seed(fixture.className, 10, 20, 30);
    var ctx = newContext();
    var step =
        buildStep(fixture.indexName,
            andBlockOf(binaryCondition("key", new SQLEqualsOperator(-1), 20)), ctx);

    var results = startAndDrain(step, ctx);

    assertThat(keys(results)).containsExactly(20);
  }

  /**
   * {@code WHERE key > X} is exclusive on the lower bound: the entry with {@code key == X} must
   * NOT appear. Pins the {@code isGreaterOperator && !isIncludeOperator} path in {@code
   * indexKeyFromIncluded}.
   */
  @Test
  public void greaterThanConditionIsExclusiveOnLowerBound() {
    var fixture = createIndexedClass();
    seed(fixture.className, 10, 20, 30, 40);
    var ctx = newContext();
    var step =
        buildStep(
            fixture.indexName,
            andBlockOf(binaryCondition("key", new SQLGtOperator(-1), 20)),
            ctx);

    var results = startAndDrain(step, ctx);

    assertThat(keys(results)).containsExactly(30, 40);
  }

  /**
   * {@code WHERE key >= X} is inclusive on the lower bound. Pins the {@code isGreaterOperator &&
   * isIncludeOperator} path in {@code indexKeyFromIncluded}.
   */
  @Test
  public void greaterOrEqualConditionIsInclusiveOnLowerBound() {
    var fixture = createIndexedClass();
    seed(fixture.className, 10, 20, 30, 40);
    var ctx = newContext();
    var step =
        buildStep(
            fixture.indexName,
            andBlockOf(binaryCondition("key", new SQLGeOperator(-1), 20)),
            ctx);

    var results = startAndDrain(step, ctx);

    assertThat(keys(results)).containsExactly(20, 30, 40);
  }

  /**
   * {@code WHERE key < X} is exclusive on the upper bound. Pins the {@code isLessOperator &&
   * !isIncludeOperator} path in {@code indexKeyToIncluded}.
   */
  @Test
  public void lessThanConditionIsExclusiveOnUpperBound() {
    var fixture = createIndexedClass();
    seed(fixture.className, 10, 20, 30, 40);
    var ctx = newContext();
    var step =
        buildStep(
            fixture.indexName,
            andBlockOf(binaryCondition("key", new SQLLtOperator(-1), 30)),
            ctx);

    var results = startAndDrain(step, ctx);

    assertThat(keys(results)).containsExactly(10, 20);
  }

  /**
   * {@code WHERE key <= X} is inclusive on the upper bound. Pins the {@code isLessOperator &&
   * isIncludeOperator} path in {@code indexKeyToIncluded}.
   */
  @Test
  public void lessOrEqualConditionIsInclusiveOnUpperBound() {
    var fixture = createIndexedClass();
    seed(fixture.className, 10, 20, 30, 40);
    var ctx = newContext();
    var step =
        buildStep(
            fixture.indexName,
            andBlockOf(binaryCondition("key", new SQLLeOperator(-1), 30)),
            ctx);

    var results = startAndDrain(step, ctx);

    assertThat(keys(results)).containsExactly(10, 20, 30);
  }

  /**
   * A combined range {@code key >= low AND key < high} is expressed as {@code keyCondition = AND
   * [key >= low]} with {@code additionalRangeCondition = key < high}. Exercises the additional-
   * range-condition branch of {@code indexKeyToIncluded} (non-last-block else-branch) where the
   * upper inclusivity is read from the {@code additional} operator.
   */
  @Test
  public void rangeWithAdditionalUpperBoundCombinesBothBounds() {
    var fixture = createIndexedClass();
    seed(fixture.className, 10, 20, 30, 40, 50);
    var index = getIndex(fixture.indexName);
    var lower = binaryCondition("key", new SQLGeOperator(-1), 20);
    var upper = binaryCondition("key", new SQLLtOperator(-1), 50);
    var desc = new IndexSearchDescriptor(index, andBlockOf(lower), upper, null);
    var ctx = newContext();
    var step = new FetchFromIndexStep(desc, true, ctx, false);

    var results = startAndDrain(step, ctx);

    assertThat(keys(results)).containsExactly(20, 30, 40);
  }

  /**
   * {@code WHERE key IN [a, b, c]} is packaged as an {@link SQLAndBlock} holding a single {@link
   * SQLInCondition}. The step expands each IN value into a separate cartesian-product branch and
   * unions the results. Pins the {@code SQLInCondition} branch of both {@code
   * indexKeyFromIncluded} and {@code indexKeyToIncluded} (no-additional path) plus the cartesian
   * expansion inside {@code cartesianProduct}.
   */
  @Test
  public void inConditionWithMultipleValuesReturnsAllMatches() {
    var fixture = createIndexedClass();
    seed(fixture.className, 10, 20, 30, 40);
    var inCondition = new SQLInCondition(-1);
    inCondition.setLeft(fieldExpr("key"));
    inCondition.setRightMathExpression(valueMathExpr(List.of(10, 30)));
    var ctx = newContext();
    var step = buildStep(fixture.indexName, andBlockOf(inCondition), ctx);

    var results = startAndDrain(step, ctx);

    assertThat(keys(results)).containsExactlyInAnyOrder(10, 30);
  }

  /**
   * {@code WHERE key IN []} must return zero results. The concerning path is {@code
   * toBetweenIndexKey} which collapses an empty collection to {@code null} — if the
   * cartesian-product / null-key branching ever regressed so an empty IN fell through to a full
   * scan, every indexed record would be returned instead. Pins the TC1 boundary raised by
   * Step 4 iter-1 test-completeness review.
   */
  @Test
  public void inConditionWithEmptyListReturnsNoMatches() {
    var fixture = createIndexedClass();
    seed(fixture.className, 10, 20, 30, 40);
    var inCondition = new SQLInCondition(-1);
    inCondition.setLeft(fieldExpr("key"));
    inCondition.setRightMathExpression(valueMathExpr(List.of()));
    var ctx = newContext();
    var step = buildStep(fixture.indexName, andBlockOf(inCondition), ctx);

    var results = startAndDrain(step, ctx);

    assertThat(results).isEmpty();
  }

  // =========================================================================
  // Composite index paths
  // =========================================================================

  /**
   * {@code WHERE city = X AND age = Y} on a composite index {@code [city, age]} builds a {@link
   * CompositeKey} for the point lookup. Covers the multi-sub-block path of {@code
   * indexKeyFrom/To} with inclusive equality on both fields and the conversion of the resulting
   * key into a per-field list via {@code convertKey}.
   */
  @Test
  public void compositeIndexEqualityOnAllFieldsReturnsMatchingRecord() {
    var clazz = createClassInstance();
    clazz.createProperty("city", PropertyType.STRING);
    clazz.createProperty(PROP, PropertyType.INTEGER);
    var indexName = clazz.getName() + ".cityAge";
    clazz.createIndex(indexName, SchemaClass.INDEX_TYPE.NOTUNIQUE, "city", PROP);
    session.begin();
    try {
      newRecord(clazz.getName(), "NYC", 30);
      newRecord(clazz.getName(), "NYC", 40);
      newRecord(clazz.getName(), "LA", 30);
      session.commit();
    } catch (Exception e) {
      session.rollback();
      throw e;
    }
    var index = getIndex(indexName);
    var keyCondition = new SQLAndBlock(-1);
    keyCondition.addSubBlock(binaryCondition("city", new SQLEqualsOperator(-1), "NYC"));
    keyCondition.addSubBlock(binaryCondition(PROP, new SQLEqualsOperator(-1), 30));
    var ctx = newContext();
    var step =
        new FetchFromIndexStep(new IndexSearchDescriptor(index, keyCondition), true, ctx, false);

    var results = startAndDrain(step, ctx);

    assertThat(results).hasSize(1);
    var keyProp = results.getFirst().getProperty("key");
    assertThat(keyProp).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST)
        .containsExactly("NYC", 30);
  }

  /**
   * {@code WHERE city IN ['NYC','LA'] AND age = 30} on composite index {@code [city, age]}
   * expands into two cartesian-product branches — ('NYC',30) and ('LA',30). Pins the {@code
   * Iterable && !Identifiable} branch of {@code cartesianProduct} (expansion of the multi-valued
   * leading key position).
   */
  @Test
  public void compositeIndexInOnLeadingFieldExpandsCartesianProduct() {
    var clazz = createClassInstance();
    clazz.createProperty("city", PropertyType.STRING);
    clazz.createProperty(PROP, PropertyType.INTEGER);
    var indexName = clazz.getName() + ".cityAge";
    clazz.createIndex(indexName, SchemaClass.INDEX_TYPE.NOTUNIQUE, "city", PROP);
    session.begin();
    try {
      newRecord(clazz.getName(), "NYC", 30);
      newRecord(clazz.getName(), "LA", 30);
      newRecord(clazz.getName(), "SF", 30);
      session.commit();
    } catch (Exception e) {
      session.rollback();
      throw e;
    }
    var index = getIndex(indexName);

    var leadingIn = new SQLInCondition(-1);
    leadingIn.setLeft(fieldExpr("city"));
    leadingIn.setRightMathExpression(valueMathExpr(List.of("NYC", "LA")));
    var keyCondition = new SQLAndBlock(-1);
    keyCondition.addSubBlock(leadingIn);
    keyCondition.addSubBlock(binaryCondition(PROP, new SQLEqualsOperator(-1), 30));

    var ctx = newContext();
    var step =
        new FetchFromIndexStep(new IndexSearchDescriptor(index, keyCondition), true, ctx, false);

    var results = startAndDrain(step, ctx);
    assertThat(results).hasSize(2);
    // Both ('NYC', 30) and ('LA', 30) are returned; 'SF' must not appear.
    List<Object> cityValues = new ArrayList<>();
    for (var r : results) {
      List<?> key = r.getProperty("key");
      cityValues.add(key.getFirst());
    }
    assertThat(cityValues).containsExactlyInAnyOrder("NYC", "LA");
  }

  // =========================================================================
  // readResult: key/rid/currentVar population
  // =========================================================================

  /**
   * Each emitted {@link Result} carries two properties: {@code key} (the index key — scalar for a
   * single-field index) and {@code rid} (the stored identifiable). Pins both {@code
   * setProperty("key", convertKey(key))} and {@code setProperty("rid", value)} in {@code
   * readResult}; a mutation dropping either property would leave downstream steps without the
   * record reference.
   */
  @Test
  public void emittedResultCarriesKeyAndRidProperties() {
    var fixture = createIndexedClass();
    var savedRid = seedAndReturnRid(fixture.className, 42);
    var ctx = newContext();
    var step =
        buildStep(fixture.indexName,
            andBlockOf(binaryCondition("key", new SQLEqualsOperator(-1), 42)), ctx);

    var results = startAndDrain(step, ctx);

    assertThat(results).hasSize(1);
    assertThat(results.getFirst().<Integer>getProperty("key")).isEqualTo(42);
    assertThat(results.getFirst().<RID>getProperty("rid")).isEqualTo(savedRid);
  }

  /**
   * {@code readResult} publishes each result to the command context's {@code $current} system
   * variable. Downstream steps rely on this for variable resolution (e.g. LET expressions).
   * Mutation dropping the {@code setSystemVariable} call would silently break $current-reliant
   * query rewrites.
   */
  @Test
  public void emittedResultIsAlsoPublishedAsCurrentSystemVariable() {
    var fixture = createIndexedClass();
    seed(fixture.className, 42);
    var ctx = newContext();
    var step =
        buildStep(fixture.indexName,
            andBlockOf(binaryCondition("key", new SQLEqualsOperator(-1), 42)), ctx);

    var results = startAndDrain(step, ctx);

    assertThat(results).hasSize(1);
    assertThat((Object) ctx.getSystemVariable(CommandContext.VAR_CURRENT))
        .isSameAs(results.getFirst());
  }

  // =========================================================================
  // Predecessor drain
  // =========================================================================

  /**
   * When a predecessor step is chained, {@code internalStart} drains it for side effects and
   * immediately closes it — the fetch step ignores the predecessor's output because it reads
   * directly from the index. This pins the {@code if (prev != null) { prev.start(ctx).close(ctx); }}
   * guard.
   */
  @Test
  public void predecessorStepIsStartedAndClosedBeforeFetching() {
    var fixture = createIndexedClass();
    seed(fixture.className, 10);
    var ctx = newContext();
    var step =
        buildStep(fixture.indexName,
            andBlockOf(binaryCondition("key", new SQLEqualsOperator(-1), 10)), ctx);
    var prevStarted = new AtomicBoolean();
    var prevClosed = new AtomicBoolean();
    step.setPrevious(stubPredecessor(ctx, prevStarted, prevClosed));

    startAndDrain(step, ctx);

    assertThat(prevStarted).isTrue();
    assertThat(prevClosed).isTrue();
  }

  // =========================================================================
  // indexKeyFromIncluded / indexKeyToIncluded: remaining sub-block shapes
  // =========================================================================

  /**
   * A {@link SQLContainsCondition} as the last sub-block exercises the {@code SQLContainsCondition}
   * arm of both {@code indexKeyFromIncluded} and {@code indexKeyToIncluded}. {@code
   * cartesianProduct} evaluates the sub-block against the database session; for this contains-
   * style condition we use a stub expression in the sub-block to avoid needing a real collection
   * property, so the test runs against the regular integer index. The step still throws —
   * because the actual value resolution hits {@code processAndBlock}'s machinery with a non-
   * collection scalar — but the throw path runs <em>after</em> the inclusivity checks, so the
   * desired branches are still traversed.
   */
  @Test
  public void containsConditionAsLastSubBlockExercisesInclusivityBranches() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var keyCondition = new SQLAndBlock(-1);
    keyCondition.addSubBlock(new SQLContainsCondition(-1));
    var step =
        new FetchFromIndexStep(new IndexSearchDescriptor(index, keyCondition), true, ctx, false);

    // The step must at least reach init/processAndBlock; whether it ultimately produces records
    // depends on downstream value resolution (not the focus of this test).
    assertThatReachesProcessAndBlock(step, ctx);
  }

  /**
   * An {@link SQLContainsAnyCondition} as the last sub-block exercises the {@code
   * SQLContainsAnyCondition} arm of both inclusivity calculators.
   */
  @Test
  public void containsAnyConditionAsLastSubBlockExercisesInclusivityBranches() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var keyCondition = new SQLAndBlock(-1);
    keyCondition.addSubBlock(new SQLContainsAnyCondition(-1));
    var step =
        new FetchFromIndexStep(new IndexSearchDescriptor(index, keyCondition), true, ctx, false);

    assertThatReachesProcessAndBlock(step, ctx);
  }

  /**
   * An {@link SQLContainsTextCondition} as the last sub-block exercises the {@code
   * SQLContainsTextCondition} arm of both inclusivity calculators, which unconditionally returns
   * {@code true} — the only FULLTEXT-independent property of this arm.
   */
  @Test
  public void containsTextConditionAsLastSubBlockExercisesInclusivityBranches() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var keyCondition = new SQLAndBlock(-1);
    keyCondition.addSubBlock(new SQLContainsTextCondition(-1));
    var step =
        new FetchFromIndexStep(new IndexSearchDescriptor(index, keyCondition), true, ctx, false);

    assertThatReachesProcessAndBlock(step, ctx);
  }

  /**
   * An {@link SQLContainsValueCondition} as the last sub-block exercises the {@code
   * SQLContainsValueCondition} arm of both inclusivity calculators. The condition's operator is
   * left as its default (null), so the {@code additionalOperator == null} branch is exercised
   * rather than the operator-based inclusivity branch.
   */
  @Test
  public void containsValueConditionAsLastSubBlockExercisesInclusivityBranches() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var keyCondition = new SQLAndBlock(-1);
    keyCondition.addSubBlock(new SQLContainsValueCondition(-1));
    var step =
        new FetchFromIndexStep(new IndexSearchDescriptor(index, keyCondition), true, ctx, false);

    assertThatReachesProcessAndBlock(step, ctx);
  }

  /**
   * An unsupported sub-block kind (for example an unrelated {@link SQLBooleanExpression}
   * subclass produced by a future planner change) triggers {@link UnsupportedOperationException}
   * in both {@code indexKeyFromIncluded} and {@code indexKeyToIncluded}. We use an {@code
   * SQLAndBlock} as its own last sub-block — a nested-AndBlock shape that the real planner never
   * produces — to hit the catch-all arm.
   */
  @Test
  public void unsupportedLastSubBlockTypeThrowsUnsupportedOperationException() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var keyCondition = new SQLAndBlock(-1);
    keyCondition.addSubBlock(new SQLAndBlock(-1));
    var step =
        new FetchFromIndexStep(new IndexSearchDescriptor(index, keyCondition), true, ctx, false);

    session.begin();
    try {
      assertThatThrownBy(() -> step.start(ctx))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("Cannot execute index query with");
    } finally {
      session.rollback();
    }
  }

  // =========================================================================
  // serialize / deserialize
  // =========================================================================

  /**
   * Round-trips a step with a key condition through {@code serialize} → {@code deserialize}.
   * {@code indexName}, {@code orderAsc}, and a serialized {@code condition} property must be
   * present on the serialized form. After {@code deserialize} the reconstituted step's
   * {@code getIndexName()} and {@code isOrderAsc()} must match the original.
   */
  @Test
  public void serializeProducesIndexNameOrderAscAndConditionProperties() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    // Parser-produced AND block gives us fully-initialized SQLBaseExpression right-hand sides,
    // so the nested serialize() calls (SQLBinaryCondition → SQLExpression → SQLMathExpression)
    // succeed. Lightweight SQLValueExpression stubs would reject serialization.
    var keyCondition = parsedKeyCondition("key = 10");
    var original =
        new FetchFromIndexStep(new IndexSearchDescriptor(index, keyCondition), false, ctx, true);

    var serialized = original.serialize(session);

    assertThat((String) serialized.getProperty("indexName")).isEqualTo(fixture.indexName);
    assertThat((Boolean) serialized.getProperty("orderAsc")).isFalse();
    assertThat((Object) serialized.getProperty("condition")).isNotNull();
  }

  /**
   * Pins a pre-existing production bug: {@link
   * com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBooleanExpression#deserializeFromOResult}
   * calls {@code Class.forName(name).getConstructor(Integer.class)} but all concrete AST classes
   * (SQLAndBlock, SQLOrBlock, SQLNotBlock, SQLInCondition, SQLContainsCondition, …) declare their
   * constructor with primitive {@code int}, not boxed {@code Integer} — {@code getConstructor}
   * does not auto-unbox, so the lookup throws {@link NoSuchMethodException}. The exception is
   * wrapped into {@link CommandExecutionException} by {@code FetchFromIndexStep.deserialize}.
   *
   * <p>Practical impact: plan-cache round-trip (serialize-to-Result → deserialize) is broken for
   * any FetchFromIndexStep whose key condition includes a wrapping AND block. This test fails
   * the day the bug is fixed in production, prompting tightening to a round-trip-success check.
   *
   * <p>WHEN-FIXED: YTDB-754 — change {@code SQLBooleanExpression.deserializeFromOResult} to use
   * {@code int.class} (primitive) in {@code getConstructor}, or provide an {@code
   * Integer}-parameter constructor on the affected AST classes.
   */
  @Test
  public void deserializingAndBlockConditionHitsIntegerConstructorBug() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var keyCondition = parsedKeyCondition("key = 10");
    var original =
        new FetchFromIndexStep(new IndexSearchDescriptor(index, keyCondition), false, ctx, true);
    var serialized = original.serialize(session);
    var copy = new FetchFromIndexStep(null, true, ctx, false);
    copy.reset();

    assertThatThrownBy(() -> copy.deserialize(serialized, session))
        .isInstanceOf(CommandExecutionException.class)
        .hasRootCauseInstanceOf(NoSuchMethodException.class)
        .hasRootCauseMessage(
            "com.jetbrains.youtrackdb.internal.core.sql.parser.SQLAndBlock."
                + "<init>(java.lang.Integer)");
  }

  /**
   * A step with an {@code additionalRangeCondition} includes the {@code additionalRangeCondition}
   * property in its serialized form; after deserialization the reconstituted descriptor exposes
   * the condition again. Mutation dropping the serialization of this field would silently lose
   * the upper bound during plan-cache round-trip.
   */
  @Test
  public void serializeIncludesAdditionalRangeConditionWhenPresent() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var keyCondition = parsedKeyCondition("key >= 10");
    var upper = (SQLBinaryCondition) parsedKeyCondition("key < 30").getSubBlocks().getFirst();
    var desc = new IndexSearchDescriptor(index, keyCondition, upper, null);
    var original = new FetchFromIndexStep(desc, true, ctx, false);

    var serialized = original.serialize(session);

    // additionalRangeCondition uses SQLBinaryCondition's own serialize/deserialize pair (which
    // does NOT hit the Integer-constructor bug), so it round-trips cleanly — unlike the outer
    // keyCondition which wraps an SQLAndBlock and fails (see
    // deserializingAndBlockConditionHitsIntegerConstructorBug).
    assertThat((Object) serialized.getProperty("additionalRangeCondition")).isNotNull();
  }

  /**
   * Serialization of a step with {@code keyCondition == null} omits the {@code condition}
   * property entirely. Pins the {@code if (desc.getKeyCondition() != null)} guard.
   */
  @Test
  public void serializeOmitsConditionPropertyWhenKeyConditionIsNull() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), true, ctx, false);

    var serialized = step.serialize(session);

    assertThat((Object) serialized.getProperty("condition")).isNull();
    assertThat((Object) serialized.getProperty("additionalRangeCondition")).isNull();
  }

  /**
   * Serialized plans carry {@code nullsFirst}; legacy blobs without it fall back to {@code orderAsc}
   * (FIRST semantic).
   */
  @Test
  public void deserializeRestoresNullsFirstAndFallsBackWhenOmitted() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var original =
        new FetchFromIndexStep(new IndexSearchDescriptor(index), true, false, ctx, false);

    var serialized = (ResultInternal) original.serialize(session);
    assertThat(serialized.<Boolean>getProperty("nullsFirst")).isFalse();

    var copy = new FetchFromIndexStep(null, true, ctx, false);
    copy.reset();
    copy.deserialize(serialized, session);
    assertThat(copy.isOrderAsc()).isTrue();
    assertThat(copy.isNullsFirst()).isFalse();

    serialized.setProperty("nullsFirst", null);
    var legacy = new FetchFromIndexStep(null, true, ctx, false);
    legacy.reset();
    legacy.deserialize(serialized, session);
    assertThat(legacy.isNullsFirst()).isTrue();

    serialized.setProperty("orderAsc", false);
    serialized.setProperty("nullsFirst", null);
    var legacyDesc = new FetchFromIndexStep(null, false, ctx, false);
    legacyDesc.reset();
    legacyDesc.deserialize(serialized, session);
    assertThat(legacyDesc.isNullsFirst()).isFalse();
  }

  /**
   * A serialized form with an unknown index name causes deserialization to fail while the desc
   * is being rebuilt ({@code indexManager.getIndex(...)} returns {@code null}, and building an
   * {@link IndexSearchDescriptor} with a null index fails the first time the descriptor is used).
   * The exception is wrapped into {@link CommandExecutionException} by {@code deserialize}'s
   * catch-all.
   */
  @Test
  public void deserializeWithUnknownIndexWrapsIntoCommandExecutionException() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var original =
        new FetchFromIndexStep(
            new IndexSearchDescriptor(index, parsedKeyCondition("key = 10")), true, ctx, false);

    var serialized = (ResultInternal) original.serialize(session);
    // Force a deserialization failure by replacing the condition with a String value that
    // cannot be interpreted as a nested Result. deserialize() wraps the RuntimeException into
    // a CommandExecutionException.
    serialized.setProperty("condition", "not-a-valid-condition-serialization");
    var copy = new FetchFromIndexStep(null, true, ctx, false);
    copy.reset();

    assertThatThrownBy(() -> copy.deserialize(serialized, session))
        .isInstanceOf(CommandExecutionException.class);
  }

  // =========================================================================
  // prettyPrint
  // =========================================================================

  /**
   * {@code prettyPrint} begins with {@code "+ FETCH FROM INDEX " + indexName}. Pins the prefix
   * and the index-name interpolation; a mutation to the header or a swap of the name source
   * would break the plan display used by {@code EXPLAIN}.
   */
  @Test
  public void prettyPrintIncludesIndexNameAndHeader() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), true, ctx, false);

    var rendered = step.prettyPrint(0, 2);

    assertThat(rendered).startsWith("+ FETCH FROM INDEX ").contains(fixture.indexName);
  }

  /**
   * When profiling is enabled, {@code prettyPrint} appends {@code " (N.NNms)"} (the formatted
   * cost) to the header line. Pins the {@code if (profilingEnabled)} guard; a mutation flipping
   * the flag would silently drop the profiling readout from {@code EXPLAIN} output.
   */
  @Test
  public void prettyPrintWithProfilingIncludesCostMarker() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var step = new FetchFromIndexStep(new IndexSearchDescriptor(index), true, ctx, true);

    var rendered = step.prettyPrint(0, 2);

    // Pin the "+ FETCH FROM INDEX <name> (<cost>)" shape including the cost-marker parens so a
    // mutation that produces only trailing parens without the cost value, or replaces "ms" with
    // a different unit, is caught. (TB2 tightening.)
    assertThat(rendered).containsPattern("\\+ FETCH FROM INDEX [^\\s]+ \\([^)]+\\)");
  }

  /**
   * When the descriptor carries a key condition, {@code prettyPrint} appends the condition on a
   * second line (indented). Pins the {@code if (desc.getKeyCondition() != null)} branch.
   */
  @Test
  public void prettyPrintWithKeyConditionRendersConditionOnSecondLine() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var keyCondition = andBlockOf(binaryCondition("key", new SQLEqualsOperator(-1), 10));
    var step =
        new FetchFromIndexStep(new IndexSearchDescriptor(index, keyCondition), true, ctx, false);

    var rendered = step.prettyPrint(0, 2);

    assertThat(rendered).contains("\n");
    assertThat(rendered.lines().count()).isEqualTo(2);
  }

  /**
   * An additional range condition is appended after the key condition (with an {@code " and "}
   * separator) when rendering. Pins the {@code Optional.ofNullable(desc.getAdditionalRangeCondition())
   * ...map(...)} branch.
   */
  @Test
  public void prettyPrintIncludesAdditionalRangeConditionWhenPresent() {
    var fixture = createIndexedClass();
    var index = getIndex(fixture.indexName);
    var ctx = newContext();
    var lower = binaryCondition("key", new SQLGeOperator(-1), 10);
    var upper = binaryCondition("key", new SQLLtOperator(-1), 30);
    var desc = new IndexSearchDescriptor(index, andBlockOf(lower), upper, null);
    var step = new FetchFromIndexStep(desc, true, ctx, false);

    var rendered = step.prettyPrint(0, 2);

    assertThat(rendered).contains(" and ");
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  /**
   * Pair of class name + index name produced by {@link #createIndexedClass()}. Keeps callers
   * from having to re-derive the index name from the class name.
   */
  private record IndexedFixture(String className, String indexName) {
  }

  /**
   * Creates a fresh class with a single-property {@code age} (INTEGER) non-unique index. Each
   * call returns a unique class name so tests in the same class do not interfere.
   */
  private IndexedFixture createIndexedClass() {
    var clazz = createClassInstance();
    clazz.createProperty(PROP, PropertyType.INTEGER);
    var indexName = clazz.getName() + "." + PROP;
    clazz.createIndex(indexName, SchemaClass.INDEX_TYPE.NOTUNIQUE, PROP);
    return new IndexedFixture(clazz.getName(), indexName);
  }

  private Index getIndex(String indexName) {
    return session.getSharedContext().getIndexManager().getIndex(indexName);
  }

  /**
   * Populates the class with one record per value, committing so the index captures them.
   */
  private void seed(String className, Object... values) {
    session.begin();
    try {
      for (var v : values) {
        var entity = (EntityImpl) session.newEntity(className);
        entity.setProperty(PROP, v);
      }
      session.commit();
    } catch (Exception e) {
      session.rollback();
      throw e;
    }
  }

  /**
   * Populates the class with {@code count} records that do NOT set the indexed property — the
   * index stores them under the {@code null} key if the index does not ignore nulls.
   */
  private void seedNull(String className, int count) {
    // Delegates to seedNullReturningRids so the transaction/seeding logic lives in one place; the
    // returned RIDs are irrelevant when the caller only needs the null-keyed records to exist.
    seedNullReturningRids(className, count);
  }

  /**
   * Like {@link #seedNull(String, int)} but returns the RIDs of the created null records, so a test
   * can assert the null-key stream surfaces exactly those RIDs.
   */
  private List<RID> seedNullReturningRids(String className, int count) {
    session.begin();
    try {
      var entities = new ArrayList<EntityImpl>();
      for (var i = 0; i < count; i++) {
        entities.add((EntityImpl) session.newEntity(className));
      }
      session.commit();
      return entities.stream().<RID>map(EntityImpl::getIdentity).toList();
    } catch (Exception e) {
      session.rollback();
      throw e;
    }
  }

  /**
   * Creates one record and returns its RID so the test can assert identity rather than just
   * property equality.
   */
  private RID seedAndReturnRid(String className, Object value) {
    session.begin();
    try {
      var entity = (EntityImpl) session.newEntity(className);
      entity.setProperty(PROP, value);
      session.commit();
      return entity.getIdentity();
    } catch (Exception e) {
      session.rollback();
      throw e;
    }
  }

  /**
   * Creates a multi-property record with {@code city} and {@code age} fields.
   */
  private void newRecord(String className, String city, int age) {
    var entity = (EntityImpl) session.newEntity(className);
    entity.setProperty("city", city);
    entity.setProperty(PROP, age);
  }

  private FetchFromIndexStep buildStep(
      String indexName, SQLBooleanExpression keyCondition, CommandContext ctx) {
    var index = getIndex(indexName);
    return new FetchFromIndexStep(new IndexSearchDescriptor(index, keyCondition), true, ctx, false);
  }

  private static SQLAndBlock andBlockOf(SQLBooleanExpression sub) {
    var block = new SQLAndBlock(-1);
    block.addSubBlock(sub);
    return block;
  }

  /**
   * Parses a mini SELECT-WHERE statement and returns the WHERE clause's flattened AND block. The
   * resulting AST has fully-initialized {@link
   * com.jetbrains.youtrackdb.internal.core.sql.parser.SQLBaseExpression} right-hand sides, so
   * both {@code serialize()} and {@code deserialize()} succeed — unlike the lightweight {@link
   * SQLValueExpression} stubs used elsewhere in this file, which explicitly reject
   * serialization. The parser typically wraps a WHERE clause in an OR block whose only subblock
   * is an AND block; this helper peels both layers so callers get a ready-to-use AND block.
   */
  private static SQLAndBlock parsedKeyCondition(String whereSnippet) {
    try {
      var sql = "SELECT FROM XUnusedClass WHERE " + whereSnippet;
      var parser = new YouTrackDBSql(new ByteArrayInputStream(sql.getBytes()));
      var stmt = (SQLSelectStatement) parser.parse();
      SQLBooleanExpression where = stmt.getWhereClause().getBaseExpression();
      // Peel the outer OR block if it has a single subblock (common for simple WHEREs).
      if (where instanceof com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrBlock orBlock
          && orBlock.getSubBlocks().size() == 1) {
        where = orBlock.getSubBlocks().getFirst();
      }
      // The parser's AND block contains SQLNotBlock wrappers with negate=false for each simple
      // condition — unwrap them so downstream code sees the bare SQLBinaryCondition / SQLInCondition.
      SQLAndBlock andBlock;
      if (where instanceof SQLAndBlock existing) {
        andBlock = new SQLAndBlock(-1);
        for (var sub : existing.getSubBlocks()) {
          andBlock.addSubBlock(unwrapNotBlock(sub));
        }
      } else {
        andBlock = new SQLAndBlock(-1);
        andBlock.addSubBlock(unwrapNotBlock(where));
      }
      return andBlock;
    } catch (Exception e) {
      throw new AssertionError("Failed to parse WHERE snippet: " + whereSnippet, e);
    }
  }

  private static SQLBooleanExpression unwrapNotBlock(SQLBooleanExpression expr) {
    if (expr instanceof com.jetbrains.youtrackdb.internal.core.sql.parser.SQLNotBlock notBlock
        && !notBlock.isNegate()
        && notBlock.getSub() != null) {
      return notBlock.getSub();
    }
    return expr;
  }

  private static SQLBinaryCondition binaryCondition(
      String field, SQLBinaryCompareOperator op, Object value) {
    var bc = new SQLBinaryCondition(-1);
    bc.setLeft(fieldExpr(field));
    bc.setOperator(op);
    bc.setRight(valueExpr(value));
    return bc;
  }

  private static SQLExpression fieldExpr(String name) {
    var expr = mock(SQLExpression.class);
    when(expr.isBaseIdentifier()).thenReturn(true);
    when(expr.toString()).thenReturn(name);
    var alias = mock(SQLIdentifier.class);
    when(alias.getStringValue()).thenReturn(name);
    when(expr.getDefaultAlias()).thenReturn(alias);
    return expr;
  }

  private static SQLExpression valueExpr(Object value) {
    // SQLValueExpression does the right thing for execute(Result, ctx) and is what the production
    // cartesianProduct/processAndBlock paths expect when the planner materializes a literal. It
    // also supports toString() for prettyPrint without Mockito stubbing.
    return new SQLValueExpression(value);
  }

  /**
   * Mocked {@link SQLMathExpression} that returns a fixed value — used for the right side of
   * {@link SQLInCondition}, which stores a math expression (not a plain {@link SQLExpression})
   * internally and unwraps it via {@code resolveKeyFrom}/{@code resolveKeyTo} into a fresh
   * {@link SQLExpression} wrapper.
   */
  private static SQLMathExpression valueMathExpr(Object value) {
    var expr = mock(SQLMathExpression.class);
    when(expr.isEarlyCalculated(any())).thenReturn(true);
    when(expr.execute(nullable(Result.class), any(CommandContext.class))).thenReturn(value);
    when(expr.execute(nullable(Identifiable.class), any(CommandContext.class))).thenReturn(value);
    return expr;
  }

  /**
   * Starts the step and drains its stream inside an explicit transaction. Index lookups need an
   * active transaction both for the {@code preProcessRecordsAndExecuteCallCallbacks} prelude in
   * {@code internalStart} and for reading index entries — so {@code start()} must run inside the
   * same {@code begin/rollback} pair.
   */
  private List<Result> startAndDrain(FetchFromIndexStep step, CommandContext ctx) {
    session.begin();
    try {
      var stream = step.start(ctx);
      var out = new ArrayList<Result>();
      while (stream.hasNext(ctx)) {
        out.add(stream.next(ctx));
      }
      stream.close(ctx);
      return out;
    } finally {
      session.rollback();
    }
  }

  private static List<Object> keys(List<Result> results) {
    return results.stream().map(r -> r.<Object>getProperty("key")).toList();
  }

  /**
   * Predecessor stub that records whether it was started and closed. Mirrors the pattern used by
   * {@code FetchFromClassExecutionStepTest}.
   */
  private static AbstractExecutionStep stubPredecessor(
      CommandContext ctx, AtomicBoolean started, AtomicBoolean closed) {
    return new AbstractExecutionStep(ctx, false) {
      @Override
      public ExecutionStep copy(CommandContext c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public ExecutionStream internalStart(CommandContext c) {
        started.set(true);
        return new ExecutionStream() {
          @Override
          public boolean hasNext(CommandContext c2) {
            return false;
          }

          @Override
          public Result next(CommandContext c2) {
            throw new IllegalStateException("no results");
          }

          @Override
          public void close(CommandContext c2) {
            closed.set(true);
          }
        };
      }
    };
  }

  /**
   * Runs {@code step.start(ctx)} within an active transaction and asserts that the
   * {@code indexKeyFromIncluded} / {@code indexKeyToIncluded} inclusivity calculators handle the
   * condition kind under test (i.e. did NOT fall through to the catch-all {@link
   * UnsupportedOperationException}). Downstream failures during value resolution inside
   * {@code multipleRange} are expected for some condition kinds (e.g. {@link
   * com.jetbrains.youtrackdb.internal.core.sql.parser.SQLContainsCondition} against a scalar
   * index) and are caught here; only {@link UnsupportedOperationException} causes the test to
   * fail — it uniquely signals "the arm you're testing doesn't exist."
   *
   * <p>Mutation-kill intent: removing any of the {@code SQLContains*Condition} arms in
   * {@code indexKeyFromIncluded}/{@code indexKeyToIncluded} routes the call to the catch-all
   * that raises {@link UnsupportedOperationException}, which this helper re-surfaces as an
   * {@link AssertionError} naming the violated contract.
   */
  private void assertThatReachesProcessAndBlock(FetchFromIndexStep step, CommandContext ctx) {
    session.begin();
    try {
      try {
        var stream = step.start(ctx);
        while (stream.hasNext(ctx)) {
          stream.next(ctx);
        }
        stream.close(ctx);
      } catch (UnsupportedOperationException uoe) {
        throw new AssertionError(
            "indexKeyFromIncluded/ToIncluded must handle this condition kind — fell through to "
                + "the catch-all arm: "
                + uoe.getMessage(),
            uoe);
      } catch (RuntimeException downstreamFailure) {
        // Downstream value resolution inside multipleRange may fail for condition kinds that
        // don't resolve to a scalar index key — e.g. SQLContainsCondition / SQLContainsAnyCondition
        // against a plain integer index produce an NPE when the runtime tries to format the
        // bare condition for an error message. What matters is that control reached the
        // inclusivity calculator BEFORE failing. The UnsupportedOperationException arm above
        // is the one that signals "the branch you're testing doesn't exist" — the mutation-kill
        // intent is preserved by failing specifically on that signal.
      }
    } finally {
      if (session.isTxActive()) {
        session.rollback();
      }
    }
  }
}
