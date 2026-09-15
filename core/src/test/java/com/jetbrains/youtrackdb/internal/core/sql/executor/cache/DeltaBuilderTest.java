package com.jetbrains.youtrackdb.internal.core.sql.executor.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.config.GlobalConfiguration;
import com.jetbrains.youtrackdb.api.config.OrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.DbTestBase;
import com.jetbrains.youtrackdb.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrackdb.internal.core.command.CommandContext;
import com.jetbrains.youtrackdb.internal.core.db.DatabaseSessionEmbedded;
import com.jetbrains.youtrackdb.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrackdb.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrackdb.internal.core.db.record.record.Entity;
import com.jetbrains.youtrackdb.internal.core.db.record.record.RID;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.PropertyType;
import com.jetbrains.youtrackdb.internal.core.metadata.schema.schema.SchemaClass;
import com.jetbrains.youtrackdb.internal.core.record.RecordAbstract;
import com.jetbrains.youtrackdb.internal.core.sql.ResolvedOrderByNullsPlacement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLOrderBy;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrackdb.internal.core.sql.parser.SQLWhereClause;
import com.jetbrains.youtrackdb.internal.core.sql.parser.YouTrackDBSql;
import com.jetbrains.youtrackdb.internal.core.tx.FrontendTransactionImpl;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies {@link DeltaBuilder#buildForRecord} against a live {@link FrontendTransactionImpl}
 * carrying staged {@code recordOperations}. The builder is the merge-on-read correctness floor: it
 * must turn the transaction's post-populate mutations into the exact {@code (skipSet, injectList)}
 * pair a fresh uncached execution would imply, so every test pins both halves of that pair.
 *
 * <p>Each test stamps the entry's {@code populateMutationVersion} after creating the records that
 * stand in for the cache's frozen result, seeds {@link CachedEntry#getCachedRids()} with those RIDs
 * (so {@code cached_at_build} reflects what the populating stream pulled), then stages further
 * mutations and asserts the delta. The version stamp lets the assertions cover the version filter: a
 * pre-populate mutation never re-enters the delta, while a post-populate mutation on the same RID
 * does.
 */
public class DeltaBuilderTest {

  private static final String CLASS_NAME = "DeltaRec";
  private static final String FIELD = "n";

  private YouTrackDBImpl youTrackDB;
  private DatabaseSessionEmbedded db;

  @Before
  public void before() {
    youTrackDB = DbTestBase.createYTDBManagerAndDb(getClass().getSimpleName(), DatabaseType.MEMORY,
        getClass());
    db = youTrackDB.open(getClass().getSimpleName(), "admin", DbTestBase.ADMIN_PASSWORD);
    var cls = db.createClass(CLASS_NAME);
    cls.createProperty(FIELD, PropertyType.INTEGER);
  }

  @After
  public void after() {
    // Roll back any still-open transaction before closing: a test that fails mid-body skips its own
    // terminal rollback, and closing a session with an active tx turns one clean assertion failure into
    // a noisier close/drop failure that masks the real cause.
    if (db.getTransactionInternal().isActive()) {
      db.rollback();
    }
    db.close();
    youTrackDB.drop(getClass().getSimpleName());
    youTrackDB.close();
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private FrontendTransactionImpl tx() {
    return (FrontendTransactionImpl) db.getTransactionInternal();
  }

  /** Parses a full SELECT and returns its WHERE clause (or null when the query has none). */
  private static SQLWhereClause parseWhere(String selectSql) {
    return parseSelect(selectSql).getWhereClause();
  }

  private static SQLOrderBy parseOrderBy(String selectSql) {
    return parseSelect(selectSql).getOrderBy();
  }

  private static SQLSelectStatement parseSelect(String selectSql) {
    try {
      var parser = new YouTrackDBSql(new ByteArrayInputStream(selectSql.getBytes()));
      return (SQLSelectStatement) parser.parse();
    } catch (Exception e) {
      throw new AssertionError("Failed to parse: " + selectSql, e);
    }
  }

  /** A command context bound to the session, optionally carrying named input parameters. */
  private CommandContext ctx(Map<Object, Object> params) {
    var ctx = new BasicCommandContext(db);
    if (params != null) {
      ctx.setInputParameters(params);
    }
    return ctx;
  }

  /** Creates a record of CLASS_NAME with FIELD=value and returns it as an Entity. */
  private Entity newRec(int value) {
    var e = db.newEntity(CLASS_NAME);
    e.setProperty(FIELD, value);
    return e;
  }

  /**
   * Creates and commits a record of CLASS_NAME with FIELD=value in its own transaction, then opens a
   * fresh transaction and returns the record reloaded into it. The caller therefore inherits an open
   * transaction and must NOT call {@code db.begin()} itself. The returned record carries a persistent
   * RID and no operation in the open transaction, so a later {@link #stage} call produces a genuine
   * UPDATED / DELETED operation rather than a collapsed CREATE.
   */
  private Entity committedRec(int value) {
    db.begin();
    var e = db.newEntity(CLASS_NAME);
    e.setProperty(FIELD, value);
    var rid = ridOf(e);
    db.commit();
    db.begin();
    return db.load(rid);
  }

  /**
   * Stages a mutation of the given type on the record by driving {@code addRecordOperation} directly,
   * mirroring the production save path. Setting a property only marks the record dirty; the operation
   * (and its version re-stamp) lands when the record is registered with the transaction, which this
   * call forces deterministically. The collapse rules apply: an UPDATED on a record whose existing
   * operation is CREATED keeps it CREATED, while a DELETED always wins.
   */
  private void stage(Entity e, byte type) {
    tx().addRecordOperation((RecordAbstract) e, type);
  }

  private static RID ridOf(Entity e) {
    return ((RecordAbstract) e).getIdentity();
  }

  /**
   * Builds a RECORD entry whose frozen result is exactly {@code cachedRecords}: their RIDs seed
   * {@link CachedEntry#getCachedRids()} and the populate version is the transaction's current
   * mutation version, so any mutation already staged on these records is treated as pre-populate
   * (filtered out) and only later mutations enter the delta.
   */
  private CachedEntry recordEntry(SQLWhereClause where, SQLOrderBy orderBy,
      List<Entity> cachedRecords) {
    var entry = new CachedEntry(
        CacheableShape.RECORD, Set.of(CLASS_NAME), where, orderBy, null, null, null,
        tx().getMutationVersion());
    for (var e : cachedRecords) {
      entry.getCachedRids().add(ridOf(e));
    }
    return entry;
  }

  /**
   * Like {@link #recordEntry} but populates the class-filter closure through the production
   * {@link CachedEntry#computeEffectiveFromClasses} path (resolving the live {@link SchemaClass} and
   * its subclasses via {@code SchemaClass.getName()}) rather than the {@code Set.of(CLASS_NAME)}
   * literal shortcut. The two must agree on the name form the filter probes with
   * ({@code Entity.getSchemaClassName()}); building through the closure exercises that equivalence.
   */
  private CachedEntry recordEntryViaClosure(SQLWhereClause where, SQLOrderBy orderBy,
      List<Entity> cachedRecords) {
    var effectiveFromClasses =
        CachedEntry.computeEffectiveFromClasses(db.getClass(CLASS_NAME));
    var entry = new CachedEntry(
        CacheableShape.RECORD, effectiveFromClasses, where, orderBy, null, null, null,
        tx().getMutationVersion());
    for (var e : cachedRecords) {
      entry.getCachedRids().add(ridOf(e));
    }
    return entry;
  }

  // ===========================================================================
  // Tests
  // ===========================================================================

  /**
   * A true post-populate CREATE that matches the WHERE clause is injected and not skipped: the row
   * was never in the cache, so the merged view adds it. A post-populate CREATE that fails the WHERE
   * contributes nothing.
   */
  @Test
  public void postPopulateCreateInjectsWhenMatchingAndIsIgnoredWhenNot() {
    db.begin();
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    // Empty cache: no pre-populate rows. Populate version is now.
    var entry = recordEntry(where, null, List.of());

    var matching = newRec(5); // post-populate CREATE, matches WHERE
    newRec(-1); // post-populate CREATE, fails WHERE

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertEquals("Only the matching create is injected", 1, cursor.injectSize());
    assertEquals(ridOf(matching), cursor.getInjectList().get(0).getIdentity());
    assertTrue("A true post-populate create needs no skip entry (temp RID never streamed)",
        cursor.getSkipSet().isEmpty());
    db.rollback();
  }

  /**
   * A post-populate UPDATE of a record already in the cache must skip the stale cached copy and
   * inject the post-mutation copy when it still matches, so the view re-positions the row rather than
   * emitting it twice.
   */
  @Test
  public void postPopulateUpdateOfCachedRecordSkipsAndReinjects() {
    var rec = committedRec(1);
    var rid = ridOf(rec);
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    // rec is in the cache; populate version is captured before the update so the update is
    // post-populate and enters the delta.
    var entry = recordEntry(where, null, List.of(rec));

    rec.setProperty(FIELD, 2); // post-populate UPDATE, still matches
    stage(rec, RecordOperation.UPDATED);

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertTrue("Stale cached copy must be skipped", cursor.shouldSkip(rid));
    assertEquals("Post-mutation copy must be injected", 1, cursor.injectSize());
    assertEquals(rid, cursor.getInjectList().get(0).getIdentity());
    db.rollback();
  }

  /**
   * A post-populate UPDATE that drives a cached record out of the WHERE clause must skip the cached
   * copy and inject nothing, so the view drops the row.
   */
  @Test
  public void postPopulateUpdateThatFailsWhereSkipsWithoutInject() {
    var rec = committedRec(5);
    var rid = ridOf(rec);
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    var entry = recordEntry(where, null, List.of(rec));

    rec.setProperty(FIELD, -1); // post-populate UPDATE, no longer matches
    stage(rec, RecordOperation.UPDATED);

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertTrue("Cached row dropped from result is skipped", cursor.shouldSkip(rid));
    assertEquals("No injection for a row that no longer matches", 0, cursor.injectSize());
    db.rollback();
  }

  /**
   * A post-populate DELETE of a cached record must skip it (regardless of WHERE) and inject nothing,
   * so the view removes the row.
   */
  @Test
  public void postPopulateDeleteSkipsAndDoesNotInject() {
    var rec = committedRec(3);
    var rid = ridOf(rec);
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    var entry = recordEntry(where, null, List.of(rec));

    stage(rec, RecordOperation.DELETED); // post-populate DELETE

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertTrue("Deleted cached row is skipped", cursor.shouldSkip(rid));
    assertEquals("A delete never injects", 0, cursor.injectSize());
    db.rollback();
  }

  /**
   * A post-populate CREATE that is then DELETED in the same transaction collapses to a DELETED op (the
   * delete always wins the collapse). The DELETED dispatch arm must skip the RID and inject nothing,
   * exactly like a genuine delete — the row was created and removed within the tx, so a fresh execution
   * would not show it. This drives the collapse-to-DELETED path the genuine-delete test does not: here
   * the RID was never committed and is not in {@code cachedRids}, yet the DELETED arm still skips
   * unconditionally, proving the dispatch consults only the op type for DELETED, not cache membership or
   * WHERE.
   */
  @Test
  public void collapsedCreateThenDeleteSkipsAndNeverInjects() {
    db.begin();
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    // Empty cache: the create/delete pair is entirely post-populate.
    var entry = recordEntry(where, null, List.of());

    var rec = newRec(5); // post-populate CREATE (matches WHERE), op typed CREATED
    var rid = ridOf(rec);
    stage(rec, RecordOperation.DELETED); // collapses CREATE -> DELETE to a DELETED op
    assertEquals("the create+delete pair must collapse to a single DELETED op",
        RecordOperation.DELETED, tx().getRecordEntry(rid).type);

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertTrue("a collapsed create+delete skips the RID", cursor.shouldSkip(rid));
    assertEquals("a collapsed create+delete never injects", 0, cursor.injectSize());
    db.rollback();
  }

  /**
   * A post-populate UPDATE followed by a DELETE on a cached record collapses to a DELETED op. The
   * DELETED dispatch arm must skip the cached copy and inject nothing, so the view drops the row — the
   * same outcome as a direct delete. This pins that a collapsed update-then-delete reaches the DELETED
   * arm and is treated identically to a genuine delete even though the record IS in {@code cachedRids}
   * and would still match WHERE, proving the DELETED arm ignores both facts.
   */
  @Test
  public void collapsedUpdateThenDeleteOfCachedRecordSkipsAndNeverInjects() {
    var rec = committedRec(5); // committed before the tx, so staged ops are genuine (not a CREATE)
    var rid = ridOf(rec);
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    // The record is in the cache; populate version is captured before the mutations.
    var entry = recordEntry(where, null, List.of(rec));

    rec.setProperty(FIELD, 7); // still matches WHERE
    stage(rec, RecordOperation.UPDATED); // post-populate UPDATE
    stage(rec, RecordOperation.DELETED); // collapses UPDATE -> DELETE to a DELETED op
    assertEquals("the update+delete pair must collapse to a single DELETED op",
        RecordOperation.DELETED, tx().getRecordEntry(rid).type);

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertTrue("a collapsed update+delete of a cached row skips it", cursor.shouldSkip(rid));
    assertEquals("a collapsed update+delete never injects", 0, cursor.injectSize());
    db.rollback();
  }

  /**
   * The twin of {@link #collapsedCreateAlreadyCachedSkipsAndReinjects}: a collapsed CREATE (still typed
   * CREATED after absorbing an in-place update) whose post-mutation value drives it OUT of the WHERE
   * clause. The {@code CREATED, cached=true, matchAfter=false} dispatch cell must skip the stale cached
   * copy and inject NOTHING — a re-injection would emit a row a fresh execution no longer returns.
   * Exercises the {@code if (matchAfter)} false branch for the cached-CREATED arm, the
   * symmetric counterpart to the already-tested UPDATED-fails-WHERE case.
   */
  @Test
  public void collapsedCreateDrivenOutOfWhereSkipsWithoutReinject() {
    db.begin();
    var rec = newRec(5); // CREATED op, matches WHERE n > 0
    var rid = ridOf(rec);
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    // Treat the freshly-created record as already cached; stamp populate BEFORE the update so the
    // collapse (still typed CREATED, version re-stamped) is post-populate and enters the delta.
    var entry = recordEntry(where, null, List.of(rec));

    rec.setProperty(FIELD, -1); // collapsed update drives it out of WHERE (n > 0 now false)
    stage(rec, RecordOperation.UPDATED); // collapses onto the CREATED op, re-stamping its version

    // Sanity: the op is still typed CREATED after the collapse, exercising the CREATED cached arm.
    assertEquals(RecordOperation.CREATED, tx().getRecordEntry(rid).type);

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertTrue("stale cached copy of a collapsed create is skipped", cursor.shouldSkip(rid));
    assertEquals("no reinject once the collapsed create fails WHERE", 0, cursor.injectSize());
    db.rollback();
  }

  /**
   * Mutations that landed at or before the entry's populate version are already reflected in the
   * cached result, so the version filter ({@code op.version > populateMutationVersion}) must exclude
   * them: an update made before populate produces an empty delta.
   */
  @Test
  public void prePopulateMutationIsFilteredOut() {
    var rec = committedRec(1);
    rec.setProperty(FIELD, 2);
    stage(rec, RecordOperation.UPDATED); // mutation staged BEFORE the populate version is captured
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    // recordEntry stamps populateMutationVersion = current version, i.e. after the update above, so
    // op.version <= populateMutationVersion and the version filter must exclude it.
    var entry = recordEntry(where, null, List.of(rec));

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertTrue("Pre-populate mutation must not enter the delta", cursor.getSkipSet().isEmpty());
    assertEquals(0, cursor.injectSize());
    db.rollback();
  }

  /**
   * The collapse path keeps a CREATE→UPDATE record typed CREATED but re-stamps its version past
   * populate. When such a record is already in the cache ({@code cached_at_build=true}) it must be
   * skipped and re-injected, exactly like an UPDATED — proving {@code cached_at_build}, not the op
   * type, drives the dispatch for the collapse case.
   */
  @Test
  public void collapsedCreateAlreadyCachedSkipsAndReinjects() {
    db.begin();
    var rec = newRec(1); // CREATED op staged here
    var rid = ridOf(rec);
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    // Treat the freshly-created record as already cached, and stamp populate BEFORE the update so the
    // collapse (still typed CREATED, version re-stamped) is post-populate.
    var entry = recordEntry(where, null, List.of(rec));

    rec.setProperty(FIELD, 9);
    stage(rec, RecordOperation.UPDATED); // collapses onto the CREATED op, re-stamping its version

    // Sanity: the op is still typed CREATED after collapse, yet its version advanced past populate.
    var op = tx().getRecordEntry(rid);
    assertEquals(RecordOperation.CREATED, op.type);
    assertTrue(op.version > entry.getPopulateMutationVersion());

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertTrue("Collapsed-create-in-cache must skip the stale cached copy", cursor.shouldSkip(rid));
    assertEquals("And re-inject the post-mutation copy", 1, cursor.injectSize());
    assertEquals(rid, cursor.getInjectList().get(0).getIdentity());
    db.rollback();
  }

  /**
   * The injected rows must be ordered by the query's ORDER BY, not by mutation-iteration order, so
   * the view's sorted-merge stays correct. Three post-populate creates inserted out of order must
   * come back sorted ascending.
   */
  @Test
  public void injectListIsSortedByOrderBy() {
    db.begin();
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var entry = recordEntry(null, orderBy, List.of());

    newRec(30);
    newRec(10);
    newRec(20);

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertEquals(3, cursor.injectSize());
    assertEquals(Integer.valueOf(10), cursor.getInjectList().get(0).getProperty(FIELD));
    assertEquals(Integer.valueOf(20), cursor.getInjectList().get(1).getProperty(FIELD));
    assertEquals(Integer.valueOf(30), cursor.getInjectList().get(2).getProperty(FIELD));
    db.rollback();
  }

  /**
   * The WHERE re-evaluation must resolve {@code :param} bindings from the original query's command
   * context. A parameterized predicate ({@code n > :threshold}) re-evaluated with the same context
   * must include only records above the bound threshold; a fresh context would diverge.
   */
  @Test
  public void whereReEvalReusesContextParamBindings() {
    db.begin();
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > :threshold");
    var entry = recordEntry(where, null, List.of());

    var above = newRec(10); // > 5, matches
    newRec(3); // <= 5, does not match

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(Map.of("threshold", 5)));

    assertEquals("Only the record above the bound threshold is injected", 1, cursor.injectSize());
    assertEquals(ridOf(above), cursor.getInjectList().get(0).getIdentity());
    db.rollback();
  }

  /**
   * Two views built on the same entry at the same mutation version must share the identical immutable
   * {@code (skipSet, injectList)} pair rather than rebuild it, while each cursor still tracks its own
   * inject position. The shared halves prove cross-view delta sharing keyed on the mutation version.
   */
  @Test
  public void crossViewSharingReusesPairAtSameVersion() {
    db.begin();
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    var entry = recordEntry(where, null, List.of());

    newRec(1);
    newRec(2);

    var first = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));
    var versionAfterFirst = entry.getCachedDeltaVersion();
    var second = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertSame("Skip-set must be the same shared instance across views at one version",
        first.getSkipSet(), second.getSkipSet());
    assertSame("Inject-list must be the same shared instance across views at one version",
        first.getInjectList(), second.getInjectList());
    assertEquals("The entry's cached version must not change on the reuse path",
        versionAfterFirst, entry.getCachedDeltaVersion());
    db.rollback();
  }

  /**
   * A view built after a further mutation (a higher mutation version) must rebuild the pair rather
   * than reuse the stale one, so the entry's cached version advances and the new delta reflects the
   * extra mutation.
   */
  @Test
  public void higherVersionRebuildsDelta() {
    db.begin();
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    var entry = recordEntry(where, null, List.of());

    newRec(1);
    var firstCursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));
    assertEquals(1, firstCursor.injectSize());
    var firstVersion = entry.getCachedDeltaVersion();

    newRec(2); // advances the mutation version
    var secondCursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertTrue("A fresher mutation version must rebuild",
        entry.getCachedDeltaVersion() > firstVersion);
    assertEquals("The rebuilt delta must include the extra create", 2, secondCursor.injectSize());
    db.rollback();
  }

  /**
   * Builds the entry through the production {@link CachedEntry#computeEffectiveFromClasses} closure
   * (whose set is seeded from {@code SchemaClass.getName()}) instead of the {@code Set.of(CLASS_NAME)}
   * literal, then stages an in-class post-populate update. The delta must still skip the stale cached
   * copy and re-inject the post-mutation row, which can only happen if the filter's probe
   * ({@code Entity.getSchemaClassName()}) returns the same name form the closure stored. This pins the
   * name-form equivalence the other tests assume, so a future naming drift between the two accessors
   * (qualified vs simple name, namespace prefix) is caught here rather than silently emitting a stale
   * cached row.
   */
  @Test
  public void inClassMutationProducesDeltaWhenEntryBuiltThroughProductionClosure() {
    var rec = committedRec(1);
    var rid = ridOf(rec);
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    // Closure path: effectiveFromClasses comes from SchemaClass.getName(), not the literal shortcut.
    var entry = recordEntryViaClosure(where, null, List.of(rec));
    // Sanity: the closure stored the same name form the filter probes with, so the class filter
    // accepts this record rather than dropping it.
    assertTrue("Closure must contain the record's schema-class name form",
        entry.getEffectiveFromClasses().contains(rec.getSchemaClassName()));

    rec.setProperty(FIELD, 2); // post-populate UPDATE, still matches
    stage(rec, RecordOperation.UPDATED);

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertTrue("Stale cached copy must be skipped via the production-closure filter",
        cursor.shouldSkip(rid));
    assertEquals("Post-mutation copy must be injected", 1, cursor.injectSize());
    assertEquals(rid, cursor.getInjectList().get(0).getIdentity());
    db.rollback();
  }

  /**
   * The class filter must exclude mutations on records outside the query's class closure: a
   * post-populate create on an unrelated class contributes neither a skip nor an inject.
   */
  @Test
  public void mutationOnUnrelatedClassIsFiltered() {
    db.createClass("OtherRec");
    db.begin();
    var where = parseWhere("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD + " > 0");
    var entry = recordEntry(where, null, List.of());

    var other = db.newEntity("OtherRec");
    other.setProperty(FIELD, 7);

    var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

    assertTrue("Unrelated-class mutation contributes no skip", cursor.getSkipSet().isEmpty());
    assertEquals("Unrelated-class mutation contributes no inject", 0, cursor.injectSize());
    assertFalse(cursor.hasNextInject());
    db.rollback();
  }

  /**
   * Null placement belongs to the entry, so the sorted inject list and the later view merge cannot
   * disagree. The first build reads LAST from the storage setting and sorts the null key
   * last. Changing the storage setting and rebuilding at a fresher mutation version must keep the
   * first placement. The frozen cached rows were produced under it. A second reading would sort the
   * null key first here, and the merge would then rank rows the other way.
   */
  @Test
  public void nullPlacementIsFixedOncePerEntry() {
    var storageConfig = db.getStorage().getContextConfiguration();
    storageConfig.setValue(
        GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.LAST);
    try {
      db.begin();
      var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
      var entry = recordEntry(null, orderBy, List.of());

      newRec(10);
      db.newEntity(CLASS_NAME); // no sort key, so it compares as null
      var first = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

      assertEquals(2, first.injectSize());
      assertEquals(Integer.valueOf(10), first.getInjectList().get(0).getProperty(FIELD));
      assertNull("LAST puts the null key last",
          first.getInjectList().get(1).getProperty(FIELD));

      storageConfig.setValue(
          GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.FIRST);
      newRec(20); // advances the mutation version, which forces a rebuild
      var second = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

      assertEquals(new ResolvedOrderByNullsPlacement(
          OrderByNullsPlacement.LAST, OrderByNullsPlacement.LAST), entry.fixedNullsDefault());
      assertEquals(3, second.injectSize());
      assertNull("the entry keeps its placement across rebuilds",
          second.getInjectList().get(2).getProperty(FIELD));
      db.rollback();
    } finally {
      storageConfig.setValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, null);
    }
  }

  /**
   * A seed installed at populate governs every later comparison. The delta build must not read the
   * configuration again, so a change landing between populate and the first build cannot drift the
   * order. Here the seed says nulls last, the storage setting says nulls first, and the sorted
   * inject list has to follow the seed.
   */
  @Test
  public void seededPlacementSurvivesALaterConfigurationChange() {
    var storageConfig = db.getStorage().getContextConfiguration();
    storageConfig.setValue(
        GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, OrderByNullsPlacement.FIRST);
    try {
      db.begin();
      var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
      var entry = recordEntry(null, orderBy, List.of());
      entry.seedNullsDefault(ResolvedOrderByNullsPlacement.REVERSED);

      newRec(10);
      db.newEntity(CLASS_NAME); // no sort key, so it compares as null
      var cursor = DeltaBuilder.buildForRecord(entry, tx(), ctx(null));

      assertEquals(2, cursor.injectSize());
      assertEquals(Integer.valueOf(10), cursor.getInjectList().get(0).getProperty(FIELD));
      assertNull("the seed decides, not the current setting",
          cursor.getInjectList().get(1).getProperty(FIELD));
      db.rollback();
    } finally {
      storageConfig.setValue(GlobalConfiguration.QUERY_ORDER_BY_NULLS_PLACEMENT_ASC, null);
    }
  }

  /**
   * Seeding twice is a construction bug, because two comparisons of one entry must rank rows alike.
   * The assertion fails it here, where tests run with assertions enabled. Production keeps the first
   * placement instead of throwing. A throw would roll back a user transaction, and earlier
   * comparisons on this entry already used that first value.
   */
  @Test
  public void secondSeedIsRejectedAndTheFirstPlacementSurvives() {
    db.begin();
    var orderBy = parseOrderBy("SELECT FROM " + CLASS_NAME + " ORDER BY " + FIELD + " ASC");
    var entry = recordEntry(null, orderBy, List.of());
    entry.seedNullsDefault(ResolvedOrderByNullsPlacement.REVERSED);

    assertThrows(
        AssertionError.class,
        () -> entry.seedNullsDefault(ResolvedOrderByNullsPlacement.SHIPPED));
    assertEquals(
        "the first placement must survive a rejected second seed",
        ResolvedOrderByNullsPlacement.REVERSED,
        entry.fixedNullsDefault());
    db.rollback();
  }
}
