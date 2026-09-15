# Gremlin Order By and Missing Properties

YouTrackDB changes one Apache TinkerPop behavior on purpose. A global-scope Gremlin
`order()` step now keeps a record that lacks the ordered property. It sorts that record
the way the YouTrackDB Query Language (YQL) sorts a missing column.

## What changed

Standard order semantics treats a `by(...)` modulator as a filter. The modulator is a
traversal. An element without the property produces no value, so the traverser is dropped
before any comparison runs. `order().by("age")` therefore acts as a filter on `age`.

YQL never drops such a row. `SELECT FROM Person ORDER BY age` returns every person and
sorts a missing `age` as a null key.

YouTrackDB now applies the YQL rule to a global-scope Gremlin order.

Take three people. Alice is 30. Bob is 25. Nobody has no `age` property.

```groovy
g.V().hasLabel("Person").order().by("age").values("name")
```

Standard order semantics returns two rows.

```
Bob
Alice
```

YouTrackDB returns three rows.

```
Nobody
Bob
Alice
```

The null key sorts where YQL puts it. An ascending order puts the null key first. A
descending order puts it last. The two engines agree, so one query returns the same order
through Gremlin and through YQL.

A following step reads the kept record too. `order().by("age").count()` counts three
records, not two.

## What did not change

The change covers a GLOBAL-scope order modulator only. Every other modulator keeps its
filtering behavior.

- `order(Scope.local)` still drops an entry that lacks the key.
- `select("a").by("age")` still drops a record that lacks the key.
- `values("age")` still emits nothing for a record that lacks the key.
- `group().by("age")` and `groupCount().by("age")` still form no null bucket.

This produces one intentional difference between two spellings of a sort:

```groovy
g.V().hasLabel("Person").order().by("age")              // keeps the ageless person
g.V().hasLabel("Person").fold().order(Scope.local).by("age")  // drops the ageless entry
```

The two spellings differ by one argument. The difference is deliberate. Local scope orders
the entries inside one collection. A missing key kept there would change the size of a
collection inside a row rather than the set of rows. YQL has no local-order analogue.
The YouTrackDB translator never maps a local-scope order into a YQL `MATCH` query.

## Restoring standard order semantics

Three configuration routes carry a boolean value. Set the value to `false` to restore
standard order semantics. A traversal strategy provides another per-traversal route.

The routes rank in this order, highest first.

1. The explicit per-traversal option decides for one traversal source.
2. `StandardOrderSemanticsStrategy` selects standard order semantics when no explicit option
   conflicts with the strategy.
3. The database configuration decides for every traversal on that database.
4. The global setting has a default value of `true`, which keeps records.

An explicit `true` option and the strategy are contradictory. YouTrackDB rejects that
combination instead of choosing either route.

### The deployment-wide setting

The setting key is `youtrackdb.query.gremlin.orderIncludesMissingKey`. The enum constant is
`GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY`. Set the value to `false`
to restore standard order semantics filtering for every traversal.

```java
GlobalConfiguration.QUERY_GREMLIN_ORDER_INCLUDES_MISSING_KEY.setValue(false);
```

That call takes effect on an open database. The next traversal reads the new value.

A deployment can also set the value without recompiling. YouTrackDB reads the key from a
Java system property at startup.

```
-Dyoutrackdb.query.gremlin.orderIncludesMissingKey=false
```

### The per-traversal override

The public constant `YTDBQueryConfigParam.orderIncludesMissingKey` overrides the setting
for one traversal source. The constant is part of the public application programming interface (API).

```java
import com.jetbrains.youtrackdb.api.gremlin.tokens.YTDBQueryConfigParam;

var g = graph.traversal().with(YTDBQueryConfigParam.orderIncludesMissingKey, false);
var names = g.V().hasLabel("Person").order().by("age").values("name").toList();
```

The override wins over the deployment-wide setting. It works in both directions. A
deployment that turns the setting off can still ask one traversal for the including
behavior by passing `true`.

The override is read for each traversal, so a running database needs no restart to change
the answer for a single query.

### The traversal strategy

`StandardOrderSemanticsStrategy` selects standard order semantics for one traversal source.

```java
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.StandardOrderSemanticsStrategy;

var g = graph.traversal().withStrategies(StandardOrderSemanticsStrategy.instance());
```

The strategy can only select record removal. The strategy never overrides a `false` setting.
Do not combine the strategy with an explicit `orderIncludesMissingKey=true` option.
YouTrackDB rejects that contradiction. Remove the strategy with
`withoutStrategies(StandardOrderSemanticsStrategy.class)` when the option must keep records.

### The per-database configuration

A database opened with the key in its own configuration keeps that value. Neither the
global setting nor the system property can change it afterwards. Pass the key when the
graph is opened.

```java
var configuration = new BaseConfiguration();
configuration.setProperty("youtrackdb.query.gremlin.orderIncludesMissingKey", false);
var graph = GraphFactory.open(configuration);
```

This route outranks only the global setting. An explicit option or the strategy still wins.
The conformance executions set their order mode through dedicated test fixtures.

## Per-query null placement

Two public parameters set null placement for one traversal source.
`YTDBQueryConfigParam.orderByNullsPlacementAsc` controls `Order.asc`.
`YTDBQueryConfigParam.orderByNullsPlacementDesc` controls `Order.desc`.

Both parameters declare `String` as their value type. They accept `FIRST` and `LAST`, with
case-insensitive matching. Embedded callers can also pass an `OrderByNullsPlacement` enum value.
YouTrackDB stores that enum value as its portable string name.

```java
import com.jetbrains.youtrackdb.api.gremlin.tokens.YTDBQueryConfigParam;

var g = graph.traversal()
    .with(YTDBQueryConfigParam.orderByNullsPlacementAsc, "LAST")
    .with(YTDBQueryConfigParam.orderByNullsPlacementDesc, "FIRST");
```

Each parameter overrides the corresponding deployment-wide server setting and its shipped
value. The shipped values are `FIRST` for ascending order and `LAST` for descending order.

The parameters change only the framework comparators `Order.asc` and `Order.desc`.
A caller-supplied comparator keeps its own null handling. `Order.shuffle` is also unchanged.

## Known limitation

One shape loses rows under the including default. The limitation stands in the version that
ships this page. A later version repairs it, and this page states the repair when it lands.
Read this section before you run an ordered query on production data.

A query loses duplicate rows when all three of the following hold:

- Several source records reach the same target record over a hop, which is a fan-in.
- The order key carries an index.
- The order applies to the fan-in target.

```groovy
g.V().hasLabel("Person").as("src").out("knows").as("dst").hasLabel("Person").order().by("id")
```

With two people knowing the same two targets, this traversal returns two rows instead of
four. Cause: the order key carries no presence filter under the including default. The planner
then roots an ordered index scan on the target and visits each target once.

Three workarounds exist today. Order by a property that carries no index. Move the order
off the fan-in target. Set the per-traversal override to `false` for that query.

Only duplicate rows are lost, and only on the translated path. A record that lacks the
ordered key survives and sorts as a null key. A query without a hop is unaffected. A query
whose order key carries no index is unaffected. A query under the standard order semantics mode is
unaffected.

## Indexes that ignore null values

The including default is a lie if an ordered scan walks an index that stores no null
bucket. `CREATE INDEX … METADATA {ignoreNullValues: true}` builds that kind of index. The
planner then refuses the ordered scan and falls back to an in-memory sort, so the key-less
record still appears. The project default for a new index keeps the null bucket
(`youtrackdb.index.ignoreNullValuesDefault` is `false`).

## Conformance suites

The primary Apache TinkerPop conformance executions run with the shipped default.
Two additional executions run with standard order semantics.
The additional executions exclude only expectations changed by the fork.
