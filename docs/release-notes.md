# Release Notes

Behavioral changes that affect an existing deployment. Read the entry for every version
between your current version and your target version.

## Unreleased

### ORDER BY null placement is configurable

YouTrackDB now accepts `NULLS FIRST` and `NULLS LAST` on each YouTrackDB Query Language
(YQL) `ORDER BY` sort item.
The clause controls one item and comes before its optional `COLLATE` clause.

**Who is affected.** Queries that need a null order other than the shipped direction-specific
placement can now request it. Existing queries keep their prior results under the shipped values.
Ascending order ships with nulls first. Descending order ships with nulls last.

**How to configure the server.** Set `youtrackdb.query.orderBy.nullsPlacementAsc` or
`youtrackdb.query.orderBy.nullsPlacementDesc` to `FIRST` or `LAST`. Set these keys as Java
system properties at startup:

```
-Dyoutrackdb.query.orderBy.nullsPlacementAsc=LAST
-Dyoutrackdb.query.orderBy.nullsPlacementDesc=FIRST
```

An explicit YQL clause wins over the setting for that sort item. The values take effect when
YouTrackDB reads the properties at startup.

**How to configure one Gremlin traversal.** Use the public application programming interface
(API) parameters `YTDBQueryConfigParam.orderByNullsPlacementAsc` and
`YTDBQueryConfigParam.orderByNullsPlacementDesc`. Both parameters declare `String` values and
accept `FIRST` or `LAST` without case sensitivity.

```java
graph.traversal()
    .with(YTDBQueryConfigParam.orderByNullsPlacementAsc, "LAST")
    .with(YTDBQueryConfigParam.orderByNullsPlacementDesc, "FIRST");
```

The per-query parameters override the corresponding server settings and shipped values.
Caller-supplied comparators retain their own null handling. `Order.shuffle` is unchanged.

### Gremlin order by keeps a record that lacks the ordered property

A global-scope Gremlin `order()` step used to drop a record that carries no value for the
ordered property. That drop acted as a filter. It now keeps the record and sorts it as a
null key, the way the YouTrackDB Query Language (YQL) sorts a missing column.

**Who is affected.** Any query of the form `order().by(key)` over records where the key is
absent on some of them. Such a query returns MORE ROWS after the upgrade. A count after an
order returns a larger number. A `limit` after an order can return different records.

**Why the change.** The Gremlin result and the YQL result for the same sort disagreed. One
dropped the record and the other kept it. The two now agree.

**Nothing else changed.** A local-scope order, a `select` modulator, a `values` step, a
`group` modulator and a `dedup` modulator all keep their current filtering behavior.

**How to restore the old behavior.** Set
`youtrackdb.query.gremlin.orderIncludesMissingKey` to `false`. A deployment can set the
value without recompiling, through a Java system property at startup.

```
-Dyoutrackdb.query.gremlin.orderIncludesMissingKey=false
```

One traversal source can also opt out through the public application programming
interface (API) constant `YTDBQueryConfigParam.orderIncludesMissingKey`.

```java
graph.traversal().with(YTDBQueryConfigParam.orderIncludesMissingKey, false);
```

`StandardOrderSemanticsStrategy` also selects standard order semantics for one traversal source.
Do not combine the strategy with an explicit `orderIncludesMissingKey=true` option.
YouTrackDB rejects that contradiction.

An explicit per-traversal option has the highest precedence. The strategy ranks next.
A database configuration ranks below both traversal routes and above the global setting.

**Known limitation.** Under the including default, an ordered query can lose duplicate
rows. Three conditions must hold together. Several source records reach one target over a
hop, the order key carries an index, and the order applies to that target. The loss affects
the translated path only, and it drops duplicates rather than whole records. The limitation
stands in this version, and a later version repairs it. The
[Gremlin order by guide](gremlin-order-by.md) states the conditions and the workarounds.

**No detection tool exists.** Nothing reports which of your queries change their result.
Review every query that orders by a property which some records do not carry.
