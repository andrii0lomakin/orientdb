# YQL - `SELECT`

YouTrackDB supports the YQL language to execute queries against the database engine.  
For more information, see [operators](YQL-Where.md#operators) and [functions](YQL-Where.md#functions).  

**Syntax**:

```sql
SELECT [ <Projections> ] [ FROM <Target> [ LET <Assignment>* ] ]
    [ WHERE <Condition>* ]
    [ GROUP BY <Property>* ]
    [ ORDER BY <Properties>* [ ASC|DESC ] [ NULLS FIRST|NULLS LAST ] * ]
    [ UNWIND <Property>* ]
    [ SKIP <SkipRecords> ]
    [ LIMIT <MaxRecords> ]
    [ TIMEOUT <Timeout> [ <STRATEGY> ] ]
```

- **[`<Projections>`](YQL-Query.md#projections)** Indicates the data you want to extract from the query as the result-set.
- Note: In YouTrackDB, this clause is optional. In the projections, you can define aliases for single fields using the `AS` keyword. Aliases cannot be used in the WHERE condition, GROUP BY, or ORDER BY (they will be evaluated to null).
- **`FROM`** Designates the object to query. This can be a class, a single Record ID, or a
set of Record IDs.
    - When querying a class, for `<target>` use the class name.
    - When querying Record IDs, you can specify one or a small set of records to query. This is useful when you need to specify a starting point in navigating graphs.
- **[`WHERE`](YQL-Where.md)** Designates conditions to filter the result-set.
- **[`LET`](YQL-Query.md#let-block)** Binds context variables to use in projections, conditions or sub-queries.
- **`GROUP BY`** Designates property on which to group the result-set.
<a id="order-by"></a>
- **`ORDER BY`** Designates the property with which to order the result-set.
  - The `ASC` or `DESC` direction keyword is optional. The default direction is ascending.
  - An explicit `NULLS FIRST` or `NULLS LAST` clause sets null placement for one sort item.
  - Without that clause, the setting for the item's direction supplies the default. The server
    value falls back to the shipped value:
    - `youtrackdb.query.orderBy.nullsPlacementAsc` ships as `FIRST`.
    - `youtrackdb.query.orderBy.nullsPlacementDesc` ships as `LAST`.
  - An optional `COLLATE` clause selects the collation. Put null placement before collation, as in
    `ORDER BY name DESC NULLS FIRST COLLATE ci`.

The same settings apply to Gremlin `order()`. An explicit clause overrides the setting.
If you are using a [projection](YQL-Query.md#projections), include the `ORDER BY` field in the
projection. `ORDER BY` works only on returned projection properties, not on `LET` variables.
- **[`UNWIND`](YQL-Query.md#unwinding)** Designates the property on which to unwind the collection. 
- **`SKIP`** Defines the number of records you want to skip from the start of the result-set.
- **`LIMIT`** Defines the maximum number of records in the result-set.  
- **`TIMEOUT`** Defines the maximum time in milliseconds for the query.  By default, queries have no timeouts.  If you don't specify a timeout strategy, it defaults to `EXCEPTION`.  These are the available timeout strategies:
    - `RETURN` Truncate the result-set, returning the data collected up to the timeout.
    - `EXCEPTION` Raises an exception.

**Examples**:

- Return all records of the class `Person`, where the name starts with `Luk`:
```sql
    SELECT FROM Person WHERE name LIKE 'Luk%'
```

  Alternatively, you might also use either of these queries:
```sql
  SELECT FROM Person WHERE name.left(3) = 'Luk'
  SELECT FROM Person WHERE name.substring(0,3) = 'Luk'
```

- Return all records of the class `AnimalType` where the collection `races` contains at least one entry where the first character is `e`, ignoring case:
```sql
   SELECT FROM AnimalType WHERE races CONTAINS( name.toLowerCase().substring(0, 1) = 'e')
```
 
- Return all records of the class `AnimalType` where the collection `races` contains at least one entry with names `European` or `Asiatic`:
```sql
     SELECT * FROM AnimalType WHERE races CONTAINS(name in ['European', 'Asiatic']) 
```

- Return all records in the class `Profile` where any field contains the word `danger`:
```sql
    SELECT FROM Profile WHERE ANY() LIKE '%danger%'
```

- Return all results on class `Profile`, ordered by the field `name` in descending order:
```sql
    SELECT FROM Profile ORDER BY name DESC
```

- Return results with null `name` values first, regardless of sort direction:
```sql
    SELECT FROM Profile ORDER BY name ASC NULLS FIRST
    SELECT FROM Profile ORDER BY name DESC NULLS FIRST
```

- Return results with null `name` values last:
```sql
    SELECT FROM Profile ORDER BY name ASC NULLS LAST
```
  
- Return the number of records in the class `Account` per city:
```sql
    SELECT COUNT(*) FROM Account GROUP BY city
```
  
- Return only a limited set of records:

```sql
   SELECT FROM [#10:3, #10:4, #10:5]
```
  
- Return three properties from the class `Profile`:

```sql
   SELECT nick, followings, followers FROM Profile
```
  
- Return the field `name` in uppercase and the field country name of the linked city of the address:

```sql
   SELECT name.toUpperCase(), address.city.country.name FROM Profile
```

## Projections
In the standard implementations of SQL, projections are mandatory. 
When projections are omitted, YouTrackDB returns the entire record.
That is, it reads no projection as the equivalent of the `*` wildcard.

```sql
  SELECT FROM Account
```

For all projections except the wildcard `*`, it creates a new temporary Map.
```sql
  SELECT name, age FROM Account
```

The key conventions for the returned Map entries are:
- Property name for plain properties, like `invoice` becoming `invoice`.
- First property name for chained properties, like `invoice.customer.name` becoming `invoice`.
- Function name for functions, like `MAX(salary)` becoming `max`.

If the key already exists, it uses a numeric progression.  For instance,

```sql
  SELECT MAX(incoming), MAX(cost) FROM Balance
```

```
------+------
 max  | max2
------+------
 1342 | 2478
------+------
```

To override the field names in the output, use the `AS` keyword.

```sql
 SELECT MAX(incoming) AS max_incoming, MAX(cost) AS max_cost FROM Balance
```

```
---------------+----------
 max_incoming  | max_cost
---------------+----------
 1342          | 2478
---------------+----------
```



## `LET` Block

The `LET` block contains context variables to assign each time YouTrackDB evaluates a record.
YouTrackDB discards these values once the query execution ends.  You can use context variables in projections, conditions, and sub-queries.

### Assigning Fields for Reuse

YouTrackDB allows crossing relationships in queries.  
In some cases, you may need to evaluate the same nested relationship branch multiple times.  
This can be optimized by using a `LET` variable that traverses the relationship only once.

```sql
SELECT FROM Profile WHERE address.city.name LIKE '%Saint%' AND 
          ( address.city.country.name = 'Italy' OR 
            address.city.country.name = 'France' )
```

Using the `LET` makes the query shorter and faster, because it traverses the relationships only once:

```sql
SELECT FROM Profile LET $city = address.city WHERE $city.name LIKE 
          '%Saint%' AND ($city.country.name = 'Italy' OR $city.country.name = 'France')
```

In this case, it traverses the path till `address.city` only once.

### Sub-query

The `LET` block allows you to assign a context variable to the result of a sub-query.

```sql
SELECT name, $avgSalary[0].avg AS companyAvg FROM Employee
          LET $avgSalary = ( SELECT AVG(salary) AS avg FROM Employee )
```

This query returns each employee's name alongside the company-wide average salary, computed once by the sub-query and reused for every row.

### `LET` Block in Projection

You can use context variables as part of a result-set in [projections](#projections).  For instance, the query below displays the city name from the previous example:

```sql
SELECT $temp.name FROM Profile LET $temp = address.city WHERE $temp.name
          LIKE '%Saint%' AND ( $temp.country.name = 'Italy' OR
          $temp.country.name = 'France' )
```


## Unwinding

YouTrackDB allows unwinding of collection fields and obtaining multiple records as a result, one for each element in the collection:

```sql
SELECT name, OUT("Friend").name AS friendName FROM Person
```

```
--------+-------------------
 name   | friendName
--------+-------------------
 'John' | ['Mark', 'Steve']
--------+-------------------
```

If you want one record for each element in `friendName`, you can rewrite the query using `UNWIND`:

```sql
SELECT name, OUT("Friend").name AS friendName FROM Person UNWIND friendName
```

```
--------+-------------
 name   | friendName
--------+-------------
 'John' | 'Mark'
 'John' | 'Steve'
--------+-------------
```

>**NOTE**: For more information on other YQL commands, see [YQL Commands](YQL-Commands.md).