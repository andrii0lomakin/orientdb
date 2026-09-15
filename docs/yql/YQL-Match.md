# YQL - `MATCH`

Queries the database in a declarative manner, using pattern matching.

**Simplified Syntax**


```
MATCH 
  {
    [class: <class>], 
    [as: <alias>], 
    [where: (<whereCondition>)]
  }
  .<functionName>(){
    [class: <className>], 
    [as: <alias>], 
    [where: (<whereCondition>)], 
    [while: (<whileCondition>)],
    [maxDepth: <number>],    
    [depthAlias: <identifier> ], 
    [pathAlias: <identifier> ],     
    [optional: (true | false)]
  }*
  [,
    [NOT]
    {
      [as: <alias>], 
      [class: <class>], 
      [where: (<whereCondition>)]
    }
    .<functionName>(){
      [class: <className>], 
      [as: <alias>], 
      [where: (<whereCondition>)], 
      [while: (<whileCondition>)],
      [maxDepth: <number>],    
      [depthAlias: <identifier> ], 
      [pathAlias: <identifier> ],     
      [optional: (true | false)]
    }*
  ]*
RETURN [DISTINCT] <expression> [ AS <alias> ] [, <expression> [ AS <alias> ]]*
GROUP BY <expression> [, <expression>]*
ORDER BY <expression> [, <expression>]* [ ASC|DESC ] [ NULLS FIRST|NULLS LAST ]
SKIP <number>
LIMIT <number>
```

A MATCH sort item accepts the same optional direction, null placement, and collation clauses
as a SELECT sort item. The shipped placement is `FIRST` for ascending order and `LAST` for
descending order. See the [`ORDER BY` section](YQL-Query.md#order-by) for clause order and
configuration details.

- **`<class>`** Defines a valid target class.
- **`as: <alias>`** Defines an alias for a node in the pattern.
- **`<whereCondition>`** Defines a filter condition to match a node in the pattern.  It supports the normal YQL [`WHERE`](YQL-Where.md) clause.  
You can also use the `$currentMatch` and `$matched` [context variables](#context-variables).
- **`<functionName>`** Defines a graph function to represent the connection between two nodes.  For instance, `out()`, `in()`, `outE()`, `inE()`, etc.
  For `out()`, `in()`, and `both()`, a shortened *arrow* syntax is also supported:
    - `{...}.out(){...}` can be written as `{...}-->{...}`
    - `{...}.out("EdgeClass"){...}` can be written as `{...}-EdgeClass->{...}`
    - `{...}.in(){...}` can be written as `{...}<--{...}`
    - `{...}.in("EdgeClass"){...}` can be written as `{...}<-EdgeClass-{...}`
    - `{...}.both(){...}` can be written as `{...}--{...}`
    - `{...}.both("EdgeClass"){...}` can be written as `{...}-EdgeClass-{...}`
- **`<whileCondition>`** Defines a condition that the statement must meet to allow the traversal of this path.
It supports the normal YQL [`WHERE`](YQL-Where.md) clause.  You can also use the `$currentMatch`, `$matched`, and `$depth` [context variables](#context-variables). 
For more information, see [Deep Traversal While Condition](#deep-traversal), below.
- **`<maxDepth>`** Defines the maximum depth for this single path.
- **`<depthAlias>`** This is valid only if you have a `while` or a `maxDepth`. It defines the alias to be used to store the depth of this traversal. 
This alias can be used in the `RETURN` block to retrieve the depth of the current traversal.
- **`<pathAlias>`** This is valid only if you have a `while` or a `maxDepth`. It defines the alias to be used to store the elements traversed to reach this alias. 
This alias can be used in the `RETURN` block to retrieve the elements traversed to reach this alias.
- **`RETURN <expression> [ AS <alias> ]`** Defines elements in the pattern that you want returned.  It can use one of the following:
    - Aliases defined in the `as:` block.
    - `$matches` Indicating all defined aliases.
    - `$paths` Indicating the full traversed paths.
    - `$elements` Indicating that all the elements that would be returned by the $matches have to be returned flattened, without duplicates.
    - `$pathElements` Indicating that all the elements that would be returned by the $paths have to be returned flattened, without duplicates.
- **`optional`** If set to true, allows you to evaluate and return a pattern even if that particular node does not match the pattern itself (i.e., there is no value for that node in the pattern). In the current version, optional nodes are allowed only on right terminal nodes, e.g., `{} --> {optional:true}` is allowed, `{optional:true} <-- {}` is not.
- **`NOT` patterns** Together with normal patterns, you can also define negative patterns.
A result will be returned only if it also DOES NOT match any of the negative patterns, i.e., if it matches at least one of the negative patterns it won't be returned.


**Examples**

The following examples are based on this sample dataset from the class `Person`:

```
+---------+----------+---------+-----------+-------+-------------------+----------------+------------------+
|         |       METADATA      |           |    PROPERTIES       |       IN       |       OUT        |
| @rid    | @version | @class  | surname   | name  | fullName          | Friend         | Friend           |
+---------+----------+---------+-----------+-------+-------------------+----------------+------------------+
| #12:0   | 8        | Person  | Doe       | John  | #12:0-John Doe    |                | #12:1,#12:3,#12:2|
| #12:1   | 7        | Person  | Smith     | John  | #12:1-John Smith  | #12:0          | #12:2            |
| #12:2   | 8        | Person  | Smith     | Jenny | #12:2-Jenny Smith | #12:1,#12:0    | #12:4            |
| #12:3   | 7        | Person  | Bean      | Frank | #12:3-Frank Bean  | #12:0          | #12:4            |
| #12:4   | 7        | Person  | Bean      | Mark  | #12:4-Mark Bean   | #12:3,#12:2    |                  |
+---------+----------+---------+-----------+-------+-------------------+----------------+------------------+
```

```
#12:3-Frank Bean <--Friend-- #12:0-John Doe --Friend--> #12:1-John Smith
       |                          |                           |
     Friend                     Friend                      Friend
       |                          |                           |
       v                          v                           v
#12:4-Mark Bean  <--Friend-- #12:2-Jenny Smith <--------------+
```

- Find all people with the name John:

```sql
   MATCH {class: Person, as: people, where: (name = 'John')} 
            RETURN people
```
```
  ---------
    people 
  ---------
    #12:0
    #12:1
  ---------
```

- Find all people with the name John and the surname Smith:

```sql
   MATCH {class: Person, as: people, where: (name = 'John' AND surname = 'Smith')} 
     RETURN people
```
```  
  -------
  people
  -------
   #12:1
  -------
```


- Find people named John with their friends:


```sql
MATCH {class: Person, as: person, where: (name = 'John')}.both('Friend') {as: friend} 
            RETURN person, friend
```

```
  --------+---------
   person | friend 
  --------+---------
   #12:0  | #12:1
   #12:0  | #12:2
   #12:0  | #12:3
   #12:1  | #12:0
   #12:1  | #12:2
  --------+---------
```


- Find friends of friends:

```sql
MATCH  {class: Person, as: person, where: (name = 'John' AND surname = 'Doe')}
      .both('Friend').both('Friend') {as: friendOfFriend} 
  RETURN person, friendOfFriend
```


```
  --------+----------------
   person | friendOfFriend
  --------+----------------
   #12:0  | #12:0
   #12:0  | #12:2
   #12:0  | #12:0
   #12:0  | #12:1
   #12:0  | #12:4
   #12:0  | #12:0
   #12:0  | #12:4
  --------+----------------
```

  Note: MATCH returns all occurrences of a pattern, including duplicates.
  John Doe (#12:0) appears three times as `friendOfFriend` because three different
  two-hop paths lead back to him (via each of his three friends).

- Find people, excluding the current user:

```sql
   MATCH {class: Person, as: person, where: (name = 'John' AND 
            surname = 'Doe')}.both('Friend').both('Friend'){as: friendOfFriend,
  	where: ($matched.person != $currentMatch)} 
  	RETURN person, friendOfFriend
```
```
  --------+----------------
   person | friendOfFriend
  --------+----------------
   #12:0  | #12:2
   #12:0  | #12:1
   #12:0  | #12:4
   #12:0  | #12:4
  --------+----------------
```

- Find friends of friends to the sixth degree of separation:

```sql
MATCH {class: Person, as: person, where: (name = 'John' AND 
            surname = 'Doe')}.both('Friend'){as: friend, 
  	where: ($matched.person != $currentMatch), while: ($depth < 6)}
  	RETURN person, friend
```
```
  --------+---------
   person | friend
  --------+---------
   #12:0  | #12:0
   #12:0  | #12:1
   #12:0  | #12:2
   #12:0  | #12:3
   #12:0  | #12:4
  --------+---------
```


- Finding friends of friends to six degrees of separation, since a particular date:

```sql
  MATCH {class: Person, as: person, 
            where: (name = 'John')}.(bothE('Friend'){
  	where: (date < ?)}.bothV()){as: friend, 
  	while: ($depth < 6)} RETURN person, friend
```

  In this case, the condition `$depth < 6` refers to traversing the block `bothE('Friend')` six times.


- Find friends of my friends who are also my friends, using multiple paths:


```sql
  MATCH {class: Person, as: person, where: (name = 'John' AND 
            surname = 'Doe')}.both('Friend').both('Friend'){as: friend},
  	{ as: person }.both('Friend'){ as: friend } 
  	RETURN person, friend
```
```
  --------+--------
   person | friend
  --------+--------
   #12:0  | #12:1
   #12:0  | #12:2
  --------+--------
```

  In this case, the statement matches two expressions: the first to friends of friends, the second to direct friends.
  Each expression shares the common aliases (`person` and `friend`). To match the whole statement, 
  the result must match both expressions, where the alias values for the first expression are the same as those of the second.

- Find common friends of John and Jenny:

```sql
MATCH {class: Person, where: (name = 'John' AND 
            surname = 'Doe')}.both('Friend'){as: friend}.both('Friend')
  	{class: Person, where: (name = 'Jenny')} RETURN friend
```
```  
  --------
   friend
  --------
   #12:1
  --------
```

  The same, with two match expressions:


```sql
MATCH {class: Person, where: (name = 'John' AND 
            surname = 'Doe')}.both('Friend'){as: friend}, 
  	{class: Person, where: (name = 'Jenny')}.both('Friend')
  	{as: friend} RETURN friend
```


## DISTINCT

The MATCH statement returns all the occurrences of a pattern, even if they are duplicated. To have unique, distinct records
as a result, you need to specify the DISTINCT keyword in the RETURN statement.

Example: suppose you have a dataset like the following:

```sql
  CREATE VERTEX Person SET name = 'John', surname = 'Smith';
  CREATE VERTEX Person SET name = 'John', surname = 'Harris';
  CREATE VERTEX Person SET name = 'Jenny', surname = 'Rose';
```

This is the result of the query without a DISTINCT clause:

```sql
  MATCH {class: Person, as:p} RETURN p.name as name
```

```
  --------
   name
  --------
   John
  --------
   John
  --------
   Jenny
  --------
```

And this is the result of the query with a DISTINCT clause:

```sql
   MATCH {class: Person, as:p} RETURN DISTINCT p.name as name
```

```
  --------
   name
  --------
   John
  --------
   Jenny
  --------
```


## Context Variables

When running these queries, you can use any of the following context variables:

| Variable | Description |
|---|---|
|`$matched`| Gives access to the aliases matched so far in the current pattern.  Use `$matched.<alias>` to refer to a previously matched node (e.g., `$matched.person`).  You can use this in the `where:` and `while:` conditions to refer to current partial matches or as part of the `RETURN` value.|
|`$currentMatch`| Gives the current complete node during the match.|
|`$depth`| Gives the traversal depth, following a single path item where a `while:` condition is defined.|



## Use Cases


### Expanding Attributes

You can run this statement as a sub-query inside of another statement.  Doing this allows you to obtain details and aggregate data from the inner [`SELECT`](YQL-Query.md) query.


```sql
SELECT person.name AS name, person.surname AS surname,
          friend.name AS friendName, friend.surname AS friendSurname
		  FROM (MATCH {class: Person, as: person,
		  where: (name = 'John')}.both('Friend'){as: friend}
		  RETURN person, friend)
```

```
--------+----------+------------+---------------
 name   | surname  | friendName | friendSurname
--------+----------+------------+---------------
 John   | Doe      | John       | Smith
 John   | Doe      | Jenny      | Smith
 John   | Doe      | Frank      | Bean
 John   | Smith    | John       | Doe
 John   | Smith    | Jenny      | Smith
--------+----------+------------+---------------
```

As an alternative, you can use the following:


```sql
MATCH {class: Person, as: person,
		  where: (name = 'John')}.both('Friend'){as: friend}
		  RETURN 
		  person.name as name, person.surname as surname, 
		  friend.name as friendName, friend.surname as friendSurname
```

```
--------+----------+------------+---------------
 name   | surname  | friendName | friendSurname
--------+----------+------------+---------------
 John   | Doe      | John       | Smith
 John   | Doe      | Jenny      | Smith
 John   | Doe      | Frank      | Bean
 John   | Smith    | John       | Doe
 John   | Smith    | Jenny      | Smith
--------+----------+------------+---------------
```


### Incomplete Hierarchy

Consider building a database for a company that shows a hierarchy of departments within the company.  For instance,

```
           [manager] department        
          (employees in department)    
                                       
                                       
                [m0]0                   
                 (e1)                  
                 /   \                 
                /     \                
               /       \               
           [m1]1        [m2]2
          (e2, e3)     (e4, e5)        
             / \         / \           
            3   4       5   6          
          (e6) (e7)   (e8)  (e9)       
          /  \                         
      [m3]7    8                       
      (e10)   (e11)                    
       /                               
      9                                
  (e12, e13)                         
```

This loosely shows that,
- Department `0` is the company itself, manager 0 (`m0`) is the CEO
- `e10` works at department `7`, his manager is `m3`
- `e12` works at department `9`, this department has no direct manager, so `e12`'s manager is `m3` (the upper manager)


In this case, you would use the following query to find out who's the manager to a particular employee:


```sql
SELECT EXPAND(manager) FROM (MATCH {class:Employee, 
          where: (name = ?)}.out('WorksAt').out('ParentDepartment')
		  {while: (out('Manager').size() == 0), 
		  where: (out('Manager').size() > 0)}.out('Manager')
		  {as: manager} RETURN manager)
```



### Deep Traversal

Match path items act in different manners, depending on whether or not you use `while:` conditions in the statement.

For instance, consider the following graph:

``` 
[name='a'] -FriendOf-> [name='b'] -FriendOf-> [name='c']
```

Running the following statement on this graph only returns `b`:

```sql
MATCH {class: Person, where: (name = 'a')}.out("FriendOf")
          {as: friend} RETURN friend
```
```
--------
 friend 
--------
 b
--------
```

What this means is that it traverses the path item `out("FriendOf")` exactly once.  It only returns the result of that traversal.
If you add a `while` condition:

```sql
MATCH {class: Person, where: (name = 'a')}.out("FriendOf")
          {as: friend, while: ($depth < 2)} RETURN friend
```

```
---------
 friend
---------
 a
 b
 c
---------
```

Including a `while:` condition on the match path item causes YouTrackDB to evaluate this item as zero to *n* times.
That means that it returns the starting node (`a`, in this case) as the result of zero traversal.
The `while:` condition controls whether to continue traversing from the current node, but the node itself is still included in the result.  In this example, `while: ($depth < 2)` stops traversal at `c` (depth 2), but `c` is still returned.

To exclude the starting point, you need to add a `where:` condition, such as:

```sql
MATCH {class: Person, where: (name = 'a')}.out("FriendOf")
          {as: friend, while: ($depth < 2), where: ($depth > 0)}
		  RETURN friend
```

As a general rule,
- **`while` Condition:** Defines whether to continue to the next traversal level (evaluated starting at depth zero, on the origin node).
- **`where` Condition:** Defines whether the current element (the origin node at depth zero, or the target node at greater depths) should be included in the result.


For instance, suppose that you have a genealogical tree.  
In the tree, you want to show a person, the grandparent, and the grandparent of that grandparent, and so on.
The result: saying that the person is at level zero, parents at level one, grandparents at level two, etc., you would see all ancestors on even levels.  That is, `level % 2 == 0`.

To get this, you might use the following query:


```sql
MATCH {class: Person, where: (name = 'a')}.out("Parent")
          {as: ancestor, while: (true), where: ($depth % 2 = 0)}
		  RETURN ancestor
```


## Best Practices

Queries can involve multiple operations, based on the domain model and use case. 
In some cases, like projection and aggregation, you can easily manage them with a [`SELECT`](YQL-Query.md) query.
With others, such as pattern matching and deep traversal, [`MATCH`](YQL-Match.md) statements are more appropriate.

Use [`SELECT`](YQL-Query.md) and [`MATCH`](YQL-Match.md) statements together (that is, through sub-queries) to give each statement the correct responsibilities.

### Filtering Record Attributes for a Single Class

Filtering based on record attributes for a single class is straightforward with both statements.  That is, finding all people named John can be written as:


```sql
   SELECT FROM Person WHERE name = 'John'
```

You can also write it as,
```sql
MATCH {class: Person, as: person, where: (name = 'John')} 
          RETURN person
```

The efficiency remains the same.  Both queries use an index.
With [`SELECT`](YQL-Query.md), you obtain expanded records, while with [`MATCH`](YQL-Match.md), you obtain the Record IDs.


### Filtering on Record Attributes of Connected Elements

Filtering based on the record attributes of connected elements, such as neighboring vertices, 
can grow tricky when using [`SELECT`](YQL-Query.md), while with [`MATCH`](YQL-Match.md) it is simple.

For instance, find all people living in Kyiv that have a friend called John.
There are three different ways you can write this, using [`SELECT`](YQL-Query.md):

```sql
SELECT FROM Person WHERE BOTH('Friend').name CONTAINS 'John' AND out('LivesIn').name CONTAINS 'Kyiv'
```
```sql
SELECT FROM (SELECT BOTH('Friend') FROM Person WHERE name = 'John') WHERE out('LivesIn').name CONTAINS 'Kyiv'
```
```sql
SELECT FROM (SELECT in('LivesIn') FROM City WHERE name = 'Kyiv') WHERE BOTH('Friend').name CONTAINS 'John'
```

In the first version, the query is more readable, but it does not use indexes, so it is less efficient in terms of execution time.
The second and third use indexes if they exist, (on `Person.name` or `City.name`, both in the sub-query), but they're harder to read.
Which index they use depends only on the way you write the query. 
That is, if you only have an index on `City.name` and not `Person.name`, the second version doesn't use an index.

Using a [`MATCH`](YQL-Match.md) statement, the query becomes:


```sql
MATCH {class: Person, where: (name = 'John')}.both("Friend")
          {as: result}.out('LivesIn'){class: City, where: (name = 'Kyiv')}
		  RETURN result
```

Here, the query executor optimizes the query for you, choosing indexes where they exist.  
Moreover, the query becomes more readable, especially in complex cases, such as multiple nested [`SELECT`](YQL-Query.md) queries.

### Projections and Grouping Operations

Projections and grouping operations are better expressed with a [`SELECT`](YQL-Query.md) query.  
If you need to filter and do projection or aggregation in the same query, you can use [`SELECT`](YQL-Query.md) and [`MATCH`](YQL-Match.md) in the same statement.

This is particularly important when you expect a result that contains attributes from different connected records (Cartesian product).
For instance, to retrieve names, their friends, and the date since they became friends:


```sql
SELECT person.name AS name, friendship.since AS since, friend.name 
          AS friend FROM (MATCH {class: Person, as: person}.bothE('Friend')
		  {as: friendship}.bothV(){as: friend, 
		  where: ($matched.person != $currentMatch)} 
		  RETURN person, friendship, friend)
```

The same can be also achieved with the MATCH only:


```sql
MATCH {class: Person, as: person}.bothE('Friend')
		  {as: friendship}.bothV(){as: friend, 
		  where: ($matched.person != $currentMatch)} 
		  RETURN person.name as name, friendship.since as since, friend.name as friend
```

### RETURN expressions

In the RETURN section you can use:

**multiple expressions**, with or without an alias (if no alias is defined, YouTrackDB will generate a default alias for you), comma-separated

```sql
MATCH 
  {class: Person, as: person}
  .bothE('Friend'){as: friendship}
  .bothV(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN person, friendship, friend
```
result: 

```
| person | friendship | friend |
--------------------------------
| #12:0  | #13:0      | #12:2  |
| #12:0  | #13:1      | #12:3  |
| #12:1  | #13:2      | #12:3  |
```

```sql
MATCH 
  {class: Person, as: person}
  .bothE('Friend'){as: friendship}
  .bothV(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN person.name as name, friendship.since as since, friend.name as friend
```
result:
```
| name | since | friend |
-------------------------
| John | 2015  | Frank  |
| John | 2015  | Jenny  |
| John | 2016  | Jenny  |
```

```sql
MATCH 
  {class: Person, as: person}
  .bothE('Friend'){as: friendship}
  .bothV(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN person.name + " is a friend of " + friend.name as friends
```

result: 
```
| friends                    |
------------------------------
| John is a friend of Frank  |
| John is a friend of Jenny  |
| John is a friend of Jenny  |
```

**$matches** — returns all the patterns that match the current statement. Each row in the result set will be a single pattern, containing only nodes in the statement that have an `as:` defined

```sql
MATCH 
  {class: Person, as: person}
  .bothE('Friend'){}                                                  // no 'as:friendship' in this case
  .bothV(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN $matches
```
result: 

```
| person |  friend | 
--------------------
| #12:0  |  #12:2  |
| #12:0  |  #12:3  |
| #12:1  |  #12:3  |

```

**$paths** — returns all the patterns that match the current statement. 
Each row in the result set will be a single pattern, containing all the nodes in the statement. 
For nodes that have an `as:`, the alias will be returned; for the others, a default alias is generated (automatically generated aliases start with `$YOUTRACKDB_DEFAULT_ALIAS_`)

```sql
MATCH 
  {class: Person, as: person}
  .bothE('Friend'){}                                                  // no 'as:friendship' in this case
  .bothV(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN $paths
```
result: 
```
| person | friend | $YOUTRACKDB_DEFAULT_ALIAS_0 |
---------------------------------------------
| #12:0  | #12:2  | #13:0                   |
| #12:0  | #12:3  | #13:1                   |
| #12:1  | #12:3  | #13:2                   |
```

**$elements** — the same as `$matches`, but for each node present in the pattern, a single row is created in the result set (no duplicates)

```sql
MATCH 
  {class: Person, as: person}
  .bothE('Friend'){}                                                  // no 'as:friendship' in this case
  .bothV(){as: friend, where: ($matched.person != $currentMatch)} 
RETURN $elements
```
result: 

```
| @rid   |  @class | name   |  .....   |
----------------------------------------
| #12:0  |  Person | John   |  .....   |
| #12:1  |  Person | John   |  .....   |
| #12:2  |  Person | Jenny  |  .....   |
| #12:3  |  Person | Frank  |  .....   |
```

**$pathElements** — the same as `$paths`, but for each node present in the pattern, a single row is created in the result set (no duplicates)

```sql
MATCH
  {class: Person, as: person}
  .bothE('Friend'){}                                                  // no 'as:friendship' in this case
  .bothV(){as: friend, where: ($matched.person != $currentMatch)}
RETURN $pathElements
```

result:

```
| @rid   |  @class | name   | since  |  .....   |
-------------------------------------------------
| #12:0  |  Person | John   |        |  .....   |
| #12:1  |  Person | John   |        |  .....   |
| #12:2  |  Person | Jenny  |        |  .....   |
| #12:3  |  Person | Frank  |        |  .....   |
| #13:0  |  Friend |        |  2015  |  .....   |
| #13:1  |  Friend |        |  2015  |  .....   |
| #13:2  |  Friend |        |  2016  |  .....   |
```


### Arrow Notation

`out()`, `in()`, and `both()` operators can be replaced with arrow notation `-->`, `<--`, and `--`

E.g., the query

```sql
MATCH {class: V, as: a}.out(){}.out(){}.out(){as:b}
RETURN a, b
```

can be written as

```sql
MATCH {class: V, as: a} --> {} --> {} --> {as:b}
RETURN a, b
```

E.g., the query (things that belong to friends)

```sql
MATCH {class: Person, as: a}.out('Friend'){as:friend}.in('BelongsTo'){as:b}
RETURN a, b
```

can be written as

```sql
MATCH {class: Person, as: a}  -Friend-> {as:friend} <-BelongsTo- {as:b}
RETURN a, b
```

Using arrow notation, the curly braces are mandatory on both sides, e.g.:

```sql
MATCH {class: Person, as: a} --> {} --> {as:b} RETURN a, b            -- allowed
MATCH {class: Person, as: a} --> --> {as:b} RETURN a, b               -- NOT allowed
MATCH {class: Person, as: a}.out().out(){as:b} RETURN a, b            -- allowed
MATCH {class: Person, as: a}.out(){}.out(){as:b} RETURN a, b          -- allowed
```

### Negative (NOT) patterns
Together with normal patterns, you can also define negative patterns.
A result will be returned only if it also DOES NOT match any of the negative patterns, 
i.e., if the result matches at least one of the negative patterns it won't be returned.

As an example, consider the following problem: given a social network, 
choose a single person and return all the people that are friends of their friends,
but that are not their direct friends.

The pattern can be calculated as follows:

```sql
MATCH
  {class:Person, as:a, where:(name = "John")} -FriendOf-> {as:b} -FriendOf-> {as:c},
  NOT {as:a} -FriendOf-> {as:c}
RETURN c.name
```