# Operations with subquery results: UNION, INTERSECT, EXCEPT

## Joining (UNION) {#union}

Joins the results of multiple `SELECT` queries (or subqueries), removing any duplicates.
Its behavior is identical to executing `UNION ALL` followed by `SELECT DISTINCT *`.
See [UNION ALL](#union-all) for detailed information about its behavior.

```yql
SELECT key FROM T1
UNION
SELECT key FROM T2;
```

You can explicitly specify the `DISTINCT` keyword, though it won't affect the execution result.

```yql
SELECT key FROM T1
UNION DISTINCT
SELECT key FROM T2;
```

#### Example

```yql
SELECT * FROM (VALUES (1, 2)) AS t(x, y)
UNION
SELECT * FROM (VALUES (1, 2)) AS t(x, y);
```
```
x	y
1	2
```

## Joining with duplicates (UNION ALL) {#union-all}

Joins the results of multiple `SELECT` queries (or subqueries) without removing duplicates.

```yql
SELECT key FROM T1
UNION ALL
SELECT key FROM T2;
```

#### Example

```yql
SELECT * FROM (VALUES (1, 2)) AS t(x, y)
UNION ALL
SELECT * FROM (VALUES (1, 2)) AS t(x, y);
```
```
x	y
1	2
1	2
```

## Intersection (INTERSECT) {#intersect}

Finds the intersection of the results of two `SELECT` queries (or subqueries), removing any duplicates.
Its behavior is identical to executing `INTERSECT ALL` followed by `SELECT DISTINCT *`.
See [INTERSECT ALL](#intersect-all) for detailed information about its behavior.

```yql
SELECT key FROM T1
INTERSECT
SELECT key FROM T2;
```

You can explicitly specify the `DISTINCT` keyword, though it won't affect the execution result.

```yql
SELECT key FROM T1
INTERSECT DISTINCT
SELECT key FROM T2;
```

#### Example

```yql
SELECT * FROM (VALUES (1), (1), (1), (2)) AS t(x)
INTERSECT
SELECT * FROM (VALUES (1), (1)) AS t(x);
```
```
x
1
```

## Intersection with duplicates (INTERSECT ALL) {#intersect-all}

Finds the intersection of the results of two `SELECT` queries (or subqueries) without removing duplicates.

```yql
SELECT key FROM T1
INTERSECT ALL
SELECT key FROM T2;
```

The query returns a table of duplicate rows that occur in both source tables.  
The number of duplicate rows equals the minimum number of occurrences in either of the source tables.

#### Example

```yql
SELECT * FROM (VALUES (1), (1), (1), (2)) AS t(x)
INTERSECT ALL
SELECT * FROM (VALUES (1), (1)) AS t(x);
```
```
x
1
1
```

## Exclusion (EXCEPT) {#except}

Excludes the results of one `SELECT` query (or subquery) from another.

```yql
SELECT key FROM T1
EXCEPT
SELECT key FROM T2;
```

You can explicitly specify the `DISTINCT` keyword, though it won't affect the execution result.

```yql
SELECT key FROM T1
EXCEPT DISTINCT
SELECT key FROM T2;
```

The query returns a table of unique rows that occur in the first table but not in the second.

#### Example

```yql
SELECT * FROM (VALUES (1), (1), (1), (2)) AS t(x)
EXCEPT
SELECT * FROM (VALUES (1)) AS t(x);
```
```
x
2
```

## Exclusion with duplicates (EXCEPT ALL) {#except-all}

Excludes the results of one `SELECT` query (or subquery) from another, taking duplicates (the number of occurrences) into account.

```yql
SELECT key FROM T1
EXCEPT ALL
SELECT key FROM T2;
```

The query returns a table of duplicate rows that occur more frequently in the first table than in the second.  
The number of duplicate rows equals the difference in their occurrences between the first and second source tables.

#### Example

```yql
SELECT * FROM (VALUES (1), (1), (1), (2)) AS t(x)
EXCEPT ALL
SELECT * FROM (VALUES (1)) AS t(x);
```
```
x
2
1
1
```

## "By name" and "by position" modes {#positional-mode}

The operations above can be launched in two modes: by column names (the default mode) and by column positions (corresponds to the ANSI SQL standard and is enabled by the [PRAGMA](../pragma/global.md#positionalunionall)).

### By column names

In the "by name" mode, the output of the resulting data schema uses the following rules:

* The resulting table includes all columns that were found in at least one of the input tables.
* if a column wasn't present in any input table, it's automatically assigned [optional data type](../../types/optional.md) (that permits `NULL` value);
* If a column in different input tables had different types, then the shared type (the broadest one) is output.
* If a column in different input tables had a heterogeneous type, for example, string and numeric, an error is raised.

The order of output columns in this mode is equal to the largest common prefix of the order of inputs, followed by all other columns in the alphabetic order.
If the largest common prefix is empty (for example, if the order isn't specified for one of the inputs), then the output order is undefined.

#### Example

In "by name" mode, which is the default, the result of executing this query will be a selection with three columns: `x`, `y`, and `z`:
```yql
SELECT 1 AS x
UNION ALL
SELECT 2 AS y
UNION ALL
SELECT 3 AS z;
```
```
x	y	z
1		
	2	
		3
```

### By column positions

In the "by position" mode, the output of the resulting data schema uses the following rules:

* All inputs must have equal number of columns.
* The order of columns must be defined for all inputs.
* The names of the resulting columns must match the names of columns in the first table.
* The type of the resulting columns is output as a common (widest) type of input column types having the same positions.

The order of the output columns in this mode is the same as the order of columns in the first input.

#### Example

When `PRAGMA PositionalUnionAll;` is enabled, the following query returns only the `x` column:
```yql
PRAGMA PositionalUnionAll;

SELECT 1 AS x
UNION ALL
SELECT 2 AS y
UNION ALL
SELECT 3 AS z;
```
```
x
1
2
3
```

If the column order is undefined (for example, when using `AS_TABLE`), the query fails:
```yql
PRAGMA PositionalUnionAll;

SELECT 1 AS x, 2 as y
UNION ALL
SELECT * FROM AS_TABLE([<|x:3, y:4|>]);
```
```
Input #1 does not have ordered columns...
```
