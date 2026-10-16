# MATERIALIZE

Materializes the specified source or expression on the current or specified cluster. When an expression is materialized, its type must be a list of structures. The cluster is taken from the `ON` clause or, if it is omitted, from the [USE](use.md) statement.
The materialized source preserves all columns and sort order. It also creates a barrier that prevents optimizers from merging computations across the materialization boundary.

## Syntax {#syntax}

```yql
MATERIALIZE
    <source>        -- a table name, named expression, or nested SELECT
INTO $<bind_name>   -- parameter used to reference the materialized result
ON <cluster>        -- cluster where the source is materialized (optional)
WITH <hints>        -- additional modifiers (optional)
```

## Availability {#availability}

`MATERIALIZE` is available starting with language version [2026.02](../changelog/2026.02.md).

In Query Tracker, select language version 2026.02 or later to use `MATERIALIZE`.

## Modifiers {#modifiers}

A modifier follows the `WITH` keyword. Its value is separated with `=`. Enclose multiple modifiers in parentheses: `WITH (SOME_HINT1=value, SOME_HINT2)`.

The `prune_unused_columns` modifier removes columns that are not used by consumers from the materialized source. Systems that perform materialization may support additional modifiers.

## Examples {#examples}

```yql
USE cluster;

MATERIALIZE (SELECT 1 AS a, 2 AS b) INTO $materialized;

SELECT * FROM $materialized;
```

```yql
USE cluster;

$input = SELECT key, value FROM my_table ORDER BY key;

MATERIALIZE $input INTO $materialized ON another_cluster;

SELECT * FROM another_table AS a
JOIN $materialized AS b USING key;
```

The materialized source in the second example preserves sorting by `key`, so the `JOIN` strategy may take this sorting into account.

```yql
USE cluster;

$input = SELECT a, b, c, d FROM my_table;

MATERIALIZE $input INTO $materialized WITH prune_unused_columns;
SELECT a, b FROM $materialized;
SELECT c FROM $materialized;
```

After optimization, the last example materializes only the `a`, `b`, and `c` columns.
