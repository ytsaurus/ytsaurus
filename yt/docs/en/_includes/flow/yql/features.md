# YQL features in {{product-name}} Flow

{% note info %}

This page describes YQL constructs specific to working with Flow.

The general YQL syntax is described in the [YQL documentation](../../../yql/index.md).

{% endnote %}

## Row-wise stream transformation (map) {#map}

The main construct is `INSERT INTO ... SELECT ... FROM`.<br>
Use it to read a [stream](../../../flow/concepts/glossary.md#stream-and-computation) from an ordered dynamic table (a {{product-name}} queue), transform it, and write the result to another queue.

Example query:

```yql
INSERT INTO
    {{flow-data-cluster}}.`//home/my-project/sink_queue`
SELECT
    string_field || "_processed" AS processed_field,
    int64_field * 2 AS doubled,
    EndsWith(string_field, "bar") AS predicate
FROM
    {{flow-data-cluster}}.`//home/my-project/source_queue`
WHERE int64_field > 0;
```

You can work with files and UDFs (built-in and user-defined), lambda expressions, and code generation.

You can combine multiple `INSERT INTO ... SELECT` operations in a single query.

## Splitting a stream {#multiple-outputs}

You can reuse named streams in multiple `SELECT` queries to split a stream into parts that can be processed independently.

In following example, input stream rows with a positive `value` go to `positive_queue` queue, and remaining rows go to `other_queue` queue:

```yql
$events = SELECT
    key,
    value
FROM {{flow-data-cluster}}.`//home/my-project/source_queue`;

INSERT INTO {{flow-data-cluster}}.`//home/my-project/positive_queue`
SELECT * FROM $events WHERE value > 0;

INSERT INTO {{flow-data-cluster}}.`//home/my-project/other_queue`
SELECT * FROM $events WHERE value <= 0;
```

## Combining streams (UNION ALL) {#union-all}

`UNION ALL` combines multiple streams for further processing as a single stream.

In this example, output queue receives rows from both input queues:

```yql
$events =
    SELECT
        key,
        value
    FROM {{flow-data-cluster}}.`//home/my-project/first_queue`
    UNION ALL
    SELECT
        key,
        value
    FROM {{flow-data-cluster}}.`//home/my-project/second_queue`;

INSERT INTO {{flow-data-cluster}}.`//home/my-project/sink_queue`
SELECT
    key,
    value + 1 AS value
FROM $events;
```

## Stream join with a dynamic table (lookup join) {#lookup-join}

You can join a stream with a sorted dynamic table (a key-value table).<br>
Supported join types for the “stream + KV table” pair are `LEFT`, `LEFT ONLY`, `LEFT SEMI`, and `INNER`.<br>
For the “KV table + stream” pair, the types are symmetric.

```yql
$input_stream =
    SELECT key, value, key || "_before" AS key_before
    FROM {{flow-data-cluster}}.`//home/my-project/source_queue`
    WHERE value > 2;

$joined_stream =
    SELECT
        left_arg.key AS key,
        left_arg.value AS value,
        left_arg.key_before,
        right_arg.kv_value
    FROM $input_stream AS left_arg
    INNER JOIN
        {{flow-data-cluster}}.`//home/my-project/kv_table` AS right_arg
    USING (key);

INSERT INTO {{flow-data-cluster}}.`//home/my-project/sink_queue`
SELECT * from $joined_stream
WHERE value * 2 <= kv_value;
```

You can chain joins with multiple key-value tables in one query, for example:

```yql
INSERT INTO {{flow-data-cluster}}.`//home/my-project/enriched_queue`
SELECT
    events.key AS key,
    events.value AS value,
    first_table.value AS first_value,
    second_table.value AS second_value
FROM {{flow-data-cluster}}.`//home/my-project/source_queue` AS events
LEFT JOIN {{flow-data-cluster}}.`//home/my-project/first_table` AS first_table
    ON events.key = first_table.key
LEFT JOIN {{flow-data-cluster}}.`//home/my-project/second_table` AS second_table
    ON events.key = second_table.key;
```

## Windowed aggregations (GROUP BY ... HOP) {#group-by-hop}

Use `GROUP BY ... HOP` to calculate aggregates over fixed-length time intervals (windows).

`HOP` arguments specify event time, grid of window start times, window duration, and window-closing delay for late events.
Besides `HOP`, you must specify at least one additional grouping key.

For general syntax, see [YQL reference](../../../yql/syntax/group_by.md#group-by-hop).

A window closes when event-time [watermark](../../../flow/concepts/watermarks.md#event-watermark) passes window end plus window-closing delay.<br>
Events whose timestamps fall within a window but arrive after it has closed (late events) are discarded for that window.

Aggregate functions without the `DISTINCT` modifier are supported.

Following example calculates the sum of `value` and the row count for each key and window.<br>
Windows start every 15 seconds, last 30 seconds, and close one minute after window end.

```yql
INSERT INTO {{flow-data-cluster}}.`//home/my-project/window_totals`
SELECT
    HOP_START() AS window_start,
    key,
    COUNT(*) AS record_count,
    SUM(value) AS value_total
FROM {{flow-data-cluster}}.`//home/my-project/source_queue`
GROUP BY key, HOP(ts, "PT15S", "PT30S", "PT1M");
```

## Writing to a sorted dynamic table {#replace-into}

Use `REPLACE INTO ... WITH PRIMARY_KEY = ...` to write to a sorted dynamic table.

When writing, a new row fully replaces previous row with same key.

`PRIMARY_KEY` hint must be a YSON list of one or more key columns.<br>
If output table already exists, its non-computed key columns and their order must match
key columns specified in `PRIMARY_KEY`.<br>
If output table does not exist, it is created before query starts.

Output tables with descending key sort order are not supported.

Example:

```yql
REPLACE INTO {{flow-data-cluster}}.`//home/my-project/window_totals_table`
WITH PRIMARY_KEY = '[key;window_start]'
SELECT
    key,
    window_start,
    record_count
FROM {{flow-data-cluster}}.`//home/my-project/window_totals`;
```

## Working with multiple clusters {#multiple-clusters}

Input and output tables can reside on different {{product-name}} clusters.<br>
`Ytflow.Cluster` specifies cluster for the pipeline's internal tables, not a restriction on location of all sources and sinks.

In this example, `source_cluster` and `destination_cluster` may be different from `Ytflow.Cluster` value:

```yql
INSERT INTO destination_cluster.`//home/my-project/sink_queue`
SELECT
    key,
    value
FROM source_cluster.`//home/my-project/source_queue`;
```
