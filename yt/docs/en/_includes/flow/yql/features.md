# Supported YQL constructs in {{product-name}} Flow

{% note info %}

This page describes YQL constructs specific to working with Flow. The general YQL syntax is described in the [YQL documentation](../../../yql/index.md).

{% endnote %}

## Row-wise stream transformation (map) {#map}

The main construct is `INSERT INTO ... SELECT ... FROM`. It reads a [stream](../../../flow/concepts/glossary.md#stream-and-computation) from an ordered dynamic table (a {{product-name}} queue), applies a transformation, and writes the result to another queue.

Example of a query part:

```yql
INSERT INTO
    {{flow-data-cluster}}.`//home/my-project/output_queues/sink_queue`
SELECT
    string_field || "_processed" AS processed_field,
    int64_field * 2 AS doubled,
    EndsWith(string_field, "bar") AS predicate
FROM
    {{flow-data-cluster}}.`//home/my-project/input_queues/source_queue`
WHERE int64_field > 0;
```

You can work with files and UDFs (built-in and user-defined), lambda expressions, and code generation.

You can combine multiple `INSERT INTO ... SELECT` operations in a single query.

{% if audience == "internal" %}

## Reading from and writing to Logbroker {#logbroker}

Logbroker topics can also serve as data sources and sinks. To read from Logbroker, you need to additionally set the `Ytflow.LogbrokerConsumerPath` pragma.

```yql
PRAGMA Ytflow.LogbrokerConsumerPath = "yt/my-project/lb_consumer";

INSERT INTO logbroker.`yt/my-project/lb_sink_topic`
SELECT * FROM logbroker.`yt/my-project/lb_source_topic`;
```

To split a stream into multiple outputs, you can use the `PROCESS ... USING` construct:

```yql
PRAGMA Ytflow.LogbrokerConsumerPath = "yt/my-project/lb_consumer";

-- the lambda splits incoming rows into two streams
$lambda = ($row) -> {
    $lb_type = Struct<Data: String>;
    $yt_type = Struct<StringField: String?>;
    $variant_type = Variant<$lb_type, $yt_type>;
    return If(
        StartsWith($row.Data, "foo"),
        Variant($row, "0", $variant_type),
        Variant(AsStruct($row.Data as StringField), "1", $variant_type)
    );
};

$lb_data, $yt_data = process
    logbroker.`yt/my-project/lb_source_topic`
    using $lambda(TableRow());

INSERT INTO logbroker.`yt/my-project/lb_sink_topic`
SELECT * FROM $lb_data;

INSERT INTO {{flow-data-cluster}}.`//home/my-project/output_queues/yt_sink_queue`
SELECT * FROM $yt_data;
```

You can freely combine reads and writes to/from Logbroker and ordered dynamic tables of {{product-name}} in a single query.

{% note warning %}

You must create the Logbroker output topics and the consumer for the input topic in advance.

{% endnote %}

{% endif %}

## Stream join with a dynamic table (lookup join) {#lookup-join}

You can join a stream with a sorted dynamic table (a key-value table). Supported join types for the “stream + KV table” pair are `LEFT`, `LEFT ONLY`, `LEFT SEMI`, and `INNER`. For the “KV table + stream” pair, the types are symmetric.

```yql
$input_stream =
    SELECT key, value, key || "_before" AS key_before
    FROM {{flow-data-cluster}}.`//home/my-project/input_queues/source_queue`
    WHERE value > 2;

$joined_stream =
    SELECT
        left_arg.key AS key,
        left_arg.value AS value,
        left_arg.key_before,
        right_arg.kv_value
    FROM $input_stream AS left_arg
    INNER JOIN
        {{flow-data-cluster}}.`//home/my-project/states/kv_table` AS right_arg
    USING (key);

INSERT INTO {{flow-data-cluster}}.`//home/my-project/output_queues/sink_queue`
SELECT * from $joined_stream
WHERE value * 2 <= kv_value;
```

## Roadmap {#roadmap}

Under development:
- Aggregations over fixed-size windows (hopping windows)
- Join with static tables
- Join by prefix of key columns
- Join of multiple streams with each other

## See also

- [Computation (concept)](../../../flow/concepts/computation.md)
- [Quick start (YQL)](../../../flow/yql/getting-started.md)