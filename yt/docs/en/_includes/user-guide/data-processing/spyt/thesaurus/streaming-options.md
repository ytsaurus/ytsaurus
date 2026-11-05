# Streaming options

This section lists SPYT options for Structured Streaming, service columns, and a version compatibility matrix. Spark session parameters (including `spark.yt.streaming.transactional`) are available on the [Configuration parameters](../../../../../user-guide/data-processing/spyt/thesaurus/configuration.md) page.

## `yt` source and sink options { #options }

Passed via `.option(...)` when reading or writing a streaming DataFrame. For an input queue, the `path` parameter is usually passed via `.load(...)`.

| **Option** | **Description** | **Required** | **Default value** | **Since version** |
| --------- | ------------ | ---------------- | ------------------------- | ------------------ |
| `consumer_path` | Path to the consumer table for reading from the queue | Yes, when reading | — | 1.77.0 |
| `path` | Path to the input queue when reading or to the output table when writing | Yes | — | 1.77.0 |
| `include_service_columns` | Add service columns `__spyt_streaming_src_tablet_index` and `__spyt_streaming_src_row_index` to the DataFrame (correspond to `$tablet_index` and `$row_index` of a row in the original queue) | No | `false` | 2.6.0 |
| `max_rows_per_partition` | Maximum number of rows read from a single queue partition within one batch | No | `∞` | 2.6.0 |
| `parsing_type_v3` | Read composite types while preserving the type. If the option is not specified, `spark.yt.read.typeV3.enabled` is used | No | `spark.yt.read.typeV3.enabled` | 2.6.0 |
| `write_type_v3` | Write composite types while preserving the type. If the option is not specified, `spark.yt.write.typeV3.enabled` is used | No | `spark.yt.write.typeV3.enabled` | 2.6.0 |

## Spark Structured Streaming parameters { #spark-streaming-options }

`checkpointLocation` is a Spark Structured Streaming parameter, not a specific `yt` source or sink option.

| **Parameter** | **Description** | **When to specify** | **Since version** |
| ------------ | ------------ | ------------------- | ------------------ |
| `checkpointLocation` | Path to the directory with checkpoint files for a streaming query. Spark stores the intermediate state of stateful operations here: for example, groupBy, windowing, or join.&#10;&#10;To store in {{product-name}}, specify a path like `yt:///...` | Always — this is a mandatory requirement of Spark Structured Streaming | 1.77.0 |

## Service columns { #service-columns }

When `include_service_columns = true`, the following columns are added to the streaming DataFrame:

| **Column** | **Description** |
| ----------- | ------------ |
| `__spyt_streaming_src_tablet_index` | Value of `$tablet_index` for a row in the original queue |
| `__spyt_streaming_src_row_index` | Value of `$row_index` for a row in the original queue |

## Compatibility matrix { #compatibility-matrix }

| **Functionality** | **Minimum SPYT version** |
| -------------------- | --------------------------- |
| Checkpoint storage on {{product-name}} | 1.77.0 |
| Structured Streaming over {{product-name}} Queue API | 1.77.0 |
| Support for composite data types | 2.6.0 |
| `max_rows_per_partition` option | 2.6.0 |
| `include_service_columns` option | 2.6.0 |
| `spark.yt.write.dynBatchSize` parameter | Configurable for streaming since 2.6.5 (previously hard‑coded to 50 000) |
| Transactional mode (exactly‑once) | 2.10 |

## See also

- [Structured Streaming](../../../../../user-guide/data-processing/spyt/structured-streaming/index.md) — overview and main scenarios
- [Exactly‑once guarantee](../../../../../user-guide/data-processing/spyt/structured-streaming/exactly-once/index.md) — choosing an approach to guarantees
- [Transactional mode](../../../../../user-guide/data-processing/spyt/structured-streaming/exactly-once/transactional-mode.md) — instructions for enabling exactly‑once
- [Configuration parameters](../../../../../user-guide/data-processing/spyt/thesaurus/configuration.md) — Spark session parameters, including `spark.yt.streaming.transactional` and `spark.yt.write.dynBatchSize`
