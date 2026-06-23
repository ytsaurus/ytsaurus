<!-- TODO: add redirect -->
# Structured Streaming

SPYT version 1.77.0 introduces support for [streaming processes](https://spark.apache.org/docs/latest/streaming/index.html) on top of {{product-name}}.

## General scheme of Spark Structured Streaming in SPYT

![](../../../../../../images/spyt-streaming-general-scheme.png){ .center }

### Key concepts

`Queue` — any ordered dynamic table.

`Consumer` — a sorted table with a fixed schema. A consumer has a many‑to‑many relationship with queues and acts as a consumer of one or more queues. The consumer’s task is to store offsets for partitions of the queues being read.

`Source` — a data source. In streaming over the Queue API, this is a queue (an ordered dynamic table).

`Sink` — a data sink. For example, an output dynamic table.

`Microbatch` — a batch of data processed in a single streaming iteration.


`Streaming Query` — a continuously running process that processes a data stream in microbatches. It reads data from the `Source`, applies specified transformations, and writes the results to the `Sink`. Created by calling the `start()` method of the `DataStreamWriter` class.


## Checkpoint storage { #checkpoint-location }

{{product-name}} can serve as a reliable storage for offsets and other metadata. To do so, specify the `checkpointLocation` option with the value `yt:///...`. After that, all metadata for this task will be saved at the specified path.

<small>Listing 1 — Example of using checkpoint storage</small>

```scala
val numbers = spark
  .readStream
  .format("rate")
  .option("rowsPerSecond", 1)
  .load()
  .select($"timestamp", floor(rand() * 10).as("num"))

val groupedNumbers = numbers
  .withWatermark("timestamp", "5 seconds")
  .groupBy(window($"timestamp", "5 seconds", "3 seconds"), $"num")
  .count()

val job = groupedNumbers
  .writeStream
  .option("checkpointLocation", "yt:///tmp/spark-streaming/sample01/checkpoints")
  .trigger(ProcessingTime(1000))
  .foreachBatch { (frame: DataFrame, batchNum: Long) =>
    frame.write.mode(SaveMode.Append).yt("//tmp/spark-streaming/sample01/result")
  }

val query = job.start()
query.awaitTermination()
```

## Streaming queues in {{product-name}} { #queues }

{{product-name}} provides its own implementation of [queues](../../../../../user-guide/dynamic-tables/queues.md) based on ordered dynamic tables.


{% note info %}

Currently, Spark Streaming running on a {{product-name}} cluster can only operate on data located in the same {{product-name}} cluster.

{% endnote %}

Before starting a streaming task, create and configure queues according to the [documentation](../../../../../user-guide/dynamic-tables/queues.md#api). For reading, create queue and consumer tables and register them. Streaming results are written to a pre‑created and mounted ordered dynamic table.

After processing the latest data batch, a new offset is committed, notifying the input table that it can delete obsolete rows.

When using queues for reading and writing, at‑least‑once guarantees apply.


<small>Listing 2 — Example of using queues in Scala</small>

```scala
val numbers = spark
  .readStream
  .format("yt")
  .option("consumer_path", "//tmp/spark-streaming/sample02/consumer")
  .load("//tmp/spark-streaming/sample02/queue")

val job = numbers
  .writeStream
  .option("checkpointLocation", "yt:///tmp/spark-streaming/sample02/checkpoints")
  .trigger(ProcessingTime(2000))
  .format("yt")
  .option("path", "//tmp/spark-streaming/sample02/result")

val query = job.start()
query.awaitTermination()
```

<small>Listing 3 — Example of using queues in Python</small>

```python
from pyspark.sql.functions import length
from spyt import spark_session

with spark_session() as spark:
    df = spark \
        .readStream \
        .format("yt") \
        .option("consumer_path", "//tmp/spark-streaming/sample02/consumer") \
        .load("//tmp/spark-streaming/sample02/queue")
    df\
        .select("data") \
        .withColumn('data_length', length("data")) \
        .writeStream \
        .format("yt") \
        .option("checkpointLocation", "yt:///tmp/spark-streaming/sample02/checkpoints") \
        .option("path", "//tmp/spark-streaming/sample02/result") \
        .start().awaitTermination()
```

## Microbatch processing mechanism in Streaming Query { #streaming-query-processing-mechanism }

1. **Batch initialization**

   The driver starts processing a new microbatch **#N**:
   * Retrieves the current offsets (lowerIndex) for all queue partitions.
   * Calculates the upperIndex for each partition using the formula:
     ```
     upperIndex = min(
         lowerIndex + max_rows_per_partition,
         current_end_of_queue
     )
     ```

2. **Comparing lowerIndex and upperIndex**

   For each partition:
   * If lowerIndex < upperIndex:
     * Calls the [advanceConsumer](../../../../../user-guide/dynamic-tables/queues.md#rabota-s-konsyumerom) method to commit offsets for batch `#N‑1` in the consumer.
     * Generates tasks for executors.
   * Otherwise: skips processing (empty batch).

3. **Execution on executors**

   Each executor for its partition:
   * Reads data using the [pullConsumer](../../../../../user-guide/dynamic-tables/queues.md#chtenie-dannyh) method.
   * Applies transformations (if any).
   * Writes data to the output table.

4. **Creating checkpoint files on Cypress for batch #N**
   * A file in the `offsets` directory.
   * A file in the `commits` directory.

5. **New iteration**
   * The system moves to batch `#N+1`.
   * Repeats steps 1–3 with new offsets.
   * Only in step 2 of iteration `#N+1` is batch **#N** committed if there are available rows.

## Working with offsets { #streaming-offsets }

Currently, offsets are stored in two locations:

1. In [checkpoint files](../../../../../user-guide/data-processing/spyt/structured-streaming#checkpoint-location) automatically created by Spark in a directory.
2. In the consumer table.

### Determining lowerIndex and upperIndex for each partition

1. Spark tries to find the latest checkpoint file on Cypress:
   * If the file exists, it retrieves an offset structure containing `lowerIndex` for each partition.
   * If not, it uses offsets from the consumer.
2. For each input queue partition, it retrieves the maximum $row_index — `upper_row_index`.
3. It analyzes the `max_rows_per_partition` option:
   * If set, it calculates `upperIndex` for each partition using the formula:
     ```
     upperIndex = min(
         lowerIndex + max_rows_per_partition,
         upper_row_index
     )
     ```

### Possible offset desynchronization

Due to the late call of the `commit` method implemented in Spark itself, offsets in the consumer may lag behind offsets in checkpoint files by one batch if:
* Batch **#N** is fully processed, but batch `#N+1` has not yet been initialized.
* Batch `#N+1` is empty and will not be processed because there are no (or no more) unread rows in the input queue.

In this case, offsets in the latest checkpoint file will correspond to the `upperIndex` of batch **#N**, and the `offset` field in the consumer will correspond to `upperIndex`.


## Write guarantees { #exactly-once }


SPYT Structured Streaming supports several levels of write guarantees. By default, `at‑least‑once` applies: on restarts, a microbatch may be processed again, leading to duplicates. If duplicates are not acceptable, tools are available to achieve `exactly‑once`.

| **Mode** | **Guarantee** | **When to use** |
|----------|---------------|-----------------|
| Non‑transactional (default) | `at‑least‑once` | When duplicates are acceptable: logs, metrics, cache warming. No additional configuration required |
| [Transactional mode](../../../../../user-guide/data-processing/spyt/structured-streaming/exactly-once/transactional-mode.md) (SPYT 2.10+) | `exactly‑once` for any transformations | When data accuracy is critical: financial analytics, ML features, incremental data mart construction |
| [Idempotent receiver](../../../../../user-guide/data-processing/spyt/structured-streaming/exactly-once/idempotent-receiver.md) | `exactly‑once` for stateless 1:1 transformations | When you want to avoid the additional load on the RPC proxy from transactional mode, or when maintaining legacy code |


For a comparison of modes, operation schemes, and instructions on enabling them, see the article [Exactly‑once guarantee](../../../../../user-guide/data-processing/spyt/structured-streaming/exactly-once/index.md).

## Configuring the number of rows per batch { #rows-limit }


By default, Spark attempts to read all available rows in a queue in one streaming microbatch. For queues with many unread rows, this will lead to an `OutOfMemoryError` on executors. There are two options for setting a row limit per microbatch:

1. The `max_rows_per_partition` option sets the maximum number of rows that can be read from a single queue partition within one batch. For example:
   * If a queue consists of 3 partitions and `max_rows_per_partition` is 1 000, no more than 1 000 rows per partition will be read — i.e., no more than 3 000 rows per batch. Partitions are distributed evenly among executors. If there are at least as many executors as partitions, each will process no more than one partition.
   * In the simplest case, when a queue consists of one partition, `max_rows_per_partition` effectively sets the row limit per batch.

2. The configuration parameter `spark.yt.write.dynBatchSize` sets the maximum number of rows that can be written to a dynamic table in one call of the `modifyRows` command. For example:
   * If an executor reads 1 000 rows and `spark.yt.write.dynBatchSize` is 100, 10 `ModifyRowsRequest` instances will be sequentially generated, each containing 100 rows.

<small>Listing 5 — Using the max_rows_per_partition option</small>


```python
spark = SparkSession.builder.appName('streaming example') \
   .config("spark.yt.write.dynBatchSize", 100) \
   .getOrCreate()

df = spark \
  .readStream \
  .format("yt") \
  .option("consumer_path", consumer_path) \
  .option("max_rows_per_partition", 1000) \
  .load(queue_path)

query = df\
  .writeStream \
  .outputMode("append") \
  .format("yt") \
  .option("checkpointLocation", checkpoints_path) \
  .option("path", result_table_path) \
  .start()
```

## Composite types { #type-v3 }

To process [composite data types](../../../../../user-guide/storage/data-types.md) with streaming, you must enable the `parsing_type_v3` and `write_type_v3` options, just as for batch jobs.


<small>Listing 6 — Processing composite types in Structured Streaming</small>

```python
df = spark \
  .readStream \
  .format("yt") \
  .option("consumer_path", consumer_path) \
  .option("parsing_type_v3", "true") \
  .load(queue_path)

query = df\
  .writeStream \
  .outputMode("append") \
  .format("yt") \
  .option("write_type_v3", True) \
  .option("checkpointLocation", checkpoints_path) \
  .option("path", result_table_path) \
  .start()
```

## Spark Structured Streaming parameters set via Spark methods { #streaming-required-params }

| Parameter | Description | Required | Default value |
|---------|-------------|----------|---------------|
| format | Format. Specified separately for readStream and writeStream | Yes (for Structured Streaming over dynamic tables, you must specify `yt`) | — |
| load | Input queue | Yes | — |
| outputMode | Write mode | No | "append" |

## Options and parameters { #options }


For the complete reference of streaming options (DataFrameReader/Writer options, service columns, version compatibility matrix), see the [Streaming options](../../../../../user-guide/data-processing/spyt/thesaurus/streaming-options.md) page. For Spark session parameters (including `spark.yt.streaming.transactional` and `spark.yt.write.dynBatchSize`), see the [Configuration parameters](../../../../../user-guide/data-processing/spyt/thesaurus/configuration.md) page.


## Monitoring in Spark webUI { #monitoring }


The Jobs, Stages, and SQL/DataFrames pages, as in a regular Spark application, are used to monitor jobs, execution stages, and query plans.


The Environment page is used to view all Spark session configuration parameters and some metrics.

On the Executors page, you can see the number of active, failed, and completed tasks and assess how optimally the executor cores are loaded. You can also view memory usage statistics. In case of a memory leak, it is sometimes useful to go to Thread Dump or Heap Histogram to find the cause.


![](../../../../../../images/spyt-streaming-monitoring-executors.png){ .center }


For streaming processes, the webUI displays a Structured Streaming page by default. Here you can view statistics for active and completed streams. In particular, it is useful to check what error caused a Streaming Query to fail.


![](../../../../../../images/spyt-streaming-monitoring-streaming.png){ .center }


By clicking Run ID, you can view more detailed statistics for a specific Streaming Query.


![](../../../../../../images/spyt-streaming-monitoring-streaming-query-stats.png){ .center }


## Best practices { #best-practices }


* Properly configure the `max_rows_per_partition` option and the `spark.yt.write.dynBatchSize` config to limit the number of rows processed per batch. Set them not too high, otherwise you risk an `OutOfMemoryError`. But also not too low, otherwise batches will be created too frequently, significantly increasing the load on master servers and proxy servers in {{product-name}}.
* Set a low value for the `spark.sql.streaming.minBatchesToRetain` config. This parameter sets the minimum number of recent batches whose metadata must be stored. These are files in the checkpoint directory on Cypress and objects in the driver’s internal structures. By default, this config’s value is 100. For streaming without stateful transformations, it is sufficient to set `--conf spark.sql.streaming.minBatchesToRetain=2`. This saves chunks on Cypress and driver memory.
* When creating a Spark session, set the following configuration parameters:
  * `.config("spark.streaming.stopGracefullyOnShutdown", True)` — Gracefully complete processing of all created batches and clean up resources before stopping the stream.
  * `.config("spark.streaming.stopGracefullyOnShutdown.timeout", ...)` — Time for graceful shutdown in milliseconds.
  * `.config("spark.sql.adaptive.enabled", False)` — Regular adaptive execution does not work for streaming. Disable it.
  * `.config("spark.sql.streaming.adaptiveExecution.enabled", True)` — Enable special adaptive execution for streaming jobs.
* Configure memory allocation using the `spark.memory.fraction` and `spark.memory.storageFraction` parameters, for example:
  * `.config("spark.memory.fraction", 0.5)` — 50 % of memory for execution, 50 % for storage.
  * `.config("spark.memory.storageFraction", 0.2)` — 20 % of memory allocated to cache and 80 % to data processing (suitable if there is no caching).
  