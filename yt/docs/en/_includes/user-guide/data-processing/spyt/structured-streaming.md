# Structured Streaming

SPYT 1.77.0 introduced support for [streaming processes](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) over {{product-name}}.

## Checkpoint storage { #checkpoint-location }

{{product-name}} can serve as a reliable storage for offsets and other metadata. For this, specify the `checkpointLocation` option with the `yt:///...` value. This saves all subsequent metainformation about the task at the specified path.

<small>Listing 1 — Example of using checkpoint storage</small>

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
  .option("checkpointLocation", "yt:///home/spark-streaming/sample01/checkpoints")
  .trigger(ProcessingTime(1000))
  .foreachBatch { (frame: DataFrame, batchNum: Long) =>
    frame.write.mode(SaveMode.Append).yt("//home/spark-streaming/sample01/result")
  }

val query = job.start()
query.awaitTermination()
```

## Streaming {{product-name}} queues { #queues }

{{product-name}} has its own implementation of [queues](../../../../user-guide/dynamic-tables/queues.md) based on ordered dynamic tables.

Currently, the SPYT cluster can operate only on data located on the same {{product-name}} cluster. This limitation also applies to queues and consumers.

Before starting a streaming task, create and configure queues as described in the [documentation](../../../../user-guide/dynamic-tables/queues.md#api). For read tasks, create queue and consumer tables, and then register the consumers. Streaming results are written to an ordered dynamic table that is created and mounted in advance.

After processing a chunk of data, a new offset is committed to notify the input table to delete unnecessary rows.

When using queues for reading and writing, at-least-once guarantees are applicable.

<small>Listing 2 — Example of using queues in Scala</small>

```scala
val numbers = spark
  .readStream
  .format("yt")
  .option("consumer_path", "//home/spark-streaming/sample02/consumer")
  .load("//home/spark-streaming/sample02/queue")

val job = numbers
  .writeStream
  .option("checkpointLocation", "yt:///home/spark-streaming/sample02/checkpoints")
  .trigger(ProcessingTime(2000))
  .format("yt")
  .option("path", "//home/spark-streaming/sample02/result")

val query = job.start()
query.awaitTermination()
```

<small>Listing 3 — Example of using queues in Python</small>

```python
from pyspark.sql.functions import length
from spyt import spark_session

with spark_session() as spark:
    df = spark \
        .readStream \
        .format("yt") \
        .option("consumer_path", "//home/spark-streaming/sample02/consumer") \
        .load("//home/spark-streaming/sample02/queue")
    df\
        .select("data") \
        .withColumn('data_length', length("data")) \
        .writeStream \
        .format("yt") \
        .option("checkpointLocation", "yt:///home/spark-streaming/sample02/checkpoints") \
        .option("path", "//home/spark-streaming/sample02/result") \
        .start().awaitTermination()
```
