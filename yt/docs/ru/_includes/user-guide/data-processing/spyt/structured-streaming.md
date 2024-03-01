# Structured Streaming

В версии SPYT 1.77.0 появилась поддержка [стриминговых процессов](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) поверх {{product-name}}.

## Хранилище чекпоинтов { #checkpoint-location }

{{product-name}} может выступать в роли надежного хранилища оффсетов и других метаданных. Для этого необходимо указать опцию `checkpointLocation` со значением `yt:///...`. После чего вся метаинформация об этой задаче будет сохраняться по указанному пути.

<small>Листинг 1 — Пример использования хранилища чекпоинтов</small>

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

## Стриминг очередей {{product-name}} { #queues }

{{product-name}} имеет собственную реализацию [очередей](../../../../user-guide/dynamic-tables/queues.md), основанных на упорядоченных динамических таблицх.

На текущий момент SPYT кластер способен оперировать только с данными, расположенными на том же кластере {{product-name}}. Это ограничение накладывается и на консьюмеры/очереди.

Перед запуском стриминговой задачи необходимо создать и настроить очереди в соответствии с [документацией](../../../../user-guide/dynamic-tables/queues.md#api). В случае чтения — создать таблицы очередей и консьюмеров, произвести регистрацию. Запись результатов стриминга производится в упорядоченную динамическую таблицу, созданную и примонтированную заранее.

После обработки очередной порции данных совершается коммит нового смещения, что позволяет уведомлять входную таблицу о возможности удалить неактуальные строки.

При использовании очередей на чтение и на запись действуют гарантии at-least-once.

<small>Листинг 2 — Пример использования очередей на языке Scala</small>

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

<small>Листинг 3 — Пример использования очередей на языке Python</small>

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
