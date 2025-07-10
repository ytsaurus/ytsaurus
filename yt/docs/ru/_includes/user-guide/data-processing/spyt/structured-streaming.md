# Structured Streaming

В версии SPYT 1.77.0 появилась поддержка [стриминговых процессов](https://spark.apache.org/docs/latest/streaming/index.html) поверх {{product-name}}.

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
  .option("checkpointLocation", "yt:///tmp/spark-streaming/sample01/checkpoints")
  .trigger(ProcessingTime(1000))
  .foreachBatch { (frame: DataFrame, batchNum: Long) =>
    frame.write.mode(SaveMode.Append).yt("//tmp/spark-streaming/sample01/result")
  }

val query = job.start()
query.awaitTermination()
```

## Стриминг очередей {{product-name}} { #queues }

{{product-name}} имеет собственную реализацию [очередей](../../../../user-guide/dynamic-tables/queues.md), основанных на упорядоченных динамических таблицах.

На текущий момент SPYT кластер способен оперировать только с данными, расположенными на том же кластере {{product-name}}. Это ограничение накладывается и на консьюмеры/очереди.

Перед запуском стриминговой задачи необходимо создать и настроить очереди в соответствии с [документацией](../../../../user-guide/dynamic-tables/queues.md#api). В случае чтения — создать таблицы очередей и консьюмеров, произвести регистрацию. Запись результатов стриминга производится в упорядоченную динамическую таблицу, созданную и примонтированную заранее.

После обработки очередной порции данных совершается коммит нового смещения, что позволяет уведомлять входную таблицу о возможности удалить неактуальные строки.

При использовании очередей на чтение и на запись действуют гарантии at-least-once.

<small>Листинг 2 — Пример использования очередей на языке Scala</small>

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

<small>Листинг 3 — Пример использования очередей на языке Python</small>

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

## Достижение семантики exactly-once { #exactly-once }

Spark Structured Streaming, работающий поверх {{product-name}} очередей, предоставляет гарантию at-least-once.

В некоторых сценариях возможно достичь гарантии exactly-once при одновременном выполнении двух условий:

1. Преобразования без сохранения состояния (stateless processing)
   
   К таким операциям относятся:
   - Простые трансформации (`select`, `filter`, `withColumn`)
   - Проекции и переименование колонок
   - Любые операции, где каждая выходная строка зависит **строго от одной входной строки**

2. Инъективное отображение данных

   Должны соблюдаться требования:
   - Каждая входная строка преобразуется **не более чем в одну выходную**
   - Отсутствуют операции, порождающие дубликаты (например, `join`, `groupBy`, `union` без дедупликации).


Необходимые действия:

1. Установить опцию `include_service_columns` в значение `true`. Тогда стриминговый датафрейм будет содержать столбцы
   `__spyt_streaming_src_tablet_index` и `__spyt_streaming_src_row_index`, соответствующие столбцам `$tablet_index` и 
`$row_index` читаемой очереди.
2. Создать выходную сортированную динамическую таблицу с ключевыми колонками `__spyt_streaming_src_tablet_index` и
  `__spyt_streaming_src_row_index`. Можно назвать эти колонки в выходной таблице по-другому, но тогда и в датафрейме 
  нужно переименовать их (как в примере ниже)
3. Если чтение происходит из более чем одной очереди, можно добавить в датафрейм (с помощью `withColumn()`) и в выходную 
таблицу ключевую колонку, содержащую уникальный идентификатор исходной очереди. Например, id или путь к очереди 
(как в примере ниже)
4. Достигаем at-most-once благодаря тому, что 
   - в любой очереди комбинация значений столбцов `$tablet_index` и `$row_index` уникальна
   - в выходной сортированной динамической таблице колонки `__spyt_streaming_src_tablet_index` и
      `__spyt_streaming_src_row_index` ключевые
5. at-least-once + at-most-once = exactly-once

Важно помнить, что использование сортированной динамической таблицы вместо упорядоченной добавляет накладные
расходы на сортировку. Поэтому при отсутствии необходимости в семантике at-most-once лучше писать в упорядоченные 
динамические таблицы.

<small>Листинг 4 — Использование опции include_service_columns</small>

```python
import spyt
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from yt.wrapper import YtClient
import os


yt = YtClient(proxy="hume.yt.yandex.net", token=os.environ['YT_SECURE_VAULT_YT_TOKEN'])
spark = SparkSession.builder.appName('streaming example').getOrCreate()

schema = [
    {"name": "src_queue_path", "type": "string"},
    {"name": "tablet_idx", "type": "int64"},
    {"name": "row_idx", "type": "int64"},
    {"name": "some_data", "type": "string"},
]
yt.create("table", result_table_path, recursive=True, attributes={"dynamic": "true", "schema": schema})
yt.mount_table(result_table_path, sync=True)

df = spark \
  .readStream \
  .format("yt") \
  .option("consumer_path", consumer_path) \
  .option("include_service_columns", True) \
  .load(queue_path)
  .withColumnRenamed("__spyt_streaming_src_tablet_index", "tablet_idx")
  .withColumnRenamed("__spyt_streaming_src_row_index", "row_idx")
  .withColumn("src_queue_path", lit(queue_path))

query = df\
  .writeStream \
  .outputMode("append") \
  .format("yt") \
  .option("checkpointLocation", checkpoints_path) \
  .option("path", result_table_path) \
  .start()
```

## Конфигурация количества строк на 1 батч { #rows-limit }

По умолчанию spark пытается прочитать все доступные в очереди строки за 1 стриминговый микробатч. Для очередей с большим
количеством непрочитанных строк это приведет к OutOfMemoryError на экзекьюторах. Есть 2 опции для установки лимита строк на 1 микробатч:

1. Опция `max_rows_per_partition` задает максимальное количество строк, которые могут быть прочитаны из одной партиции 
очереди в рамках одного батча. Например:
   - Если очередь состоит из 3 партиций и `max_rows_per_partition` равно 1000, то будет прочитано не более 1000 
   строк каждой партиции, то есть не более 3000 строк за 1 батч. При этом партиции распределяются между экзекьюторами равномерно. 
   Если экзекьюторов не меньше, чем партиций, то каждый будет обрабатывать не более 1 партиции
   - В простейшем случае, когда очередь состоит из 1 партиции, `max_rows_per_partition` фактически устанавливает лимит строк 
   на 1 батч.
2. Конфигурационный параметр `spark.yt.write.dynBatchSize` устанавливает максимально количество строк, которые могут быть 
записаны в динамическую таблицу за 1 вызов команды insertRows. Например:
   - Если экзекьютор прочитал 1000 строк, а `spark.yt.write.dynBatchSize` равно 100, то будет 10 раз последовательно 
   будет сформирован `ModifyRowsRequest`, содержащий по 100 строк.

<small>Листинг 5 — Использование опции max_rows_per_partition</small>

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

## Композитные типы { #type-v3 }
Для того, чтобы обрабатывать стримингом [композитные типы данных](../../../../user-guide/storage/data-types.md), нужно, 
как и для батчовых джоб, включать опции parsing_type_v3 и write_type_v3.

<small>Листинг 6 — Обработка композитных типов в Structured Streaming</small>

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

## Обязательные параметры Spark Structured Streaming, задаваемые через spark методы

| Опция  | Описание                                                      | Обязательный                                                                | Значение по умолчанию |
|--------|---------------------------------------------------------------|-----------------------------------------------------------------------------|-----------------------|
| format | Формат. Указывается отдельно для readStream и для writeStream | да (для Structured Streaming поверх дин таблиц нужно указать значение `yt`) | -                     |
| load   | Входная очередь                                               | да                                                                          | -                     |


## Параметры spark сессии для конфигурации стриминга

| Опция                       | Описание                                                                     | Обязательный | Значение по умолчанию |
|-----------------------------|------------------------------------------------------------------------------|--------------|-----------------------|
| spark.yt.write.dynBatchSize | Максимальное количество строк в одной операции записи в динамическую таблицу | нет          | 50000                 |


## Опции

| Опция                   | Описание                                                                                | Обязательный | Значение по умолчанию |
|-------------------------|-----------------------------------------------------------------------------------------|--------------|-----------------------|
| consumer_path           | Путь к таблице-консьюмеру                                                               | да           | -                     |
| checkpointLocation      | Путь к директории с чекпоинт-файлами                                                    | да           | -                     |
| path                    | Путь к выходной таблице                                                                 | да           | -                     |
| include_service_columns | Добавить колонки `$tablet_index` и `$row_index` читаемой очереди в датафрейм            | нет          | false                 |
| max_rows_per_partition  | Максимальное количество строк, читаемых из одной партиции очереди в рамках одного батча | нет          | ∞                     |
| parsing_type_v3         | Читать композитные типы с сохранением типа                                              | нет          | false                 |
| write_type_v3           | Писать композитные типы с сохранением типа                                              | нет          | false                 |


## Матрица совместимости

| Функциональность                                       | Минимальная версия SPYT |
|--------------------------------------------------------|-------------------------|
| Хранение чекпоинтов на {{product-name}}                | 1.77.0                  |
| Structured Streaming поверх {{product-name}} Queue API | 1.77.0                  |
| Поддержка композитных типов данных                     | 2.6.0                   |
| Опция max_rows_per_partition                           | 2.6.0                   |
| Опция include_service_columns                          | 2.6.0                   |
