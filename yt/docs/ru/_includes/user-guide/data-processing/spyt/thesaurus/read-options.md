
# Опции чтения

## Мультикластерное чтение { #multicluster_reading }

По умолчанию, расчет обращается к данным на кластере, который предоставляет вычислительные мощности (при запуске напрямую или при использовании Standalone кластера). При этом SPYT поднимает собственные RPC прокси для разгрузки общих кластерных прокси.

В версии 2.2.0 появилась возможность чтения данных с разных кластеров {{product-name}}. Для этого необходимо явно указать адрес кластера в пути к таблице.

```python
spark.read.yt('<cluster="localhost:8000">//home/table').show() # Таблица на кластере localhost:8000

spark.read.yt('//home/table').show() # Таблица на домашнем кластере
```

{% note info "Примечание" %}

Чтение с других кластеров {{product-name}} происходит через общие прокси, что может вызывать большую нагрузку на них при высоких объемах данных.

{% endnote %}

## schema_hint { #schema_hint }

Жесткое указание [типа колонки](../../../../../user-guide/storage/data-types.md). Полезно в случае, когда колонка имеет тип `any` (сериализованный в `yson` сложный тип).
Значение будет десериализовано в указанный тип.

Пример на Python:
```python
spark.read.schema_hint({"value": MapType(StringType(), LongType())}).yt("//sys/spark/examples/example_yson")
```

Пример на Scala:
```scala
df.write
    .schemaHint(Map("a" ->
        YtLogicalType.VariantOverTuple(Seq(
          (YtLogicalType.String, Metadata.empty), (YtLogicalType.Double, Metadata.empty)))))
    .yt(tmpPath)
```

## transaction { #transaction }

Чтение под [транзакцией](../../../../../user-guide/storage/transactions.md). Подробнее можно узнать в разделе [Чтение и запись под транзакцией](../../../../../user-guide/data-processing/spyt/read-transaction.md).

Пример на Scala:

```scala
val transaction = YtWrapper.createTransaction(None, 10 minute)
df.write.transaction(transaction.getId.toString).yt(tmpPath)
transaction.commit().get(10, TimeUnit.SECONDS)
```

## Schema v3 { #type_v3 }

Чтение таблиц со схемой в формате [type_v3](../../../../../user-guide/storage/data-types.md) вместо `type_v1`. Настраивается в [Spark конфигурации](../../../../../user-guide/data-processing/spyt/cluster/configuration.md) или опции записи.

Python example:
```python
spark.read.option("parsing_type_v3", "true").yt("//sys/spark/examples/example_yson")
```

## readParallelism { #readParallelism }

Настройка целевого числа партиций при чтении таблицы. Определяет максимальный размер партиции как `totalBytes / readParallelism`, тем самым устанавливая количество Spark‑тасков при чтении.

Python example:
```python
spark.read.option("readParallelism", "5").yt("//home/table")
```

## recursiveFileLookup { #recursiveFileLookup }

Опция управляет порядком обхода файловой структуры при чтении данных. Она определяет, будет ли система рекурсивно спускаться во вложенные директории.

Опция может принимать значения:

- `true` — система рекурсивно обходит все поддиректории от указанной точки и находит все файлы, которые лежат внутри дерева;
- `false` — система не спускается рекурсивно в поддиректории и читает только файлы, которые лежат в указанной директории.

В Apache Spark при чтении данных из HDFS и S3 опция `recursiveFileLookup` установлена по умолчанию в `false`. В SPYT значение по умолчанию — `true`. Это сделано для единообразия с поведением YQL и CHYT.

Установите опцию в `false`, если читаете данные с Hive‑совместимой схемой партиционирования по директориям в Кипарисе. В таком случае достаточно обработать только файлы верхнего уровня — рекурсивный обход не требуется.

Например, для структуры директорий:

```text
home
├── table
│   ├── dt=2026-01-01
│   ├── dt=2026-01-02
│   ├── dt=2026-01-03
│   └── dt=2026-01-04
│   ...
```

Пример кода Python будет таким:

```python
df = spark.read.option("recursiveFileLookup", "false").yt("//home/table")
df.printSchema()
...
root
 |-- id: long (nullable = false)
 |-- value: long (nullable = false)
 ...
 |-- dt: string (nullable = false)
```
