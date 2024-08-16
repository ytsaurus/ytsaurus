
# Опции чтения

## Мультикластерное чтение

По умолчанию, расчет обращается к данным на кластере, который предоставляет вычислительные мощности (при запуске напрямую или при использовании Standalone кластера). При этом SPYT поднимает собственные RPC прокси для разгрузки общих кластерных прокси.

В версии 2.2.0 появилась возможность чтения данных с разных кластеров {{product-name}}. Для этого необходимо явно указать адрес кластера в пути к таблице.

```python
spark.read.yt('<cluster="localhost:8000">//home/table').show() # Таблица на кластере localhost:8000

spark.read.yt('//home/table').show() # Таблица на домашнем кластере
```

{% note info "Примечание" %}

Чтение с других кластеров {{product-name}} происходит через общие прокси, что может вызывать большую нагрузку на них при высоких объемах данных.

{% endnote %}

## schema_hint

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

## transaction

Чтение под [транзакцией](../../../../../user-guide/storage/transactions.md). Подробнее можно узнать в разделе [Чтение и запись под транзакцией](../../../../../user-guide/data-processing/spyt/read-transaction.md).

Пример на Scala:

```scala
val transaction = YtWrapper.createTransaction(None, 10 minute)
df.write.transaction(transaction.getId.toString).yt(tmpPath)
transaction.commit().get(10, TimeUnit.SECONDS)
```


