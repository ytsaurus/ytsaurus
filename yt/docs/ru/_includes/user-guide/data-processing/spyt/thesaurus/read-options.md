
# Опции чтения

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

Чтение под [транзакцией](../../../../../user-guide/storage/transactions.md). Подробнее можно узнать в разделе [Чтение и запись под транзакцией](../read-transaction.md).

Пример на Scala:

```scala
val transaction = YtWrapper.createTransaction(None, 10 minute)
df.write.transaction(transaction.getId.toString).yt(tmpPath)
transaction.commit().get(10, TimeUnit.SECONDS)
```


