# Конфигурации

Список всех поддерживаемых опций Spark содержится в [документации](https://spark.apache.org/docs/latest/configuration.html).

## Основные опции { #main }

Большинство опций доступны начиная с версии 1.23.0, если не указано иное.

| **Имя** | **Значение по умолчанию** | **Описание** |
| ------------------- | --------------- | ------------------------------------------------------------ |
| `spark.yt.write.batchSize` | `500000` | Размер данных, отправляемых через одну операцию `WriteTable` |
| `spark.yt.write.miniBatchSize` | `1000` | Размер блока данных, отправляемого в `WriteTable` |
| `spark.yt.write.timeout` | `60 seconds` | Ограничение на ожидание записи одного блока данных |
| `spark.yt.write.typeV3.enabled` (`spark.yt.write.writingTypeV3.enabled` до 1.75.2) | `true` | Запись таблиц со схемой в формате [type_v3](../../../../../user-guide/storage/data-types.md) вместо `type_v1` |
| `spark.yt.read.vectorized.capacity` | `1000` | Максимальное количество строк в батче при чтении через `wire` протокол |
| `spark.yt.read.arrow.enabled` | `true` | Использовать `arrow` формат для чтения данных (если это возможно) |
| `spark.hadoop.yt.timeout` | `300 seconds` | Таймаут на чтение из {{product-name}} |
| `spark.yt.read.typeV3.enabled` (`spark.yt.read.parsingTypeV3.enabled` до 1.75.2) | `true` | Чтение таблиц со схемой в формате [type_v3](../../../../../user-guide/storage/data-types.md) вместо `type_v1` |
| `spark.yt.read.keyColumnsFilterPushdown.enabled` | `true` | Использовать фильтры Spark-запроса для выборочного чтения из {{product-name}} |
| `spark.yt.read.keyColumnsFilterPushdown.union.enabled` | `false` | Объединять все фильтры в непрерывный диапазон при выборочном чтении |
| `spark.yt.read.keyColumnsFilterPushdown.ytPathCount.limit` | `100` | Максимальное количество диапазонов таблицы при выборочном чтении |
| `spark.yt.transaction.timeout` | `5 minutes` | Таймаут на транзакцию записывающей операции |
| `spark.yt.transaction.pingInterval` | `30 seconds` | Периодичность пингования транзакции записывающей операции |
| `spark.yt.globalTransaction.enabled` | `false` | Использовать [глобальную транзакцию](../../../../../user-guide/data-processing/spyt/read-transaction.md) |
| `spark.yt.globalTransaction.id` | `None` | Идентификатор глобальной транзакции |
| `spark.yt.globalTransaction.timeout` | `5 minutes` | Таймаут глобальной транзакции |
| `spark.hadoop.yt.user` | - | Имя пользователя {{product-name}} |
| `spark.hadoop.yt.token` | - | Токен пользователя {{product-name}} |
| `spark.yt.read.ytPartitioning.enabled` | `true` | Использовать партиционирование таблиц средствами {{product-name}} |
| `spark.yt.read.planOptimization.enabled` | `false` | Оптимизировать агрегации и джойны на сортированных входных данных |
| `spark.yt.read.keyPartitioningSortedTables.enabled` | `true` | Использовать партиционирование по ключам для сортированных таблиц, необходимо для оптимизации планов |
| `spark.yt.read.keyPartitioningSortedTables.unionLimit` | `1` | Максимальное количество объединений партиций при переходе от чтения по индексам к чтению по ключам |

## Оптимизация агрегаций и джойнов

Spark кластер при чтении игнорирует метаинформацию о сортированности таблиц {{product-name}}, создавая планы с множеством Shuffle и Sort стадий. Для более эффективной работы были внедрены дополнительные правила оптимизации агрегаций и джойнов поверх сортированных данных. На этапе построения логического плана к вершинам чтения добавляются пометки о сортированности. Позже при создании физического плана эти пометки превращаются в физические вершины, которые не производят действий над данными, но уведомляют планировщик о способе сортировки и партиционирования данных.

Партиционирование статических таблиц производится по индексам строк, однако опция `spark.yt.read.keyPartitioningSortedTables.enabled` включает партиционирование и чтение по ключам. При таком переходе возможно уменьшение количества партиций, если ключи оказываются достаточно большими. Это может повлечь увеличение количества данных, которые приходятся на один экзекьютор.

## Дополнительные опции конфигурации кластера { #add }

Дополнительные опции передаются через `--params`:

```bash
spark-launch-yt \
  --proxy <cluster-name> \
  --discovery-path my_discovery_path \
  --params '{"spark_conf"={"spark.yt.jarCaching"="True";};"layer_paths"=["//.../ubuntu_xenial_app_lastest.tar.gz";...;];"operation_spec"={"max_failed_job_count"=100;};}' \
  --spark-cluster-version '1.36.0'
```

При использовании `spark-submit-yt` для настройки задачи существует опция `spark_conf_args`:

```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path my_discovery_path \
  --deploy-mode cluster \
  --conf spark.sql.shuffle.partitions=1 \
  --conf spark.cores.max=1 \
  --conf spark.executor.cores=1 \
  yt:///sys/spark/examples/grouping_example.py
```
```bash
spark-submit-yt \
  --proxy <cluster-name> \
  --discovery-path my_discovery_path \
  --deploy-mode cluster \
  --spark_conf_args '{"spark.sql.shuffle.partitions":1,"spark.cores.max":1,"spark.executor.cores"=1}' \
  yt:///sys/spark/examples/grouping_example.py
```

При запуске из кода можно производить настройку через `spark_session.conf.set("...", "...")`.

Пример на Python:

```python
from spyt import spark_session

print("Hello world")
with spark_session() as spark:
    spark.conf.set("spark.yt.read.parsingTypeV3.enabled", "true")
    spark.read.yt("//sys/spark/examples/test_data").show()
```

Пример на Java:

```java
protected void doRun(String[] args, SparkSession spark, CompoundClient yt) {
    spark.conf.set("spark.sql.adaptive.enabled", "false");
    spark.read().format("yt").load("/sys/spark/examples/test_data").show();
}
```


