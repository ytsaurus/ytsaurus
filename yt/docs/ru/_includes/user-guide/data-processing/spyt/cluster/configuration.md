# Конфигурации

Список всех поддерживаемых опций Spark содержится в [документации](https://spark.apache.org/docs/latest/configuration.html).

## Основные опции { #main }

Большинство опций доступны, начиная с версии 1.23.0 если не указано иное.

- `spark.yt.write.batchSize`: по умолчанию – `500 000`, размер данных, отправляемых через одну операцию `WriteTable`;
- `spark.yt.write.miniBatchSize`: по умолчанию – `1000`, размер данных, отправляемых на запись в {{product-name}};
- `spark.yt.write.timeout`: по умолчанию – `60 seconds`, ограничение на ожидание записи одного мини-батча;
- `spark.yt.write.typeV3.enabled`: по умолчанию – `false`, (с версии 1.75.2) запись таблиц со схемой в формате [type_v3](../../../../../user-guide/storage/data-types.md), по умолчанию `type_v1`;
- `spark.yt.write.writingTypeV3.enabled`: по умолчанию – `false`, то же, что и `spark.yt.write.typeV3.enabled` для версий старше 1.75.2;
- `spark.yt.read.vectorized.capacity`: по умолчанию – `1000`, максимальное количество строк в батче при чтении через `wire protocol ()`;
- `spark.yt.read.arrow.enabled`: по умолчанию – `true`, использование считывания батчами при возможности;
- `spark.yt.read.typeV3.enabled`: по умолчанию – `false`, (с версии 1.75.2) чтение таблиц со схемой в формате [type_v3](../../../../../user-guide/storage/data-types.md), по умолчанию `type_v1`;
- `spark.yt.read.parsingTypeV3.enabled`: по умолчанию – `false`, то же, что и `spark.yt.read.typeV3.enabled` для версий старше 1.75.2;
- `spark.yt.read.keyColumnsFilterPushdown.enabled`: по умолчанию – `false` или `Predicate pushdown`. Использование фильтров Spark-запроса для чтения из {{product-name}} оптимизирует объем полученных данных из {{product-name}} и, соответственно, уменьшает время чтения. При формировании пути добавляется range необходимых строк;
- `spark.yt.read.keyColumnsFilterPushdown.union.enabled`: по умолчанию – `false`, при пробросе фильтров происходит объединение в один и из таблицы запрашивается непрерывный диапазон строк;
- `spark.yt.read.keyColumnsFilterPushdown.ytPathCount.limit`: по умолчанию – `100`, максимальное количество диапазонов, на которое распадется Spark-запрос чтения;
- `spark.yt.transaction.timeout`: по умолчанию – `5 minutes`, timeout на транзакцию записывающей операции;
- `spark.yt.transaction.pingInterval`: по умолчанию – `30 seconds`;
- `spark.yt.globalTransaction.enabled`: по умолчанию – `false`, использование [глобальной транзакции](../../../../../user-guide/data-processing/spyt/read-transaction.md);
- `spark.yt.globalTransaction.id`: по умолчанию – `None`, id созданной глобальной транзакции;
- `spark.yt.globalTransaction.timeout`: по умолчанию – `2 minutes`, timeout глобальной транзакции;
- `spark.hadoop.yt.user`: по умолчанию – первое доступное: переменные окружения `YT_SECURE_VAULT_YT_USER` или `YT_USER`, `user.name` из сис. свойств, пользователь для {{product-name}};
- `spark.hadoop.yt.user`: по умолчанию – первое доступное: переменные окружения `YT_SECURE_VAULT_YT_TOKEN` или `YT_TOKEN`, содержимое файла `~/.yt/token`, токен для {{product-name}};
- `spark.hadoop.yt.timeout`: по умолчанию – `300 seconds`, timeout на чтения из {{product-name}}.

## Дополнительные опции { #add }

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


