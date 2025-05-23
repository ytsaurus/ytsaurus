# Spark History Server (SHS)

History Server может входить как в состав внутреннего кластера, так и запускаться отдельно. Второй случай необходим, если используется запуск задач [напрямую в {{product-name}}](../../../../../user-guide/data-processing/spyt/launch.md#submit).

## Сохранение event logs при запуске задач напрямую в {{product-name}} { #shs-direct-submit }

Чтобы сохранить event logs запуска задачи, команде `spark-submit` следует указать параметры:
`--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=ytEventLog:/<history server discovery path>/logs/event_log_table`

В этом случае event logs запуска будут сохранены в соответствующей таблице и будут доступны в History Server, который использует эту таблицу в качестве хранилища логов.

## Запуск отдельного History Server-а { #shs-launch-yt }

Основные параметры команды `shs-launch-yt` являются подмножеством параметров команды [spark-launch-yt](../../../../../user-guide/data-processing/spyt/cluster/cluster-start.md#spark-launch-yt-params), релевантные для запуска History Server. Пример запуска History Server-а:

```bash
$ shs-launch-yt --proxy <proxy address> --discovery-path <discovery path>
```

Пример:

```bash
$ shs-launch-yt --proxy my.ytsaurus.cluster.net --discovery-path //home/user/spark/discovery
```
