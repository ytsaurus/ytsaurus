# Spark History Server (SHS)

History Server может входить как в состав внутреннего кластера, так и запускаться отдельно. Второй случай необходим, если используется запуск задач [напрямую в {{product-name}}](../../../../../user-guide/data-processing/spyt/launch.md#submit).

## Сохранение event logs при запуске задач напрямую в {{product-name}} { #shs-direct-submit }

Чтобы сохранить event logs запуска задачи, команде `spark-submit` следует указать параметры:
`--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=ytEventLog:/<history server discovery path>/logs/event_log_table`

В этом случае event logs запуска будут сохранены в соответствующей таблице и будут доступны в History Server, который использует эту таблицу в качестве хранилища логов.

{% note info "Примечание" %}

При запуске History Server индексирует все записи из источника event logs, при этом обработка каждой записи может занимать 10–15 секунд. Новые логи становятся доступны только после завершения полной индексации всех существующих данных.

В кластерах с высокой нагрузкой это может привести к значительным задержкам при старте или перезапуске History Server. Поэтому для production‑кластеров с интенсивным потокам задач рекомендуется использовать новую (пустую) таблицу event logs при запуске нового кластера Spark. При необоходимости старую таблицу можно архивировать (переместить таблицу).

{% endnote %}

## Запуск отдельного History Server-а { #shs-launch-yt }

Основные параметры команды `shs-launch-yt` являются подмножеством параметров команды [spark-launch-yt](../../../../../user-guide/data-processing/spyt/cluster/cluster-start.md#spark-launch-yt-params), релевантные для запуска History Server. Пример запуска History Server-а:

```bash
$ shs-launch-yt --proxy <proxy address> --discovery-path <discovery path>
```

Пример:

```bash
$ shs-launch-yt --proxy my.ytsaurus.cluster.net --discovery-path //home/user/spark/discovery
```
