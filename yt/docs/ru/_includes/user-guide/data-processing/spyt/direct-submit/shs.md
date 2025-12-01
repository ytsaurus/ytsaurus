# Spark History Server (SHS)

Для использования History server в этом режиме его необходимо поднять отдельно.

Основные параметры команды `shs-launch-yt` являются подмножеством параметров команды [spark-launch-yt](../../../../../user-guide/data-processing/spyt/cluster/cluster-start.md#spark-launch-yt-params), релевантные для запуска History Server. Пример запуска History Server-а:

```bash
$ shs-launch-yt --proxy <proxy address> --discovery-path <discovery path>
```

Пример:

```bash
$ shs-launch-yt --proxy my.ytsaurus.cluster.net --discovery-path //home/user/spark/discovery
```

Чтобы сохранить event logs запуска задачи, команде `spark-submit` следует указать параметры:
`--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=ytEventLog:/<history server discovery path>/logs/event_log_table`

В этом случае event logs запуска будут сохранены в соответствующей таблице и будут доступны в History Server, который использует эту таблицу в качестве хранилища логов.

{% include [shs предупреждение](../common/shs-disclaimer.md) %}
