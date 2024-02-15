# Логи

## Event log  { #event-log }
*Event log* – это специальный формат лога в Spark. Лог пишет [драйвер](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-app), а читает специальный сервис *Spark History Server (SHS)*. SHS запускается вместе с мастером и воркерами при старте кластера SPYT. SHS по логу рисует UI, аналогичный Spark UI.

Отличие между ними в том, что Spark UI доступен на драйвере в момент выполнения джобы, а SHS поднят всегда и может прочитать логи джобов, которые уже завершились.
Логи записываются в [динамическую таблицу](../../../../user-guide/dynamic-tables/overview.md) внутри `discovery-path`. SHS умеет удалять их [автоматически](#log-settings).

## Включение { #log-on }

При запуске джобы укажите в конфигурации `spark.eventLog.enabled=true`, например:
```bash
$ spark-submit-yt ... --conf spark.eventLog.enabled=true
```


## Настройка { #log-settings }

Подробно настройки SHS описаны в [документации Spark](https://spark.apache.org/docs/latest/monitoring.html#spark-history-server-configuration-options). Любые настройки из приведённого описания можно передавать в `spark-launch-yt ... --params '{spark_conf={"spark.param"=value;...}}'`.

По умолчанию значение параметра `spark.history.fs.cleaner.enabled` выставлено в `true`. Значения по умолчанию других параметров такое же как в документации к Spark. При настройках по умолчанию SHS удаляет логи старше недели. Срок хранения логов можно изменить следующей настройкой: `spark.history.fs.cleaner.maxAge` (`spark-launch-yt ... --params '{spark_conf={"spark.history.fs.cleaner.maxAge"="14d";...}}'`).


## Логи с воркеров {# worker-logs }

При запуске кластера можно настроить отправку логов с [воркеров](../../../../user-guide/data-processing/spyt/cluster/cluster-desc.md#spark-standalone) в {{product-name}} таблицы. Логи будут записываться по пути `{cluster_path}/logs/worker_log`, под каждую дату будет создаваться отдельная таблица. Для включения отправки логов при запуске кластера необходимо использовать следующие опции:
* `--enable-worker-log-transfer` – включает пересылку логов;
* `--enable-worker-log-json-mode` – включает json-режим работы логов. Данные в таблице представляются в более удобном формате, разбиваются на составные части. При отключенной опции логи записываются в одну колонку `message`;
* `--worker-log-update-interval ...` – задает периодичность отправки логов, по умолчанию – 10 минут (10m), не может быть меньше 1 минуты;
* `--worker-log-table-ttl ...` – устанавливает время жизни таблиц, по умолчанию – 7 дней (7d).

Посмотреть логи можно в SHS, при просмотре вкладки *executors* любого *application* у каждого воркера есть ссылки с префиксом `[HS]`. Страница логов формируется по таблицам из {{product-name}} и, следовательно, доступна, даже если воркера уже нет. Лог существует, пока соответствующая таблица не удалена:

```bash
spark-launch-yt ... \
  --enable-worker-log-transfer \
  --enable-worker-log-json-mode \
  --worker-log-update-interval 30m \
  --worker-log-table-ttl 30d
```

