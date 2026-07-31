# Логирование вызовов {{product-name}} (доступно с версии SPYT 2.11.0)

SPYT умеет писать в лог каждый RPC-вызов к {{product-name}}: тип вызова, идентификатор запроса и длительность, а для чтения и записи таблиц — ещё и ypath. Это основной способ понять, сколько времени джоба проводит в вызовах кластера и к каким именно таблицам обращается.

## Включение { #enable }

Логирование включается системным свойством `spyt.yt.calls.log.level` — отдельно для драйвера и экзекьюторов:

```bash
--conf spark.driver.extraJavaOptions=-Dspyt.yt.calls.log.level=debug
--conf spark.executor.extraJavaOptions=-Dspyt.yt.calls.log.level=debug
```

Если свойство не задано, уровень этих логгеров совпадает с уровнем корневого логгера профиля логирования, то есть поведение по умолчанию не меняется.

Свойство поддерживается во всех профилях логирования, которые поставляются со SPYT, поэтому работает и для драйвера с экзекьюторами при прямом сабмите, и для компонентов standalone-кластера SPYT. Логируются как обычное чтение и запись таблиц, так и распределённые режимы чтения и записи.

{% note warning "Внимание" %}

Уровень `debug` даёт десятки строк логов на каждую операцию с таблицей. Включайте его для разовой диагностики конкретной джобы, а не постоянно.

{% endnote %}

## Что попадает в лог { #loggers }

| Логгер | Что пишет |
| --- | --- |
| `tech.ytsaurus.client.rpc.DefaultRpcBusClient` | каждый RPC-запрос и его длительность |
| `tech.ytsaurus.client.rpc.FailoverRpcExecutor` | повторные попытки запросов |
| `tech.ytsaurus.spyt.wrapper` | ypath и идентификатор запроса для вызовов чтения |
| `tech.ytsaurus.spyt.format.YtOutputWriter`, `tech.ytsaurus.spyt.format.YtDistributedOutputWriter` | ypath и идентификатор запроса для записи |

## Пример вывода { #example }

```
26/07/30 13:46:47 DEBUG YtWrapper$: Formatting path ytTable:/tmp/keepling/spyt-1152/2-debug-on/dst
26/07/30 13:46:47 DEBUG YtWrapper$: Formatting path /tmp/keepling/spyt-1152/2-debug-on/dst
26/07/30 13:46:47 DEBUG DefaultRpcBusClient: Sending request `ApiService/LockNode/b4-7e2cf11b-894d8647-fa6ae0dc` Session: Session(/slot/pipes/yt-node-9012-489-job-proxy-6@a56d753)
26/07/30 13:46:47 DEBUG DefaultRpcBusClient: Request `ApiService/LockNode/b4-7e2cf11b-894d8647-fa6ae0dc` finished in 478 ms Session: Session(/slot/pipes/yt-node-9012-489-job-proxy-6@a56d753)
26/07/30 13:46:47 DEBUG YtWrapper$: YT partition tables: #5a9acae4-1a5-139f0191-e941a650, splitBytes: 268435456, enableCookies: false, requestId: b9-82970b11-ccb8c93d-c261d589
26/07/30 13:46:47 DEBUG DefaultRpcBusClient: Sending request `ApiService/PartitionTables/b9-82970b11-ccb8c93d-c261d589` Session: Session(/slot/pipes/yt-node-9012-489-job-proxy-6@4aabcb7b)
26/07/30 13:46:48 DEBUG DefaultRpcBusClient: Request `ApiService/PartitionTables/b9-82970b11-ccb8c93d-c261d589` finished in 474 ms Session: Session(/slot/pipes/yt-node-9012-489-job-proxy-6@4aabcb7b)
```

Длительность вызова пишет клиент — это строка `Request ... finished in N ms`. С конкретной таблицей она связывается через идентификатор запроса: в примере вызов `PartitionTables` с идентификатором `b9-82970b11-ccb8c93d-c261d589` занял 474 мс, а строка `YT partition tables` с тем же идентификатором показывает, для какого ypath он был сделан.
