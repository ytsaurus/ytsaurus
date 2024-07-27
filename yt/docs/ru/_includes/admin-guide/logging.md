# Настройка логирования серверных компонент

Серверные компоненты кластера {{product-name}} пишут подробные логи, которые можно использовать для аудита и анализа проблем при эксплуатации. В продакшн-инсталляциях рекомендуется выделять под логи отдельные [локации на персистентных томах](../../admin-guide/locations.md). Отсутствие логов может существенно ограничить возможности поддержки.

Для анализа работы подсистемы логирования можно воспользоваться prometheus-метриками с префиксом `yt_logging_*`.

## Отладочные логи { #debug_logs }

Отладочные логи описываются в секции `loggers` спецификации компонент {{product-name}}.

<small>Таблица 1 — Настройки отладочных логгеров YTsaurus </small>

| **Поле**            | **Возможные значения** | **Описание** |
| ------------------- | --------------- | ------- |
| `name`         | произвольная строка | Имя логгера; рекомендуется выбирать короткие и понятные названия `debug`, `info` и т. п. |
| `format` |  `plain_text` (default), `yson`, `json` | Формат строки лога. |
| `minLogLevel` |  `trace`, `debug`, `info`, `error` | Минимальный уровень записей, попадающих в лог. |
| `categoriesFilter` |   | Фильтр, позволяющий писать логи только от некоторых подсистем, см. [ниже](#debug_log_categories). |
| `writerType` |  `file`, `stderr` | Писать логи в файл или в stderr. При записи в stderr настройки ротации будут игнорироваться.  |
| `compression`         | `none` (default), `gzip`, `zstd` | Если задано значение, отличное от `none`, сервер {{product-name}} будет писать сжатые логи. |
| `useTimestampSuffix` |  `true`, `false` (default) | Если `true`, к имени файла в момент открытия или ротации дописывается timestamp. При этом механизм нумерации старых сегментов при ротации не используется. Имеет смысл только при записи в файл. |
| `rotationPolicy` |   | Настройки ротации логов, см. [ниже](#log_rotation). Имеет смысл только при записи в файл.|


Путь к каталогу, куда будут записываться логи с `writerType=file` , задаётся в описании локации типа `Logs`. В случае, если локация `Logs` не указана, логи будут записываться в каталог `/var/log`.

Имена файлов с логом формируются следующим образом: `[component].[name].log(.[format])(.[compression])(.[timestamp_suffix])`. Примеры:
  - `controller-agent.error.log`
  - `master.debug.log.gzip`
  - `scheduler.info.log.json.zstd.2023-01-01T10:30:00`

Записи отладочных логов содержат следующие поля:
  - `instant` — время в локальной временной зоне;
  - `level` — уровень записи: `T` — trace, `D` — debug, `I` — info, `W` — warning, `E` — error;
  - `category` — имя подсистемы, к которой относится запись, например `ChunkClient`, `ObjectServer` или `RpcServer`;
  - `message` — тело сообщения;
  - `thread_id` — id (или имя) треда, породившего запись; пишется только в формате `plain_text`;
  - `fiber_id` — id файбера, породившего запись; пишется только в формате `plain_text`;
  - `trace_id` — trace_context id, в контексте которого появилась запись; пишется только в формате `plain_text`.

{% cut "Пример записи" %}
```
2023-09-15 00:00:17,215385      I       ExecNode        Artifacts prepared (JobId: d15d7d5f-164ff08a-3fe0384-128e0, OperationId: cd56ab80-d21ef5ab-3fe03e8-d05edd49, JobType: Map)      Job     fff6d4149ccdf656    2bd5c3c9-600a44f5-de721d58-fb905017
```
{% endcut %}

### Рекомендации по настройке категорий { #debug_log_categories }

Существует два вида фильтра по категориям (`categoriesFilter`):
  - включающий — пишутся записи только тех категорий, которые были явно перечислены;
  - исключающий — пишутся записи любых категорий, кроме тех, которые были перечислены.

Зачастую, в больших инсталляциях приходится исключать категории `Bus` и `Concurrency`.

{% cut "Примеры фильтров" %}
```yaml
categoriesFilter:
  type: exclude
  values: ["Bus", "Concurrency"]

categoriesFilter:
  type: include
  values: ["Scheduler", "Strategy"]
```
{% endcut %}


## Структурированные логи { #structured_logs }
Некоторые компоненты {{product-name}} способны писать структурированные логи, которые можно использовать впоследствии для аудита, аналитики и автоматической обработки. Структурированные логи описываются в секции `structured_loggers` спецификации компонент {{product-name}}.

Для описания структурированных логгеров используются те же поля, что и для отладочных логов, кроме:
 - `writerType` — не задаётся; структурированные логи всегда пишутся в файл;
 - `categoriesFilter` — вместо него задаётся обязательное поле `category` — равно одна категория.

Структурированные логи рекомендуется всегда писать в одном из структурированных форматов — `json` или `yson`. События в структурированный лог обычно пишутся на уровне `info`. Набор полей структурированного лога зависит от конкретного типа лога.

Основные типы структурированных логов:
 - `master_access_log` — лог доступа к данным;  пишется на мастере, категория `Access`;
 - `master_security_log` — лог событий безопасности, например добавление пользователя в группу или изменение ACL; пишется на мастере, категория `SecurityServer`;
 - `structured_http_proxy_log` — лог запросов к http proxy, по одной строке на запрос; пишется на http proxy, категория `HttpStructuredProxy`;
 - `chyt_log` — лог запросов к CHYT, по одной строке на запрос; пишется на http proxy, категория `ClickHouseProxyStructured`;
 - `structured_rpc_proxy_log` — лог запросов к rpc proxy, по одной строке на запрос; пишется на rpc proxy, категория `RpcProxyStructuredMain`;
 - `scheduler_event_log` — лог событий планировщика; пишется планировщиком, категория `SchedulerEventLog`;
 - `controller_event_log` — лог событий контроллер-агента; пишется на контроллер-агенте, категория `ControllerEventLog`.

### Лог доступа к таблице { #access_log }

{% include [Подробно про наполнение лога](access-log-details.md) %}

#### Замечания

{% include [Замечания](access-log-note.md) %}

### Лог запросов к HTTP-проксе { #http_proxy_log }

В данном логе журналируются записи обо всех запросах, которые пришли в HTTP-проксю.

<small>Таблица 1 — Описание полей лога</small>

| Поле                                         | Описание                                                                                                                      |
|----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `instant`                                    | Время события в формате `YYYY-MM-DD hh:mm:ss,sss`                                                                             |
| `cluster`                                    | Короткое имя кластера                                                                                                         |
| `request_id`                                 | Идентификатор данного запроса                                                                                                 |
| `correlation_id`                             | Специальный идентификатор запроса, который генерируется клиентом и не изменяется в случае совершения ретрая запроса           |
| `user`                                       | Пользователь, выполняющий запрос                                                                                              |
| `method`                                     | Метод HTTP-запроса                                                                                                            |
| `http_path`                                  | Путь HTTP-запроса                                                                                                             |
| `user_agent`                                 | Содержимое хедера `User-Agent` в запросе                                                                                      |
| `command`                                    | [Команда]((../../../api/commands.md)                                                                                          |
| `parameters`                                 | Параметры данной команды                                                                                                      |
| `path`                                       | Значение параметра `path`                                                                                                     |
| `error`                                      | Стуктурированная описание ошибки, в случае если запрос завершился неуспешно                                                   |
| `error_code`                                 | Код ошибки, в случае если запрос завершился неуспешно                                                                         |
| `http_code`                                  | HTTP-код ответа на данный запрос                                                                                              |
| `start_time`                                 | Фактическое время начала исполнения запроса на проксе                                                                         |
| `cpu_time`                                   | Время, потраченное проксей на данный запрос (в него не входит время ожидания исполнения запроса в других компонентах кластера)|
| `duration`                                   | Общая длительность данного запроса                                                                                            |
| `in_bytes`                                   | Размер входных данных запроса в байтах                                                                                        |
| `out_bytes`                                  | Размер выходных данных запроса в байтах                                                                                       |
| `remote_address`                             | Адрес, с которого пришел данный запрос                                                                                        |


## Настройка ротации логов { #log_rotation }
Для отладочных и структурированных логов, которые пишутся в файл, можно настроить встроенный механизм ротации (поле `rotationPolicy`). Настройки ротации приведены в таблице. Если не используется опция `useTimestampSuffix`, в момент ротации файлы старых сегментов переименовываются, с дописыванием порядкового номера.

<small>Таблица 2 — Настройки ротации логов </small>

| **Поле**            | **Описание** |
| ------------------- |  ------- |
| `rotationPeriodMilliseconds` | Период ротации, в миллисекундах. Может задаваться вместе с `maxSegmentSize`. |
| `maxSegmentSize` | Ограничение на размер одного сегмента лога, в байтах. Может задаваться вместе с `rotationPeriodMilliseconds`.|
| `maxTotalSizeToKeep` | Ограничение на размер всех сегментов, в байтах. В момент ротации, удаляются самые старые логи так, чтобы уложиться в заданное ограничение.|
| `maxSegmentCountToKeep` | Ограничение на количество хранимых сегментов лога, в штуках. Самые старые сегменты сверх ограничения удаляются. |


## Динамическая конфигурация { #log_dynamic_configuration }
Компоненты, поддерживающие динамическую конфигурацию, позволяют дополнительно уточнить настройки системы логирования с помощью динамического конфига в Кипарисе, секция `logging`.

Основные параметры:
 - `enable_anchor_profiling` — включает prometheus-метрики по отдельным префиксам записей;
 - `min_logged_message_rate_to_profile` — минимальная частота сообщения, для попадания в отдельную метрику;
 - `suppressed_messaged` — список префиксов сообщений отладочных логов, которые будут исключены из логирования.

Пример конфигурации:
```yson
{
  logging = {
    enable_anchor_profiling = %true;
    min_logged_message_rate_to_profile = 100;
    suppressed_messaged = [
      "Skipping out of turn block",
      "Request attempt started",
      "Request attempt acknowledged"
    ];
  }
}
```

## Пример настройки логирования { #logging_examples }
```yaml
  primaryMasters:
    ...
    loggers:
	  - name: debug
        compression: zstd
        minLogLevel: debug
        writerType: file
        rotationPolicy:
          maxTotalSizeToKeep: 50_000_000_000
          rotationPeriodMilliseconds: 900000
        categoriesFilter:
          type: exclude
          values: ["Bus", "Concurrency", "ReaderMemoryManager"]
      - name: info
        minLogLevel: info
        writerType: file
        rotationPolicy:
    	  maxTotalSizeToKeep: 10_000_000_000
          rotationPeriodMilliseconds: 900000
      - name: error
        minLogLevel: error
        writerType: stderr
    structuredLoggers:
      - name: access
        minLogLevel: info
        category: Access
        rotationPolicy:
          maxTotalSizeToKeep: 5_000_000_000
          rotationPeriodMilliseconds: 900000
    locations:
      - locationType: Logs
        path: /yt/logs
      - ...

    volumeMounts:
      - name: master-logs
        mountPath: /yt/logs
      - ...

    volumeClaimTemplates:
      - metadata:
          name: master-logs
        spec:
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: 100Gi
      - ...
```
