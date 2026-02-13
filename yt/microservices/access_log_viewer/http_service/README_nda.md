# HTTP-Сервис для просмотра access-логов мастера

## Эндпоинты

### `GET /liveness`

Запущен ли сервис. Всегда возвращает `200 OK`

Тело ответа:
```
OK
```

### `GET /readiness`

Готов ли сервис принимать запросы. Чувствителен к тому поднята ли клика.

Тело ответа:
```
OK
```

### `POST /info`

Пока не возвращает никакой конкретной информации

### `POST /whoami`

Использует Blackbox для аутентификации пользователя и возвращает его имя.

Тело ответа:
```json
{
    "user": "ilyaibraev"
}
```

Тело ответа о неуспехе:
```json
{
  "code": 1,
  "message": "Unsupported auth method"
}
```

### `POST /served-clusters` или `POST /served_clusters`

Возвращает список кластеров которые обслуживает

Тело ответа:
```json
{
  "clusters": [
    "hahn",
    ...
  ]
}
```

### `GET /metrics`

Предоставляет метрики сервиса в формате `application/x-solomon-spack`

### `POST /visible-time-range` или `POST /visible_time_range`

Возвращает диапазон временных меток, для которых доступны access-логи мастера по указанному кластеру

Тело запроса:
```json
{
  "cluster": "hahn"
}
```

Тело ответа:
```json
{
  "earliest": 1605474000,
  "latests": 1763634600
}
```

### `POST /access-log` или `POST /access_log`

Принимает в теле различные фильтры, возвращает список совпадающих записей

Параметры тела запроса:

* `cluster` (string, обязательный): Имя кластера YT для запроса (например, `freud`).
* `path` (string, обязательный): Путь в Cypress для запроса (например, `//home/dev`).
* `recursive` (bool, опциональный): Если `true`, запрос будет включать указанный путь и всех его потомков. По умолчанию `false`.
* `begin` (float, обязательный): Начало временного диапазона (как Unix метка) времени.
* `end` (float, опциональный): Конец временного диапазона (как Unix метка) времени. Если не указан, запрос выполняется до самых последних доступных данных.
* `user_regex` (string, опциональный): regexp для фильтрации записей лога по `username`.
* `path_regex` (string, опциональный): regexp для фильтрации записей лога по `path`.
* `method_group` (object, опциональный): Объект для фильтрации по группам методов. Ключи — это [группы методов](https://a.yandex-team.ru/arcadia/yt/microservices/access_log_viewer/http_service/data_model.go?rev=r18072634#L42) или конкретные методы. Надо установить `true`, чтобы включить их:

    ```
    "method_group": {
        "read": true
    }
    ```

* `scope` (object, опциональный): Объект для фильтрации по типу объекта. Ключи — это [типы объектов](https://a.yandex-team.ru/arcadia/yt/microservices/access_log_viewer/http_service/data_model.go?rev=r18072725#L170) [(и отдельный тип `"other"`)](https://a.yandex-team.ru/arcadia/yt/microservices/access_log_viewer/http_service/query_builder.go?rev=r18072653#L136). Надо установить `true`, чтобы включить их.
* `metadata` (bool, опциональный): Если установлено, фильтрует запросы, нацеленные на атрибуты объектов. Если установить `true`, то будут отображаться только обращения к атрибутам, `false` для запросов только не к атрибутам. Если не указано, отображаются все обращения.
* `user_type` (object, опциональный): Объект для фильтрации по типу пользователя. [Ключами могут быть `"human"`, `"robot"`, `"system"`.](https://a.yandex-team.ru/arcadia/yt/microservices/access_log_viewer/http_service/data_model.go?rev=r18072806#L110). Надо установить `true`, чтобы включить их.
* `distinct_by` (string, опциональный): По какой колонке access-лога смотреть уникальные строки, отобразится только первый из них.
* `pagination` (object, опциональный): Пагинация:
    * `index` (int): Номер страницы
    * `size` (int): Сколько записей на странице
* `field_selector` (object, опциональный): Объект для включения дополнительных полей в ответ. По умолчанию возвращаются только основные поля. Надо установить `true`, чтобы включить их.
    * Поля: `original_path`, `target_path`, `transaction_info`, `method_group`, `scope`, `user_type`.

Тело запроса
```json
{
  "recursive": true,
  "pagination": {
    "index": 0,
    "size": 2
  },
  "path_regex": ".*bad.*",
  "user_regex": ".*cron.*",
  "begin": 1763628592.98,
  "end": 1763642992.98,
  "cluster": "freud",
  "path": "//home/dev",
  "field_selector": {
    "user_type": true
  },
  "method_group": {
    "read": true
  },
  "user_type": {
    "robot": true
  },
  "scope": {
    "table": true
  }
}
```

Тело ответа:
```json
{
  "accesses": [
    {
      "instant": "2025-11-20 13:58:17",
      "method": "Get",
      "user": "robot-yt-cron-merge",
      "path": "//home/dev/ermolovd/bad_event_log_copy/@",
      "type": "table",
      "original_path": "",
      "target_path": "",
      "destination_path": "",
      "original_destination_path": "",
      "source_path": "",
      "original_source_path": "",
      "transaction_id": "",
      "transaction_info": null,
      "method_group": "",
      "scope": "",
      "user_type": "robot"
    },
    {
      "instant": "2025-11-20 13:58:14",
      "method": "Get",
      "user": "robot-yt-cron-merge",
      "path": "//home/dev/ermolovd/bad_event_log_copy/@account",
      "type": "table",
      "original_path": "",
      "target_path": "",
      "destination_path": "",
      "original_destination_path": "",
      "source_path": "",
      "original_source_path": "",
      "transaction_id": "",
      "transaction_info": null,
      "method_group": "",
      "scope": "",
      "user_type": "robot"
    }
  ],
  "total_row_count": 36
}
```

### `POST /chquery`

Возвращает для CHYT сырой запрос и его настройки.

Тело запроса то же, что и для `/access-log`.

Тело ответа:
```json
{
  "query": "SELECT\n  method,\n  user,\n  substr(instant, 1, 19) as instant,\n  path,\n  type,\n  destination_path,\n  original_destination_path,\n  source_path,\n  original_source_path,\n  transaction_id,\n  (multiIf(((startsWith(user, 'robot-') OR startsWith(user, 'zomb-')) as is_robot_user), 'robot', (user IN ('root','system','scheduler','table_mount_informer','yt-clickhouse-cache') as is_system_user), 'system', 'human') as user_type)\nFROM\n  ytTables(ytListLogTables('//sys/admin/yt-microservices/access_log_viewer/freud', toDateTime(1763628592), toDateTime(1763642992)))\nWHERE\n  ((startsWith(user, 'robot-') OR startsWith(user, 'zomb-')) as is_robot_user) AND match(user, '.*cron.*') AND\n  type IN ('table') AND\n  ((method IN ('CheckPermission', 'Exists', 'Fetch', 'Get', 'GetBasicAttributes', 'List') as is_read)) AND\n  (path = '//home/dev' OR startsWith(path, '//home/dev/')) AND match(path, '.*bad.*') AND\n  toDateTime(instant) >= toDateTime(1763628592) AND toDateTime(instant) < toDateTime(1763642992) AND\n  dictGetString('ACL', 'action', tuple('freud', 'ilyaibraev', path)) == 'allow'\nORDER BY instant DESC",
  "settings": {
    "priority": "63643231"
  }
}
```

### `POST /qt-access-log` или `POST /qt_access_log`

Возвращает ID запущенного запроса и кластер где запрос запущен.

Тело запроса то же, что и для `/access-log`.

Тело ответа:
```json
{
  "stage": "",
  "cluster": "hahn",
  "query_id": "d1eae0fd-ac4cf06c-8a820fc7-dea3b20f"
}
```
