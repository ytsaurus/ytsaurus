# Спецификация таблиц
__ВАЖНО__: Для использования спецификации необходимо выставить настройку `Settings.use_deprecated_spec_format=False`.

Спецификация таблиц в коде описана [здесь](https://a.yandex-team.ru/arcadia/yt/yt_sync/core/spec/table.py).

## Свойства федерации таблиц
Описаны в классе `Table`:
- `chaos: bool` - определяет является таблица реплицированной или хаосной
- `in_collocation: bool` - определяет находится реплицированная/хаосная таблица в коллокации
- __`schema: list[Column]`__ - схема таблиц (в случае федерации распространяется так же на все реплики), подробности см.
ниже
- __`clusters: dict[str, ClusterTable]`__ - описание свойств таблицы на конкретном кластере. Если указан только один
кластер, то таблица не считается реплицированной или хаосной

__ВАЖНО__: для упорядоченных реплицированных таблиц, если необходимо, чтобы `$tablet_index` для записываемых строк
сохранялся в таблицах репликах, необходимо выставить у реплицированной таблицы свойство `preserve_tablet_index=true`.
По-умолчанию этот атрибут выставлен в `false`, и до таблиц-реплик строки доезжают не в тех таблетах,
которые были указаны при записи.

### Схема таблицы
Документацию про схему таблицы YT можно почитать [тут](https://yt.yandex-team.ru/docs/user-guide/storage/static-schema).

Схема таблицы представляет собой список колонок. Спецификация колонки в коде описана
[здесь](https://a.yandex-team.ru/arcadia/yt/yt_sync/core/spec/table.py), в классе `Column`.

Атрибуты колонки:
- __`name: str`__ - название колонки
- `type: str` - тип колонки (см. `TypeV1`), должен быть указан если не указан `type_v3` (так же можно указывать оба)
- `type_v3: str | dict` - тип колонки в формате v3 (см. `TypeV3`), должен быть указан `type` (так же можно укаывать оба)
- `sort_order: str` - порядок сортировки `ascending` или `descending` (см. `Column.SortOrder`). `descending` лучше не
использовать
- `expression: str` - выражение для вычислимой ключевой колонки
- `required: bool` - является ли колонка обязательной, в обязательную колонку нельзя записать `null`
- `lock: str` - группа для транзакционных блокировок
- `group: str` - группа для чтения данных
- `aggregate: str` - функция для агрегирующих колонок (см. `Column.AggregateFunc`)
- `max_inline_hunk_size: int` - лимит на размер данных для выноса в ханки

_Примечание_: если используете `type_v3`, то лучше использовать хелпер `SchemaSpec.parse()`, потому что руками собирать
спецификацию очень муторно.

#### Парсинг и валидация
Для парсинга спецификации схемы из объекта типа `list[dict]`, и для валидации корректности схемы есть вспомогательный
класс `yt.yt_sync.core.spec.details.SchemaSpec`.

Примеры использования:
```python
from yt.yt_sync.core.spec.details import SchemaSpec
from yt.yt_sync.core.spec import Column

# parse and validate
schema: list[Column] = SchemaSpec.parse([
    {"name": "data", "type": "string"}
])

# only validate
Schema.ensure([
    Column(name="data", type=Column.Type.STRING)
])
```

### Свойства таблицы на кластере
Описаны в классе `ClusterTable`:
* `main: bool`
  > Определяет будет на данном кластере реплицированная/хаосная таблица или реплика с данными. Если
    кластер в спецификации таблицы указан один, то `main` можно не указывать. Для всех таблиц и нод в рамках одного запуска
`YtSync` должен быть одинаковый `main` кластер.
* __`path: str`__
  > Путь до таблицы на данном кластере.
* `schema_override: list[Column]`
  > Переопределение схемы для таблицы на конкретном кластере. Может быть нужно для включения ханков только на данном
    кластере, например.
* `replicated_table_tracker_enabled: bool`
  > Включен ли RTT для данной реплики (нельзя указать для `main` кластера).
    Если RTT включен хотя бы для одной реплики, то считается что вся реплицированная/хаосная таблица находится под
    управлением RTT.
* `preferred_sync: bool`
  > Определяет является ли данная реплика синхронной (нельзя указать для `main` кластера). Если реплика
    находится под управлением RTT, то форсит добавление кластера в атрибут
    `replicated_table_options/preferred_sync_replica_clusters` у реплицированной/хаосной таблицы, иначе напряму
    управляет свойством `mode` у объектов `table_replica`/`chaos_table_replica`.
* `attributes: dict[str, Any]`
  > Набор атрибутов таблицы. Для реплицированной/хаосной таблицы бесполезно указывать свойство
  `replicated_table_options/preferred_sync_replica_clusters`, т.к. оно вычисляется `YtSync` автоматически на
  основании атрибута `preferred_sync` в спецификации. Остальные свойства применяются as is. Если какое-то свойство
  таблицы необходимо удалить, ему нужно указать значение `None` в спецификации.
* `replication_log: ReplicationLog`
  > Настройки лога репликации. При желании можно переопределить путь, который автоматически вычисляется как
  `f"{path}_log"`, а так же атрибуты таблицы. См класс `ReplicationLog`.


## Использование и примеры
Спецификация таблицы передается в метод `YtSync.add_desired_table()`.

Переданная спецификация таблицы валидируется на корректность.

Спецификацию можно передать в виде заполненного экземпляра класса `Table`, либо в виде `dict`, заполненного в
соответствии с полями классов спецификации.

### Как можно передать спецификацию
```python
from yt.yt_sync import ClusterTable
from yt.yt_sync import Column
from yt.yt_sync import SchemaSpec
from yt.yt_sync import Settings
from yt.yt_sync import Table
from yt.yt_sync import YtSync

yt_sync = YtSync(settings=Settings(db_type=Settings.REPLICATED_DB, use_deprecated_spec_format=False, ...), ...)

# spec from dataclasses
yt_sync.add_desired_table(
    Table(
        schema=[Column(name="data", type=Column.Type.STRING)],
        clusters={
            "primary": ClusterTable(path="table_path", attributes={"dynamic": True})
        }
    )
)

# spec from dataclasses with from dict schema
yt_sync.add_desired_table(
    Table(
        schema=SchemaSpec.parse([{"name": "data", "type": "string"}]),
        clusters={
            "primary": ClusterTable(path="table_path", attributes={"dynamic": True})
        }
    )
)

# spec from dict
yt_sync.add_desired_table(
    {
        "schema": [{"name": "data", "type": "string"}],
        "clusters": {
            "primary": {"path": "table_path", "attributes": {"dynamic": True}}
        }
    }
)
```

### Пример спецификации реплицированной таблицы
```python
from yt.yt_sync import Settings
from yt.yt_sync import Table
from yt.yt_sync import YtSync

yt_sync = YtSync(settings=Settings(db_type=Settings.REPLICATED_DB, use_deprecated_spec_format=False, ...), ...)

# replicated table is primary:table_path
#  > assert primary_yt_client.get(table_path/@replicated_table_options/preferred_sync_replica_clusters) = ["remote0"]
#
# replicas under RTT are:
#  - remote0:table_path_sync_replica_rtt (preferred sync)
#  - remote1:table_path_async_replica_rtt
#
# async replica without RTT is remote2:table_path_async_replica
yt_sync.add_desired_table(
    {
        "schema": [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "data", "type": "string"},
        ],
        "in_collocation": True,
        "clusters": {
            "primary": {"main": True, "path": "table_path", "attributes": {"dynamic": True}},
            "remote0": {
                "main": False,
                "path": "table_path_sync_replica_rtt",
                "replicated_table_tracker_enabled": True,
                "preferred_sync": True,
                "attributes": {"dynamic": True}
            },
            "remote1": {
                "main": False,
                "path": "table_path_async_replica_rtt",
                "replicated_table_tracker_enabled": True,
                "preferred_sync": False,
                "attributes": {"dynamic": True}
            },
            "remote2": {
                "main": False,
                "path": "table_path_async_replica",
                "replicated_table_tracker_enabled": False,
                "preferred_sync": False,
                "attributes": {"dynamic": True}
            },
        }
    }
)

# replicated table is primary:table_path
#  > assert primary_yt_client.get(table_path/@replicated_table_options/preferred_sync_replica_clusters) = []
#
# replicas under RTT are:
#  - remote0:table_path_sync_replica_rtt
#  - remote1:table_path_async_replica_rtt
#  - remote2:table_path_async_replica
#
#  sync replicas any two of remote0, remote1, remote2
yt_sync.add_desired_table(
    {
        "schema": [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "data", "type": "string"},
        ],
        "clusters": {
            "primary": {
                "main": True,
                "path": "table_path",
                "attributes": {
                    "dynamic": True,
                    "replicated_table_options": {
                        "min_sync_replica_count": 2
                    }
                }
            },
            "remote0": {
                "main": False,
                "path": "table_path_sync_replica_rtt",
                "replicated_table_tracker_enabled": True,
                "preferred_sync": False,
                "attributes": {"dynamic": True}
            },
            "remote1": {
                "main": False,
                "path": "table_path_async_replica_rtt",
                "replicated_table_tracker_enabled": True,
                "preferred_sync": False,
                "attributes": {"dynamic": True}
            },
            "remote2": {
                "main": False,
                "path": "table_path_async_replica",
                "replicated_table_tracker_enabled": True,
                "preferred_sync": False,
                "attributes": {"dynamic": True}
            },
        }
    }
)
```

### Пример спецификации хаосной таблицы
```python
from yt.yt_sync import Settings
from yt.yt_sync import Table
from yt.yt_sync import YtSync

yt_sync = YtSync(settings=Settings(db_type=Settings.CHAOS_DB, use_deprecated_spec_format=False, ...), ...)

# chaos table is primary:table_path
#  > assert primary_yt_client.get(table_path/@replicated_table_options/preferred_sync_replica_clusters) = ["remote0"]
#
# replicas under RTT are:
#  - remote0:table_path_sync_replica_rtt [content_type=data]
#  - remote0:table_path_sync_replica_rtt_log [content_type=queue]
#  - remote1:table_path_async_replica_rtt [content_type=data]
#  - remote1:table_path_async_replica_rtt_log [content_type=queue]
#
# async replicas without RTT are:
#  - remote2:table_path_async_replica [content_type=data]
#  - remote2:table_path_async_replica_custom_log [content_type=queue]
yt_sync.add_desired_table(
    {
        "chaos": True,
        "schema": [
            {"name": "key", "type": "string", "sort_order": "ascending"},
            {"name": "data", "type": "string"},
        ],
        "clusters": {
            "primary": {"main": True, "path": "table_path", "attributes": {"dynamic": True}},
            "remote0": {
                "main": False,
                "path": "table_path_sync_replica_rtt",
                "replicated_table_tracker_enabled": True,
                "preferred_sync": True,
                "attributes": {"dynamic": True}
            },
            "remote1": {
                "main": False,
                "path": "table_path_async_replica_rtt",
                "replicated_table_tracker_enabled": True,
                "preferred_sync": False,
                "attributes": {"dynamic": True}
            },
            "remote2": {
                "main": False,
                "path": "table_path_async_replica",
                "replicated_table_tracker_enabled": False,
                "preferred_sync": False,
                "attributes": {"dynamic": True},
                "replication_log": {
                    "path": "table_path_async_replica_custom_log"
                }
            },
        }
    }
)
```
