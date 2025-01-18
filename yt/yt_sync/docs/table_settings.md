# Настройки таблиц (deprecated)
Настройки таблиц - это `dict` определённого содержания, который должен быть передан в метод `add_desired_table()`.

Формат частично совместим с настройками [старого yt_sync](https://a.yandex-team.ru/arcadia/ads/caesar/libs/yt_sync):
- не поддержаны динамические вычисления, например, вычисление схемы через поле `schema_generator`
- нет автоматического перекладывания свойств таблицы из синхронной реплики на главную для однокластерной инсталляции,
необходимо напрямую указывать корректные атрибуты для таблицы на основном кластере

## Обязательные атрибуты
- `path: str` - полный путь до таблицы на основном кластере
- `master: dict` - свойства таблицы на основном кластере лежат здесь
  - `cluster: str` - название кластера, где располагается основная таблица
  - `attributes: dict` - атрибуты таблицы на основном кластере
    - `schema: list` - набор полей таблицы

{% cut "Пример минимального описания таблицы" %}
```python
table_settings = {
    "path": "//<path>/<to>/<my>/<table>",
    "master": {
        "cluster": "markov",
        "attributes": {
            "schema": [
                {
                    "name": "my_key_column",
                    "type": "uint64",
                    "sort_order": "ascending",
                },
                {
                    "name": "my_value_column",
                    "type": "string",
                }
            ]
        }
    }
}
```
{% endcut %}

Если указана только секция `master`, то инсталляция является однокластерной, тип таблицы автоматически выберется `table`.

Если в свойствах таблицы присутствуют секции `sync_replicas`/`async_replicas` (описание см. ниже), то тип таблицы на
основном кластере будет `replicated_table`/`chaos_replicated_table` в зависимости от того, обычная у нас инсталляция или
хаосная.

Для всех таблиц автоматически проставляется атрибут `dynamic: True`.

С остальными свойствами таблиц, которые можно добавлять в секцию `attributes`, можно ознакомится в документации YT на
 [динамические таблицы](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/overview).


## Схема таблицы
[Схема таблицы](https://yt.yandex-team.ru/docs/user-guide/storage/static-schema) описывается следующим образом:
```python
{  # inside attributes
    "schema": [
        {
            "name": "<column_name>",  # mandatory
            "type": "<column_type>",  # mandatory
            "sort_order": "ascending|descending",  # key columns only
            "expression": "<expression>",
            "aggregate": "<aggregate_func>",
            "required": True|False,  # default is False
            "lock": "<lock_name>",
            "group": "<group_name>",
        },
        # other columns
    ]
}
```

Обязательными атрибутами для колонки являются:
- `name: str` - уникальное название колонки в таблице
- `type: str` - один из поддерживаемых YT [типов](https://yt.yandex-team.ru/docs/user-guide/storage/data-types)

Необязательные атрибуты колонки:
- `sort_order: str = ascending|descending` - порядок сортировки, задается для ключевой колонки
(см. [сортированные таблицы](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/sorted-dynamic-tables) и
[упорядоченные таблицы](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/ordered-dynamic-tables))
  - __примечание__: использовать порядок сортировки `descending`  не рекомендуется
- `expression: str` - выражение для вычислимой колонки, подробнее можно почитать в документации про
[шардирование](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/resharding)
- `aggregate: str` - выражение для агрегирующей колонки, подробности [здесь](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/sorted-dynamic-tables#aggr_columns)
- `required: bool = False` - является ли колонка обязательной (в неё нельзя записать `null`), сделать колонку
обязательной можно только при создании таблицы, при модификации существующей таблицы значение `required` можно поменять
только на `False`
- `lock: str` - лок-группа колонки: если колонки находятся в разных группах, то обновление двух колонок у одной строки в
параллельных транзакциях не приводит к ошибке `RowLockConflict`, только для неключевых колонок
- `group: str` - если две колонки имеют одинаковое имя группы, то данные этих колонок попадут в одни и те же блоки
(блок - минимальный набор данных, читаемых с диска), полезно указывать разные группы для колонок, которые редко читаются
вместе или какое-то подмножество колонок читается чаще другого - это сэкономит чтения с диска в YT


Схема всегда является [строгой](https://yt.yandex-team.ru/docs/user-guide/storage/static-schema#strict_schematization_advantages)
и для сортированных таблиц всегда выставлен атрибут схемы `unique_keys=true`.

## Инсталляции с репликами
Если нам нужна база с [реплицированными](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/replicated-dynamic-tables)
таблицами, то в настройки таблицы нужно добавить секции `sync_replicas`/`async_replicas`.

Формат у секций одинаковый, выглядит следующим образом:
- `sync_replicas|async_replicas: dict` - настройки синхронных и асинхронных реплик соответственно
  - `cluster: list[str]` - кластера, на которых располагаются синхронные/асинхронные реплики
  - `replica_path: str` - путь до таблицы с репликой на кластере
  - `attributes: dict[str, dict]` - свойства таблиц-реплик покластерно, ключ - название кластера из `clusters`, значение
  `dict` со свойствами таблиц, свойства таблиц-реплик описываются так же как для основной таблицы

Если в настройках таблицы появляются секции по реплики, то тип таблицы на основном кластере будет
`replicated_table`/`chaos_replicated_table` (в зависимости от типа базы).

В случае хаосной инсталляции для кластеров с репликами так же автоматически сгенерируются таблицы с логами репликации по
пути `{replica_path}_log` (т.е. их не надо указывать в настройках таблицы).

В настройках таблицы для реплицированных баз появляются дополнительные атрибуты:
 - `in_collocation: bool` - только для основной таблицы, указывает участвует таблица в коллокации или нет,
 рекомендуется включать для таблиц с включенным RTT
 - `enable_replicated_table_tracker: bool`:
   -  для основной таблицы указывает включен ли
 [RTT](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/replicated-dynamic-tables#repliki1) для реплицированной
 таблицы (так же подтягивается из `replicated_table_options/enable_replicated_table_tracker`)
   - для таблицы-реплики (из секций `sync_replicas`/`async_replicas`) указывает включен ли RTT для конкретной реплики

_Примечание_: свойство `replicated_table_options/preferred_sync_replica_clusters` в настройках основной таблицы
игнорируется, вычисляется автоматически из кластеров, перечисленных в `sync_replicas/clusters`.

__ВАЖНО__: для упорядоченных реплицированных таблиц, если необходимо, чтобы `$tablet_index` для
записываемых строк сохранялся в таблицах репликах, необходимо выставить у реплицированной таблицы
свойство `preserve_tablet_index=true`. По-умолчанию этот атрибут выставлен в `false`, и до таблиц-реплик
строки доезжают не в тех таблетах, которые были указаны при записи.

{% cut "Пример минимального описания таблицы с репликами" %}
```python
table_settings = {
    "path": "//<path>/<to>/<my>/<table>",
    "master": {
        "cluster": "markov",
        "attributes": {
            "in_collocation": True,  # table in collocation
            "enable_replicated_table_tracker": True,  # RTT enabled
            "schema": [
                {
                    "name": "my_key_column",
                    "type": "uint64",
                    "sort_order": "ascending",
                },
                {
                    "name": "my_value_column",
                    "type": "string",
                }
            ]
        }
    },
    "sync_replicas": {
        "cluster": ["seneca-sas"],
        "replica_path": "//<path>/<to>/<my>/<replica_table>",
        "attributes": {
            "seneca-sas": {
                "enable_replicated_table_tracker": True,  # RTT enabled
                "schema": [
                    {
                        "name": "my_key_column",
                        "type": "uint64",
                        "sort_order": "ascending",
                    },
                    {
                        "name": "my_value_column",
                        "type": "string",
                    }
                ]
            }
        }
    },
    "async_replicas": {
        "cluster": ["seneca-vla", "hahn"],
        "replica_path": "//<path>/<to>/<my>/<replica_table>",
        "attributes": {
            "seneca-vla": {
                "enable_replicated_table_tracker": True,  # RTT enabled
                "schema": [
                    {
                        "name": "my_key_column",
                        "type": "uint64",
                        "sort_order": "ascending",
                    },
                    {
                        "name": "my_value_column",
                        "type": "string",
                    }
                ]
            },
            "hahn": {
                "enable_replicated_table_tracker": False,  # RTT disabled!
                "schema": [
                    {
                        "name": "my_key_column",
                        "type": "uint64",
                        "sort_order": "ascending",
                    },
                    {
                        "name": "my_value_column",
                        "type": "string",
                    }
                ]
            }
        }
    }
}
```
{% endcut %}
