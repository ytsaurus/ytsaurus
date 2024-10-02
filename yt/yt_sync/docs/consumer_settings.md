# Настройки консюмеров (deprecated)
Консюмер - сущность, отслеживающая состояние [очереди](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/queues).

Очередь -
[упорядоченная динамическая таблица](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/ordered-dynamic-tables),
может быть создана как и остальные таблицы через `YtSync`.

Настройки консюмеров - это `dict` определённого содержания, который должен быть передан в метод `add_desired_consumer()`.

__Примечание__: работа с консюмерами происходит только если настройка `ensure_native_queue_consumers` выставлена в
`True`.

## Настройка консюмера
Настроки консюмеров состоят из следующих атрибутов:
- `table_settings: dict` - [настройки таблиц](table_settings.md) консюмера, с некоторыми особенностями (см. ниже)
- `queues: list[dict]` - очереди, которые отслеживает данный консюмер

### Настройки таблиц консюмера
У таблиц консюмеров [фиксированная схема](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/queues#data_model):
```python
schema = [
    {"name": "queue_cluster", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "queue_path", "type": "string", "sort_order": "ascending", "required": True},
    {"name": "partition_index", "type": "uint64", "sort_order": "ascending", "required": True},
    {"name": "offset", "type": "uint64", "required": True},
]
```

Так же таблице консюмера необходимо выставить атрибут `treat_as_queue_consumer = True` в разделе `attributes` настроек
таблицы

### Описание отслеживаемых консюмером очередей
Очередь описывается следующими атрибутами:
- `cluster: str` - кластер где располагается основная таблица очереди
- `path: str` - путь до основной таблицы очереди
- `vital: bool` - включение
[автоматического тримминга очередей](https://yt.yandex-team.ru/docs/user-guide/dynamic-tables/queues#automatic_trimming)
