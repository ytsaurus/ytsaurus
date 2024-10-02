# Быстрый старт
YtSync управляет совокупностью динамических таблиц YT на одном или несколькних кластерах YT.

Совокупность таблиц на всех кластерах называется база данных.

## Как работать с YtSync
- создаем экземпляр `YtSync`, указываем какой именно сценарий изменения таблиц нам нужен (про сценарии см. ниже)
- добавляем в него желаемые настройки таблиц/консюмеров через методы `add_desired_table()`/`add_desired_consumer()`
- из переданных настроек строится желаемая модель базы данных
- запускаем метод `sync()`
- `YtSync` по желаемой модели базы опрашивает реальные таблицы на кластерах (существует ли, схему, атрибуты, и т.п.) и
строит реальную модель базы данных
- `YtSync` запускает выбранный сценарий, в который передается желаемая и актуальная модели базы
- сценарий по двум состояниям базы производит какие-то воздействия на реальные таблицы в YT


## Пример кода
Библиотека YtSync написана на языке программирования Python, поэтому код её использующий тоже должен быть
написан на Python.

### `ya.make`:
```
PY3_PROGRAM()

PY_SRCS(__main__.py)

PEERDIR(
    yt/yt_sync
)

END()

```

### `__main.py__`:
```python
from yt.yt_sync import get_yt_client_factory
from yt.yt_sync import Settings
from yt.yt_sync import YtSync
from yt.yt_sync.lock import YtSyncLock


def main():
    # See docs for settings for all available settings.
    settings = Settings(
        db_type=Settings.REPLICATED_DB,  # Settings.CHAOS_DB for chaos
    )

    # Creates factory for YT client wrapper, supporting dry run mode.
    # YT client credentials must be passed explicitly via param token
    # or YT_TOKEN environment variable should be set.
    # Invoke with dry_run=False for real changes.
    yt_client_factory = get_yt_client_factory(dry_run=True)

    # Lock guard to prevent run several YtSync processes simultaneously.
    # my_yt_folder is YT folder on main cluster where all tables are located.
    # You can specify several folders as positional args, they will all be locked.
    db_lock = YtSyncLock(yt_client_factory("some-yt-lock-cluster"), my_yt_folder)

    # Create instance of YtSync.
    # my_scenario is something like "dump_diff" or "ensure", see docs for scenarios.
    yt_sync = YtSync(yt_client_factory, settings, db_lock, scenario_name=my_scenario)

    # Add desired tables settings if needed.
    # table_settings is dict, see docs on table settings content.
    for table_settings in my_tables:
        yt_sync.add_desired_table(table_settings)

    # Add desired consumer settings if needed.
    # consumer_settings is dict, see docs on table settings content.
    for consumer_settings in my_consumers:
        yt_sync.add_desired_consumer(consumer_settings)

    # All real job is here.
    # Params are optional, passed to scenario, see scenarios description for details.
    yt_sync.sync(param1=value1, ..., paramN=valueN)


if __name__ == "__main__":
    main()
```

## Доступные сценарии
У сценария есть уникальный строковый идентификатор, которое передается при создании экземпляра `YtSync`.

Сценарии живут в папке [`yt/yt_sync/scenarios`](https://a.yandex-team.ru/arcadia/yt/yt_sync/scenarios).

_Примечание_: если `yt_client_factory` создан с параметром `dry_run=True`, то модифицирующие операции в любом сценарии
будут только залогированы, но не будут применены, бывает полезно для проверки что ничего криминального не произойдёт.

Более подробное описание сценариев смотри [здесь](scenarios.md).

На данный момент доступны:
- `dump_diff` - пишет в лог отличия между желаемым и реальным состоянием базы данных
- `dump_spec` - просто печатает desired-спеки таблиц
- `clean` - (пере)создает все таблицы базы (если какие-то таблицы существовали, то они будут удалены)
- `ensure_attributes` - синхронизирует только атрибуты таблиц
- `ensure` - синхронизирует атрибуты таблиц и изменения схемы, которые можно применить через команду `alter_table`
(свойства колонок, добавление неключевых колонок)
- `ensure_heavy` - синхронизирует атрибуты таблиц и изменения схемы, при этом для тяжелых изменений схемы вроде
удаления колонки или модификации ключевых колонок таблицы запускаются `map`/`sort` операции, они занимают сильно больше
времени чем простой `alter_table`
- `switch_replica` - переключает синхронные реплики таблиц
- `reshard` - решардирует таблицы в соответствии с желаемыми настройками `tablet_count`/`pivot_keys`
- `sync_replicas` - добавить новую реплику с данными для баз с реплицированными таблицами
- `force_compaction` - форсированная компактификация таблиц
- `migrate_to_replicated` - миграция с однокластерной инсталляции в реплицированную
- `migrate_to_chaos` - миграция с реплицированной инсталляции в хаосную
