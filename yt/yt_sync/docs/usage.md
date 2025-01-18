# Подключение и использование
Есть три способа подключить YtSync в свой проект:
- вызов главной функции [run_yt_sync_easy_mode()](stages_specification.md#run_yt_sync_easy_mode), которая обеспечивает типовое использование YtSync: парсит аргументы командной строки, по ним понимает задание, выполняет его. Принимает описание стейджей и их объектов в высокоуровневом формате [StagesSpec](stages_specification.md).
- вызов главной функции [run_yt_sync()](#run_yt_sync) - более низкоуровневый вариант `run_yt_sync_easy_mode`, принимает описание стейджей в виде словарей низкоуровневых описаний объектов.
- [использование библиотеки YtSync](#yt_sync_lib), которая позволяет более тонко настраивать поведение


## Функция run_yt_sync {#run_yt_sync}
Предоставляет типовой сценарий использования YtSync.
Достаточно позвать функцию `run_yt_sync()` в `main` своего ПО.

Пример есть в [example](https://a.yandex-team.ru/arcadia/yt/yt_sync/example/hard_mode).

### `ya.make`
```
PY3_PROGRAM()

PY_SRCS(__main__.py)

PEERDIR(
    yt/yt_sync/runner
)

END()
```

### `__main.py__`
```python
from yt.yt_sync.runner import Description
from yt.yt_sync.runner import run_yt_sync


def _get_description() -> Description:
    # stuff here

if __name__ == "__main__":
    run_yt_sync("my project", _get_description())
```

### Функция `run_yt_sync`
#### Входные параметры
- __`name: str`__ - название вашего проекта, используется для отображения в help
- __`description: Description`__ - описание стейджей и их таблиц/консьюмеров/нод (см. ниже)
- `settings: Settings` - настройки YtSync (см. [здесь](settings.md)), если не указать, то используются настройки,
  которые возвращает функция `default_settings()` из модуля `yt.yt_sync.runner`. Если необходимо передать настройки,
  отличающиеся от настроек по-умолчанию, то рекомендуется получить экземпляр настроек через вышеупомянутую функцию
  `default_settings()` и поправить в нем необходимые параметры.
- `yt_client_factory_producer: Callable[[bool], YtClientFactory]` - функция, возвращающая экземпляр `YtClientFactory`,
  на вход принимает параметр (`dry_run: bool`) запускается ли YtSync в режиме dry run. Если параметр не указать,
  то используется `YtClientFactory` с настройками по-умолчанию.
- `log_setup: LogSetup` - настройки логирования. Нужно передать наследника класса `LogSetup` из модуля
  `yt.yt_sync.runner.log`, если не устраивают настройки логирования по-умолчанию.
- `exit_on_finish: bool` - по-умолчанию `True`, если выставлен, то в конце работы функция `run_yt_sync` зовет
  `sys.exit()` с нужным кодом возврата. Если передать значение `False`, то функция `run_yt_sync` вернет код возврата,
  который был бы передан в `sys.exit()`

#### Описание для YtSync
Задается в классе `Description` в пакете `yt.yt_sync.runner`.
Состоит из stage-ей (группировка таблиц по какому-то признаку, например разделение на stable и testing).
Для каждого stage задается именованный набор таблиц/консьюмеров/узлов.

Имена используются для того, чтобы можно было запускать YtSync не для всех таблиц stage, а только заданных.

Для сценариев, требующих блокировки (например, `ensure`), путь для блокировки выбирается либо как LCA для
всех таблиц/консьюмеров/узлов, либо задается в свойстве `locks` у описания stage.

#### Что делает run_yt_sync
- конфигурирует парсер аргументов на базе переданного описания
- конфигурирует логирование
- на базе переданных пользователем параметров формирует набор таблиц/консьюмеров/узлов, которые надо обработать и
  разбивает их на группы по main cluster (один экземпляр YtSync может работать только с набором сущностей, у которых
  совпадает main cluster)
- для каждой группы создает экземпляр YtSync, заполняет его сущностями, и запускает выбранный пользователем сценарий
- когда все группы обработаны - либо вызывает `sys.exit()` c кодом возврата, либо просто возвращает код возврата (в
  зависимости от значение параметра `exit_on_finish`)

#### Коды возврата
Для сценария `dump_diff`, если есть реальный дифф, код возврата будет `2`, для всех остальных сценариев, если все
прошло успешно, код возврата будет `0`.

## Библиотека YtSync {#yt_sync_lib}
Пользователь сам обеспечивает все настройки YtSync.


### `ya.make`
```
PY3_PROGRAM()

PY_SRCS(__main__.py)

PEERDIR(
    yt/yt_sync/sync
)

END()
```

### `__main.py__`
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
