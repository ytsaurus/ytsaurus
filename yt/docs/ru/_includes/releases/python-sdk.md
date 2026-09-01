## Python SDK

Доступен как пакет в [PyPI](https://pypi.org/project/ytsaurus-client/).


**Релизы:**

{% cut "**0.13.52**" %}

**Дата релиза:** 2026-07-08


**Страница релиза:** [0.13.52](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.52)


**Пакет PyPI:** [0.13.52](https://pypi.org/project/ytsaurus-client/0.13.52/)


#### Новые возможности
- Вывод предупреждения при использовании короткого алиаса кластера без настроенного `proxy/default_suffix`; при сбое разрешения DNS — информативная ошибка `YtError` вместо непонятной ошибки соединения [5677a1bceebbf064b5e0dc234d411ac8f0a0c82e]
- Добавлены экспериментальные (интерфейс может измениться) команды CLI `yt devtools image` (`get-cluster-env`, `prepare`, `run`, `list`, `install`) для сборки и запуска Docker-образов, соответствующих ОС и окружению Python целевого кластера [7320a23fe95a982ac0bbb6442cca957a968066bc]

#### Исправления
- Редактирование заголовков аутентификации (включая `request_headers`) в ответах об ошибках HTTP и в журналах [98fdfad3f8561fc4cb9a932cc0e645189895891f, 6f9fde83692386df25e285223ff0d625c1a0e040]
- Исправлено автодополнение путей в CLI `yt` при настроенном `prefix` пути Кипариса [1ca058a74d91dc21991d6d125f629f6c5609f60f]
- Исправлено незаметное удаление атрибутов в `YPath.join()`/`ypath_join()` при объединении путей; добавлен параметр `with_attributes` для управления объединением [82b104d3f3e4e70aba4c995e221f56d31411af40]


{% endcut %}


{% cut "**0.13.51**" %}

**Дата релиза:** 2026-06-26


**Страница релиза:** [0.13.51](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.51)


**Пакет PyPI:** [0.13.51](https://pypi.org/project/ytsaurus-client/0.13.51/)


#### Новые возможности
- Добавлены команды CLI `yt-admin` `get-user-banned`, `set-user-banned` и `list-banned-users` для управления блокировками пользователей [4a19859b389eb1668fb4f14396aab7d96801cfb6]
- Добавлена команда `yt admin remove-master-unrecognized-options` для удаления нераспознанных опций из динамической конфигурации мастера после обновления [7beacb9aed028a9cf961a9e18cf43f28d95c453a]
- Добавлен метод `check_cluster_liveness` в `YtClient` (и на уровне модуля `yt.check_cluster_liveness`) с опциями `check_cypress_root`, `check_secondary_master_cells` и `check_tablet_cell_bundle` [9286c63af6bee36ccd40d17f3d856da4cd0cfa11]

#### Исправления
- Добавлен флаг `--timestamp` в команду CLI `lookup-rows` [973bea764d9c8038dc8727ea75c94f0bb43ea3da]
- Исправлена ошибка `TypeError` на Python 3 при использовании переменной окружения `YT_DRIVER_CONFIG` [9118cf7dc5a0f66cd042cc07f60ffd5e0108dad3]

#### Критические изменения
- Удалены классы типизированного API `TzDate`, `TzDatetime`, `TzTimestamp`, `TzDate32`, `TzDatetime64`, `TzTimestamp64` из `yt.wrapper.schema` [e12b3891d56424113226d891995706fb09da93f7]


{% endcut %}


{% cut "**0.13.50**" %}

**Дата релиза:** 2026-05-29


**Страница релиза:** [0.13.50](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.50)


**Пакет PyPI:** [0.13.50](https://pypi.org/project/ytsaurus-client/0.13.50/)


##### Новые возможности
- Увеличен `data_size_per_thread` по умолчанию для параллельного чтения файлов [99a0a95dee050b88a3415233d29214b0902ce3a9]
- Поддержка алиаса операции в YT CLI и Python API [12fc8945f15a20c645c87667fad287c606ea47ec]
- Добавлены команды `freeze-hydra-peer`, `truncate-changelog` и `schedule-restart` в `yt-admin` [ae0cb902cca1e0caef31e7ed96983c0e05fd3126]
- Добавлены экспериментальные команды `yt admin metrics` для выгрузки метрик Prometheus и локального воспроизведения через Docker [d6d286ce7d084eab62be0c545fc967aa2e190a64]
- Поддержка `omit_inaccessible_columns` и `omit_inaccessible_rows` в `read-table` [20d3bead559f0df0fa05d86a64b700e26c25fc14]

##### Исправления
- Использование тяжелого прокси для метода `get_table_columnar_statistics` [d8890491c7a333b0902f41e964fedcbe677a0781]


{% endcut %}


{% cut "**0.13.49**" %}

**Дата релиза:** 2026-04-30


**Страница релиза:** [0.13.49](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.49)


**Пакет PyPI:** [0.13.49](https://pypi.org/project/ytsaurus-client/0.13.49/)


##### Новые возможности
- Экспериментально! Добавлены команды `yt admin describe` и `yt admin logs k8s` для проверки кластера и получения журналов через Kubernetes API [ff53dc4ccd39e574364e1b7846fa3f47b7badab3, d2d8f8bf40eba0e96ffb6a615b642931ffad92d5]
- Добавлены команды `build_master_snapshots` и `master_exit_read_only` [fc953aa8229231760b219500d122ac87a9d16175, 986be02ca04da7b0b9c13827cbc94fd55cf4f3e0]
- Добавлен параметр `backoff_config` в `run_with_retries` для настройки политики повторных попыток с задержкой [56ed6b2223cc14f1fefb547d1ffd1ce68e494283]

##### Исправления
- Исправлена совместимость журнала с Python 3.14 [93d89672ca44353644e68bed805cc9c9613d3eae]
- Скрытие содержимого `secure_vault` из журналов запросов в RPC-драйверах [983f55e3f1d86a59f2be5f058c8e3ebacd48fa8c]
- CLI `yt execute` теперь вызывает понятную ошибку `YtError` при вызове команды, не поддерживаемой кластером, вместо сбоя с `KeyError` [d768e16d3069d0faf569eabe05aad042c9328b1f]


{% endcut %}


{% cut "**0.13.48**" %}

**Дата релиза:** 2026-03-27


**Страница релиза:** [0.13.48](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.48)


**Пакет PyPI:** [0.13.48](https://pypi.org/project/ytsaurus-client/0.13.48/)


##### Новые возможности
- Добавлен параметр `read_from` в `lookup_rows` и `select_rows` [2f0b0b0ae0aa23390785c7913866202ae03dbcf8]
- Добавлена опция `--no-enable-slicing` в команду CLI `reshard-table` [62c8c5ab4ce0c6efd8b4ac190279ca55d9c69b2e]
- Добавлены аннотации типов в команду `lock` [a3ac56216b9c9ddc860df1ff9942d532051e42b0]

##### Исправления
- Исправлено использование конфигурации резолвера адресов в серверном формате в нативном драйвере [2816f6fe94f7feffc04d3ff547333cc4ffc1b8e8]
- Исправлено `make_read_request` для вызова исходной ошибки вместо возможной ошибки прерывания транзакции [c223a8f738c04524828fb7268fd757beb4dd93cc]
- Исправлена логика повторных попыток, когда `retry_count` равно `None` [db1ab343327831b0ea499e6e5c7a47db77fa08df]

{% endcut %}


{% cut "**0.13.47**" %}

**Дата релиза:** 2026-02-16


**Страница релиза:** [0.13.47](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.47)


**Пакет PyPI:** [0.13.47](https://pypi.org/project/ytsaurus-client/0.13.47/)


#### Новые возможности
- Добавлена команда `run-job-shell-command` в CLI [394c049deb1460f767be591036f5d55b7d5d58db]
- Добавлена поддержка атрибута `lock` для `ColumnSchema` [87a9d8809a144c64d72fc767999c8c9d25616911]
- Добавлена поддержка распределенного чтения в режиме `read_parallel` [01912a6703b7fea296efc3eb5fbaebd69ea2d046]

#### Исправления
- Исправлена подготовка Docker-образа с помощью CLI [2788466412f56e941044e833dbfc201d1937807f]

{% endcut %}


{% cut "**0.13.46**" %}

**Дата релиза:** 2026-01-18


**Страница релиза:** [0.13.46](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.46)


**Пакет PyPI:** [0.13.46](https://pypi.org/project/ytsaurus-client/0.13.46/)


#### Исправления
- Исправлено `yt execute` для команд без входных данных
- Удалено отображение заголовков авторизации в журналах

{% endcut %}


{% cut "**0.13.45**" %}

**Дата релиза:** 2025-12-29


**Страница релиза:** [0.13.45](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.45)


**Пакет PyPI:** [0.13.45](https://pypi.org/project/ytsaurus-client/0.13.45/)


#### Новые возможности
* Передача compression_level в parquet writer
* Добавлены queue_tag и consumer_tag для метрик очереди и потребителя


{% endcut %}


{% cut "**0.13.44**" %}

**Дата релиза:** 2025-12-12


**Страница релиза:** [0.13.44](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.44)


**Пакет PyPI:** [0.13.44](https://pypi.org/project/ytsaurus-client/0.13.44/)


#### Новые возможности
* Добавлен `list-job-traces`
* Добавлен `check-operation-permission`

#### Исправления
* Параметр `trace_id` для `get-job-trace` стал необязательным
* Исправлены ошибки `transform` при указании `data size_per_job` или `data_size` в пользовательском спеке


{% endcut %}


{% cut "**0.13.43**" %}

**Дата релиза:** 2025-11-22


**Страница релиза:** [0.13.43](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.43)


**Пакет PyPI:** [0.13.43](https://pypi.org/project/ytsaurus-client/0.13.43/)


#### Новые возможности
  * Добавлена опция annotations в команду `start_query` в CLI.

#### Исправления
  * Исправлены повторные попытки `push_queue_producer`.
  * Исправлено определение слоя на неизвестной ОС.

{% endcut %}

{% cut "**0.13.42**" %}

**Дата релиза:** 2025-11-14


**Страница релиза:** [0.13.42](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.42)


**PyPI-пакет:** [0.13.42](https://pypi.org/project/ytsaurus-client/0.13.42/)


#### Новые возможности
* Включены ретраи для RPC-вызовов
* Добавлено больше type-hints
* Добавлена команда `get-job-trace` в CLI
* Добавлена опция `--stderr-type` для `get-job-stderr`
* Добавлены предупреждения об использовании `multithreading`

#### Исправления
* Команда `transform` сохраняет атрибуты (`compression_codec`, `erasure_codec`, `optimize_for`) целевой таблицы, если они не переопределены явно
* Исправлен параметр `--config` для `yt-fuse`

{% endcut %}


{% cut "**0.13.41**" %}

**Дата релиза:** 2025-10-24


**Страница релиза:** [0.13.41](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.41)


**PyPI-пакет:** [0.13.41](https://pypi.org/project/ytsaurus-client/0.13.41/)


#### Новые возможности
  * Добавлена опция `--with-env-patch` для CLI-команды `show-default-config`, позволяющая выгрузить конфигурацию по умолчанию с применёнными переменными окружения

#### Исправления
  * Исправлен разбор `YPath` при указании кластера и диапазонов
  * Исправлен `spec_builder` при передаче `client=None`

{% endcut %}


{% cut "**0.13.40**" %}

**Дата релиза:** 2025-10-13


**Страница релиза:** [0.13.40](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.40)


**PyPI-пакет:** [0.13.40](https://pypi.org/project/ytsaurus-client/0.13.40/)


#### Новые возможности
  * YT-26355: Вывод типа Null из схемы Arrow
  * YT-26389: Поддержка `omit_inaccessible_rows`
  * Добавлена функция `log_once`

{% endcut %}


{% cut "**0.13.39**" %}

**Дата релиза:** 2025-10-10


**Страница релиза:** [0.13.39](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.39)


**PyPI-пакет:** [0.13.39](https://pypi.org/project/ytsaurus-client/0.13.39/)


#### Новые возможности
* Обработка YQL-запросов в отдельных процессах (Commit: 3c09bed1d8d4ed07c1b4fe9393c39bb420c7dbc0)
* Добавлена опция `clip_timestamp` (Commit: 4e6889f0cd0615cee3d5d5ae0d85602233e2412f)
* Добавлено количество задач при параллельном чтении (Commit: 506e97dd397e29eeaa4e7b88f48467aa4419c48c)


#### Исправления
* Исправлена передача сообщений об abort (Commit: b2b49815a043b78e8a3160f05400864a0fef678c)
* Исправлена обработка некоторых переменных окружения с типами (`YT_CHUNK_SIZE`) (Commit: d2109522d473a3126eb5f9258089d41689549621)
* Исправлен dirtable reader (Commit: 9ea085c12bdff6c8a67b5ad1ea6236db4da32771)
* Добавлено предупреждение о ретраях (Commit: 507d120389cb8963d25efe21102e2c35428a9d2f)


{% endcut %}


{% cut "**0.13.36**" %}

**Дата релиза:** 2025-08-29


**Страница релиза:** [0.13.36](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.36)


**PyPI-пакет:** [0.13.36](https://pypi.org/project/ytsaurus-client/0.13.36/)


#### Новые возможности

  * Поддержка формата таблиц [blob](https://ytsaurus.tech/docs/en/user-guide/storage/formats#BLOB)

#### Исправления

  * Исправлена логика выбора тяжелого прокси для тяжелых запросов
  * Исправлен `get_table_schema` для реплицированных таблиц
  * Исправлен `yt execute` для команд с входными данными

{% endcut %}


{% cut "**0.13.35**" %}

**Дата релиза:** 2025-08-12


**Страница релиза:** [0.13.35](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.35)


**PyPI-пакет:** [0.13.35](https://pypi.org/project/ytsaurus-client/0.13.35/)


#### Новые возможности
  * Добавлена опция кодека сжатия для parquet.
  * Добавлены методы для API распределённой записи.
  * Добавлена типизация для конфигурации и spec builder'ов.

#### Исправления
  * Добавлены детали в сообщение об ошибке импорта при пиклинг-шифровании.
  * Исправлена формулировка описания `write_table`.
  * Исправлено преобразование YSON для bytes-объектов.
  * Удалены устаревшие термины *_ratio и *_share из YT CLI.

{% endcut %}


{% cut "**0.13.34**" %}

**Дата релиза:** 2025-07-27


**Страница релиза:** [0.13.34](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.34)


**PyPI-пакет:** [0.13.34](https://pypi.org/project/ytsaurus-client/0.13.34/)


#### Новые возможности
  * Добавлена поддержка `YT_LOG_PATH` для RPC-запросов
  * Добавлен аргумент `--attribute` для CLI-команды `list_operations`
  * Переработана конфигурация локального RPC-подключения

{% endcut %}


{% cut "**0.13.33**" %}

**Дата релиза:** 2025-07-14


**Страница релиза:** [0.13.33](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.33)


**PyPI-пакет:** [0.13.33](https://pypi.org/project/ytsaurus-client/0.13.33/)


#### Новые возможности
  * Поддержка tz-типов в python
  * Добавлены type hints для конфигурации YtClient
  * Поддержка пользовательского класса аутентификации в конфигурации Python SDK

#### Исправления
  * Исправлено скрытие токенов в случае исключения YtProxyUnavailable

{% endcut %}


{% cut "**0.13.31**" %}

**Дата релиза:** 2025-06-20


**Страница релиза:** [0.13.31](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.31)


**PyPI-пакет:** [0.13.31](https://pypi.org/project/ytsaurus-client/0.13.31/)


#### Новые возможности
 * Незначительные улучшения
 * Обновлены py-зависимости 2f5dc26abd27401d7c775b4e7406b4c85c1c4105

{% endcut %}


{% cut "**0.13.30**" %}

**Дата релиза:** 2025-06-16


**Страница релиза:** [0.13.30](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.30)


**PyPI-пакет:** [0.13.30](https://pypi.org/project/ytsaurus-client/0.13.30/)


#### Новые возможности
  * Добавлена команда `list_operation_events`


{% endcut %}


{% cut "**0.13.29**" %}

**Дата релиза:** 2025-06-02


**Страница релиза:** [0.13.29](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.29)


**PyPI-пакет:** [0.13.29](https://pypi.org/project/ytsaurus-client/0.13.29/)


#### Новые возможности
  * Добавлен параметр `annotate_with_types` в функцию `yson_to_json`
  * Улучшено сообщение-предупреждение о забаненном прокси

#### Исправления
  * Удалена ошибка `YtSequoiaRetriableError`
  * Исправлена обработка ошибок в `write_table` с включённым framing


{% endcut %}

{% cut "**0.13.28**" %}

**Дата релиза:** 2025-04-30


**Страница релиза:** [0.13.28](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.28)


**Пакет PyPI:** [0.13.28](https://pypi.org/project/ytsaurus-client/0.13.28/)


#### Новые возможности

- Включен `redirect_stdout_to_stderr` по умолчанию
- Добавлена проверка надежности пароля в запросе `set_user_password`

{% endcut %}


{% cut "**0.13.27**" %}

**Дата релиза:** 2025-04-18


**Страница релиза:** [0.13.27](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.27)


**Пакет PyPI:** [0.13.27](https://pypi.org/project/ytsaurus-client/0.13.27/)


#### Новые возможности
* Активным пользователям API динамических таблиц рекомендуется использовать RPC-прокси
* Добавлена поддержка обработчика /api/v4/discover_proxies вместо /hosts

#### Исправления
* Исправлена ошибка получения настройки `impersonation_user` из конфигурации

{% endcut %}


{% cut "**0.13.26**" %}

**Дата релиза:** 2025-03-25


**Страница релиза:** [0.13.26](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.26)


**Пакет PyPI:** [0.13.26](https://pypi.org/project/ytsaurus-client/0.13.26/)


#### Новые возможности
* Добавлена поддержка свойств `expression` и `aggregate` в TableSchema.
* Добавлена поддержка имперсонализации.
* Исправлено удаление docker-хоста в spec builder.
* Добавлено журналирование некорректных запросов.
* Обновлены зависимости ytsaurus-client.

{% endcut %}


{% cut "**0.13.25**" %}

**Дата релиза:** 2025-03-12


**Страница релиза:** [0.13.25](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.25)


**Пакет PyPI:** [0.13.25](https://pypi.org/project/ytsaurus-client/0.13.25/)


#### Новые возможности
* Добавлена команда `yt whoami`

#### Исправления
* Исправлен формат вывода issue-token


{% endcut %}


{% cut "**0.13.24**" %}

**Дата релиза:** 2025-03-02


**Страница релиза:** [0.13.24](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.24)


**Пакет PyPI:** [0.13.24](https://pypi.org/project/ytsaurus-client/0.13.24/)


#### Новые возможности
* Замена опции `include_scheduler` на `include_runtime` в команде `get_operation` (обратно несовместимое изменение)
* Запрос атрибутов `type` вместо атрибута `operation_type` в команде `get_operation`
* Добавлена поддержка `redirect_stdout_to_stderr`
* Добавлена поддержка `require_sync_replica` в `push_queue_producer`
* Добавлен метод `is_prerequisite_check_fail` для ошибок, добавлен `YtAuthenticationError`
* Добавлена поддержка причины приостановки операции

#### Исправления
* Удален код, связанный с python2, в `_py_runner.py`
* Добавлено `python_requires=">=3.8"` в настройки пакета
* Исправлен запрос всех атрибутов при проверке существования операции
* Исправлена обработка таймаута запроса команды `start_operation` 

{% endcut %}


{% cut "**0.13.23**" %}

**Дата релиза:** 2025-02-04


**Страница релиза:** [0.13.23](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.23)


**Пакет PyPI:** [0.13.23](https://pypi.org/project/ytsaurus-client/0.13.23/)


#### Новые возможности
  * Добавлена опция `min_batch_row_count` для dump parquet
  * Добавлен метод `patch_operation_spec`
  * Добавлены методы queue producer в YT cli
  * Добавлен параметр `trimmed_row_counts`
  * Добавлен параметр `versioned_read_options`
  * Добавлен параметр `ignore_type_mismatch`
  * Командная строка не записывается в started_by (по умолчанию)
  * Отображение версии нативных библиотек в CLI
  * Сделана возможность удаленного патчинга pickling->dynamic_libraries->enable_auto_collection
  * Применение атрибутов пути назначения к временным объектам при параллельной загрузке

#### Исправления
  * YSON: корректное экранирование некорректных последовательностей, как в реализации bingings.
  * Исправлен `generate_traceparent`
  * Удалены импорты модуля `typing_extensions` для новых версий python
  * Исправлена конфигурация нативного драйвера

{% endcut %}


{% cut "**0.13.22**" %}

**Дата релиза:** 2025-01-10


**Страница релиза:** [0.13.22](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.22)


**Пакет PyPI:** [0.13.22](https://pypi.org/project/ytsaurus-client/0.13.22/)


#### Исправления:
* Исправлены проверки импорта для функций, связанных с `orc`

{% endcut %}


{% cut "**0.13.21**" %}

**Дата релиза:** 2024-12-26


**Страница релиза:** [0.13.21](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.21)


**Пакет PyPI:** [0.13.21](https://pypi.org/project/ytsaurus-client/0.13.21/)


Новые возможности:
* Добавлена поддержка формата YAML
* Добавлены примитивы более высокого уровня для отслеживания запросов
* Добавлен сеттер опции `network_project` для `UserJobSpecBuilder`
* Добавлен параллельный режим для формата ORC
* Добавлена поддержка `omit_inaccessible_columns` для команд чтения
* Добавлена поддержка опции `preserve_acl` в командах copy/move
* Переработаны команды аутентификации в CLI на основе getpass
* Улучшена загрузка Dirtable
* Добавлены команды queue producer
* Улучшен SpecBuilder: добавлены use_columnar_statistics, ordered, data_size_per_reduce_job

Исправления:
* Исправлены повторы для команд загрузки parquet/orc

Косметические изменения:
* Удалена устаревшая константа из operation_commands.py
* Приведены в порядок импорты: удалена поддержка Python 2
* Вынесены ссылки на документацию в константы для команды `--help`

Большое спасибо @zlobober за значительный вклад!

{% endcut %}


{% cut "**0.13.19**" %}

**Дата релиза:** 2024-10-15


**Страница релиза:** [0.13.19](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.19)


**Пакет PyPI:** [0.13.19](https://pypi.org/project/ytsaurus-client/0.13.19/)


Новые возможности:
* Добавлена возможность загрузки и выгрузки таблиц в формате ORC с помощью команд CLI: `upload-orc` и `dump-orc`
* Поддержка параллельного режима для команды `dump-parquet`
* Поддержка nullable полей при разборе схемы YT из схемы parquet
* Поддержка параллельного режима для команды `read_table_structured`
* Добавлены параметры CLI в декоратор docker respawn (PR: #849). Спасибо @thenno за PR!

Исправления:
* Исправлены повторы при ошибке `LineTooLong`
* Исправлено `read_query_result`, который всегда возвращал необработанные результаты (PR: #800). Спасибо @zlobober за PR!
* Исправлены циклические ссылки, вызывавшие утечки памяти
* Уменьшено значение по умолчанию для `write_parallel/concatenate_size` со 100 до 20
* Исправлены повторы в команде `upload-parquet`

{% endcut %}


{% cut "**0.13.18**" %}

**Дата релиза:** 2024-07-26


**Страница релиза:** [0.13.18](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.18)


**Пакет PyPI:** [0.13.18](https://pypi.org/project/ytsaurus-client/0.13.18/)


Новые возможности:
* Использование expanduser для `config["token_path"]`
* Поддержка пользовательских параметров dill
* Поддержка Nullable patchable элемента конфигурации
* Добавлен max_replication_factor в конфигурацию
* Использование адреса strawberry ctl из client_config в Кипарисе

Исправления:
* Исправления E721: не сравнивайте типы, для точных проверок используйте `is` / `is not`, для проверок экземпляров используйте `isinstance()`
* Исправлена ошибка в обертке YT python: остановка пингера транзакции перед выходом из транзакции

Спасибо многочисленным внешним участникам за активное участие в разработке Python SDK.

{% endcut %}

{% cut "**0.13.17**" %}

**Дата релиза:** 2024-06-26


**Страница релиза:** [0.13.17](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.17)


**Пакет PyPI:** [0.13.17](https://pypi.org/project/ytsaurus-client/0.13.17/)


Новые возможности: 
  - Поддержка профилей в конфигурационном файле
  - Добавлена версионированная команда select
  - Добавлена поддержка enum.StrEnum и enum.IntEnum для yt_dataclasses

Исправления:
  - Исправлен тест test_operation_stderr_output в окружении py.test

Благодарим @thenno за значительный вклад!



{% endcut %}


{% cut "**0.13.16**" %}

**Дата релиза:** 2024-06-19


**Страница релиза:** [0.13.16](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.16)


**Пакет PyPI:** [0.13.16](https://pypi.org/project/ytsaurus-client/0.13.16/)


Новые возможности:
- Добавлена возможность указывать идентификаторы транзакций, необходимых для выполнения, в контекстном менеджере client.Transaction (PR: #638). Благодарим @chegoryu за PR!
- Добавлены параметры client и chunk_count в dirtable_commands
- Добавлена команда alter_query для Query Tracker
- Добавлена команда dump_job_proxy_log (PR: #594). Благодарим @tagirhamitov за PR!

Исправления:
- Исправлен возвращаемый результат команды lock при использовании пакетного клиента
- Исправлена работа jupyter notebooks для операций в отдельных ячейках (PR: #654). Благодарим @dmi-feo за PR!

{% endcut %}


{% cut "**0.13.14**" %}

**Дата релиза:** 2024-03-09


**Страница релиза:** [0.13.14](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.14)


**Пакет PyPI:** [0.13.14](https://pypi.org/project/ytsaurus-client/0.13.14/)


Новые возможности:
- Добавлена опция пропуска слияния строк в select
- Поддержка составных типов в QL
- Добавлена опция `preserve_account` в команды резервного копирования таблиц
- Расширен список повторяемых ошибок динамических таблиц
- Улучшено создание таблиц с указанным атрибутом append
- Различные улучшения API обслуживания
- Поддержка команды `upload_parquet`

Исправления:
- Поддержка сериализации SortColumn
- Исправлена утечка файловых дескрипторов при разборе конфигурации
- Исправлена проверка выходного потока для TypedJobs


{% endcut %}


{% cut "**0.13.12**" %}

**Дата релиза:** 2023-12-14


**Страница релиза:** [0.13.12](https://github.com/ytsaurus/ytsaurus/releases/tag/python/ytsaurus-client/0.13.12)


**Пакет PyPI:** [0.13.12](https://pypi.org/project/ytsaurus-client/0.13.12/)


Новые возможности:
* Поддержка типов `double` и `float` в `@yt_dataclass`.
* Добавлена команда `get_query_result`.

Исправления:
* Исправлена настройка конфигурации из переменных окружения.
* Понятное сообщение об ошибке, если тип узла не является таблицей в спецификации операции.

{% endcut %}
