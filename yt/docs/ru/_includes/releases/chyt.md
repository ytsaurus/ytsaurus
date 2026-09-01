## CHYT


Поставляется в виде docker-образа.



**Релизы:**

{% cut "**2.18.6**" %}

**Дата релиза:** 2026-06-22


**Страница релиза:** [2.18.6](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.18.6)


**Docker-образ:** [ghcr.io/ytsaurus/chyt:2.18.6](https://github.com/orgs/ytsaurus/packages/container/chyt/967753290?tag=2.18.6)


**Исправления:**

* Исправлено использование функций `enable_distinct_read_optimization` и `enable_min_max_optimization` со словарями поверх таблиц YTsaurus (ea82d76ea5beedc7405a30275ae37e8426953f85)
* Исправлен вывод диапазона чтения для сложных фильтров (cdb1a6c84ee02e9ee1e65cc878c3f5ba288d592b, 03fdd7e3a2785702604419df0d3ef5e9ac305d57)


**Примечания:**

Если вы используете флаг `enable_read_range_inferring` начиная с версии 2.18, настоятельно рекомендуем обновиться до этой версии, чтобы избежать возможной потери данных из-за указанных ошибок.

{% endcut %}


{% cut "**2.18.5**" %}

**Дата релиза:** 2026-06-09


**Страница релиза:** [2.18.5](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.18.5)


**Docker-образ:** [ghcr.io/ytsaurus/chyt:2.18.5](https://github.com/orgs/ytsaurus/packages/container/chyt/939459874?tag=2.18.5)


**Новые возможности:**

* Поддержка omit_inaccessible_rows (90b6b7ae985f849f63b876ae3d60b79217e65a03)
* Отдельный пул потоков для обработки pull-задач (fa4c50776aed759301acf4ebc20fa6934d7ffe3f)

**Исправления:**

* Исправлена ошибка в ytTables(<empty_arr>) (7787c0803dd9d654722f199220761f8aa857302a)
* Использование часового пояса UTC для аргументов ytListLogTables (ab598fffbf0ce97f3a5c36e349edcfa09b9edc1a)



{% endcut %}


{% cut "**2.18.4**" %}

**Дата релиза:** 2026-05-15


**Страница релиза:** [2.18.4](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.18.4)


**Docker-образ:** [ghcr.io/ytsaurus/chyt:2.18.4](https://github.com/orgs/ytsaurus/packages/container/chyt/873819001?tag=2.18.4)


**Исправления:**

* Добавлены недостающие функции extractKeyValuePairs в целевой компонент CHYT (a5206fe1757f4a57fdafa48c15e314a3245ad4f7)
* Использование FetcherInvoker для TableAttributeCache (bf1f96f4707ae86b977649d3ef7f143f67ce280f)


{% endcut %}


{% cut "**2.18.3**" %}

**Дата релиза:** 2026-04-02


**Страница релиза:** [2.18.3](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.18.3)


**Docker-образ:** [ghcr.io/ytsaurus/chyt:stable-2.18.3](https://github.com/orgs/ytsaurus/packages/container/chyt/785932269?tag=stable-2.18.3)


**Исправления:**

* Исправлено преобразование константных множеств при выводе диапазона чтения (7bdb3cf)
* Исправлена обработка массивов в ключевом условии (8a34c91)


{% endcut %}


{% cut "**2.18.2**" %}

**Дата релиза:** 2026-03-23


**Страница релиза:** [2.18.2](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.18.2)


**Docker-образ:** [ghcr.io/ytsaurus/chyt:stable-2.18.2](https://github.com/orgs/ytsaurus/packages/container/chyt/757077349?tag=stable-2.18.2)


#### Исправления:
* Исправлена оптимизация чтения distinct, из-за которой блок читался не полностью (056b99bd53dc92c2fdb2d59b8c0f30dafca9c80c)

{% endcut %}


{% cut "**2.18.1**" %}

**Дата релиза:** 2026-03-04


**Страница релиза:** [2.18.1](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.18.1)


**Docker-образ:** [ghcr.io/ytsaurus/chyt:stable-2.18.1](https://github.com/orgs/ytsaurus/packages/container/chyt/721252481?tag=stable-2.18.1)


**Исправления:**

* Исправлен учет пустой статистики в TColumnarStatisticsFetcher после неудачной выборки (75c3baf)


{% endcut %}


{% cut "**2.18.0**" %}

**Дата релиза:** 2026-01-20


**Страница релиза:** [2.18.0](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.18.0)


#### Новые возможности:
- Поддержка RLS в CHYT, [3fe297c](https://github.com/ytsaurus/ytsaurus/commit/3fe297cd8ffc38e019c0121126ceaf5f636166ef).
- Добавлен вывод диапазона чтения из предиката, [3a9eb82](https://github.com/ytsaurus/ytsaurus/commit/3a9eb82c7ec5495632f13dc3e8884a158312de4d).
- Добавлена статистика chyt\_query\_statistics для insert-запросов и завершившихся с ошибкой запросов, [974b2c2](https://github.com/ytsaurus/ytsaurus/commit/974b2c28d5dcf44316516e285596b9c13090ee71), [99a08a3](https://github.com/ytsaurus/ytsaurus/commit/99a08a3449806035c48dd74ee587486569b7a6e1).
- Добавлена выходная таблица в переменные времени выполнения, [05cd02a](https://github.com/ytsaurus/ytsaurus/commit/05cd02ade583eaacd9ecf13cd8e27689e4bceefb).
- Добавлена возможность распределять входные спецификации для вторичных запросов в pull-режиме, [c4ba9c4](https://github.com/ytsaurus/ytsaurus/commit/c4ba9c46ffd8522946ef34fe505b61805a21f504).
- Добавлена оптимизация для повышения производительности запросов с min/max на основе колоночной статистики с узлов, [d83cd46](https://github.com/ytsaurus/ytsaurus/commit/d83cd46c0b5830fbbcdd23c00106b387b439542b).
- Добавлена оптимизация для повышения производительности при rle- и словарном кодировании (с использованием только уникальных значений без материализации), [c777591](https://github.com/ytsaurus/ytsaurus/commit/c77759151d82bbfc8720cc269a3c295555974bdf).
- Добавлена проверка ревизии в AttributeCache для более стабильной производительности, [3f3be85](https://github.com/ytsaurus/ytsaurus/commit/3f3be8596e05dd30054e7ddc7bfb15421ffd9afc).
- Добавлен EnableComlexOptionalConversion для предотвращения преобразования массивов (не могут быть nullable в ClickHouse; в будущем значение по умолчанию будет изменено на false), [57a298a](https://github.com/ytsaurus/ytsaurus/commit/57a298ac4c98659fe014d43cea872925d9750424).
- Добавлена поддержка параллельного insert-select в storage\_distributor, [4632de8](https://github.com/ytsaurus/ytsaurus/commit/4632de8546d04ef8f3dedb9b9a26007a5384288b).

#### Исправления:
- Исправлено использование мастер-фетчера спецификаций чанков для упорядоченных динамических таблиц. Применимо для CHYT поверх версий YT-сервера до 24.2 включительно, [d3df92f](https://github.com/ytsaurus/ytsaurus/commit/d3df92fd4fa2756f397d329e79243be008512311).
- Исправлены ошибки CTE в LEFT JOIN с условием IN, [b51e5db](https://github.com/ytsaurus/ytsaurus/commit/b51e5db56c9867a4b6615e24d791b59cef7becab).
- Отслеживание общего прогресса на координаторе, [0d5fc5c](https://github.com/ytsaurus/ytsaurus/commit/0d5fc5ce2b0f1c65922627f6d7dfb8bc7d215dd5).
- Исправлена многопоточная запись в secondaryProgress из разных пайпов, [8db79f4](https://github.com/ytsaurus/ytsaurus/commit/8db79f457cf71f0e00b8f65bd079aa03aaa9ad52).

{% endcut %}


{% cut "**2.17.4**" %}

**Дата релиза:** 2025-09-23


**Страница релиза:** [2.17.4](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.17.4)


**Docker-образ:** [ghcr.io/ytsaurus/chyt:2.17.4](https://github.com/orgs/ytsaurus/packages/container/chyt/524445395?tag=2.17.4)


- Бэкпорт YT-25206: настройка сервиса транзакций Cypress на прокси-серверах Cypress (коммит: eb104f198aeb5bd30208e0214c03fd50f0535655)
- Бэкпорт поддержки TLS в YT (коммит: fde3ac361bd81d5c8df21e3bcc13c9710cb446a8)
- Исправлено использование CTE в распределенных запросах (коммит: e275fa81599ff28fbb6a41de4c7c6c9fee0417fd)
- Добавлена поддержка aklomp-base64 для функций base64 (коммит: 5708583fcc58051627bfcd4f9849de6f7915afcf)
- Добавлено использование нового анализатора в функции ytTables (коммит: 29a8f6cefa043b8365949e2f6e54aadf40434c6b)

{% endcut %}


{% cut "**2.17.2**" %}

**Дата релиза:** 2025-07-04


**Страница релиза:** [2.17.2](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.17.2)


**Docker-образ:** [ghcr.io/ytsaurus/chyt:2.17.2](https://github.com/orgs/ytsaurus/packages/container/chyt/454419671?tag=2.17.2)


Описание отсутствует

{% endcut %}


{% cut "**2.16.0**" %}

**Дата релиза:** 2024-11-06


**Страница релиза:** [2.16.0](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.16.0)


**Docker-образ:** [ghcr.io/ytsaurus/chyt:2.16.0](https://github.com/orgs/ytsaurus/packages/container/chyt/301743715?tag=2.16.0)


- Поддержка кэша запросов ClickHouse (может быть настроен через `clickhouse_config`)
- Оптимизация чтения по порядку (PR #757)
- Новый алгоритм PREWHERE на уровне преобразования данных, включен по умолчанию
- Преобразование типа данных `bool` в `Bool` вместо `YtBoolean`. Тип `YtBoolean` устарел
- Преобразование типа данных `dict` в `Map` вместо `Array(Typle(Key, Value))`
- Преобразование типа данных `timestamp` в `DateTime64` вместо `UInt64`
- Поддержка чтения и записи типов данных `date32`, `datetime64`, `timestamp64`, `interval64`
- Поддержка чтения типа данных `json` как `String`
- Поддержка функций JSON_* из ClickHouse
- Возможность указать директорию Cypress в качестве базы данных
- Поддержка экспорта системных таблиц журнала в Cypress (query_log, metric_log и т. д.)

**Примечание**: типы данных `date32`, `datetime64`, `timestamp64` и `interval64` были добавлены в YTsaurus 24.1. Если версия кластера YTsaurus старше, попытка сохранить эти типы данных в таблице приведет к ошибке `not a valid type`.

{% endcut %}


{% cut "**2.14.0**" %}

**Дата релиза:** 2024-02-15


**Страница релиза:** [2.14.0](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.14.0)


- Поддержка SQL UDF
- Поддержка чтения динамических и статических таблиц через concat-функции

{% endcut %}


{% cut "**2.13.0**" %}

**Дата релиза:** 2024-01-19


**Страница релиза:** [2.13.0](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.13.0)


- Обновление версии кода ClickHouse до последнего LTS-релиза (22.8 -> 23.8)
- Поддержка чтения и записи упорядоченных динамических таблиц
- Вынос выгрузки отладочной информации реестра запросов в отдельный поток
- Настройка временного хранилища данных

{% endcut %}