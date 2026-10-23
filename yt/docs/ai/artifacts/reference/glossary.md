# Глоссарий терминов YTsaurus (EN → RU)

Единый источник истины для перевода release notes и документации YTsaurus с английского на русский.

## Как пользоваться глоссарием

1. Для каждого термина из таблиц ниже используйте **только** указанный перевод. Синонимы и вольные варианты запрещены.
2. Если в колонке «Русский» термин совпадает с английским — он не переводится, сохраняйте исходное написание и регистр.
3. Если термина нет в глоссарии — переводите по общим правилам промта и не изобретайте новых соответствий для названий компонентов.
4. Ссылки вида [Кипарис](*cypress) — это попапы с пояснением термина. Все пояснения собраны в конце файла.

---

## 1. Компоненты YTsaurus

Названия компонентов — имена собственные. По умолчанию они не переводятся и пишутся так, как указано в колонке «Русский».

### 1.1. Компоненты с собственным релизным циклом

Соответствует файлам в `yt/docs/ru/_includes/releases/`.

| English | Русский | Пояснение |
|---|---|---|
| YTsaurus server | YTsaurus server | [что это](*ytsaurus-server) |
| CHYT | CHYT | [что это](*chyt) |
| SPYT | SPYT | [что это](*spyt) |
| Query Tracker | Query Tracker | [что это](*query-tracker) |
| Flow | Flow | [что это](*flow) |
| Odin | Odin | [что это](*odin) |
| Cron | Cron | [что это](*cron) |
| Strawberry | Strawberry | [что это](*strawberry) |
| Kubernetes operator / k8s-operator | Kubernetes-оператор| [что это](*k8s-operator) |
| UI | UI | [что это](*ui) |
| Java SDK | Java SDK | [что это](*sdk) |
| Python SDK | Python SDK | [что это](*sdk) |
| Python YSON bindings | Python YSON bindings | [что это](*yson) |
| Excel-integration | Excel-интеграция | [что это](*excel) |
| Task-proxy | Task-прокси | [что это](*task-proxy) |
| Tutorial | Tutorial | Название компонента, не переводится |
| MCP server | MCP-сервер | [что это](*mcp) |

### 1.2. Серверные компоненты кластера

Используются как подзаголовки внутри release notes YTsaurus server.

| English | Русский | Пояснение |
|---|---|---|
| Master / Master Server | мастер-сервер | [что это](*master) |
| Data Node / Data Nodes | Data ноды | [что это](*data-node) |
| Exec Node / Exec Nodes | Exec ноды | [что это](*exec-node) |
| Tablet Node / Tablet Nodes | Tablet Nodes | [что это](*tablet-node) |
| Node / Nodes | узлы кластера | [что это](*node) |
| Scheduler | планировщик | [что это](*scheduler) |
| Scheduler and GPU | Планировщик и GPU | Типовой заголовок раздела |
| Controller agent | контроллер-агент | [что это](*controller-agent) |
| Proxy | прокси | [что это](*proxy) |
| HTTP proxy | HTTP-прокси | — |
| RPC proxy | RPC-прокси | — |
| gRPC proxy | gRPC-прокси | — |
| Kafka proxy | Kafka-прокси | — |
| Queue Agent | Queue Agent | [что это](*queue-agent) |
| YQL Agent | YQL-агент | [что это](*yql-agent) |
| Tablet Balancer | Tablet Balancer | [что это](*tablet-balancer) |
| MapReduce | MapReduce | [что это](*mapreduce) |
| Dynamic Tables | динамические таблицы | В заголовке — «Динамические таблицы» |
| Storage | Storage | Название раздела release notes, не переводится |
| Compute | Compute | Название раздела release notes, не переводится |
| API | API | — |
| Jupyter / JupYT | Jupyter | [что это](*jupyter) |

---

## 2. Термины предметной области

### 2.1. Архитектура и хранение

| English | Русский | Пояснение |
|---|---|---|
| Cypress | Кипарис | [что это](*cypress) |
| Cypress node | узел Кипариса | — |
| Hydra | Hydra | [что это](*hydra) |
| chunk | чанк | [что это](*chunk) |
| tablet | таблет | [что это](*tablet) |
| tablet cell | таблет-селл | [что это](*tablet-cell) |
| bundle | бандл | [что это](*bundle) |
| medium | медиум | [что это](*medium) |
| dynamic table | динамическая таблица | — |
| static table | статическая таблица | — |
| replica | реплика | — |
| shard | шард | — |
| snapshot | снапшот | «Слепок» — устаревший вариант, не используйте |
| journal | журнал | — |
| log | лог, журнал | Выберите один вариант в пределах документа |
| path | путь | — |
| attribute | атрибут | — |
| link / symlink | ссылка, символическая ссылка | — |
| lock | блокировка | — |
| transaction | транзакция | — |
| account | аккаунт | — |
| quota | квота | — |
| vitality | витальность | — |
| YSON | YSON | [что это](*yson) |

### 2.2. Вычисления и операции

| English | Русский | Пояснение |
|---|---|---|
| operation | операция | — |
| job | джоб | [что это](*job) |
| task | задача | — |
| clique | клика | [что это](*clique) |
| pool | пул | — |
| preemption | вытеснение | — |
| fair-share | fair-share | Не переводится |
| mount / mounting | монтирование | — |
| unmount / unmounting | Отмонтирование | — |
| queue | очередь | — |
| consumer | консьюмер | — |
| pipeline | пайплайн | Термин Flow |
| stream | стрим | Термин Flow |
| computation | компьютейшен | Термин Flow |
| exactly-once | exactly-once | Не переводится |
| engine | движок | — |
| controller | контроллер | — |
| cluster | кластер | — |
| cluster node | узел кластера | — |
| map / dict | словарь | — |
    
### 2.3. Общие IT-термины

| English | Русский |
|---|---|
| release | релиз |
| deploy / deployment | развёртывание, деплой |
| feature | возможность, функция |
| support (сущ.) | поддержка |
| support (глаг.) | поддерживать |
| fix (сущ.) | исправление |
| to fix | исправить |
| crash | аварийное завершение, сбой |
| dynamic config | динамическая конфигурация |
| upcoming releases | следующие релизы |
| backend / frontend | бэкенд, фронтенд |
| cache | кеш |
| tag | тег |
| checkbox | чекбокс |
| checklist | чеклист |
| parser | парсер |
| framework | фреймворк |
| pull request | пул-реквест |
| helm chart | helm-чарт |
| docker image | docker-образ |

---

## 3. Служебные поля release notes

Переводятся всегда одинаково, без вариантов.

| English | Русский |
|---|---|
| `**Release date:**` | `**Дата релиза:**` |
| `**Release page:**` | `**Страница релиза:**` |
| `**Docker image:**` | `**Docker-образ:**` |
| `**Releases:**` | `**Релизы:**` |

---

## 4. Типовые заголовки разделов

Переводятся одинаково независимо от уровня заголовка и от того, оформлены они заголовком или жирным начертанием.

| English | Русский |
|---|---|
| Overview | Обзор |
| Significant changes | Значимые изменения |
| Breaking changes / Backward incompatible changes | Обратно несовместимые изменения |
| Features / New features | Новые возможности |
| New Features & Changes | Новые возможности и изменения |
| Fixes / Bug fixes / Bugfixes | Исправления |
| Fixes & Optimizations | Исправления и оптимизации |
| Improvements | Улучшения |
| Notes | Примечания |
| Known Issue | Известная проблема |
| What's Changed | Что изменилось |
| New Contributors | Новые участники |
| Full changelog | Полный список изменений |
| Query language features | Возможности языка запросов |
| Default values | Значения по умолчанию |
| Default changes and deprecations | Изменения по умолчанию и устаревшие функции |
| Code Refactoring | Рефакторинг кода |
| Dependencies | Зависимости |
| Tests / Testing | Тесты |
| Misc / Other | Прочее |

---

## 5. Примеры перевода (few-shot)

### Пример 1. Кат с разделом исправлений

Вход:

```
{% cut "**2.18.6**" %}

**Release date:** 2026-06-22

**Release page:** [2.18.6](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.18.6)

**Docker image:** [ghcr.io/ytsaurus/chyt:2.18.6](https://github.com/orgs/ytsaurus/packages/container/chyt/967753290?tag=2.18.6)

**Fixes:**

* Fix usage of `enable_distinct_read_optimization` and `enable_min_max_optimization` features with dictionaries over YTsaurus tables (ea82d76ea5beedc7405a30275ae37e8426953f85)
* Fix read range inference over complex filters (cdb1a6c84ee02e9ee1e65cc878c3f5ba288d592b)

**Notes:**

If you are using the `enable_read_range_inferring` flag since version 2.18, we strongly recommend that you upgrade to this version to avoid potential data loss due to the mentioned bugs.

{% endcut %}
```

Выход:

```
{% cut "**2.18.6**" %}

**Дата релиза:** 2026-06-22

**Страница релиза:** [2.18.6](https://github.com/ytsaurus/ytsaurus/releases/tag/chyt/2.18.6)

**Docker-образ:** [ghcr.io/ytsaurus/chyt:2.18.6](https://github.com/orgs/ytsaurus/packages/container/chyt/967753290?tag=2.18.6)

**Исправления:**

* Исправлено использование возможностей `enable_distinct_read_optimization` и `enable_min_max_optimization` со словарями над таблицами YTsaurus (ea82d76ea5beedc7405a30275ae37e8426953f85)
* Исправлен вывод диапазонов чтения для сложных фильтров (cdb1a6c84ee02e9ee1e65cc878c3f5ba288d592b)

**Примечания:**

Если вы используете флаг `enable_read_range_inferring` начиная с версии 2.18, настоятельно рекомендуем обновиться до этой версии, чтобы избежать возможной потери данных из-за указанных ошибок.

{% endcut %}
```

### Пример 2. Заголовки внутри ката и ссылка на коммит

Вход:

```
{% cut "**25.3.1**" %}

**Release date:** 2026-03-25

#### Queue Agent
##### Fixes:
- Fix secondary index creation in init_queue_agent_state script, [9840c02](https://github.com/ytsaurus/ytsaurus/commit/9840c02)

{% endcut %}
```

Выход:

```
{% cut "**25.3.1**" %}

**Дата релиза:** 2026-03-25

#### Queue Agent
##### Исправления:
- Исправлено создание вторичного индекса в скрипте init_queue_agent_state, [9840c02](https://github.com/ytsaurus/ytsaurus/commit/9840c02)

{% endcut %}
```

### Пример 3. Названия компонентов в заголовках

| SOURCE | GOOD | BAD |
|---|---|---|
| `#### Master Server` | `#### Мастер-сервер` | `#### Мастер Сервер`, `#### Master Server` |
| `#### Scheduler and GPU` | `#### Планировщик и GPU` | `#### Шедулер и GPU` |
| `#### Data Nodes` | `#### Data Nodes` | `#### Дата-ноды` |
| `#### Queue Agent` | `#### Queue Agent` | `#### Агент очередей` |
| `#### Tablet Balancer` | `#### Tablet Balancer` | `#### Балансировщик Таблетов` |
| `#### Kafka proxy` | `#### Kafka-прокси` | `#### Kafka прокси` |
| `#### Task-proxy` | `#### Task-прокси` | `#### Прокси задач` |
| `## Tutorial` | `## Tutorial` | `## Инструкция` |
| `## Query Tracker` | `## Query Tracker` | `## Query tracker`, `## Трекер запросов` |

### Пример 4. Служебные подписи и типовые заголовки

> SOURCE: `**Release date:**` / `What's Changed` / `Fixes` / `New Features` / `Full changelog`
> GOOD: `**Дата релиза:**` / `Что изменилось` / `Исправления` / `Новые возможности` / `Полный список изменений`
> BAD: `**Дата релизы:**` / `Что поменялось` / `Фиксы` / `Новые фичи` / `Полный Changelog`

### Пример 5. Стиль и залог

> SOURCE: "Warning: Failure to complete this step may result in data loss."
> GOOD: "Если пропустить этот шаг, данные могут быть потеряны."
> BAD: "ВНИМАНИЕ! Если Вы пропустите этот шаг, произойдёт потеря данных!"

> SOURCE: "The setting can be configured using the API."
> GOOD: "Настройте параметр через API."
> BAD: "Параметр может быть настроен с помощью API."

> SOURCE: "Press Ctrl+Shift+Del to delete the data."
> GOOD: "Чтобы удалить данные, нажмите **Ctrl** + **Shift** + **Del**."
> BAD: "Нажмите **Ctrl** + **Shift** + **Del**, чтобы удалить данные."

---

## 6. Пояснения к терминам

Ниже собраны определения компонентов и сложных терминов. Они отображаются как попапы при наведении на ссылки вида [Кипарис](*cypress) в тексте.

[*ytsaurus-server]: YTsaurus server — серверные компоненты кластера YTsaurus: мастер-серверы, узлы, планировщик, контроллер-агенты и прокси. Поставляются одним docker-образом с общей версией.

[*cypress]: Кипарис — дерево метаинформации, содержащее в себе различную системную информацию и данные о том, где хранятся пользовательские данные.

[*master]: Мастер-сервер — компонент, отвечающий за отказоустойчивое хранение метаданных кластера: информации о пользователях, объектах и местоположении данных. Файловая система, реализуемая мастерами, называется Кипарис.

[*hydra]: Hydra — алгоритм консенсуса, похожий на протокол Raft. Обеспечивает идентичность состояния мастер-серверов: кластер остаётся доступен, пока доступно строго больше половины мастеров.

[*data-node]: Data Nodes — компоненты, хранящие данные кластера. Файлы и таблицы разбиваются на чанки, которые Data Nodes хранят в файловой системе на диске.

[*exec-node]: Exec Nodes — компоненты, исполняющие джобы на вычислительных ресурсах кластера. Отвечают за подготовку окружения джоба, запуск контейнеров и управление пользовательскими процессами.

[*tablet-node]: Tablet Nodes — компоненты, отвечающие за работу динамических таблиц. Каждая динамическая таблица разбита на таблеты, которые обслуживаются таблет-селлами, запущенными в Tablet Node.

[*node]: Узел кластера (node) — сервер кластера YTsaurus. В зависимости от роли бывает Data Node, Exec Node или Tablet Node.

[*scheduler]: Планировщик — компонент, отвечающий за честное разделение ресурсов между пользователями кластера по алгоритму HDRF. Взаимодействует с Exec Nodes, заказывая запуск джобов и получая информацию об их завершении.

[*controller-agent]: Контроллер-агент — вторая часть подсистемы планирования. Отвечает за логику исполнения операций: какие чанки обработать, как разбить входные данные на джобы и как объединить результаты в выходные таблицы.

[*proxy]: Прокси — точка входа в кластер: все пользовательские запросы попадают сначала в прокси, а затем в нужные подсистемы. Бывают HTTP-прокси и RPC-прокси.

[*task-proxy]: Task-прокси — компонент, предоставляющий доступ к веб-сервисам, развёрнутым в операциях YTsaurus.

[*queue-agent]: Queue Agent — выделенный микросервис, следящий за очередями, консьюмерами и регистрациями.

[*yql-agent]: YQL-агент — компонент, оркестрирующий исполнение YQL-запросов: преобразует их в цепочку Map/Reduce-операций.

[*tablet-balancer]: Tablet Balancer — компонент, выполняющий автоматическое шардирование динамических таблиц и перераспределение таблетов между таблет-селлами для равномерного распределения нагрузки по кластеру.

[*mapreduce]: MapReduce — модель распределённой обработки данных, на которой построены операции YTsaurus. Название пишется слитно и не переводится.

[*chyt]: CHYT powered by ClickHouse — это технология, которая позволяет поднять кластер из серверов ClickHouse непосредственно на вычислительных узлах YTsaurus.

[*clique]: Клика — Vanilla-операция, внутри которой поднят ClickHouse, работающий с данными в YTsaurus. Клики бывают публичные и приватные.

[*spyt]: SPYT — технология запуска Apache Spark поверх YTsaurus. SPYT-кластер — это Spark Standalone кластер, развёрнутый внутри кластера YTsaurus.

[*query-tracker]: Query Tracker — компонент YTsaurus, позволяющий работать с системой с помощью человекочитаемых запросов на SQL-подобных языках. Запросы исполняются в движке, пользователю возвращается результат.

[*flow]: YTsaurus Flow — фреймворк для потоковой кросс-ДЦ обработки событий с гарантиями exactly-once в рамках экосистемы YTsaurus. Поддерживает API для C++, Java, Kotlin, Python, Go и декларативное описание пайплайнов на YQL.

[*odin]: Odin — сервис, осуществляющий качественный мониторинг работы YTsaurus-кластера. Подробнее можно почитать в разделе Мониторинг.

[*cron]: Cron — набор cron-задач, полезных при эксплуатации кластера: очистка временных директорий, удаление неактивных нод, валидация и экспорт мастер-снапшотов. Поддерживаются встроенные и пользовательские задачи.

[*strawberry]: Strawberry — контроллер, управляющий жизненным циклом вычислительных клик поверх YTsaurus, в том числе клик CHYT и JupYT. Поставляется в виде docker-образа.

[*k8s-operator]: Kubernetes operator (k8s-operator) — оператор для развёртывания и обновления кластера YTsaurus в Kubernetes. Распространяется в виде helm-чартов.

[*ui]: UI — веб-интерфейс YTsaurus для работы с Кипарисом, таблицами, операциями и запросами.

[*sdk]: SDK — клиентские библиотеки для работы с YTsaurus из кода. Выпускаются для Python, Java, Go и C++.

[*yson]: YSON — формат представления данных YTsaurus, расширение JSON с поддержкой атрибутов и бинарного представления.

[*excel]: Excel-интеграция — компонент, позволяющий выгружать данные из схематизированных статических таблиц YTsaurus или результатов Query Tracker в виде Excel-таблицы.

[*mcp]: MCP-сервер — сервер протокола Model Context Protocol, через который AI-инструменты работают с данными и объектами YTsaurus.

[*jupyter]: Jupyter — среда работы с Jupyter Notebooks поверх YTsaurus. Клики для запуска ноутбуков называются JupYT.

[*chunk]: Чанк — часть, на которые разбиваются файлы и таблицы при хранении. Чанки хранятся на Data Nodes, а информация о них — на мастер-серверах.

[*tablet]: Таблет — шард динамической таблицы. Обслуживается таблет-селлом на Tablet Node.

[*tablet-cell]: Таблет-селл — сущность, обслуживающая таблеты. Каждый таблет-селл принадлежит некоторому бандлу.

[*bundle]: Бандл — группа таблет-селлов с выделенными ресурсами, в рамках которой обслуживаются динамические таблицы.

[*medium]: Медиум — группа дисков одного типа, позволяющая хранить данные на различных типах носителей.

[*job]: Джоб — единица вычислений в рамках операции. Джобы запускаются планировщиком на Exec Nodes.
