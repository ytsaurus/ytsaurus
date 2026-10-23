---
title: "Справочник прагм YQL over Flow"
description: "Настройки для управления YQL-запросами через Flow"
---

# Справочник прагм YQL over {{product-name}} Flow

## Синтаксис

Названия прагм [YQL over Flow](../../../flow/yql/features.md) содержат префикс `Ytflow`.<br>
Исключение — общая прагма `Engine`, которая выбирает движок выполнения запроса:

```yql
PRAGMA Ytflow.<pragma_name> = '<value>';
PRAGMA Engine = 'ytflow';
```

{% note warning %}

Передавайте значения прагм как строки, в кавычках. Используйте одинарные `'...'` или двойные `"..."` кавычки.

{% endnote %}

## Engine {#engine}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| `'ytflow'` | — | Статическая |

Выбор движка Flow для выполнения запроса.<br>
Обязательно укажите эту прагму, без неё запрос не будет выполняться через Flow.

```yql
PRAGMA Engine = 'ytflow';
```

## Ytflow.Cluster {#cluster}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Строка | — | Статическая |

Имя кластера {{product-name}} для внутренних таблиц пайплайна.

```yql
PRAGMA Ytflow.Cluster = '{{flow-data-cluster}}';
```

## Ytflow.RuntimeCluster {#runtime-cluster}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Строка | Значение `Ytflow.Cluster` | Статическая |

Имя кластера для запуска [vanilla операции](../../../user-guide/data-processing/operations/vanilla.md) пайплайна.

Контроллеры и воркеры запускаются как два типа тасков (`controller` и `worker`) внутри одной операции; число джобов и ресурсы каждого типа тасков задаются отдельно.

```yql
PRAGMA Ytflow.RuntimeCluster = '{{flow-runtime-cluster}}';
```

## Ytflow.PipelineDirectory {#pipeline-directory}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Путь | — | Статическая |

Директория с пайплайнами в Кипарисе на кластере `Ytflow.Cluster`, используется совместно с `Ytflow.PipelineName`.

Альтернатива — явное задание одной прагмы `Ytflow.PipelinePath`.

В примерах пути в Кипарисе начинаются с `//` и указываются без завершающего `/`.

```yql
PRAGMA Ytflow.PipelineDirectory = '//home/my-project/pipelines';
```

## Ytflow.PipelineName {#pipeline-name}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Строка | — | Статическая |

Имя пайплайна, используется для создания поддиректории внутри `Ytflow.PipelineDirectory`,
в ней будут жить внутренние таблицы пайплайна.

Альтернатива — явное задание прагмы `Ytflow.PipelinePath`.

```yql
PRAGMA Ytflow.PipelineName = 'my-pipeline';
```

## Ytflow.PipelinePath {#pipeline-path}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Путь | `{Ytflow.PipelineDirectory}/{Ytflow.PipelineName}` | Статическая |

Полный путь к пайплайну в Кипарисе на кластере `Ytflow.Cluster`,
в директории по этому пути будут жить внутренние таблицы пайплайна.

Альтернатива — задание `Ytflow.PipelineDirectory` и `Ytflow.PipelineName` по отдельности.

Если указан `Ytflow.PipelinePath`, он имеет приоритет над этими двумя прагмами.

```yql
PRAGMA Ytflow.PipelinePath = '//home/my-project/pipelines/my-pipeline';
```

## Ytflow.Auth {#auth}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Строка | Секрет `default_ytflow`, если передан средой запуска | Статическая |

Имя секрета YQL категории `ytflow`, содержащего токен для аутентификации в {{product-name}}.

Токен должен обладать правами на доступ к таблицам пайплайна на кластере `Ytflow.Cluster`,
а также правом запуска vanilla операции на кластере `Ytflow.RuntimeCluster`.

Подробнее см. в документации про [аутентификацию](../../../flow/yql/authentication.md).

```yql
PRAGMA Ytflow.Auth = 'my_ytflow_credentials';
```

## Ytflow.WorkerCount {#worker-count}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Положительное число | {% if audience == "internal" %}5{% else %}1{% endif %} | Статическая |

Количество worker джобов в vanilla операции на кластере `Ytflow.RuntimeCluster`.

```yql
PRAGMA Ytflow.WorkerCount = '5';
```

## Ytflow.WorkerCpuLimit {#worker-cpu-limit}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Число с плавающей точкой | {% if audience == "internal" %}10.0{% else %}1.0{% endif %} | Статическая |

Гарантированное потребление CPU на один worker джоб в vanilla операции на кластере `Ytflow.RuntimeCluster`.

Реальное потребление CPU может быть выше при наличии свободных ресурсов на машинке джоба.

Единица измерения - одно ядро.

```yql
PRAGMA Ytflow.WorkerCpuLimit = '10.0';
```

## Ytflow.WorkerMemoryLimit {#worker-memory-limit}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Размер (например, `20G`) | {% if audience == "internal" %}20G{% else %}1G{% endif %} | Статическая |

Лимит памяти на один worker джоб в vanilla операции на кластере `Ytflow.RuntimeCluster`.

```yql
PRAGMA Ytflow.WorkerMemoryLimit = '20G';
```

## Ytflow.EnableComputationPatternResources {#enable-computation-pattern-resources}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| `true` / `false` | `false` | Статическая |

Включение переиспользования шаблонов вычислений между графами одного воркера.

```yql
PRAGMA Ytflow.EnableComputationPatternResources = 'true';
```

## Ytflow.ControllerCount {#controller-count}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Положительное число | 1 | Статическая |

Количество controller джобов в vanilla операции на кластере `Ytflow.RuntimeCluster`.

Один [контроллер](../../../flow/concepts/glossary.md#controller) становится лидером, остальные ожидают в режиме standby.<br>
При значении `1` резервного контроллера нет, при перезапуске единственного контроллера управление пайплайном временно недоступно.

```yql
PRAGMA Ytflow.ControllerCount = '3';
```

## Ytflow.ControllerCpuLimit {#controller-cpu-limit}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Число с плавающей точкой | {% if audience == "internal" %}3.0{% else %}1.0{% endif %} | Статическая |

Гарантированное потребление CPU на один controller джоб в vanilla операции на кластере `Ytflow.RuntimeCluster`.

Реальное потребление CPU может быть выше при наличии свободных ресурсов на машинке джоба.

Единица измерения - одно ядро.

```yql
PRAGMA Ytflow.ControllerCpuLimit = '3.0';
```

## Ytflow.ControllerMemoryLimit {#controller-memory-limit}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Размер (например, `5G`) | {% if audience == "internal" %}5G{% else %}1G{% endif %} | Статическая |

Лимит памяти на один controller джоб в vanilla операции на кластере `Ytflow.RuntimeCluster`.

```yql
PRAGMA Ytflow.ControllerMemoryLimit = '5G';
```

## Ytflow.GracefulUpdate {#graceful-update}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| `true` / `false` | `true` | Статическая |

Режим обновления пайплайна:
  * при `true` — штатная остановка с дообработкой внутренних потоков ([`stop`](../../../flow/concepts/glossary.md#start-stop-pause-pipeline))
  * при `false` — немедленная пауза ([`pause`](../../../flow/concepts/glossary.md#start-stop-pause-pipeline)).

После достижения нужного состояния старая vanilla операция на кластере `Ytflow.RuntimeCluster` абортится и запускается новая версия пайплайна.<br>
Ограничения обновления без дообработки описаны в разделе [Хотфикс](../../../flow/devops/vanilla/releases.md#hotfix), команды остановки — в [справочнике CLI](../../../flow/tools/cli.md).

```yql
PRAGMA Ytflow.GracefulUpdate = 'true';
```

## Ytflow.UpdateTimeout {#update-timeout}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Интервал времени (`1d`, `5s`, `2w`) | 600s | Статическая |

Таймаут ожидания остановки или паузы старой версии пайплайна при обновлении.

Если за это время нужное состояние не достигнуто, запрос завершится с ошибкой, старая vanilla операция на кластере `Ytflow.RuntimeCluster` не будет остановлена этим обновлением, а новая версия не запустится.<br>
Команда остановки или паузы при этом уже отправлена и не отменяется.

```yql
PRAGMA Ytflow.UpdateTimeout = '600s';
```

## Ytflow.TabletCellBundle {#tablet-cell-bundle}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Строка | — | Статическая |

[Таблет-селл бандл](../../../user-guide/dynamic-tables/concepts.md#tablet_cell_bundles) (`tablet_cell_bundle`) для создаваемых таблиц.

```yql
PRAGMA Ytflow.TabletCellBundle = 'my-bundle';
```

## Ytflow.Account {#account}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Строка | — | Статическая |

[Аккаунт](../../../user-guide/storage/accounts.md) для создаваемых таблиц.

```yql
PRAGMA Ytflow.Account = 'my-account';
```

## Ytflow.PrimaryMedium {#primary-medium}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Строка | — | Статическая |

[primary medium](../../../user-guide/storage/media.md) (`primary_medium`) для создаваемых таблиц.

```yql
PRAGMA Ytflow.PrimaryMedium = 'default';
```

## Ytflow.Pool {#pool}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Строка | — | Статическая |

[Вычислительный пул](../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) для vanilla операции пайплайна на кластере `Ytflow.RuntimeCluster`.

```yql
PRAGMA Ytflow.Pool = 'my-pool';
```

## Ytflow.YtPartitionCount {#yt-partition-count}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Положительное число | 1 | Статическая |

Количество партиций новых выходных очередей в {{product-name}}.

```yql
PRAGMA Ytflow.YtPartitionCount = '4';
```

## Ytflow.YtTtl {#yt-ttl}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Интервал времени (`1d`, `5s`, `2w`) | — | Статическая |

Срок хранения строк в создаваемых выходных упорядоченных динамических таблицах (очередях) {{product-name}}.

Задаёт `max_data_ttl`, также выставляются:
  * `min_data_versions = 0`
  * `max_data_versions = 1`
  * `min_data_ttl = 0`
  * `auto_compaction_period`, равный половине TTL

Подробнее см. [Автоматическое удаление старых строк](../../../user-guide/dynamic-tables/ordered-dynamic-tables.md#remove_old_data).

```yql
PRAGMA Ytflow.YtTtl = '7d';
```

## Ytflow.YtConsumerVital {#yt-consumer-vital}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| `true` / `false` | `false` | Статическая |

Признак `vital` при регистрации консьюмера на входных очередях {{product-name}}.

При включённой [автоматической очистке очередей по консьюмерам](../../../user-guide/dynamic-tables/queues.md#automatic_trimming) очередь учитывает оффсет vital консьюмера и не удаляет ещё не прочитанные им строки.<br>
Оффсет non vital консьюмера не блокирует удаление необработанных им строк.

```yql
PRAGMA Ytflow.YtConsumerVital = 'true';
```

## Ytflow.YtConsumerDirectory {#yt-consumer-directory}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Путь | `{Ytflow.PipelinePath}/yql_ytflow/consumers` | Статическая |

Директория {{product-name}} консьюмера.

```yql
PRAGMA Ytflow.YtConsumerDirectory = '//home/my-project/consumers';
```

## Ytflow.YtConsumerName {#yt-consumer-name}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Строка | `default_consumer` | Статическая |

Имя {{product-name}} консьюмера.

```yql
PRAGMA Ytflow.YtConsumerName = 'my-consumer';
```

## Ytflow.YtProducerDirectory {#yt-producer-directory}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Путь | `{Ytflow.PipelinePath}/yql_ytflow/producers` | Статическая |

Директория {{product-name}} продьюсера.

```yql
PRAGMA Ytflow.YtProducerDirectory = '//home/my-project/producers';
```

## Ytflow.YtProducerName {#yt-producer-name}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Строка | `default_producer` | Статическая |

Имя {{product-name}} продьюсера.

```yql
PRAGMA Ytflow.YtProducerName = 'my-producer';
```

{% if audience == "internal" %}
## Ytflow.LogbrokerConsumerPath {#logbroker-consumer-path}

| Возможные значения | Значение по умолчанию | Вид |
| --- | --- | --- |
| Строка | — | Статическая |

Путь к консьюмеру в заданной инсталляции Logbroker (имеется в виду именно Logbroker путь, не путь в Кипарисе {{product-name}}).

Подробнее о сущностях Logbroker см. в [ресурсной модели Logbroker](https://logbroker.yandex-team.ru/docs/concepts/resource_model).

```yql
PRAGMA Ytflow.LogbrokerConsumerPath = 'my-account/my-directory/my-consumer';
```
{% endif %}
