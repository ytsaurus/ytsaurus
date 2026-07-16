# Быстрый старт c {{product-name}} Flow

Цель данного руководства &mdash; познакомить с инфраструктурой Flow на примере минималистичного пайплайна. В этом руководстве Вы:

1. [Локально поднимете пайплайн в долгоживущем режиме для его изучения;](#start)
1. [Изучите структуру проекта;](#structure)
1. [Запустите полезные команды для работы с пайплайном.](#commands)

## Основные компоненты пайплайна

Архитектура одного пайплайна состоит из трёх основных компонентов:
1. [Controller](../../flow/concepts/glossary.md#controller) &mdash; управляет жизненным циклом пайплайна;
1. [Worker](../../flow/concepts/glossary.md#worker) &mdash; читает из источников, выполняет вычисления;
1. {{product-name}} Cluster &mdash; хранит системные таблицы в директории пайплайна.

Каждый пайплайн требует свой набор экземпляров Controller и Worker, рабочую директорию в [Cypress](../../user-guide/storage/cypress.md), набор системных динамических таблиц.

## Пререквизиты

Для работы вам понадобится:
1. Виртуальная машина Linux x86\_64 для компиляции C++ проекта YT Flow (рекомендуется от 6 vCPU, 12 GB RAM, 120 GB SSD).
1. Локальная копия [репозитория]({{source-root}}) (ниже используется `~/arcadia` как путь к ней).
1. Установленная утилита `ya`.
1. [YT token](../../user-guide/storage/auth.md) для кластера {{product-name}} с динамическими таблицами{% if audience == "internal" %} (см. [список кластеров](../../user-guide/dynamic-tables/clusters.md)){% endif %}, например {{flow-example-cluster}}.

## Запуск пайплайна {#start}

Используйте готовую директорию для локального запуска с минималистичным NoOp пайплайном:

```bash
$ cd ~/arcadia/yt/yt/flow/examples/cpp/noop

# Соберите проект.
$ ya make

# Скрипт запустит YT Flow компоненты (Controller, Worker) и пайплайн.
# Скрипт создаст директорию //tmp/$(whoami)/pipelines/pipeline для системных объектов YT Flow.
$ ./run_noop_pipeline.sh --cluster {{flow-example-cluster}} --path //tmp/$(whoami)/pipelines
```

## Структура проекта {#structure}

Ниже представлен минимальный рабочий пример для ознакомления.

{% if audience == "internal" %}

```bash
$ tree -L 2
.
├── README.md                  # Документация для run_noop_pipeline.sh.
├── controller.config.yson     # Конфигурационный файл для Controller'а.
├── worker.config.yson         # Конфигурационный файл для Worker'а.
├── pipeline
│   ├── main.cpp               # Код пайплайна: computation TNoopComputation и запуск в `int main`.
│   ├── pipeline.yson          # Спецификация пайплайна (топология графа computations).
│   └── ya.make
└── yt_sync
    ├── __main__.py            # Создаёт структуры в {{product-name}} необходимые для работы пайплайна.
    ├── pipelines.py           # Опиcывает рабочую директорию "pipeline".
    ├── stages.py              # Описывает {{product-name}} кластер, на котором пайплайн будет запущен.
    └── ya.make
```

{% else %}

```bash
$ tree -L 2
.
├── README.md                  # Документация для run_noop_pipeline.sh.
├── controller.config.yson     # Конфигурационный файл для Controller'а.
├── worker.config.yson         # Конфигурационный файл для Worker'а.
├── pipeline
│   ├── main.cpp               # Код пайплайна: computation TNoopComputation и запуск в `int main`.
│   ├── pipeline.yson          # Спецификация пайплайна (топология графа computations).
│   └── ya.make
└── yt_sync_mini
    ├── __main__.py            # Создаёт структуры в {{product-name}} необходимые для работы пайплайна.
    └── ya.make
```

{% endif %}

Что происходит в пайплайне:

1. `TRandomSource` генерирует случайные сообщения;
1. `TNoopComputation` читает их и отбрасывает.

Последовательность запуска пайплайна (код незначительно упрощён относительно скрипта).

Сначала создаётся Cypress-объект типа `pipeline` вместе с набором служебных динамических таблиц, необходимых для работы Flow (см. раздел [Объект Pipeline](../../flow/concepts/pipeline-object.md)).

{% if audience == "internal" %}

В Yandex-инфраструктуре это делает [YtSync](../../flow/concepts/pipeline-object.md#yt-sync):

```bash
$ TEST_CLUSTER={{flow-example-cluster}} TEST_YT_PATH=//tmp/$(whoami)/pipelines \
    ./yt_sync/yt_sync --stage test --scenario ensure --parallel-factor 0 --commit
```

{% else %}

В опенсорсе для этого используется готовый helper [yt_sync_mini](../../flow/concepts/pipeline-object.md#yt-sync-mini):

```bash
$ TEST_YT_CLUSTER={{flow-example-cluster}} TEST_YT_PATH=//tmp/$(whoami)/pipelines/pipeline \
    ./yt_sync_mini/yt_sync_mini
```

{% endif %}

Затем запускаются долгоживущие Controller и Worker, и Controller'у отправляется спецификация пайплайна:

```bash
# Запускаются долгоживущие Controller и Worker.
$ YT_FLOW_MODE=Controller pipeline/pipeline --config controller.config.yson
$ YT_FLOW_MODE=Worker pipeline/pipeline --config worker.config.yson

# Отправка спецификации pipeline.yson на Controller.
# Дожидается, пока пайплайн перейдёт в состояние Working.
$ YT_FLOW_WAIT=0 pipeline/pipeline --config pipeline.yson
```

Структуру [внутренних таблиц](../../flow/concepts/pipeline-object.md#internal_tables), созданных вместе с объектом `pipeline`, можно найти тут:

`{{yt-cli}} --proxy={{flow-example-cluster}} list //tmp/$(whoami)/pipelines/pipeline`

## Полезные команды {#commands}

Проверяем, что пайплайн запущен:

```bash
$ {{yt-cli}} --proxy {{flow-example-cluster}} flow get-pipeline-state --pipeline-path //tmp/$(whoami)/pipelines/pipeline
working
```

Подробную информацию и статистику по пайплайну можно посмотреть следующими способами:

```bash
$ curl http://localhost:10002/orchid/job_tracker/jobs | {% if audience == "internal" %}ya tool {% endif %}jq
$ {{yt-cli}} --proxy {{flow-example-cluster}} flow describe-pipeline --pipeline-path //tmp/$(whoami)/pipelines/pipeline
```

Локально можно посмотреть логи Controller и Worker:

```bash
$ ls *.log
controller.log  worker.log
```

{% if audience == "internal" %}Как работать с логами, можно посмотреть в разделе [Сырые логи контроллера и воркера](../../flow/release/problems.md#raw-logs).{% endif %}

Визуализация графа пайплайна:

```bash
$ cd ~/arcadia/yt/yt/flow/tools/draw_pipeline_graph

$ ya run . -- --input {{flow-example-cluster}}://tmp/example/noop --ttl 1
```

На выходе будет создан .svg файл с изображением пайплайна. Файл можно открыть в браузере. О том, какую информацию можно получить из графа &mdash; можно почитать в README.md рядом с утилитой.

![](../../flow/images/flow_noop_pipeline.png =600x230){ .center }

## Запуск в YT vanilla-операции {#vanilla}

Если в пайплайне используется `TSimpleRunnerProgram` (как во всех примерах `examples/cpp/*`), его можно запустить в vanilla-операции, добавив блок `vanilla` в конфиг:

```yson
{
    "cluster_url" = "{{flow-example-cluster}}";
    "path" = "//tmp/example/pipeline";
    "spec" = { ... };
    "vanilla" = {
        "enable" = %true;
        "pool" = "research";
        "worker" = {"count" = 4};
    };
}
```

Обязательные параметры: `worker.count`, `pool`. Остальные имеют разумные значения по умолчанию: контроллер 1 джоба × (1 CPU, 4 GB), воркер по 4 CPU и 4 GB на джобу, порты выдаёт YT через `YT_PORT_*`. При запуске бинарь сам создаст vanilla-операцию с двумя задачами (controller + worker), отправит пайплайн на исполнение и дождётся завершения.

Полный список полей &mdash; в [TVanillaConfig](../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaConfig) (см. также [TVanillaTaskConfig](../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaTaskConfig)).

## Что дальше

Для более глубокого погружения в фреймворк следуйте инструкциям раздела [С чего начать](../../flow/start.md).
