# Быстрый старт c {{product-name}} Flow

Цель данного руководства &mdash; запустить минималистичный пайплайн и познакомиться на нём с инфраструктурой Flow. Пайплайн запускается в {{product-name}} [vanilla-операции](../../user-guide/data-processing/operations/vanilla.md): контроллер и воркер работают джобами этой операции, поэтому отдельно деплоить их не нужно. Свой код тоже не понадобится &mdash; хватит готового бинаря `flow_server` и конфига пайплайна. В этом руководстве Вы:

1. [Запустите пайплайн тремя командами;](#start)
1. [Выполните полезные команды для работы с пайплайном;](#commands)
1. [Удалите пайплайн.](#remove)

## Основные компоненты пайплайна

Архитектура одного пайплайна состоит из трёх основных компонентов:

1. [Controller](../../flow/concepts/glossary.md#controller) &mdash; управляет жизненным циклом пайплайна;
1. [Worker](../../flow/concepts/glossary.md#worker) &mdash; читает из источников, выполняет вычисления;
1. {{product-name}} Cluster &mdash; хранит системные таблицы в директории пайплайна.

Каждому пайплайну нужны свои экземпляры Controller и Worker, рабочая директория в [Cypress](../../user-guide/storage/cypress.md) и набор системных динамических таблиц.

## Пререквизиты

Для работы вам понадобится:

1. Виртуальная машина Linux x86\_64 для компиляции C++ проекта YT Flow (рекомендуется от 6 vCPU, 12 GB RAM, 120 GB SSD).
1. Локальная копия [репозитория]({{source-root}}) (ниже используется `~/arcadia` как путь к ней).
1. Установленная утилита `ya`.
1. [YT token](../../user-guide/storage/auth.md) для кластера {{product-name}} с динамическими таблицами (см. [список кластеров](../../user-guide/dynamic-tables/clusters.md)), например {{flow-example-cluster}}, и CLI `{{yt-cli}}`.

## Запуск пайплайна {#start}

### Конфиг пайплайна {#config}

[Конфиг раннера](../../flow/concepts/spec.md#runner-config) лежит в примере `yt/yt/flow/examples/cpp/noop/pipeline.yson`. Подставьте в нём свой кластер вместо `<cluster>` и логин вместо `<login>`:

```yson
{
    "cluster_url" = "<cluster>";
    "proxy_role" = #;
    "path" = "//tmp/<login>/quickstart/pipeline";
    "spec" = {
        "computations" = {
            "reader" = {
                "computation_class_name" = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
                "source_streams" = {
                    "random" = {
                        "source_class_name" = "NYT::NFlow::TRandomSource";
                    };
                };
            };
        };
    };
    "vanilla" = {
        "enable" = %true;
        "pool" = "<login>";
        "worker" = {"count" = 1};
    };
}
```

Что здесь задано:

* `cluster_url` и `proxy_role` &mdash; кластер {{product-name}}, на котором лежит пайплайн и запускается операция; `#` означает роль прокси по умолчанию.
* `path` &mdash; путь к объекту pipeline в Cypress. Сам объект создаётся отдельной командой, см. [ниже](#run).
* `spec` &mdash; [спецификация](../../flow/concepts/spec.md) пайплайна: граф из одной computation `reader`. Она читает источник `random` &mdash; `TRandomSource`, генератор случайных сообщений, &mdash; и никуда их не пишет. Реальный пайплайн вместо этого читает, например, [очередь](../../flow/connectors/queue.md), выполняет вычисления и пишет результат в sink.
* `vanilla` &mdash; блок, который включает запуск в vanilla-операции:
  * `pool` &mdash; пул планировщика. Если пула с таким именем нет, планировщик создаёт эфемерный пул без гарантий ресурсов: при нехватке ресурсов на кластере джобы пайплайна будут ждать или их вытеснят. Для экспериментов этого достаточно, для рабочего пайплайна нужен свой пул с гарантиями;
  * `worker.count` &mdash; число джоб воркера. Контроллер по умолчанию запускается одной джобой, каждая джоба получает 6 CPU и 18 GiB памяти. Остальные параметры блока описаны в разделе [Запуск пайплайна в Vanilla-операции](../../flow/devops/vanilla/initial-deploy.md).

### Команды запуска {#run}

```bash
# 1. Соберите flow_server и yt_sync.
$ cd ~/arcadia && ya make yt/yt/flow/bin/flow_server yt/yt/flow/examples/cpp/noop/yt_sync

# 2. Создайте объект pipeline вместе с его системными таблицами.
$ TEST_CLUSTER={{flow-example-cluster}} TEST_YT_PATH=//tmp/$(whoami)/quickstart \
    ./yt/yt/flow/examples/cpp/noop/yt_sync/yt_sync --stage test --scenario ensure --parallel-factor 0 --commit

# 3. Запустите пайплайн.
$ YT_FLOW_WAIT=0 ./yt/yt/flow/bin/flow_server/flow_server \
    --config yt/yt/flow/examples/cpp/noop/pipeline.yson
```

[YtSync](../../flow/concepts/pipeline-object.md#yt-sync) создаёт [объект Pipeline](../../flow/concepts/pipeline-object.md) с именем `pipeline` в указанной директории и монтирует его [внутренние таблицы](../../flow/concepts/pipeline-object.md#internal_tables). Повторный запуск над уже существующим пайплайном ничего не меняет. Пайплайны и кластеры описаны в `pipelines.py` и `stages.py` рядом с утилитой; в своём проекте вы заводите такие же файлы под свои объекты.

`flow_server` с блоком `vanilla` в конфиге работает раннером: загружает свой бинарь в кеш кластера, создаёт vanilla-операцию с двумя задачами (controller и worker), устанавливает спецификацию и стартует пайплайн. С `YT_FLOW_WAIT=0` раннер завершается, как только пайплайн перейдёт в состояние `working`; без этой переменной он продолжает работать и печатает публичный лог контроллера. Прерывание раннера на операцию не влияет: она работает, пока вы её не отмените.

## Полезные команды {#commands}

Проверяем, что пайплайн запущен:

```bash
$ {{yt-cli}} --proxy {{flow-example-cluster}} flow get-pipeline-state --pipeline-path //tmp/$(whoami)/quickstart/pipeline
working
```

Подробная информация и статистика по пайплайну:

```bash
$ {{yt-cli}} --proxy {{flow-example-cluster}} flow describe-pipeline --pipeline-path //tmp/$(whoami)/quickstart/pipeline
```

Публичный лог контроллера:

```bash
$ {{yt-cli}} --proxy {{flow-example-cluster}} flow show-logs --pipeline-path //tmp/$(whoami)/quickstart/pipeline
```

Текущая vanilla-операция пайплайна записана в атрибуте `@current_vanilla_operation` под alias'ом. Id операции по alias'у можно узнать так:

```bash
$ {{yt-cli}} --proxy {{flow-example-cluster}} get-operation --include-runtime --attribute id --operation-alias \
    "$({{yt-cli}} --proxy {{flow-example-cluster}} get --format json //tmp/$(whoami)/quickstart/pipeline/@current_vanilla_operation/alias | tr -d '"')"
```

Где искать логи контроллера и воркера, описано в разделе [Логи Vanilla-операции](../../flow/devops/vanilla/diagnostics/logs.md).

## Удаление пайплайна {#remove}

Команды `stop-pipeline` и `pause-pipeline` останавливают пайплайн, но не операцию. Чтобы удалить пайплайн полностью, отмените операцию и удалите директорию пайплайна вместе с таблицами стейта:

```bash
$ {{yt-cli}} --proxy {{flow-example-cluster}} abort-op <operation-id>
$ {{yt-cli}} --proxy {{flow-example-cluster}} remove -r //tmp/$(whoami)/quickstart
```

Id операции можно узнать командой [выше](#commands). Если `remove` сразу после отмены операции завершается ошибкой `Cannot take "exclusive" lock`, повторите команду через минуту (подробнее &mdash; в разделе [Полное удаление пайплайна](../../flow/devops/vanilla/pipeline-operations.md#remove)).

## Что дальше

- [Запуск пайплайна в Vanilla-операции](../../flow/devops/vanilla/initial-deploy.md) &mdash; ресурсы, сетевой проект и запуск пайплайнов на Python, Java и Go.
- [С чего начать](../../flow/start.md) &mdash; для более глубокого погружения в фреймворк.
