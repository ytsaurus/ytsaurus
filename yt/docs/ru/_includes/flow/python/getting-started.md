# Быстрый старт в {{product-name}} Flow (Python)

Поддержка вычислений на Python во Flow реализуется через механизм [компаньонов](../../../flow/concepts/glossary.md#companion). Python-код выполняется в отдельном gRPC-процессе, который взаимодействует с C++ [воркером](../../../flow/concepts/glossary.md#worker).

[Исходный код Python SDK для Flow]({{source-root}}/yt/yt/flow/library/python/companion)

[Примеры]({{source-root}}/yt/yt/flow/examples/python)

## Архитектура приложения {#architecture}

Любой [пайплайн](../../../flow/concepts/glossary.md#pipeline) Flow состоит из трёх составных частей:
- `Runner` — запускает пайплайн и устанавливает новую версию [спеки](../../../flow/concepts/glossary.md#spec-and-dynamic-spec).
- `Controller` — управляет работой пайплайна.
- `Worker` — выполняет непосредственно обработку данных.

## Pipeline API {#pipeline-api}

Python SDK предоставляет единый подход для настройки компаньона — класс `Pipeline`. Он позволяет зарегистрировать [компьютейшены](../../../flow/concepts/glossary.md#stream-and-computation) и запустить gRPC-сервер компаньона:

```python
from yt.yt.flow.library.python.companion import Pipeline

pipeline = Pipeline()
pipeline.add("mapper", WordCountMapper())
pipeline.run()
```

Метод `pipeline.add(computation_id, function)` регистрирует функцию обработки для компьютейшена с указанным идентификатором. Идентификатор должен совпадать с `computation_id` в [спеке](../../../flow/concepts/glossary.md#spec-and-dynamic-spec) пайплайна.

## Computation и SourceComputation {#computation-and-source}

Для создания [компьютейшена](../../../flow/concepts/glossary.md#stream-and-computation) на Python необходимо выбрать подходящий режим регистрации, соответствующий [типу Computation-а в C++](../../../flow/concepts/companion.md#vidy-computation-ov-dlya-raboty-s-kompanonami):

- `pipeline.add(id, fn)` — для `TTransformCompanionComputation` и `TSwiftMapCompanionComputation`.
- `pipeline.add(id, fn, source=True)` — для `TSwiftOrderedSourceCompanionComputation`.

```python
# SourceComputation для чтения данных из источника
pipeline.add("reader", EventMapper(), source=True)

# Computation для обработки данных
pipeline.add("mapper", WordCountMapper())
```

У `pipeline.add()` два обязательных параметра:
- **computation_id** — по нему происходит маппинг запросов между [воркером](../../../flow/concepts/glossary.md#worker) и компаньоном.
- **fn** — функция с логикой обработки [сообщений](../../../flow/concepts/glossary.md#message). Может быть экземпляром `RowFunction`, `BatchFunction`, обычной функцией или классом с методом `on_message`/`on_messages`.

Фильтрация сообщений в source-компьютейшенах выполняется через флаг [distribute](../../../flow/python/distribute.md) при эмите сообщения из Process Function.

## Process Function {#process-function}

Есть два вида ProcessFunction:

- `RowFunction` — получает [сообщения](../../../flow/concepts/glossary.md#message) и [таймеры](../../../flow/concepts/glossary.md#timer) по одному, предоставляет методы `on_message` и `on_timer`.
- `BatchFunction` — получает весь батч сообщений и таймеров, предоставляет методы `on_messages` и `on_timers`.

Подробнее — в разделе [Computation (Python)](../../../flow/python/computation.md).

## Фильтрация сообщений {#message-filtering}

Чтобы отфильтровать сообщение в SourceComputation, эмитьте его с `output.add_message(message, distribute=False)` — оно не будет опубликовано дальше по графу, но останется учтённым при оценке watermark.

Подробнее — в разделе [Флаг distribute (Python)](../../../flow/python/distribute.md).

## Node companion {#node-companion}

Точка входа в Python-компаньон — файл `__main__.py`. В нём необходимо сконфигурировать компьютейшены через `Pipeline` и вызвать `pipeline.run()`. Функция `main` из [WordCount](../../../flow/python/examples/wordcount.md) (в самом файле она вызывается из стандартного `if __name__ == "__main__":`):

{% code '/yt/yt/flow/examples/python/word_count/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

Если пользовательским функциям нужны дополнительные ресурсы (словарь, кэш и т.п.), `main` — подходящее место для их создания.

У `pipeline.run()` два режима, которые выбираются автоматически по переменной окружения `YT_FLOW_COMPANION_CONFIG`:

- Переменная не задана — это запуск с хоста. `run()` обогащает спеку пайплайна (см. [Запуск пайплайна](#launch)) и передаёт управление `flow_server`.
- Переменная задана — `flow_server` уже запустил этот же бинарь в джобе в роли компаньона. `run()` поднимает gRPC-сервер компаньона.

Один и тот же бинарь, таким образом, и запускает пайплайн, и работает компаньоном внутри джобы — отдельно деплоить компаньон не нужно.

Также доступен декоратор `@pipeline.computation` для быстрой регистрации:

```python
pipeline = Pipeline()

@pipeline.computation("mapper")
def mapper(message, output, ctx):
    word = message.payload["word"]
    state = ctx.state("word-state", message)
    data = state.get_or_default({"word": word, "count": 0})
    data["count"] += 1
    state.set(data)
```

## CPU-параллелизм компаньона {#companion-process-count}

Python-компаньон умеет масштабироваться на несколько ядер CPU: при старте он форкается на `N` процессов-интерпретаторов, которые слушают **один и тот же** gRPC-порт с опцией `SO_REUSEPORT`. Ядро Linux раскидывает входящие RPC между ними, и каждый процесс обрабатывает свои батчи под собственным GIL — это даёт почти линейный прирост производительности на N ядрах. По умолчанию (`companion_process_count = 0`) фан-аут включается только при наличии конечной CPU-квоты cgroup; без явного лимита (`unlimited`/нечитаемо — типичная ситуация в dev/CI-контейнере) компаньон остаётся однопроцессным.

`N` берётся из `companion_process_count` в конфиге компаньона:

- `0` (по умолчанию) — автоматический подбор по CPU-квоте cgroup: `ceil(quota)`, ограниченный сверху 16. Без конечной квоты — `1` (без форков).
- `>0` — явное значение (также ограничивается сверху 16). `1` возвращает однопроцессное поведение (без форков, без `SO_REUSEPORT`).

{% note warning %}

Потребление памяти может вырасти пропорционально количеству интерпретаторов: каждый процесс держит собственную копию загруженного пайплайна и любых используемых в нём объектов. При увеличении `companion_process_count` соответствующим образом увеличьте `memory_limit` компаньона.

{% endnote %}

## Сборка с ya make {#build}

Проект с Python-компаньоном собирается через `ya make`. В `ya.make` файле необходимо указать зависимости на Python SDK:

```
PEERDIR(
    yt/yt/flow/library/python/companion
)
```

{% if audience == "internal" %}

Полный `__main__.py` рабочего пайплайна [`python_vanilla_shuffle`]({{source-root}}/yt/yt/flow/yandex/dev/pipelines/python_vanilla_shuffle) выглядит так:

{% code '/yt/yt/flow/yandex/dev/pipelines/python_vanilla_shuffle/__main__.py' lang='python' %}

{% endif %}

## Запуск пайплайна {#launch}

Собранный бинарь запускается командой:

```bash
./my_pipeline --config pipeline.yson --flow-bin <путь/к/flow_server>
```

Здесь происходит следующее:

- Python-бинарь (он линкует `library/python/companion`) читает `pipeline.yson`, обогащает спеку — прописывает в неё *самого себя* как Python-компаньон, который `flow_server` доставит в джобу, — и записывает расширенный конфиг.
- После этого он через `execv` передаёт управление указанному `flow_server` (`flow_server --config <расширенный конфиг>`).

`flow_server` передаётся явно через `--flow-bin` и не встроен в Python-бинарь: так пайплайн остаётся лёгким, а версию `flow_server` выбирает тот, кто запускает пайплайн. Собрать оба бинаря можно одной командой:

```bash
cd yt/yt/flow
ya make yandex/dev/pipelines/python_vanilla_shuffle bin/flow_server
```

Весь запуск выполняет именно `flow_server`: он валидирует спеку, при необходимости создаёт vanilla-операцию, **устанавливает спеку пайплайна** (`set-pipeline-specs`) и стартует пайплайн. Python-сторона только *строит* и обогащает спеку и никогда не устанавливает её напрямую.

### Блок `vanilla` {#vanilla}

Если в `pipeline.yson` задан блок `vanilla` с `enable = %true`, `flow_server` запускает пайплайн как одну YT vanilla-операцию (контроллер + воркеры) и доставляет Python-бинарь в джобу как компаньон. Это запуск «одной кнопкой» — отдельно поднятый `flow_server` не нужен.

```yson
{
    "cluster_url" = "{{flow-example-cluster}}";
    "path" = "//home/flow-dev/python-vanilla-shuffle/pipeline";
    "spec" = { ... };
    "vanilla" = {
        "enable" = %true;
        "pool" = "yt-dev";
        "controller" = {
            "count" = 1;
            "cpu_limit" = 4;
            "memory_limit" = 12884901888;
        };
        "worker" = {
            "count" = 5;
            "cpu_limit" = 4;
            "memory_limit" = 12884901888;
        };
    };
}
```

Обязательные параметры: `pool` и `worker.count`. Для остальных полей (`cpu_limit`, `memory_limit`, число контроллеров и т. д.) есть разумные значения по умолчанию — полный список полей и их описание см. в [TVanillaConfig](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaConfig) и [TVanillaTaskConfig](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_TVanillaTaskConfig).

### Обновление спеки запущенного пайплайна {#release}

`flow_server` — единственный компонент, который устанавливает спеку пайплайна; Python-сторона спеку только строит. Поэтому процесс выкатки изменений в уже запущенный пайплайн такой:

1. Пересобрать Python-бинарь (`ya make ...`).
2. Снова запустить `./my_pipeline --config pipeline.yson --flow-bin <flow_server>`.

`flow_server` заново установит спеку и стартует пайплайн. Для vanilla-запуска используется стратегия make-before-break: новая операция подготавливается (бинарь загружается в YT-кэш), пока старая операция продолжает работать, после чего происходит переключение — старая операция завершается, и стартует уже подготовленная новая. Способом завершения старой операции управляет переменная окружения `YT_FLOW_GRACEFUL_UPDATE`: `1` (по умолчанию) — пайплайн останавливается (`stop`), `0` — ставится на паузу (`pause`).

## См. также

- [Computation (Python)](../../../flow/python/computation.md)
- [Работа со стейтами (Python)](../../../flow/python/state.md)
- [Примеры](../../../flow/python/examples/wordcount.md)
- [Companion](../../../flow/concepts/companion.md)
