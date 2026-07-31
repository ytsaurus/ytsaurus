# Computation в {{product-name}} Flow (Python)

{% note info %}

На этой странице описаны Python-специфичные детали работы с компьютейшенами. Общие концепции описаны в разделе [Computation](../../../flow/concepts/computation.md).

{% endnote %}

## Типы Computation {#computation-types}

Во Flow есть два вида `Computation`: [`Swift`](../../../flow/concepts/glossary.md#swift) и `Transform`. От их выбора зависит способ обеспечения exactly-once гарантий и то, какие преобразования возможно реализовать с их применением.

| Тип | Способ обеспечения гарантий | Применение |
|-----|-----------------------------|------------|
| `Swift`| Код преобразования детерминирован, при необходимости будет вызываться повторно | Stateless преобразования |
| `Transform` | Результат работы обязательно сохраняется в YT, поэтому нет требований на какую-либо детерминированность преобразований | Stateful преобразования [Подробнее](../../../flow/concepts/stateful.md) |

При использовании [компаньона](../../../flow/concepts/glossary.md#companion) выбор `Swift` или `Transform` осуществляется через указание `computation_class_name` в статической [спеке](../../../flow/concepts/glossary.md#spec-and-dynamic-spec):
- `NYT::NFlow::NCompanion::TTransformCompanionComputation` — для `Transform`.
- `NYT::NFlow::NCompanion::TSwiftMapCompanionComputation` — для `Swift`.

## Создание Computation {#computation}

В Python компьютейшен создаётся через `Pipeline.add()` и регистрируется в `PipelineContext` автоматически. Пример из [WordCount](../../../flow/python/examples/wordcount.md):

{% code '/yt/yt/flow/examples/python/word_count/__main__.py' lang='python' lines='[BEGIN main]-[END main]' %}

{% note warning %}

`process_function=None` недопустим: компьютейшены без бизнес-логики в Python не регистрируют. Если нужен [passthrough](../../../flow/concepts/glossary.md#passthrough) — не регистрируйте компьютейшен в Python вовсе, а в статической спеке укажите C++-класс passthrough в `computation_class_name` (см. [Passthrough Computation](../../../flow/concepts/computation.md#passthrough)).

{% endnote %}

В статической спеке создаётся Computation с таким же `id` (в данном примере `mapper`):
```yson
"mapper" = {
    "computation_class_name" = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
    "group_by_schema" = [
        ...
    ];
    "input_stream_ids" = [...];
    "output_stream_ids" = [...];
    "required_resource_ids" = {
        "CompanionManager" = {
            "worker" = true;
            "controller" = false;
        };
    };
    "parameters" = {
        ...
    };
};
```

Подробнее про спеку в разделе [Spec, DynamicSpec и Config](../../../flow/concepts/spec.md).

## SourceComputation {#sourcecomputation}

`SourceComputation` — вершина в графе [пайплайна](../../../flow/concepts/glossary.md#pipeline), осуществляющая чтение данных из внешних источников. Подробнее про [Source Computation](../../../flow/concepts/computation.md#tswiftorderedsourcecomputation).

В Python `SourceComputation` создаётся через передачу `source=True` в `Pipeline.add()`. Фильтрация [сообщений](../../../flow/concepts/glossary.md#message) выполняется внутри Process Function через флаг [distribute](../../../flow/python/distribute.md).

### Параметры

| Параметр | Обязательный | Описание |
|----------|:---:|----------|
| `computation_id` | Да | Уникальный идентификатор |
| `fn` (process function) | Да | Функция обработки сообщений |

### Создание SourceComputation

```python
pipeline.add("reader", MyParsingFunction(), source=True)
```

Для passthrough Source не используйте Python — укажите в спеке `NYT::NFlow::TSwiftPassthroughOrderedSourceComputation` в `computation_class_name` и оставьте компьютейшен незарегистрированным в Python-компаньоне. Подробнее — [Passthrough Computation](../../../flow/concepts/computation.md#passthrough).

### Взаимодействие с Worker {#companion-info}

При инициализации [Worker](../../../flow/concepts/glossary.md#worker) запрашивает у Python-компаньона информацию о зарегистрированных объектах `Computation` и `SourceComputation`. Каждое входное сообщение `TSwiftOrderedSourceCompanionComputation` отправляет в Python-компаньон, который применяет к нему `ProcessFunction` и возвращает результат. Worker выполняет один запрос к компаньону на каждое сообщение.

## Process Function {#process-function}

Бизнес-логика обработки данных реализуется через Process Function. Для реализации необходимо выбрать один из двух базовых классов: [RowFunction]({{source-root}}/yt/yt/flow/library/python/companion/computation.py) или [BatchFunction]({{source-root}}/yt/yt/flow/library/python/companion/computation.py).

{% note info %}

Использование `RowFunction` или `BatchFunction` — исключительно вопрос бизнес-логики. `RowFunction` не добавляет накладных расходов на обработку данных относительно использования `BatchFunction` благодаря тому, что Flow внутри себя осуществляет передачу данных батчами.

{% endnote %}

### RowFunction {#row-function}

`RowFunction` получает [сообщения](../../../flow/concepts/glossary.md#message) и [таймеры](../../../flow/concepts/glossary.md#timer) по одному. Класс предоставляет два метода:

- `on_message(message, output, ctx)` — вызывается для каждого входного сообщения.
- `on_timer(timer, output, ctx)` — вызывается при срабатывании таймера (опционально).

#### Пример stateless-функции

```python
from yt.yt.flow.library.python.companion.computation import RowFunction


class X2Mapper(RowFunction):
    def on_message(self, message, output, ctx):
        builder = ctx.message_builder("x2_numbers")        # 1
        number = message.payload["number"]                  # 2
        builder.set("number_x2", number * 2)                # 3
        output.add_message(builder.finish())                # 4
```

Разберём построчно:

1. `ctx.message_builder("x2_numbers")` — создаётся `MessageBuilder` для output-[стрима](../../../flow/concepts/glossary.md#stream-and-computation) с id = `x2_numbers`. Стрим с таким идентификатором должен присутствовать в списке `output_stream_ids` в статической [спеке](../../../flow/concepts/glossary.md#spec-and-dynamic-spec) компьютейшена.

2. `message.payload["number"]` — получаем значение поля `number` из входящего сообщения. Payload поддерживает dict-like доступ к полям.

3. `builder.set("number_x2", number * 2)` — записываем значение в поле `number_x2`. Это поле должно присутствовать в схеме стрима `x2_numbers` в статической спеке.

4. `output.add_message(builder.finish())` — метод `finish()` возвращает готовое сообщение и сбрасывает билдер для повторного использования. Сообщение добавляется в `OutputCollector`.

### BatchFunction {#batch-function}

`BatchFunction` получает весь список сообщений и таймеров, пришедших от [воркера](../../../flow/concepts/glossary.md#worker). Класс предоставляет два метода:

- `on_messages(messages, output, ctx)` — вызывается для батча сообщений.
- `on_timers(timers, output, ctx)` — вызывается для батча таймеров (опционально).

#### Пример batch-функции

```python
from yt.yt.flow.library.python.companion.computation import BatchFunction


class X2BatchMapper(BatchFunction):
    def on_messages(self, messages, output, ctx):
        builder = ctx.message_builder("x2_numbers")         # 1
        for message in messages:                             # 2
            number = message.payload["number"]               # 3
            builder.set("number_x2", number * 2)             # 4
            output.add_message(builder.finish())             # 5
```

Ключевое отличие от `RowFunction`:

- `MessageBuilder` создаётся один раз за весь батч (строка 1).
- Метод `finish()` одновременно с возвратом готового сообщения сбрасывает `MessageBuilder` в исходное состояние, после чего его можно переиспользовать для следующего сообщения (строка 5).

## Фильтрация сообщений {#message-filtering}

Для фильтрации сообщений в source-компьютейшенах используется per-message-флаг [distribute](../../../flow/python/distribute.md): сообщение эмитится из Process Function с `distribute=False` и не публикуется дальше по графу, но учитывается при оценке [watermark](../../../flow/concepts/watermarks.md).

## Регистрация в Pipeline {#pipeline-registration}

Все компьютейшены регистрируются через `Pipeline.add()`. Внутри `Pipeline` использует `PipelineContext` для хранения и управления зарегистрированными объектами.

```python
from yt.yt.flow.library.python.companion import Pipeline

pipeline = Pipeline()

# Transform-компьютейшен
pipeline.add("computation_id", my_function)

# Source-компьютейшен
pipeline.add("reader", my_function, source=True)
```

Также доступен декоратор `@pipeline.computation`:

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

{% note warning %}

Каждый Computation должен иметь уникальный идентификатор, соответствующий идентификаторам в статической спеке. Попытка зарегистрировать Computation с уже существующим идентификатором приведёт к ошибке и невозможности старта компаньона.

{% endnote %}

## RuntimeContext {#runtime-context}

[Исходный код]({{source-root}}/yt/yt/flow/library/python/companion/context.py)

`RuntimeContext` (`ctx`) предоставляет доступ к контексту выполнения компьютейшена. Основные методы:

| Метод | Описание |
| --- | --- |
| `ctx.message_builder(stream_id)` | Создать `MessageBuilder` для указанного output-[стрима](../../../flow/concepts/glossary.md#stream-and-computation) |
| `ctx.parameters` | Словарь параметров компьютейшена из спеки |
| `ctx.min_watermark` | Минимальный [вотермарк](../../../flow/concepts/glossary.md#timestamps-and-watermarks) по всем входным стримам |
| `ctx.watermark(stream_id)` | [Вотермарк](../../../flow/concepts/glossary.md#timestamps-and-watermarks) конкретного стрима (`int` или `None`) |
| `ctx.state(name, message)` | Получить YSON-[стейт](../../../flow/concepts/glossary.md#state), привязанный к ключу сообщения |
| `ctx.raw_state(name, message)` | Получить стейт в виде сырых байтов |
| `ctx.proto_state(name, message, ProtoClass)` | Получить Protobuf-стейт |
| `ctx.external_state(name, message)` | Получить внешний стейт |

Подробнее про работу со стейтами — в разделе [Работа со стейтами (Python)](../../../flow/python/state.md).

### MessageBuilder {#message-builder}

Для создания выходных сообщений используется `MessageBuilder`:

```python
builder = ctx.message_builder("stream_id")
builder.set("field_name", value)
message = builder.finish()
output.add_message(message)
```

Метод `finish()` возвращает готовый объект `Message` и сбрасывает билдер для повторного использования. Поле `stream_id` должно присутствовать в списке `output_stream_ids` в статической [спеке](../../../flow/concepts/glossary.md#spec-and-dynamic-spec) компьютейшена.

### Параметры компьютейшена {#parameters}

```python
wait_for_actions = ctx.parameters["wait_for_actions"]
```

Словарь `ctx.parameters` содержит параметры, указанные в статической спеке компьютейшена.

### Вотермарки {#watermarks}

```python
# Минимальный вотермарк по всем входным стримам
min_wm = ctx.min_watermark

# Вотермарк конкретного стрима (int или None)
stream_wm = ctx.watermark("stream_id")
```

## OutputCollector {#output-collector}

[Исходный код]({{source-root}}/yt/yt/flow/library/python/companion/computation.py)

`OutputCollector` используется для отправки результатов обработки:

| Метод | Описание |
| --- | --- |
| `output.add_message(message)` | Добавить выходное сообщение (объект `Message`, полученный через `builder.finish()`) |
| `output.add_timer(trigger_timestamp, event_timestamp, stream_id)` | Добавить [таймер](../../../flow/concepts/glossary.md#timer) с указанным временем срабатывания |
| `output.set_parent_ids(parent_ids)` | Задать parent ID для отслеживания [lineage](../../../flow/concepts/lineage.md) сообщений. Возвращает новый `OutputCollector` |

Пример создания выходного сообщения и таймера:

```python
def on_message(self, message, output, ctx):
    # Создание выходного сообщения
    builder = ctx.message_builder("output_stream")
    builder.set("field", value)
    output.add_message(builder.finish())

    # Создание таймера
    output.add_timer(trigger_timestamp=1000, event_timestamp=500)
```

## ExtendedMessage {#extended-message}

Входящее [сообщение](../../../flow/concepts/glossary.md#message) (`ExtendedMessage`) содержит:
- `message.payload` — Payload с dict-like доступом к полям: `message.payload["field"]`.
- `message.stream_id` — идентификатор входного [стрима](../../../flow/concepts/glossary.md#stream-and-computation) (`str`).
- `message.key` — [ключ](../../../flow/concepts/glossary.md#key) сообщения (Payload) из `group_by_schema`: `message.key["field"]`.
- `message.event_timestamp` — event timestamp сообщения (`int`).

## Timer {#timer}

Объект [таймера](../../../flow/concepts/glossary.md#timer) (`Timer`) содержит:
- `timer.key` — [ключ](../../../flow/concepts/glossary.md#key) таймера (Payload): `timer.key["field"]`.
- `timer.stream_id` — идентификатор стрима таймера (`str`).
- `timer.trigger_timestamp` — время срабатывания (`int`).
- `timer.event_timestamp` — event timestamp (`int`).

## Конфигурация ресурса CompanionManager {#companion-manager}

Для запуска Python-компаньона необходимо объявить ресурс `CompanionManager` в статической спеке:

```yson
"CompanionManager" = {
    "resource_class_name" = "NYT::NFlow::NCompanion::TCompanionManager";
    "parameters" = {
        "run_process" = %true;
        "entrypoint" = {
            "executable" = "./py_companion";
        };
    };
    "dependencies" = {};
};
```

Параметр `resource_class_name` указывает на класс ресурса, который будет осуществлять запуск компаньона.
В случае Python-компаньона `resource_class_name` всегда должен быть `NYT::NFlow::NCompanion::TCompanionManager`.

Процесс компаньона описывается параметром `entrypoint` (`executable`, `args`, `env`); при `run_process = %false` Flow ожидает, что компаньон запущен снаружи (так делают, например, интеграционные тесты). При [запуске пайплайна с хоста](../../../flow/python/getting-started.md#launch) через `pipeline.run()` заполнять `entrypoint` вручную не нужно: Python-бинарь сам прописывает `entrypoint = {"executable" = "./py_companion"}` и `run_process = %true`, а `flow_server` доставляет бинарь в джобу под этим именем.

Ключевое отличие от Java-конфигурации: для Java существует отдельный класс ресурса `NYT::NFlow::NCompanion::TJavaCompanionManager` с параметрами `jdk_bin_path`, `classpath` и `main_class`, тогда как Python-компаньон использует общий `TCompanionManager` с параметром `entrypoint`.

Подробнее про спеку в разделе [Spec, DynamicSpec и Config](../../../flow/concepts/spec.md).

## См. также

- [Computation (концепция)](../../../flow/concepts/computation.md)
- [Работа со стейтами (Python)](../../../flow/python/state.md)
- [Быстрый старт (Python)](../../../flow/python/getting-started.md)
- [Companion](../../../flow/concepts/companion.md)
