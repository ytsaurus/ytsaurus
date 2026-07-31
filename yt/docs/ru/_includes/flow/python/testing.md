# Тестирование в {{product-name}} Flow (Python)

{% note info %}

Данная страница описывает **юнит-тестирование** компонентов Python-[пайплайна](../../../flow/concepts/glossary.md#pipeline), а также **интеграционное тестирование** полного пайплайна через `FlowTestPythonBase`.

{% endnote %}

## Общая архитектура тестирования {#architecture}

В продакшене C++ [воркер](../../../flow/concepts/glossary.md#worker) отправляет gRPC-запросы [компаньону](../../../flow/concepts/companion.md), передавая [сообщения](../../../flow/concepts/glossary.md#message), таймеры, [стейты](../../../flow/concepts/glossary.md#state) и [watermark-и](../../../flow/concepts/watermarks.md). Компаньон вызывает `Computation.do_process()`, который делегирует обработку в `ProcessFunction`.

В юнит-тестах компоненты пайплайна тестируются прямым вызовом `do_process()` на объекте `Computation` с подготовленным `RequestContext`.

Все тесты используют стандартный pytest.

## Зависимости {#dependencies}

Для юнит-тестирования компаньон-библиотеки необходимо добавить зависимости в `ya.make`:

```
PEERDIR(
    yt/yt/flow/library/python/companion
)
```

При использовании Protobuf-стейтов также добавьте зависимость на proto-библиотеку.

## Тестирование ProcessFunction {#testing-process}

[Исходный код тестов]({{source-root}}/yt/yt/flow/library/python/companion/test/test_computation.py)

Для тестирования `RowFunction` / `BatchFunction` создаётся объект `Computation` и вызывается `do_process()` с подготовленным `RequestContext`.

### Создание RequestContext

`RequestContext` — это dataclass, содержащий входные данные для обработки:

```python
from yt.yt.flow.library.python.companion.context import RequestContext
from yt.yt.flow.library.python.companion.job import Job
from yt.yt.flow.library.python.companion.stream import (
    StreamIdsMapping,
    StreamSpecs,
    RawStream,
)
from yt.yt.flow.library.python.companion.row import ExtendedMessage


def make_request_ctx(messages=None, timers=None):
    """Создание минимального RequestContext для тестов."""
    mapping = StreamIdsMapping({"input": 0, "output": 1})
    specs = StreamSpecs(mapping, [RawStream("input"), RawStream("output")])

    job = Job(
        job_id="test-job",
        computation_id="test-comp",
        stream_specs=specs,
        static_spec={},
    )

    return RequestContext(
        job_id="test-job",
        request_id="test-req",
        computation_id="test-comp",
        messages=messages or [],
        timers=timers or [],
        stream_specs=specs,
        job=job,
    )
```

### Тестирование RowFunction

```python
from yt.yt.flow.library.python.companion.computation import (
    Computation,
    RowFunction,
)
from yt.yt.flow.library.python.companion.row import (
    ExtendedMessage,
    Message,
)


class PassthroughFunction(RowFunction):
    def on_message(self, message, output, ctx):
        output.add_message(
            Message(message_id=message.message_id, stream_id="output")
        )


def test_passthrough():
    comp = Computation(
        computation_id="test",
        process_function=PassthroughFunction(),
    )
    messages = [
        ExtendedMessage(message_id="m1", stream_id="input"),
        ExtendedMessage(message_id="m2", stream_id="input"),
    ]
    ctx = make_request_ctx(messages=messages)
    response = comp.do_process(ctx)

    assert len(response.transform_results) == 2
    assert response.transform_results[0].messages[0].message_id == "m1"
    assert response.transform_results[1].messages[0].message_id == "m2"
```

### Тестирование таймеров

```python
from yt.yt.flow.library.python.companion.row import Timer


class TimerFunction(RowFunction):
    def on_message(self, message, output, ctx):
        output.add_timer(trigger_timestamp=1000, event_timestamp=500)

    def on_timer(self, timer, output, ctx):
        output.add_message(
            Message(message_id="from-timer", stream_id="output")
        )


def test_timer_roundtrip():
    comp = Computation(
        computation_id="test",
        process_function=TimerFunction(),
    )
    messages = [ExtendedMessage(message_id="m1")]
    timers = [Timer(message_id="t1")]
    ctx = make_request_ctx(messages=messages, timers=timers)
    response = comp.do_process(ctx)

    results = response.transform_results
    # Сообщение создаёт таймер, таймер создаёт сообщение
    assert any(r.timers for r in results)
    assert any(r.messages for r in results)
```

## Тестирование стейтов {#testing-states}

[Исходный код тестов]({{source-root}}/yt/yt/flow/library/python/companion/test/test_context.py)

### YSON State

```python
import yt.type_info as ti

from yt.yt.flow.library.python.companion.context import DefaultRuntimeContext
from yt.yt.flow.library.python.companion.row import (
    ColumnSchema,
    ExtendedMessage,
    Payload,
    TableSchema,
)
from yt.yt.flow.library.python.companion.stream import (
    StreamIdsMapping,
    StreamSpecs,
    RawStream,
)
from yt.yt.flow.library.python.companion.wire_protocol import (
    ColumnValueType,
    UnversionedRow,
    UnversionedValue,
)


KEY_SCHEMA = TableSchema([ColumnSchema("id", ti.String)])


def make_key_payload():
    row = UnversionedRow(values=[
        UnversionedValue(column_id=0, type=ColumnValueType.STRING, value=b"test-key"),
    ])
    return Payload(row, KEY_SCHEMA)


def make_ctx(internal_state_names=None, **kwargs):
    streams = [RawStream("input"), RawStream("output")]
    mapping = StreamIdsMapping({s.stream_id: i for i, s in enumerate(streams)})
    specs = StreamSpecs(mapping, streams)
    return DefaultRuntimeContext(
        internal_state_names=internal_state_names or set(),
        stream_specs=specs,
        internal_states=kwargs.get("internal_states", {}),
        external_states=kwargs.get("external_states", {}),
        watermarks={},
        min_watermark=0,
        computation_parameters={},
        key_schema=KEY_SCHEMA,
    )


def test_yson_state_roundtrip():
    ctx = make_ctx(internal_state_names={"word-state"})
    message = ExtendedMessage(message_id="m1", key=make_key_payload())

    accessor = ctx.state("word-state", message)
    accessor.set({"word": "hello", "count": 1})

    accessor2 = ctx.state("word-state", message)
    result = accessor2.get()
    assert result["word"] == "hello"
    assert result["count"] == 1
```

### Proto State

```python
# Proto-определение общее для Java и Python примеров
from yt.yt.flow.yandex.examples.java.lb_wait_click_join.proto.message_pb2 import TJoinState


def test_proto_state_roundtrip():
    ctx = make_ctx(internal_state_names={"join-state"})
    message = ExtendedMessage(message_id="m1", key=make_key_payload())

    accessor = ctx.proto_state("join-state", message, TJoinState)
    state = TJoinState()
    state.show_time = 42
    accessor.set(state)

    accessor2 = ctx.proto_state("join-state", message, TJoinState)
    assert accessor2.get().show_time == 42
```

### External State

```python
from yt.yt.flow.library.python.companion.state import StatesHolder
from yt.yt.flow.library.python.companion.row import PayloadBuilder


STATE_SCHEMA = TableSchema([
    ColumnSchema("count", ti.Int64),
    ColumnSchema("name", ti.String),
])


def test_external_state_roundtrip():
    ext_holder = StatesHolder("ext", KEY_SCHEMA, STATE_SCHEMA)
    ctx = make_ctx(external_states={"/shuffle-state": ext_holder})
    message = ExtendedMessage(message_id="m1", key=make_key_payload())

    state = ctx.external_state("/shuffle-state", message)
    builder = state.to_builder()
    builder.set("count", 99)
    state.set(builder.finish())

    state2 = ctx.external_state("/shuffle-state", message)
    assert state2.get("count") == 99
```

## Анализ результатов {#analyzing-response}

Объект `ResponseContext`, возвращаемый `do_process()`, содержит:

| Поле | Тип | Описание |
|------|-----|----------|
| `transform_results` | `List[TransformResult]` | Список результатов обработки |
| `internal_states` | `Dict[str, StatesHolder]` | Внутренние стейты после обработки |
| `external_states` | `Dict[str, StatesHolder]` | Внешние стейты после обработки |

Каждый `TransformResult` содержит:

| Поле | Тип | Описание |
|------|-----|----------|
| `parent_ids` | `List[str]` | Идентификаторы родительских сообщений |
| `messages` | `List[Message]` | Выходные сообщения |
| `timers` | `List[NewTimer]` | Созданные таймеры |

## Интеграционное тестирование с FlowTestPythonBase {#e2e-tests}

Для полного интеграционного тестирования пайплайна (с реальными C++ воркерами, очередями и стримами) используется базовый класс `FlowTestPythonBase`.

### Зависимости {#integration-dependencies}

Помимо `PEERDIR` на `integration_test_base`, интеграционному тесту нужны рецепт кластера, `DEPENDS` на бинарь пайплайна и `flow_server`, а также `DATA` со спекой. Полный `ya.make` теста из [WordCount](../../../flow/python/examples/wordcount.md):

{% code '/yt/yt/flow/examples/python/word_count/test/ya.make' lang='text' %}

### Настройка {#python-test-setup}

Тест наследуется от `FlowTestPythonBase` и задаёт атрибут `PYTHON_COMPANION_BINARY`:

{% code '/yt/yt/flow/examples/python/word_count/test/test_wordcount.py' lang='python' lines='[BEGIN test_setup]-[END test_setup]' %}

| Атрибут | Описание |
|---------|----------|
| `PYTHON_COMPANION_BINARY` | Путь к бинарю Python-компаньона |

[Пример E2E-теста WordCount]({{source-root}}/yt/yt/flow/examples/python/word_count/test/test_wordcount.py)

{% if audience == "internal" %}[Пример E2E-теста lb_wait_click_join]({{source-root}}/yt/yt/flow/yandex/examples/python/lb_wait_click_join/test/test_lb_wait_click_join.py){% endif %}

{% note warning %}

Интеграционные тесты требуют развёрнутого кластера {{product-name}} и запускаются через `ya make -tt`. Для быстрой итерации используйте юнит-тесты, описанные выше.

{% endnote %}

{% include notitle [_](../testing-integration-body.md) %}

{% include notitle [_](../testing-test-param-body.md) %}

## Ускорение итерации: `--ext-py` {#ext-py}

Флаг `--ext-py` избавляет от линковки бинаря на каждый запуск `ya make`, если изменения с предыдущего запуска касались только `*.py` файлов:

```bash
ya make -A --ext-py
```

## См. также

- [Computation (Python)](../../../flow/python/computation.md)
- [Работа со стейтами (Python)](../../../flow/python/state.md)
- [Флаг distribute (Python)](../../../flow/python/distribute.md)
- [Тестирование (Java)](../../../flow/java/testing.md)
- Если дорабатываете сам Flow — [Фреймворк для тестирования пайплайнов](../../../flow/contributor/testing-framework.md).
