# Testing in {{product-name}} Flow (Python)

{% note info %}

This page describes **unit testing** of Python-[pipeline](../../../flow/concepts/glossary.md#pipeline) components, as well as **integration testing** of the full pipeline via `FlowTestPythonBase`.

{% endnote %}

## General testing architecture {#architecture}

In production, the C++ [worker](../../../flow/concepts/glossary.md#worker) sends gRPC requests to the [companion](../../../flow/concepts/companion.md), passing [messages](../../../flow/concepts/glossary.md#message), timers, [states](../../../flow/concepts/glossary.md#state), and [watermarks](../../../flow/concepts/watermarks.md). The companion calls `Computation.do_process()`, which delegates processing to `ProcessFunction`.

In unit tests, pipeline components are tested by directly calling `do_process()` on the `Computation` object with a prepared `RequestContext`.

All tests use the standard pytest.

## Dependencies {#dependencies}

To unit test the companion library, you need to add dependencies to `ya.make`:

```
PEERDIR(
    yt/yt/flow/library/python/companion
)
```

If you’re using Protobuf states, also add a dependency on the proto library.

## Testing ProcessFunction {#testing-process}

[Test source code]({{source-root}}/yt/yt/flow/library/python/companion/test/test_computation.py)

To test `RowFunction` / `BatchFunction`, you create a `Computation` object and call `do_process()` with a prepared `RequestContext`.

### Creating RequestContext

`RequestContext` is a dataclass that contains the input data for processing:

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
    """Create a minimal RequestContext for tests."""
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

### Testing RowFunction

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

### Testing timers

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
    # The message creates a timer, the timer creates a message
    assert any(r.timers for r in results)
    assert any(r.messages for r in results)
```

## Testing states {#testing-states}

[Test source code]({{source-root}}/yt/yt/flow/library/python/companion/test/test_context.py)

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
# Proto definition is common for Java and Python examples
from yt.yt.flow.yandex.extensions.logbroker.examples.java.lb_wait_click_join.proto.message_pb2 import TJoinState


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

## Analyzing the response {#analyzing-response}

The `ResponseContext` object returned by `do_process()` contains:

| Field | Type | Description |
|------|------|-------------|
| `transform_results` | `List[TransformResult]` | List of processing results |
| `internal_states` | `Dict[str, StatesHolder]` | Internal states after processing |
| `external_states` | `Dict[str, StatesHolder]` | External states after processing |

Each `TransformResult` contains:

| Field | Type | Description |
|------|------|-------------|
| `parent_ids` | `List[str]` | IDs of parent messages |
| `messages` | `List[Message]` | Output messages |
| `timers` | `List[NewTimer]` | Created timers |

## End-to-end testing with FlowTestPythonBase {#e2e-tests}

Use the `FlowTestPythonBase` base class for full end-to-end pipeline testing (with real C++ workers, queues, and streams).

### Dependencies {#integration-dependencies}

In addition to `PEERDIR` for `integration_test_base`, the integration test needs a cluster recipe, `DEPENDS` for the pipeline binary and `flow_server`, and `DATA` with the spec. The complete `ya.make` for the test from [WordCount](../../../flow/python/examples/wordcount.md):

{% code '/yt/yt/flow/examples/python/word_count/test/ya.make' lang='text' %}

### Setup {#python-test-setup}

The test inherits from `FlowTestPythonBase` and sets the `PYTHON_COMPANION_BINARY` attribute:

{% code '/yt/yt/flow/examples/python/word_count/test/test_wordcount.py' lang='python' lines='[BEGIN test_setup]-[END test_setup]' %}

| Attribute | Description |
|----------|-------------|
| `PYTHON_COMPANION_BINARY` | Path to the Python companion binary |

[Example of an E2E WordCount test]({{source-root}}/yt/yt/flow/examples/python/word_count/test/test_wordcount.py)

{% if audience == "internal" %}[Example of an E2E lb_wait_click_join test]({{source-root}}/yt/yt/flow/yandex/extensions/logbroker/examples/python/lb_wait_click_join/test/test_lb_wait_click_join.py){% endif %}

{% note warning %}

Integration tests require a deployed {{product-name}} cluster and are run with `ya make -tt`. For fast iteration, use the unit tests described above.

{% endnote %}

{% include notitle [_](../testing-integration-body.md) %}

{% include notitle [_](../testing-test-param-body.md) %}

## Speeding up iteration: `--ext-py` {#ext-py}

The `--ext-py` flag avoids linking the binary on every `ya make` run if the changes from the previous run affected only `*.py` files:

```bash
ya make -A --ext-py
```

## See also

- [Computation (Python)](../../../flow/python/computation.md)
- [Working with states (Python)](../../../flow/python/state.md)
- [Distribute flag (Python)](../../../flow/python/distribute.md)
- [Testing (Java)](../../../flow/java/testing.md)
- If you're extending Flow itself — [Pipeline testing framework](../../../flow/contributor/testing-framework.md).
