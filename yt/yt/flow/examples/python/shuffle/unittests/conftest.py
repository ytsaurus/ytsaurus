import pytest

from yt.yt.flow.library.python.companion.test_harness import ComputationHarness, schema
from yt.yt.flow.examples.python.shuffle.event_mapper import EventMapper
from yt.yt.flow.examples.python.shuffle.event_reducer import EventReducer

EVENT_SCHEMA = schema(value="string", key_a="string", key_b="string", key_c="string", key_d="string")


@pytest.fixture
def mapper_harness():
    return ComputationHarness(
        EventMapper(),
        streams={"input": schema(data="string"), "event": EVENT_SCHEMA},
        source=True,
    )


@pytest.fixture
def reducer_harness():
    return ComputationHarness(
        EventReducer(),
        streams={"event": EVENT_SCHEMA},
        external_states={"/shuffle-state": schema(count="int64")},
        key_schema=schema(key_a="string", key_b="string"),
    )
