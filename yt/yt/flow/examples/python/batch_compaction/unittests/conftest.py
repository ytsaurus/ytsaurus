import pytest

from yt.yt.flow.library.python.companion.test_harness import ComputationHarness, schema
from yt.yt.flow.examples.python.batch_compaction.compaction_mapper import EventCompactor


@pytest.fixture
def harness():
    return ComputationHarness(
        EventCompactor(),
        streams={
            "events": schema(word="string", count="int64"),
            "compacted": schema(word="string", count="int64"),
        },
        key_schema=schema(word="string"),
    )
