import pytest

from yt.yt.flow.library.python.companion.test_harness import ComputationHarness, schema
from yt.yt.flow.examples.python.word_count.word_count_mapper import WordCountMapper


@pytest.fixture
def harness():
    return ComputationHarness(
        WordCountMapper(),
        streams={"words": schema(word="string")},
        key_schema=schema(word="string"),
        internal_states={"word-state"},
    )
