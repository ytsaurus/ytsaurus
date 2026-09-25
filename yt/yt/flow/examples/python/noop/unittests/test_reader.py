from yt.yt.flow.library.python.companion.test_harness import ComputationHarness, schema
from yt.yt.flow.examples.python.noop.reader import Reader


def test_emits_nothing():
    harness = ComputationHarness(Reader(), streams={"random": schema(key="string", data="string")}, source=True)
    msg = harness.build_message("random", key="k", data="d")

    with harness.processing([msg]) as r:
        assert r.messages == []
