import pytest

from yt.yt.flow.library.python.companion.test_harness import ComputationHarness, schema
from yt.yt.flow.examples.python.wait_click_join.join_process_function import JoinProcessFunction


@pytest.fixture
def harness():
    return ComputationHarness(
        JoinProcessFunction(),
        streams={
            "hit": schema(hit_id="string", hit_time="uint64", hit_payload="string"),
            "action": schema(hit_id="string", hit_time="uint64", action_time="uint64", is_click="bool"),
            "joined_action": schema(
                hit_id="string",
                hit_time="uint64",
                show_time="uint64",
                hit_payload="string",
                is_click="bool",
                click_time="uint64",
            ),
        },
        external_states={
            "/join-state": schema(hit_payload="string", show_time="uint64", click_time="uint64"),
        },
        key_schema=schema(hit_id="string", hit_time="uint64"),
        parameters={"wait_for_actions": "10s"},
    )
