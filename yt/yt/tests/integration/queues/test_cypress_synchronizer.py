import pytest

from yt_commands import authors, create, select_rows

from yt_queue_agent_test_base import TestQueueAgentBase


class TestCypressSynchronizer(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @authors("panesher")
    @pytest.mark.parametrize(
        ("schema", "is_multi_consumer"),
        [
            (
                [
                    {"name": "queue_consumer_name", "type": "string", "sort_order": "ascending"},
                    {"name": "test", "type": "string"},
                    {"name": "key", "type": "string"},
                ],
                True,
            ),
            (
                [
                    {"name": "some_other", "type": "string", "sort_order": "ascending"},
                    {"name": "test", "type": "string"},
                ],
                False,
            ),
        ],
    )
    def test_detect_multi_consumer(self, schema, is_multi_consumer):
        path = "//tmp/consumer"
        create(
            "table",
            path,
            attributes={
                "dynamic": True,
                "schema": schema,
                "treat_as_queue_consumer": True,
            },
        )

        self._wait_for_component_passes()
        rows = select_rows("* from [//sys/queue_agents/consumers]")
        assert len(rows) == 1
        assert rows[0]["cluster"] == "primary"
        assert rows[0]["path"] == path
        assert rows[0]["is_multi_consumer"] == is_multi_consumer
        has_queue_consumer_name = rows[0]["schema"][0][0]["name"] == "queue_consumer_name"
        assert has_queue_consumer_name == is_multi_consumer
