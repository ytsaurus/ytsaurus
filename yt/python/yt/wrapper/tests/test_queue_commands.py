from .conftest import authors

from yt.wrapper.driver import get_api_version

import yt.environment.init_queue_agent_state as init_queue_agent_state

import yt.wrapper as yt

from .helpers import TEST_DIR, wait

import pytest

CONSUMER_REGISTRATIONS = "//sys/queue_agents/consumer_registrations"


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestQueueCommands(object):
    def _sync_create_tablet_cell(self):
        cell_id = yt.create("tablet_cell", attributes={"size": 1})
        wait(lambda: yt.get("//sys/tablet_cells/{0}/@health".format(cell_id)) == "good")
        return cell_id

    def _create_dynamic_table(self, path, **attributes):
        if "schema" not in attributes:
            attributes.update({"schema": [
                {"name": "x", "type": "string", "sort_order": "ascending"},
                {"name": "y", "type": "string"}
            ]})
        attributes.update({"dynamic": True})
        yt.create("table", path, attributes=attributes)

    @authors("achulkov2")
    # This is a very basic test, just to check that there are no bugs in the client api implementation.
    # Tests for actual logic can be found in tests/integration/queues.
    def test_register_queue_consumer(self):
        if get_api_version() != "v4":
            pytest.skip()

        self._sync_create_tablet_cell()

        queue = TEST_DIR + "/q"
        self._create_dynamic_table(queue)

        consumer = TEST_DIR + "/c"
        self._create_dynamic_table(consumer, schema=init_queue_agent_state.CONSUMER_TABLE_SCHEMA)

        yt.create("map_node", "//sys/queue_agents")
        self._create_dynamic_table(CONSUMER_REGISTRATIONS, schema=init_queue_agent_state.REGISTRATION_TABLE_SCHEMA)
        yt.mount_table(CONSUMER_REGISTRATIONS, sync=True)

        yt.register_queue_consumer(queue, consumer, vital=True)

        assert list(yt.select_rows("* from [//sys/queue_agents/consumer_registrations]")) == [
            {
                "queue_cluster": "primary", "queue_path": queue,
                "consumer_cluster": "primary", "consumer_path": consumer,
                "vital": True,
            }
        ]

        yt.unregister_queue_consumer(queue, consumer)

        assert list(yt.select_rows("* from [//sys/queue_agents/consumer_registrations]")) == []
