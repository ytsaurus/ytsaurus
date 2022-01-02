from yt_env_setup import YTEnvSetup

from yt_commands import (authors, get, ls, wait, assert_yt_error, create, sync_mount_table, sync_create_cells,
                         insert_rows, remove)

from yt.common import YtError

import copy

from yt.yson import YsonUint64

##################################################################


class TestQueueAgent(YTEnvSetup):
    NUM_QUEUE_AGENTS = 1

    DELTA_QUEUE_AGENT_CONFIG = {
        "cluster_connection": {
            # Disable cache.
            "table_mount_cache": {
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "expire_after_access_time": 0,
                "refresh_time": 0,
            },
        },
    }

    QUEUE_TABLE_SCHEMA = [
        {"name": "cluster", "type": "string", "sort_order": "ascending"},
        {"name": "path", "type": "string", "sort_order": "ascending"},
        {"name": "revision", "type": "uint64"},
        {"name": "object_type", "type": "string"},
        {"name": "dynamic", "type": "boolean"},
        {"name": "sorted", "type": "boolean"},
    ]

    def _queue_agent_orchid_path(self):
        agent_ids = ls("//sys/queue_agents/instances", verbose=False)
        assert len(agent_ids) == 1
        return "//sys/queue_agents/instances/" + agent_ids[0] + "/orchid"

    def _get_error_poll_index(self):
        return get(self._queue_agent_orchid_path() + "/queue_agent/latest_poll_error/attributes/poll_index")

    def _get_poll_index(self):
        return get(self._queue_agent_orchid_path() + "/queue_agent/poll_index")

    def _get_latest_poll_error(self):
        return YtError.from_dict(get(self._queue_agent_orchid_path() + "/queue_agent/latest_poll_error"))

    def _wait_fresh_poll_error(self):
        poll_index = self._get_error_poll_index()
        wait(lambda: self._get_error_poll_index() >= poll_index + 2)

    def _get_fresh_poll_error(self):
        self._wait_fresh_poll_error()
        return self._get_latest_poll_error()

    def _wait_fresh_poll(self):
        poll_index = self._get_poll_index()
        wait(lambda: self._get_poll_index() >= poll_index + 2)

    def _validate_no_poll_error(self):
        poll_index = self._get_poll_index()
        self._wait_fresh_poll()
        if self._get_error_poll_index() <= poll_index:
            return
        else:
            raise self._get_latest_poll_error()

    def _get_queues(self):
        return get(self._queue_agent_orchid_path() + "/queue_agent/queues")

    @authors("max42")
    def test_polling_loop(self):
        sync_create_cells(1)

        remove("//sys/queue_agents/queues", force=True)

        assert_yt_error(self._get_fresh_poll_error(), "has no child with key")

        wrong_schema = copy.deepcopy(self.QUEUE_TABLE_SCHEMA)
        wrong_schema[-1]["type"] = "int64"
        create("table", "//sys/queue_agents/queues", attributes={"dynamic": True, "schema": wrong_schema}, force=True)
        sync_mount_table("//sys/queue_agents/queues")

        assert_yt_error(self._get_fresh_poll_error(), "Row range schema is incompatible with queue table row schema")

        create("table", "//sys/queue_agents/queues", attributes={"dynamic": True, "schema": self.QUEUE_TABLE_SCHEMA},
               force=True)
        sync_mount_table("//sys/queue_agents/queues")

        self._validate_no_poll_error()

    @authors("max42")
    def test_controllers(self):
        sync_create_cells(1)

        create("table", "//sys/queue_agents/queues", attributes={"dynamic": True, "schema": self.QUEUE_TABLE_SCHEMA},
               force=True)
        sync_mount_table("//sys/queue_agents/queues")

        self._wait_fresh_poll()
        queues = self._get_queues()
        assert len(queues) == 0

        def get_queue(queue_id):
            self._wait_fresh_poll()
            queues = self._get_queues()
            assert queue_id in queues
            return queues[queue_id]

        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q"}])
        queue = get_queue("primary://tmp/q")
        assert_yt_error(YtError.from_dict(queue["error"]), "Queue is not in-sync yet")
        assert "type" not in queue

        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "revision": YsonUint64(1234)}],
                    update=True)
        queue = get_queue("primary://tmp/q")
        assert_yt_error(YtError.from_dict(queue["error"]), "Queue object type is not known yet")
        assert "type" not in queue

        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "revision": YsonUint64(2345),
                      "object_type": "map_node"}],
                    update=True)
        queue = get_queue("primary://tmp/q")
        assert_yt_error(YtError.from_dict(queue["error"]), 'Invalid queue object type "map_node"')
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "revision": YsonUint64(3456),
                      "object_type": "table", "dynamic": True, "sorted": True}],
                    update=True)
        queue = get_queue("primary://tmp/q")
        assert_yt_error(YtError.from_dict(queue["error"]), "Only ordered dynamic tables are supported as queues")

        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "revision": YsonUint64(4567), "object_type": "table",
                      "dynamic": True, "sorted": False}],
                    update=True)
        queue = get_queue("primary://tmp/q")
        assert "error" not in queue
        assert queue["type"] == "ordered_dynamic_table"

        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "revision": YsonUint64(5678), "object_type": "table",
                      "dynamic": False, "sorted": False}],
                    update=True)
        queue = get_queue("primary://tmp/q")
        assert_yt_error(YtError.from_dict(queue["error"]), "Only ordered dynamic tables are supported as queues")
        assert "type" not in queue
