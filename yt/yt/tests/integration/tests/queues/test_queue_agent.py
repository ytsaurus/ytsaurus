from yt_env_setup import (YTEnvSetup, Restarter, QUEUE_AGENTS_SERVICE)

from yt_commands import (authors, get, ls, wait, assert_yt_error, create, sync_mount_table, sync_create_cells,
                         insert_rows, delete_rows, remove)

from yt.common import YtError

import copy

from yt.yson import YsonUint64

import yt_error_codes

##################################################################

QUEUE_TABLE_SCHEMA = [
    {"name": "cluster", "type": "string", "sort_order": "ascending"},
    {"name": "path", "type": "string", "sort_order": "ascending"},
    {"name": "row_revision", "type": "uint64"},
    {"name": "revision", "type": "uint64"},
    {"name": "object_type", "type": "string"},
    {"name": "dynamic", "type": "boolean"},
    {"name": "sorted", "type": "boolean"},
]

CONSUMER_TABLE_SCHEMA = [
    {"name": "cluster", "type": "string", "sort_order": "ascending"},
    {"name": "path", "type": "string", "sort_order": "ascending"},
    {"name": "row_revision", "type": "uint64"},
    {"name": "revision", "type": "uint64"},
    {"name": "target_cluster", "type": "string"},
    {"name": "target_path", "type": "string"},
    {"name": "object_type", "type": "string"},
    {"name": "treat_as_consumer", "type": "boolean"},
]


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
        "queue_agent": {
            "poll_period": 100,
        }
    }

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
        self._wait_fresh_poll()
        return get(self._queue_agent_orchid_path() + "/queue_agent/queues")

    def _get_consumers(self):
        self._wait_fresh_poll()
        return get(self._queue_agent_orchid_path() + "/queue_agent/consumers")

    def _get_queue(self, queue_id):
        queues = self._get_queues()
        assert queue_id in queues
        return queues[queue_id]

    def _get_consumer(self, consumer_id):
        consumers = self._get_consumers()
        assert consumer_id in consumers
        return consumers[consumer_id]

    def _prepare_tables(self, queue_table_schema=QUEUE_TABLE_SCHEMA, consumer_table_schema=CONSUMER_TABLE_SCHEMA):
        sync_create_cells(1)
        create("table",
               "//sys/queue_agents/queues",
               attributes={"dynamic": True, "schema": queue_table_schema},
               force=True)
        sync_mount_table("//sys/queue_agents/queues")
        create("table",
               "//sys/queue_agents/consumers",
               attributes={"dynamic": True, "schema": consumer_table_schema},
               force=True)
        sync_mount_table("//sys/queue_agents/consumers")

    def _drop_tables(self):
        remove("//sys/queue_agents/queues", force=True)
        remove("//sys/queue_agents/consumers", force=True)

    @authors("max42")
    def test_polling_loop(self):
        self._drop_tables()

        assert_yt_error(self._get_fresh_poll_error(), "has no child with key")

        wrong_schema = copy.deepcopy(QUEUE_TABLE_SCHEMA)
        wrong_schema[-1]["type"] = "int64"
        self._prepare_tables(queue_table_schema=wrong_schema)

        assert_yt_error(self._get_fresh_poll_error(), "Row range schema is incompatible with queue table row schema")

        self._prepare_tables()

        self._validate_no_poll_error()

    @authors("max42")
    def test_queue_state(self):
        self._prepare_tables()

        self._wait_fresh_poll()
        queues = self._get_queues()
        assert len(queues) == 0

        # Missing row revision.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q"}])
        queue = self._get_queue("primary://tmp/q")
        assert_yt_error(YtError.from_dict(queue["error"]), "Queue is not in-sync yet")
        assert "type" not in queue

        # Missing object type.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(1234)}],
                    update=True)
        queue = self._get_queue("primary://tmp/q")
        assert_yt_error(YtError.from_dict(queue["error"]), "Queue is not in-sync yet")
        assert "type" not in queue

        # Wrong object type.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(2345),
                      "object_type": "map_node"}],
                    update=True)
        queue = self._get_queue("primary://tmp/q")
        assert_yt_error(YtError.from_dict(queue["error"]), 'Invalid queue object type "map_node"')

        # Sorted dynamic table.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(3456),
                      "object_type": "table", "dynamic": True, "sorted": True}],
                    update=True)
        queue = self._get_queue("primary://tmp/q")
        assert_yt_error(YtError.from_dict(queue["error"]), "Only ordered dynamic tables are supported as queues")

        # Proper ordered dynamic table.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(4567), "object_type": "table",
                      "dynamic": True, "sorted": False}],
                    update=True)
        queue = self._get_queue("primary://tmp/q")
        # This error means that controller is intantiated and works properly.
        assert_yt_error(YtError.from_dict(queue["error"]), code=yt_error_codes.ResolveErrorCode)
        assert queue["type"] == "ordered_dynamic_table"

        # Switch back to sorted dynamic table.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(5678), "object_type": "table",
                      "dynamic": False, "sorted": False}],
                    update=True)
        queue = self._get_queue("primary://tmp/q")
        assert_yt_error(YtError.from_dict(queue["error"]), "Only ordered dynamic tables are supported as queues")
        assert "type" not in queue

        # Remove row; queue should be unregistered.
        delete_rows("//sys/queue_agents/queues", [{"cluster": "primary", "path": "//tmp/q"}])

        queues = self._get_queues()
        assert len(queues) == 0

    @authors("max42")
    def test_consumer_state(self):
        self._prepare_tables()

        self._wait_fresh_poll()
        queues = self._get_queues()
        consumers = self._get_consumers()
        assert len(queues) == 0
        assert len(consumers) == 0

        # Missing row revision.
        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": "//tmp/c"}])
        consumer = self._get_consumer("primary://tmp/c")
        assert_yt_error(YtError.from_dict(consumer["error"]), "Consumer is not in-sync yet")
        assert "target" not in consumer

        # Missing target.
        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": "//tmp/c", "row_revision": YsonUint64(1234)}],
                    update=True)
        consumer = self._get_consumer("primary://tmp/c")
        assert_yt_error(YtError.from_dict(consumer["error"]), "Consumer is missing target")
        assert "target" not in consumer

        # Unregistered target queue.
        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": "//tmp/c", "row_revision": YsonUint64(2345),
                      "target_cluster": "primary", "target_path": "//tmp/q"}],
                    update=True)
        consumer = self._get_consumer("primary://tmp/c")
        assert_yt_error(YtError.from_dict(consumer["error"]), 'Target queue "primary://tmp/q" is not registered')

        # Register target queue.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(4567), "object_type": "table",
                      "dynamic": True, "sorted": False}],
                    update=True)
        queue = self._get_queue("primary://tmp/q")
        assert_yt_error(YtError.from_dict(queue["error"]), code=yt_error_codes.ResolveErrorCode)
        assert queue["type"] == "ordered_dynamic_table"

        consumer = self._get_consumer("primary://tmp/c")
        # TODO(max42): uncomment this in future.
        # assert_yt_error(YtError.from_dict(consumer["error"]), code=yt_error_codes.ResolveErrorCode)


class TestMultipleAgents(YTEnvSetup):
    NUM_QUEUE_AGENTS = 5

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
        "queue_agent": {
            "poll_period": 100,
        },
        "election_manager": {
            "transaction_timeout": 1000,
            "transaction_ping_period": 100,
            "lock_acquisition_period": 100,
        },
    }

    @authors("max42")
    def test_leader_election(self):
        instances = ls("//sys/queue_agents/instances")
        assert len(instances) == 5

        def collect_leaders(ignore_instances=[]):
            result = []
            for instance in instances:
                if instance in ignore_instances:
                    continue
                active = get("//sys/queue_agents/instances/" + instance + "/orchid/queue_agent/active")
                if active:
                    result.append(instance)
            return result

        wait(lambda: len(collect_leaders()) == 1)

        leader = collect_leaders()[0]

        # Check that exactly one queue agent instance is performing polling.

        def validate_leader(leader, ignore_instances=[]):
            wait(lambda: get("//sys/queue_agents/instances/" + leader + "/orchid/queue_agent/poll_index") > 10)

            for instance in instances:
                if instance != leader and instance not in ignore_instances:
                    assert get("//sys/queue_agents/instances/" + instance + "/orchid/queue_agent/poll_index") == 0

        validate_leader(leader)

        # Check that leader host is set in lock transaction attributes.

        locks = get("//sys/queue_agents/leader_lock/@locks")
        assert len(locks) == 1
        tx_id = locks[0]["transaction_id"]
        leader_from_tx_attrs = get("#" + tx_id + "/@host")

        assert leader == leader_from_tx_attrs

        # Test re-election.

        leader_index = get("//sys/queue_agents/instances/" + leader + "/@annotations/yt_env_index")

        with Restarter(self.Env, QUEUE_AGENTS_SERVICE, indexes=[leader_index]):
            prev_leader = leader

            def reelection():
                leaders = collect_leaders(ignore_instances=[prev_leader])
                return len(leaders) == 1 and leaders[0] != prev_leader
            wait(reelection)

            leader = collect_leaders(ignore_instances=[prev_leader])[0]

            validate_leader(leader, ignore_instances=[prev_leader])
