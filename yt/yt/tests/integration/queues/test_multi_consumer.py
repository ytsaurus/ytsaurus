from yt_queue_agent_test_base import TestQueueAgentBase, QueueAgentOrchid, GenericObjectPath

from yt_commands import (
    authors,
    insert_rows,
    select_rows,
    delete_rows,
    set,
    remove,
    sync_unmount_table,
    raises_yt_error,
    assert_yt_error,
    wait,
    get,
    ls,
    register_queue_consumer,
)

from yt.common import YtError

import pytest

##################################################################


class TestMultiConsumerController(TestQueueAgentBase):
    NUM_QUEUE_AGENTS_PRIMARY = 3

    ENABLE_MULTIDAEMON = True

    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    def _get_orchid(self) -> QueueAgentOrchid:
        instances = ls("//sys/queue_agents/instances")
        return QueueAgentOrchid(agent_id=instances[0])

    @authors("panesher")
    def test_consistent_orchid_and_table(self):
        path = self.create_consumer_path("multi_consumer")
        self._create_consumer(path, multi_consumer=True, queue_agent_stage="production")

        self._wait_for_component_passes()

        multi_consumer_ref = GenericObjectPath(path, "primary")
        orchid = self._get_orchid().get_multi_consumer_orchid(multi_consumer_ref)
        orchid.wait_fresh_pass()

        assert orchid.get_queue_consumer_names() == []
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []

        # Check controller will insert to multi_consumer_names table
        names = ["my_1", "my_2", "my_3"]
        name_to_queue_path = {}
        for name in names:
            queue_path = self.create_queue_path(name)
            self._create_queue(queue_path)
            name_to_queue_path[name] = queue_path
            register_queue_consumer(queue_path, GenericObjectPath(path, "primary", name), vital=False)

        insert_rows(path, [
            {
                "queue_consumer_name": name,
                "queue_cluster": "primary",
                "queue_path": queue_path,
                "partition_index": 0,
                "offset": 0,
            }
            for name, queue_path in name_to_queue_path.items()
        ])
        orchid.wait_fresh_pass()

        assert sorted(orchid.get_queue_consumer_names()) == sorted(names)
        assert sorted(
            select_rows("* from [//sys/queue_agents/multi_consumer_names]"),
            key=lambda r: r["name"],
        ) == sorted(
            [{"cluster": "primary", "path": path, "name": name, "queue_agent_stage": "production"} for name in names],
            key=lambda r: r["name"],
        )

        # Check controller deletes from multi_consumer_names table (stale row for unknown name)
        insert_rows(
            "//sys/queue_agents/multi_consumer_names",
            [{"cluster": "primary", "path": path, "name": "stale_name"}],
        )
        orchid.wait_fresh_pass()

        assert sorted(orchid.get_queue_consumer_names()) == sorted(names)
        assert sorted(
            select_rows("* from [//sys/queue_agents/multi_consumer_names]"),
            key=lambda r: r["name"],
        ) == sorted(
            [{"cluster": "primary", "path": path, "name": name, "queue_agent_stage": "production"} for name in names],
            key=lambda r: r["name"],
        )

        assert orchid.get_status() == get(f"{path}/@queue_consumer_status")
        named_consumer_status = orchid.get_named_consumer_status("my_1")
        assert named_consumer_status == get(f"{path}/@queue_consumer_status/consumers/my_1")
        assert named_consumer_status["registrations"] == [
            {
                "vital": False,
                "consumer": GenericObjectPath(path, "primary", "my_1").to_yson_type(),
                "queue": str(GenericObjectPath(name_to_queue_path["my_1"], "primary")),
            }
        ]

        status = orchid.get_status()
        instances = ls("//sys/queue_agents/instances")
        for instance in instances:
            instance_orchid = QueueAgentOrchid(agent_id=instance)
            multi_consumer_orchid = instance_orchid.get_multi_consumer_orchid(multi_consumer_ref)
            assert status == multi_consumer_orchid.get_status()

        # Delete all rows and check table is cleaned up
        for name, queue_path in name_to_queue_path.items():
            delete_rows(path, [{
                "queue_consumer_name": name,
                "queue_cluster": "primary",
                "queue_path": queue_path,
                "partition_index": 0,
            }])

        orchid.wait_fresh_pass()

        assert orchid.get_queue_consumer_names() == []
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []

    @authors("panesher")
    def test_delete_consumer_names_without_table(self):
        path = self.create_consumer_path("multi_consumer")
        self._create_consumer(path, multi_consumer=True, queue_agent_stage="production")

        self._wait_for_component_passes()

        orchid = self._get_orchid().get_multi_consumer_orchid(GenericObjectPath(path, "primary"))
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []

        # Check controller will insert to multi_consumer_names table
        names = ["my_1", "my_2", "my_3"]
        insert_rows(path, [
            {
                "queue_consumer_name": name,
                "queue_cluster": "primary",
                "queue_path": "//tmp/any_queue",
                "partition_index": 0,
                "offset": 0,
            }
            for name in names
        ])
        orchid.wait_fresh_pass()

        assert sorted(
            select_rows("* from [//sys/queue_agents/multi_consumer_names]"),
            key=lambda r: r["name"],
        ) == sorted(
            [{"cluster": "primary", "path": path, "name": name, "queue_agent_stage": "production"} for name in names],
            key=lambda r: r["name"],
        )

        remove(path)
        self._wait_for_component_passes()
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []

    @authors("panesher")
    def test_invalid_multi_consumer(self):
        path = self.create_consumer_path("invalid_multi_consumer")
        self._create_consumer(path, multi_consumer=True, queue_agent_stage="production")

        self._wait_for_component_passes()

        sync_unmount_table(path)

        orchid = self._get_orchid().get_multi_consumer_orchid(GenericObjectPath(path, "primary"))

        orchid.wait_fresh_pass()

        assert_yt_error(YtError.from_dict(orchid.get_status()["error"]), 'is in "unmounted" state')
        wait(lambda: orchid.get_alerts().check_matching(
            "queue_agent_multi_consumer_controller_pass_failed",
            text='is in "unmounted" state',
        ), timeout=5, ignore_exceptions=True)

        # No rows should be written to multi_consumer_names table for an invalid consumer.
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []

    @authors("panesher")
    def test_queue_agent_stage_change(self):
        path = self.create_consumer_path("consumer_stage")
        self._create_consumer(path, multi_consumer=True, queue_agent_stage="testing")

        self._wait_for_component_passes()

        orchid = self._get_orchid().get_multi_consumer_orchid(GenericObjectPath(path, "primary"))

        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []
        insert_rows(path, [{
            "queue_consumer_name": "my_1",
            "queue_cluster": "primary",
            "queue_path": "//tmp/any_queue",
            "partition_index": 0,
            "offset": 0,
        }])
        with raises_yt_error("is not mapped to any queue agent"):
            orchid.wait_fresh_pass()

        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == []

        set(f"{path}/@queue_agent_stage", "production")
        self._wait_for_component_passes()
        orchid.wait_fresh_pass()
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == [
            {"cluster": "primary", "path": path, "name": "my_1", "queue_agent_stage": "production"}
        ]

    @authors("panesher")
    def test_banned_multi_consumer(self):
        path = self.create_consumer_path("consumer_banned")
        self._create_consumer(path, multi_consumer=True, queue_agent_stage="production")

        self._wait_for_component_passes()

        orchid = self._get_orchid().get_multi_consumer_orchid(GenericObjectPath(path, "primary"))

        insert_rows(path, [{
            "queue_consumer_name": "my_1",
            "queue_cluster": "primary",
            "queue_path": "//tmp/any_queue",
            "partition_index": 0,
            "offset": 0,
        }])
        orchid.wait_fresh_pass()

        assert orchid.get_queue_consumer_names() == ["my_1"]
        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == [
            {"cluster": "primary", "path": path, "name": "my_1", "queue_agent_stage": "production"}
        ]

        # Ban the consumer — controller should stop syncing and report error.
        set(f"{path}/@queue_agent_banned", True)
        self._wait_for_component_passes()
        orchid.wait_fresh_pass()

        status = orchid.get_status()
        assert_yt_error(YtError.from_dict(orchid.get_status()["error"]), "banned")

        # Insert a new name while banned — it should NOT appear in multi_consumer_names.
        insert_rows(path, [{
            "queue_consumer_name": "my_2",
            "queue_cluster": "primary",
            "queue_path": "//tmp/any_queue",
            "partition_index": 0,
            "offset": 0,
        }])
        orchid.wait_fresh_pass()

        assert select_rows("* from [//sys/queue_agents/multi_consumer_names]") == [
            {"cluster": "primary", "path": path, "name": "my_1", "queue_agent_stage": "production"}
        ]

        # Unban — controller resumes, my_2 should now appear.
        remove(f"{path}/@queue_agent_banned")
        self._wait_for_component_passes()
        orchid.wait_fresh_pass()

        status = orchid.get_status()
        assert "error" not in status

        assert sorted(orchid.get_queue_consumer_names()) == ["my_1", "my_2"]
        assert sorted(
            select_rows("* from [//sys/queue_agents/multi_consumer_names]"),
            key=lambda r: r["name"],
        ) == [
            {"cluster": "primary", "path": path, "name": "my_1", "queue_agent_stage": "production"},
            {"cluster": "primary", "path": path, "name": "my_2", "queue_agent_stage": "production"},
        ]


class TestNamedConsumerController(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @pytest.mark.parametrize("partition_to_insert", [0, 1, 2])
    @authors("panesher")
    def test_consumer_controller(self, partition_to_insert):
        queue_path = "//tmp/queue"
        queue_ref = GenericObjectPath(queue_path, "primary")
        self._create_queue(queue_path, partition_count=3)
        insert_rows(queue_path, [{"$tablet_index": partition_to_insert, "data": "hello world"}] * 3)

        consumer_path = "//tmp/consumer"
        consumer_ref = self._create_registered_consumer(
            consumer_path, queue_path, consumer_name="my_1")

        partition_index_to_offset = {0: 0, 1: 0, 2: 0}
        self._advance_consumers(consumer_ref, queue_ref, partition_index_to_offset)

        self._wait_for_component_passes()
        orchid = QueueAgentOrchid()
        multi_consumer_orchid = orchid.get_multi_consumer_orchid(GenericObjectPath(consumer_path, "primary"))
        multi_consumer_orchid.wait_fresh_pass()

        self._wait_for_component_passes(skip_cypress_synchronizer=True)
        consumer_orch_id = orchid.get_consumer_orchid(consumer_ref)
        consumer_orch_id.wait_fresh_pass()

        row = consumer_orch_id.get_row()
        assert GenericObjectPath(row["consumer"]) == consumer_ref

        consumer_orch_id.wait_fresh_pass()
        status, partitions = consumer_orch_id.get_subconsumer(str(queue_ref))
        assert status["partition_count"] == 3
        assert partitions[partition_to_insert]["next_row_index"] == 0
        assert partitions[partition_to_insert]["unread_row_count"] == 3

        partition_index_to_offset[partition_to_insert] = 1
        self._advance_consumers(consumer_ref, queue_ref, partition_index_to_offset)

        consumer_orch_id.wait_fresh_pass()
        status, partitions = consumer_orch_id.get_subconsumer(str(queue_ref))
        assert status["partition_count"] == 3
        assert partitions[partition_to_insert]["next_row_index"] == 1
        assert partitions[partition_to_insert]["unread_row_count"] == 2
